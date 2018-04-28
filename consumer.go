package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// Consumer allows to consume topics as member of
// a consumer group cluster.
type Consumer interface {
	// Topics lists consumer subscriptions.
	Topics() []string
	// SetTopics sets the consumable topic(s) for this consumer.
	SetTopics(topics ...string)
	// Errors returns a read channel of errors that occurred during consuming, if
	// enabled. By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error
	// Claims issues notifications about new claims. It should be consumed.
	Claims() <-chan *Claim
	// Close stops the consumer. It is required to call this method before a
	// consumer object passes out of scope, as it will otherwise leak memory.
	Close() error
}

type consumer struct {
	client    sarama.Client
	ownClient bool
	groupID   string
	config    *sarama.Config
	handler   Handler

	consumers sarama.Consumer
	group     sarama.ConsumerGroup

	topics   []string
	topicsMu sync.RWMutex

	claims    chan *Claim
	errors    chan error
	shutdown  chan none // closed on shutdown to signal that shutdown in imminent
	stopped   chan none // closed on mainLoop exit to signal that all processing has stopped
	rebalance chan none
	closeOnce sync.Once
}

// NewConsumer starts a new consumer.
func NewConsumer(addrs []string, groupID string, topics []string, config *sarama.Config, handler Handler) (Consumer, error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Version = sarama.V0_10_2_0
	}

	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, err
	}
	csmr, err := NewConsumerFromClient(client, groupID, topics, handler)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	csmr.(*consumer).ownClient = true
	return csmr, nil
}

// NewConsumerFromClient starts a new consumer from an existing client.
//
// Please note that clients cannot be shared between consumers (due to Kafka internals),
// they can only be re-used which requires the user to call Close() on the first consumer
// before using this method again to initialize another one. Attempts to use a client with
// more than one consumer at a time will return errors.
func NewConsumerFromClient(client sarama.Client, groupID string, topics []string, handler Handler) (Consumer, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(sarama.V0_10_2_0) {
		return nil, sarama.ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	consumers, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	if handler == nil {
		handler = noopHandler
	}

	c := &consumer{
		groupID:   groupID,
		topics:    topics,
		client:    client,
		config:    config,
		consumers: consumers,
		group:     sarama.NewConsumerGroupFromClient(groupID, client),
		handler:   handler,
		claims:    make(chan *Claim, config.ChannelBufferSize),
		errors:    make(chan error, config.ChannelBufferSize),
		shutdown:  make(chan none),
		stopped:   make(chan none),
		rebalance: make(chan none, 1),
	}
	go c.mainLoop()
	return c, nil
}

func (c *consumer) Errors() <-chan error {
	return c.errors
}

func (c *consumer) Claims() <-chan *Claim {
	return c.claims
}

func (c *consumer) Topics() []string {
	c.topicsMu.RLock()
	topics := c.topics
	c.topicsMu.RUnlock()

	return topics
}

func (c *consumer) Close() (err error) {
	c.closeOnce.Do(func() {
		// init shutdown, wait for sessions to exit
		close(c.shutdown)

		// close consumers first to stop process loops
		if e := c.consumers.Close(); e != nil {
			err = e
		}

		// wait until all loops have exited
		<-c.stopped

		// close the group, perform a final commit
		if e := c.group.Close(); e != nil {
			err = e
		}

		// close client if one was created by us
		if c.ownClient {
			if e := c.client.Close(); e != nil {
				err = e
			}
		}

		// consume errors
		go func() {
			close(c.errors)
			close(c.claims)
		}()
		for range c.claims {
		}
		for e := range c.errors {
			if e != nil {
				err = e
			}
		}
	})
	return
}

func (c *consumer) SetTopics(topics ...string) {
	c.topicsMu.Lock()
	c.topics = topics
	c.topicsMu.Unlock()

	// trigger a rebalance
	select {
	case c.rebalance <- none{}:
	default:
	}
}

func (c *consumer) mainLoop() {
	defer close(c.stopped)

	for {
		// drain rebalance channel
		select {
		case <-c.rebalance:
		default:
		}

		// exit on shutdown
		select {
		case <-c.shutdown:
			return
		default:
		}

		// obtain current topics
		topics := c.Topics()
		if len(topics) == 0 {
			c.backoff()
			continue
		}

		// start next session
		done, err := c.nextSession(topics)
		if err != nil {
			c.handleError(err)
			c.backoff()
		} else if done {
			return
		}
	}
}

func (c *consumer) nextSession(topics []string) (bool, error) {
	sess, err := c.group.Subscribe(topics)
	if err == sarama.ErrClosedConsumerGroup {
		return true, nil
	} else if err != nil {
		return false, err
	}
	defer sess.Close()

	// stop session on rebalance and shudown
	go func() {
		select {
		case <-sess.Done():
		case <-c.rebalance:
			sess.Stop()
		case <-c.shutdown:
			sess.Stop()
		}
	}()

	// try to issue claims
	claims := make(map[string][]int32, len(sess.Claims()))
	for topic, partitions := range sess.Claims() {
		claims[topic] = append(claims[topic], partitions...)
	}
	select {
	case c.claims <- &Claim{Topics: claims}:
	default:
	}

	// create an offset manager for the session
	som, err := newSessionOffsetManager(sess, c.groupID, c.client, c.handleError)
	if err != nil {
		return false, err
	}
	defer som.Stop()

	var wg sync.WaitGroup
	for topic, partitions := range claims {
		for _, partition := range partitions {
			wg.Add(1)

			go func(topic string, partition int32) {
				defer wg.Done()
				defer sess.Stop()

				pom := som.Get(topic, partition)
				if err := c.consume(sess, pom, topic, partition); err != nil {
					c.handleError(err)
				}
			}(topic, partition)
		}
	}
	wg.Wait()

	return false, nil
}

func (c *consumer) consume(sess sarama.ConsumerGroupSession, pom *partitionOffsetManager, topic string, partition int32) *sarama.ConsumerError {
	// quick exit if relance is due
	select {
	case <-sess.Done():
		return nil
	default:
	}

	// get next offset
	offset := pom.nextOffset(c.config.Consumer.Offsets.Initial)

	// init partition consumer, resume from default offset, if requested offset is out-of-range
	// don't defer pmc.Close(), pcm will be be closed by wrap below
	pcm, err := c.consumers.ConsumePartition(topic, partition, offset)
	if err == sarama.ErrOffsetOutOfRange {
		pom.reset()
		offset = c.config.Consumer.Offsets.Initial
		pcm, err = c.consumers.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return consumerError(err, topic, partition)
	}
	// handle consumer errors
	go func() {
		for err := range pcm.Errors() {
			c.handleError(err)
		}
	}()

	// init partition consumer wrapper
	wrap := &partitionConsumer{
		topic:     topic,
		partition: partition,

		partitionOffsetManager: pom,
		PartitionConsumer:      pcm,
		ConsumerGroupSession:   sess,
	}
	defer wrap.Close()

	// close when session is done
	go func() {
		<-sess.Done()
		wrap.Close()
	}()

	// start processing
	if err := c.handler.ProcessLoop(wrap); err != nil {
		return consumerError(err, topic, partition)
	}
	return consumerError(wrap.Close(), topic, partition)
}

func (c *consumer) handleError(err error) {
	if c.config.Consumer.Return.Errors {
		select {
		case c.errors <- err:
		case <-c.shutdown:
		}
	} else {
		sarama.Logger.Println(err)
	}
}

func (c *consumer) backoff() {
	backoff := time.NewTimer(c.config.Consumer.Retry.Backoff)
	defer backoff.Stop()

	select {
	case <-backoff.C:
	case <-c.shutdown:
	}
}

func consumerError(err error, topic string, partition int32) *sarama.ConsumerError {
	if err == nil {
		return nil
	}
	if cerr, ok := err.(*sarama.ConsumerError); ok {
		return cerr
	}
	return &sarama.ConsumerError{
		Err:       err,
		Topic:     topic,
		Partition: partition,
	}
}
