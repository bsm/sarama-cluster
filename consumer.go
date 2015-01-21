package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/tomb.v2"
)

// Consumer configuration options
type ConsumerConfig struct {
	// Standard consumer configuration
	*sarama.ConsumerConfig

	// Set to true if you want to automatically ack
	// every event once it has been consumed through
	// the Consumer.Events() channel
	AutoAck bool

	// Enable automatic commits but setting this options.
	// Automatic commits will remain disabled if is set to < 10ms
	CommitEvery time.Duration

	// Notifier instance to handle info/error
	// notifications from the consumer
	// Default: *LogNotifier
	Notifier Notifier

	// Session timeout for the underlying zookeeper client
	// Default: time.Second*1
	ZKSessionTimeout time.Duration

	// Chroot path of the kafka cluster inside zookeeper
	// Default: ""
	ZKChrootPath string

	customID string
}

func (c *ConsumerConfig) normalize() {
	if c.ConsumerConfig == nil {
		c.ConsumerConfig = sarama.NewConsumerConfig()
	}
	if c.Notifier == nil {
		c.Notifier = &LogNotifier{Logger}
	}
	if c.CommitEvery < 10*time.Millisecond {
		c.CommitEvery = 0
	}
	if c.ZKSessionTimeout == 0 {
		c.ZKSessionTimeout = time.Second
	}
}

type Consumer struct {
	id, group, topic string

	client *sarama.Client
	config *ConsumerConfig
	zoo    *ZK
	events chan *sarama.ConsumerEvent

	acked   map[int32]int64
	aLock   sync.Mutex
	partIDs []int32
	pLock   sync.Mutex

	notifier Notifier
	closer   tomb.Tomb
}

// NewConsumer creates a new consumer for a given topic.
// You MUST call Close() to avoid leaks.
func NewConsumer(client *sarama.Client, zookeepers []string, group, topic string, config *ConsumerConfig) (*Consumer, error) {
	if config == nil {
		config = new(ConsumerConfig)
	}
	config.normalize()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	} else if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	} else if group == "" {
		return nil, sarama.ConfigurationError("Empty group")
	}

	// Connect to zookeeper
	zoo, err := NewZK(zookeepers, config.ZKChrootPath, config.ZKSessionTimeout)

	if err != nil {
		return nil, err
	}

	id := config.customID
	if id == "" {
		id = newGUID(group)
	}

	// Initialize consumer
	consumer := &Consumer{
		id:    id,
		group: group,
		topic: topic,

		zoo:    zoo,
		config: config,
		client: client,

		acked:   make(map[int32]int64),
		partIDs: make([]int32, 0),

		events: make(chan *sarama.ConsumerEvent),
	}

	// Register consumer group and consumer itself
	if err := consumer.register(); err != nil {
		consumer.Close()
		return nil, err
	}

	consumer.closer.Go(consumer.signalLoop)
	if config.CommitEvery != 0 {
		consumer.closer.Go(consumer.commitLoop)
	}
	return consumer, nil
}

// Events exposes the event channel
func (c *Consumer) Events() <-chan *sarama.ConsumerEvent { return c.events }

// Claims exposes the partIDs partition ID
func (c *Consumer) Claims() []int32 {
	c.pLock.Lock()
	defer c.pLock.Unlock()
	return c.partIDs
}

// ID exposes the consumer ID
func (c *Consumer) ID() string { return c.id }

// Group exposes the group name
func (c *Consumer) Group() string { return c.group }

// Topic exposes the group topic
func (c *Consumer) Topic() string { return c.topic }

// Offset manually retrives the stored offset for a partition ID
func (c *Consumer) Offset(partitionID int32) (int64, error) {
	return c.zoo.Offset(c.group, c.topic, partitionID)
}

// Ack marks a consumer event as processed and stores the offset
// for the next Commit() call.
func (c *Consumer) Ack(event *sarama.ConsumerEvent) {
	if event.Err != nil {
		return
	}

	c.aLock.Lock()
	defer c.aLock.Unlock()

	if event.Offset > c.acked[event.Partition] {
		c.acked[event.Partition] = event.Offset
	}
}

// Commit persists ack'd offsets
func (c *Consumer) Commit() error {
	snap := c.resetAcked()
	if len(snap) < 1 {
		return nil
	}

	for partitionID, offset := range snap {
		if err := c.zoo.Commit(c.group, c.topic, partitionID, offset+1); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the consumer instance.
// Also triggers a final Commit() call.
func (c *Consumer) Close() error {
	c.closer.Kill(nil)
	return c.closer.Wait()
}

// LOOPS

// Main signal loop
func (c *Consumer) signalLoop() error {
	claims := make(Claims)
	for {

		// Check if shutdown was requested
		select {
		case <-c.closer.Dying():
			return c.shutdown(claims)
		default:
		}

		// Start a rebalance cycle
		acquired, watch, err := c.rebalance(claims)
		if err != nil {
			c.config.Notifier.RebalanceError(c, err)
			continue
		}

		// Remember current claims
		claims = acquired

		// Start a goroutine for each partition
		done := make(chan struct{})
		wait := new(sync.WaitGroup)
		for _, pcsm := range claims {
			wait.Add(1)
			go c.consumeLoop(done, wait, pcsm)
		}

		// Wait for signals
		select {
		case <-c.closer.Dying():
			close(done)
			wait.Wait()
			return c.shutdown(claims)
		case <-watch:
			close(done)
			wait.Wait()
		}
	}
	return nil
}

// Commit loop, triggers periodic commits configured in CommitEvery
func (c *Consumer) commitLoop() error {
	for {
		select {
		case <-c.closer.Dying():
			return nil
		case <-time.After(c.config.CommitEvery):
		}
		if err := c.Commit(); err != nil {
			c.config.Notifier.CommitError(c, err)
		}
	}
	return nil
}

// Event consumer loop for a single partition consumer
func (c *Consumer) consumeLoop(done chan struct{}, wait *sync.WaitGroup, pcsm *sarama.Consumer) {
	defer wait.Done()

	for {
		select {
		case event := <-pcsm.Events():
			select {
			case c.events <- event:
				if c.config.AutoAck {
					c.Ack(event)
				}
			case <-done:
				return
			}
		case <-done:
			return
		}
	}
}

// PRIVATE

// Shutdown the consumer, triggered by the main loop
func (c *Consumer) shutdown(claims Claims) error {
	err := c.reset(claims)
	close(c.events)
	c.zoo.Close()
	return err
}

// Rebalance cycle, triggered by the main loop
func (c *Consumer) rebalance(claims Claims) (Claims, <-chan zk.Event, error) {
	c.config.Notifier.RebalanceStart(c)

	// Fetch consumer list
	consumerIDs, watch, err := c.zoo.Consumers(c.group)
	if err != nil {
		return nil, nil, err
	}

	// Fetch partitions list
	partitions, err := c.partitions()
	if err != nil {
		return nil, nil, err
	}

	// Determine partitions and claim if changed
	if partitions = partitions.Select(c.id, consumerIDs); !partitions.ConsistOf(claims) {

		// Commit and release existing claims
		if err := c.reset(claims); err != nil {
			return nil, nil, err
		}

		// Make new claims
		claims = make(Claims, len(partitions))
		for _, part := range partitions {
			pcsm, err := c.claim(part.ID)
			if err != nil {
				c.reset(claims)
				return nil, nil, err
			}
			claims[part.ID] = pcsm
		}

		c.pLock.Lock()
		c.partIDs = claims.PartitionIDs()
		c.pLock.Unlock()
	}

	c.config.Notifier.RebalanceOK(c)
	return claims, watch, nil
}

// Commits offset and releases all claims
func (c *Consumer) reset(claims Claims) (err error) {
	// Commit BEFORE releasing locks on partitions
	err = c.Commit()

	// Release claimed partitions, one by one, ignore errors
	for partitionID, pcsm := range claims {
		pcsm.Close()
		c.zoo.Release(c.group, c.topic, partitionID, c.id)
	}
	return
}

// Claims a partition
func (c *Consumer) claim(partitionID int32) (*sarama.Consumer, error) {
	err := c.zoo.Claim(c.group, c.topic, partitionID, c.id)
	if err != nil {
		return nil, err
	}

	offset, err := c.Offset(partitionID)
	if err != nil {
		return nil, err
	}

	return sarama.NewConsumer(c.client, c.topic, partitionID, c.group, c.pcsmConfig(offset))
}

// Registers consumer with zookeeper
func (c *Consumer) register() error {
	if err := c.zoo.RegisterGroup(c.group); err != nil {
		return err
	}
	if err := c.zoo.RegisterConsumer(c.group, c.id, c.topic); err != nil {
		return err
	}
	return nil
}

// Fetch all partitions for a topic
func (c *Consumer) partitions() (PartitionSlice, error) {
	ids, err := c.client.Partitions(c.topic)
	if err != nil {
		return nil, err
	}

	slice := make(PartitionSlice, len(ids))
	for n, id := range ids {
		broker, err := c.client.Leader(c.topic, id)
		if err != nil {
			return nil, err
		}
		slice[n] = Partition{ID: id, Addr: broker.Addr()}
	}
	return slice, nil
}

// Creates a consumer config for the offset
func (c *Consumer) pcsmConfig(offset int64) *sarama.ConsumerConfig {
	config := *c.config.ConsumerConfig
	config.OffsetMethod = sarama.OffsetMethodOldest
	if offset > 0 {
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = offset
	}
	return &config
}

// Creates a snapshot of acked and reset the current value
func (c *Consumer) resetAcked() map[int32]int64 {
	c.aLock.Lock()
	defer c.aLock.Unlock()
	snap := make(map[int32]int64, len(c.acked))
	for num, offset := range c.acked {
		snap[num] = offset
	}
	c.acked = make(map[int32]int64)
	return snap
}
