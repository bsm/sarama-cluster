package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/tomb.v2"
)

type Consumer struct {
	id, group, topic string

	client *sarama.Client
	config *sarama.ConsumerConfig
	zoo    *ZK
	events chan *sarama.ConsumerEvent

	acked   map[int32]int64
	aLock   sync.Mutex
	partIDs []int32
	pLock   sync.Mutex

	notifier Notifier
	closer   tomb.Tomb

	// Set to true if you want to automatically Ack
	// every event after it is consumed
	AutoAck bool
}

// NewConsumer creates a new consumer for a given topic.
// You MUST call Close() to avoid leaks.
func NewConsumer(client *sarama.Client, zookeepers []string, group, topic string, notifier Notifier, config *sarama.ConsumerConfig) (*Consumer, error) {
	return NewConsumerWithID(client, zookeepers, group, topic, newGUID(group), notifier, config)
}

// NewConsumer creates a new consumer for a given topic.
// You MUST call Close() to avoid leaks.
func NewConsumerWithID(client *sarama.Client, zookeepers []string, group, topic, id string, notifier Notifier, config *sarama.ConsumerConfig) (*Consumer, error) {
	if config == nil {
		config = sarama.NewConsumerConfig()
	}
	if notifier == nil {
		notifier = &LogNotifier{Logger}
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	} else if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	} else if group == "" {
		return nil, sarama.ConfigurationError("Empty group")
	} else if id == "" {
		return nil, sarama.ConfigurationError("Empty ID")
	}

	// Connect to zookeeper
	zoo, err := NewZK(zookeepers, time.Second)
	if err != nil {
		return nil, err
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

		notifier: notifier,
		events:   make(chan *sarama.ConsumerEvent),
	}

	// Register consumer group and consumer itself
	if err := consumer.register(); err != nil {
		consumer.Close()
		return nil, err
	}

	consumer.closer.Go(consumer.signalLoop)
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
// for the next Commit() call
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
	c.aLock.Lock()
	snap := make(map[int32]int64, len(c.acked))
	for num, offset := range c.acked {
		snap[num] = offset
	}
	c.acked = make(map[int32]int64)
	c.aLock.Unlock()
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

// Close closes the consumer instance
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
			c.notifier.RebalanceError(c, err)
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

// Event consumer loop for a single partition consumer
func (c *Consumer) consumeLoop(done chan struct{}, wait *sync.WaitGroup, pcsm *sarama.Consumer) {
	defer wait.Done()

	for {
		select {
		case event := <-pcsm.Events():
			select {
			case c.events <- event:
				if c.AutoAck {
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
	close(c.events)
	err := c.reset(claims)
	c.zoo.Close()
	return err
}

// Rebalance cycle, triggered by the main loop
func (c *Consumer) rebalance(claims Claims) (Claims, <-chan zk.Event, error) {
	c.notifier.RebalanceStart(c)

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

	c.notifier.RebalanceOK(c)
	return claims, watch, nil
}

// Commits offset and releases all claims
func (c *Consumer) reset(claims Claims) (err error) {
	// Commit BEFORE releasing locks on partitions
	err = c.Commit()

	// Release claimed partitions, one by one, ignore errors
	for partitionID, _ := range claims {
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
	config := *c.config
	config.OffsetMethod = sarama.OffsetMethodOldest
	if offset > 0 {
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = offset
	}
	return &config
}
