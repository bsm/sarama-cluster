package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"gopkg.in/tomb.v2"
)

// Config contains the consumer configuration options
type Config struct {
	// Standard consumer configuration
	*sarama.Config

	// Consumer IDs have the form of PREFIX:HOSTNAME:UUID.
	// This option allows to set a custom prefix.
	// Defaults to the consumer group name.
	IDPrefix string

	// Set to true if you want to automatically ack
	// every message once it has been consumed through
	// the Consumer.Messages() channel
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

	customID string
}

func (c *Config) normalize() {
	if c.Config == nil {
		c.Config = sarama.NewConfig()
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

	client   *sarama.Client
	consumer *sarama.Consumer
	config   *Config
	zoo      *ZK
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError

	acked   map[int32]int64
	aLock   sync.Mutex
	partIDs []int32
	pLock   sync.Mutex

	notifier  Notifier
	closer    tomb.Tomb
	ownClient bool
}

// NewConsumer creates a new consumer instance.
// You MUST call Close() to avoid leaks.
func NewConsumer(addrs, zookeepers []string, group, topic string, config *Config) (*Consumer, error) {
	if config == nil {
		config = new(Config)
	}

	client, err := sarama.NewClient(addrs, config.Config)
	if err != nil {
		return nil, err
	}

	c, err := NewConsumerFromClient(client, zookeepers, group, topic, config)
	if err != nil {
		client.Close()
		return nil, err
	}
	c.ownClient = true
	return c, nil
}

// NewConsumerFromClient creates a new consumer for a given topic, reuing an existing client
// You MUST call Close() to avoid leaks.
func NewConsumerFromClient(client *sarama.Client, zookeepers []string, group, topic string, config *Config) (*Consumer, error) {
	if config == nil {
		config = new(Config)
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

	// Generate unique consumer ID
	id := config.customID
	if id == "" {
		prefix := config.IDPrefix
		if prefix == "" {
			prefix = group
		}
		id = newGUID(prefix)
	}

	// Create sarama consumer instance
	scsmr, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	// Connect to zookeeper
	zoo, err := NewZK(zookeepers, config.ZKSessionTimeout)
	if err != nil {
		scsmr.Close()
		return nil, err
	}

	// Initialize consumer
	consumer := &Consumer{
		id:    id,
		group: group,
		topic: topic,

		zoo:      zoo,
		config:   config,
		client:   client,
		consumer: scsmr,

		acked:   make(map[int32]int64),
		partIDs: make([]int32, 0),

		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
	}

	// Register consumer group and consumer itself
	if err := consumer.register(); err != nil {
		consumer.closeAll()
		return nil, err
	}

	consumer.closer.Go(consumer.signalLoop)
	if config.CommitEvery != 0 {
		consumer.closer.Go(consumer.commitLoop)
	}
	return consumer, nil
}

// Messages returns the read channel for the messages that are returned by the broker
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// Errors returns the read channel for any errors that occurred while consuming the partition.
// You have to read this channel to prevent the consumer from deadlock.
func (c *Consumer) Errors() <-chan *sarama.ConsumerError { return c.errors }

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

// Ack marks a consumer message as processed and stores the offset
// for the next Commit() call.
func (c *Consumer) Ack(msg *sarama.ConsumerMessage) {
	c.aLock.Lock()
	defer c.aLock.Unlock()

	if msg.Offset > c.acked[msg.Partition] {
		c.acked[msg.Partition] = msg.Offset
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
		watch, err := c.rebalance(claims)
		if err != nil {
			c.config.Notifier.RebalanceError(c, err)
			c.reset(claims)
			continue
		}

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
}

// Message consumer loop for a single partition consumer
func (c *Consumer) consumeLoop(done chan struct{}, wait *sync.WaitGroup, pcsm *sarama.PartitionConsumer) {
	defer wait.Done()

	for {
		select {
		case msg := <-pcsm.Messages():
			select {
			case c.messages <- msg:
				if c.config.AutoAck {
					c.Ack(msg)
				}
			case <-done:
				return
			}
		case msg := <-pcsm.Errors():
			select {
			case c.errors <- msg:
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
	c.closeAll()
	return err
}

// Close all connections and channels
func (c *Consumer) closeAll() {
	close(c.messages)
	close(c.errors)
	c.zoo.Close()
	c.consumer.Close()
	if c.ownClient {
		c.client.Close()
	}
}

// Rebalance cycle, triggered by the main loop
func (c *Consumer) rebalance(claims Claims) (<-chan zk.Event, error) {
	c.config.Notifier.RebalanceStart(c)

	// Commit and release existing claims
	if err := c.reset(claims); err != nil {
		return nil, err
	}

	// Fetch consumer list
	consumerIDs, watch, err := c.zoo.Consumers(c.group)
	if err != nil {
		return nil, err
	}

	// Fetch partitions list
	partitions, err := c.partitions()
	if err != nil {
		return nil, err
	}

	// Determine partitions and claim if changed
	partitions = partitions.Select(c.id, consumerIDs)

	// Make new claims
	for _, part := range partitions {
		pcsm, err := c.claim(part.ID)
		if err != nil {
			return nil, err
		}
		claims[part.ID] = pcsm
	}

	c.pLock.Lock()
	c.partIDs = claims.PartitionIDs()
	c.pLock.Unlock()
	c.config.Notifier.RebalanceOK(c)
	return watch, nil
}

// Commits offset and releases all claims
func (c *Consumer) reset(claims Claims) (err error) {
	// Commit BEFORE releasing locks on partitions
	err = c.Commit()

	// Close all existing consumers (async)
	wait := sync.WaitGroup{}
	for _, pcsm := range claims {
		wait.Add(1)
		go func(c *sarama.PartitionConsumer) {
			defer wait.Done()
			c.Close()
		}(pcsm)
	}
	wait.Wait()

	// Release claimed partitions, ignore errors
	for partitionID := range claims {
		c.zoo.Release(c.group, c.topic, partitionID, c.id)
		delete(claims, partitionID)
	}

	return
}

// Claims a partition
func (c *Consumer) claim(partitionID int32) (*sarama.PartitionConsumer, error) {
	err := c.zoo.Claim(c.group, c.topic, partitionID, c.id)
	if err != nil {
		return nil, err
	}

	offset, err := c.Offset(partitionID)
	if err != nil {
		return nil, err
	} else if offset < 1 {
		offset = sarama.OffsetOldest
	}

	pcsm, err := c.consumer.ConsumePartition(c.topic, partitionID, offset)
	if err == sarama.ErrOffsetOutOfRange {
		pcsm, err = c.consumer.ConsumePartition(c.topic, partitionID, sarama.OffsetOldest)
	}
	return pcsm, err
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

// Creates a snapshot of acked and reset the current value
func (c *Consumer) resetAcked() map[int32]int64 {
	c.aLock.Lock()
	defer c.aLock.Unlock()

	snap := make(map[int32]int64, len(c.acked))
	for num, offset := range c.acked {
		snap[num] = offset
		delete(c.acked, num)
	}
	return snap
}
