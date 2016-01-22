package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type RebalanceEventType uint8

const (
	PartitionAssigned RebalanceEventType = iota
	PartitionRevoked
)

type RebalanceEvent struct {
	Topics map[string][]int32
	Type   RebalanceEventType
}

// Consumer is a cluster group consumer
type Consumer struct {
	client sarama.Client
	broker *sarama.Broker
	brlock sync.Mutex
	config *Config

	csmr sarama.Consumer
	subs *partitionMap

	consumerID   string
	generationID int32
	groupID      string
	memberID     string
	topics       []string

	dying, dead chan none

	errors   chan error
	messages chan *sarama.ConsumerMessage
	rebal    chan *RebalanceEvent
}

// NewConsumer initializes a new consumer
func NewConsumer(addrs []string, groupID string, topics []string, config *Config) (*Consumer, error) {
	if config == nil {
		config = NewConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(addrs, &config.Config)
	if err != nil {
		return nil, err
	}

	csmr, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	c := &Consumer{
		config: config,
		client: client,

		csmr: csmr,
		subs: newPartitionMap(),

		groupID: groupID,
		topics:  topics,

		dying: make(chan none),
		dead:  make(chan none),

		errors:   make(chan error, config.ChannelBufferSize),
		messages: make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		rebal:    make(chan *RebalanceEvent, 1),
	}
	if err := c.selectBroker(); err != nil {
		_ = client.Close()
		return nil, err
	}

	go c.mainLoop()
	return c, nil
}

// Messages returns the read channel for the messages that are returned by
// the broker.
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

// Errors returns a read channel of errors that occur during offset management, if
// enabled. By default, errors are logged and not returned over this channel. If
// you want to implement any custom error handling, set your config's
// Consumer.Return.Errors setting to true, and read from this channel.
func (c *Consumer) Errors() <-chan error { return c.errors }

// RebalanceEvents returns a channel of RebalanceEvents that occur during consumer
// rebalancing, similar to the ConsumerRebalanceListener in the Java Client API
// The partitions revoked event will always proceed partition assigned event.
func (c *Consumer) RebalanceEvents() <-chan *RebalanceEvent { return c.rebal }

// MarkOffset marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (c *Consumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	c.subs.Fetch(msg.Topic, msg.Partition).MarkOffset(msg.Offset, metadata)
	// c.debug("*", "%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
}

// Subscriptions returns the consumed topics and partitions
func (c *Consumer) Subscriptions() map[string][]int32 {
	return c.subs.Info()
}

// Close safely closes the consumer and releases all resources
func (c *Consumer) Close() (err error) {
	close(c.dying)
	<-c.dead

	if e := c.release(); e != nil {
		err = e
	}
	if e := c.csmr.Close(); e != nil {
		err = e
	}
	close(c.messages)
	close(c.errors)

	if e := c.leaveGroup(); e != nil {
		err = e
	}

	close(c.rebal)

	if e := c.client.Close(); e != nil {
		err = e
	}

	return
}

func (c *Consumer) mainLoop() {
	for {
		// rebalance
		if err := c.rebalance(); err != nil {
			c.handleError(err)
			time.Sleep(c.config.Metadata.Retry.Backoff)
			continue
		}

		// enter consume state
		if c.consume() {
			close(c.dead)
			return
		}
	}
}

// enters consume state, triggered by the mainLoop
func (c *Consumer) consume() bool {
	hbTicker := time.NewTicker(c.config.Group.Heartbeat.Interval)
	defer hbTicker.Stop()

	ocTicker := time.NewTicker(c.config.Consumer.Offsets.CommitInterval)
	defer ocTicker.Stop()

	for {
		select {
		case <-hbTicker.C:
			switch err := c.heartbeat(); err {
			case nil, sarama.ErrNoError:
			case sarama.ErrNotCoordinatorForConsumer, sarama.ErrRebalanceInProgress:
				return false
			default:
				c.handleError(err)
				return false
			}
		case <-ocTicker.C:
			if err := c.commitOffsetsWithRetry(c.config.Group.Offsets.Retry.Max); err != nil {
				c.handleError(err)
				return false
			}
		case <-c.dying:
			return true
		}
	}
}

func (c *Consumer) handleError(err error) {
	if c.config.Consumer.Return.Errors {
		select {
		case c.errors <- err:
		case <-c.dying:
			return
		}
	} else {
		sarama.Logger.Println(err)
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
func (c *Consumer) release() error {
	// Stop all consumers
	if err := c.subs.Stop(); err != nil {
		return err
	}

	// Wait for all messages to be processed
	time.Sleep(c.config.Consumer.MaxProcessingTime)

	// Commit offsets
	err := c.commitOffsetsWithRetry(c.config.Group.Offsets.Retry.Max)

	// Clear subscriptions
	// c.debug("$", "")
	c.subs.Clear()

	return err
}

// --------------------------------------------------------------------

// Performs a heartbeat, part of the mainLoop()
func (c *Consumer) heartbeat() error {
	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	resp, err := broker.Heartbeat(&sarama.HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: c.generationID,
	})
	if err != nil {
		return err
	}
	return resp.Err
}

// Performs a rebalance, part of the mainLoop()
func (c *Consumer) rebalance() error {
	sarama.Logger.Printf("cluster/consumer %s rebalance\n", c.memberID)
	// c.debug("^", "")

	if err := c.selectBroker(); err != nil {
		return err
	}

	oldSubs := c.subs.Info()

	if err := c.release(); err != nil {
		return err
	}

	c.rebal <- &RebalanceEvent{Topics: oldSubs, Type: PartitionRevoked}

	strategy, err := c.joinGroup()
	switch {
	case err == sarama.ErrUnknownMemberId:
		c.memberID = ""
		return err
	case err != nil:
		return err
	}
	// sarama.Logger.Printf("cluster/consumer %s/%d joined group %s\n", c.memberID, c.generationID, c.groupID)

	subs, err := c.syncGroup(strategy)
	switch {
	case err == sarama.ErrRebalanceInProgress:
		return err
	case err != nil:
		_ = c.leaveGroup()
		return err
	}

	offsets, err := c.fetchOffsets(subs)
	if err != nil {
		_ = c.leaveGroup()
		return err
	}

	c.rebal <- &RebalanceEvent{Topics: subs, Type: PartitionAssigned}

	for topic, partitions := range subs {
		for _, partition := range partitions {
			if err := c.createConsumer(topic, partition, offsets[topic][partition]); err != nil {
				_ = c.release()
				_ = c.leaveGroup()
				return err
			}
		}
	}

	return nil
}

// --------------------------------------------------------------------

// Performs a broker selection, caches the broker
func (c *Consumer) selectBroker() error {
	if err := c.client.RefreshCoordinator(c.groupID); err != nil {
		return err
	}
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	c.brlock.Lock()
	c.broker = broker
	c.brlock.Unlock()

	return nil
}

// Send a request to the broker to join group on rebalance()
func (c *Consumer) joinGroup() (*balancer, error) {
	req := &sarama.JoinGroupRequest{
		GroupId:        c.groupID,
		MemberId:       c.memberID,
		SessionTimeout: int32(c.config.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  c.topics,
	}
	err := req.AddGroupProtocolMetadata(string(StrategyRange), meta)
	if err != nil {
		return nil, err
	}
	err = req.AddGroupProtocolMetadata(string(StrategyRoundRobin), meta)
	if err != nil {
		return nil, err
	}

	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	resp, err := broker.JoinGroup(req)
	if err != nil {
		return nil, err
	} else if resp.Err != sarama.ErrNoError {
		return nil, resp.Err
	}

	var strategy *balancer
	if resp.LeaderId == resp.MemberId {
		members, err := resp.GetMembers()
		if err != nil {
			return nil, err
		}

		strategy, err = newBalancerFromMeta(c.client, members)
		if err != nil {
			return nil, err
		}
	}

	c.memberID = resp.MemberId
	c.generationID = resp.GenerationId

	return strategy, nil
}

// Send a request to the broker to sync the group on rebalance().
// Returns a list of topics and partitions to consume.
func (c *Consumer) syncGroup(strategy *balancer) (map[string][]int32, error) {
	req := &sarama.SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: c.generationID,
	}
	for memberID, topics := range strategy.Perform(c.config.Group.PartitionStrategy) {
		if err := req.AddGroupAssignmentMember(memberID, &sarama.ConsumerGroupMemberAssignment{
			Version: 1,
			Topics:  topics,
		}); err != nil {
			return nil, err
		}
	}

	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	sync, err := broker.SyncGroup(req)
	if err != nil {
		return nil, err
	} else if sync.Err != sarama.ErrNoError {
		return nil, sync.Err
	}

	members, err := sync.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	return members.Topics, nil
}

// Fetches latest committed offsets for all subscriptions
func (c *Consumer) fetchOffsets(subs map[string][]int32) (map[string]map[int32]offsetInfo, error) {
	offsets := make(map[string]map[int32]offsetInfo, len(subs))
	req := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: c.groupID,
	}

	for topic, partitions := range subs {
		offsets[topic] = make(map[int32]offsetInfo, len(partitions))
		for _, partition := range partitions {
			offsets[topic][partition] = offsetInfo{Offset: -1}
			req.AddPartition(topic, partition)
		}
	}

	// Wait for other cluster consumers to process, release and commit
	time.Sleep(c.config.Consumer.MaxProcessingTime * 2)

	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	resp, err := broker.FetchOffset(req)
	if err != nil {
		return nil, err
	}

	for topic, partitions := range subs {
		for _, partition := range partitions {
			block := resp.GetBlock(topic, partition)
			if block == nil {
				return nil, sarama.ErrIncompleteResponse
			}

			if block.Err == sarama.ErrNoError {
				offsets[topic][partition] = offsetInfo{Offset: block.Offset, Metadata: block.Metadata}
			} else {
				return nil, block.Err
			}
		}
	}
	return offsets, nil
}

// Send a request to the broker to leave the group on failes rebalance() and on Close()
func (c *Consumer) leaveGroup() error {
	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	_, err := broker.LeaveGroup(&sarama.LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	})
	return err
}

// --------------------------------------------------------------------

func (c *Consumer) createConsumer(topic string, partition int32, info offsetInfo) error {
	sarama.Logger.Printf("cluster/consumer %s consume %s/%d from %d\n", c.memberID, topic, partition, info.NextOffset(c.config.Consumer.Offsets.Initial))
	// c.debug(">", "%s/%d/%d", topic, partition, info.NextOffset(c.config.Consumer.Offsets.Initial))

	// Create partitionConsumer
	pc, err := newPartitionConsumer(c.csmr, topic, partition, info, c.config.Consumer.Offsets.Initial)
	if err != nil {
		return nil
	}

	// Store in subscriptions
	c.subs.Store(topic, partition, pc)

	// Start partition consumer goroutine
	go pc.Loop(c.messages, c.errors)

	return nil
}

// Commits offsets for all subscriptions
func (c *Consumer) commitOffsets() error {
	req := &sarama.OffsetCommitRequest{
		Version:                 2,
		ConsumerGroup:           c.groupID,
		ConsumerGroupGeneration: c.generationID,
		ConsumerID:              c.memberID,
	}

	var blocks int
	snap := c.subs.Snapshot()
	for tp, state := range snap {
		// c.debug("+", "%s/%d/%d, dirty: %v", tp.Topic, tp.Partition, state.Info.Offset, state.Dirty)
		if state.Dirty {
			req.AddBlock(tp.Topic, tp.Partition, state.Info.Offset, 0, state.Info.Metadata)
			blocks++
		}
	}
	if blocks == 0 {
		return nil
	}

	c.brlock.Lock()
	broker := c.broker
	c.brlock.Unlock()

	resp, err := broker.CommitOffset(req)
	if err != nil {
		return err
	}

	for topic, perrs := range resp.Errors {
		for partition, kerr := range perrs {
			if kerr != sarama.ErrNoError {
				err = kerr
			} else if state, ok := snap[topicPartition{topic, partition}]; ok {
				// c.debug("=", "%s/%d/%d", topic, partition, state.Info.Offset)
				c.subs.Fetch(topic, partition).MarkCommitted(state.Info.Offset)
			}
		}
	}

	return err
}

func (c *Consumer) commitOffsetsWithRetry(retries int) error {
	err := c.commitOffsets()
	if err != nil && retries > 0 && c.subs.HasDirty() {
		_ = c.selectBroker()
		return c.commitOffsetsWithRetry(retries - 1)
	}
	return err
}

// --------------------------------------------------------------------

// func (c *Consumer) debug(sign string, format string, argv ...interface{}) {
// 	fmt.Printf("%s %s %s\n", sign, c.consumerID, fmt.Sprintf(format, argv...))
// }
