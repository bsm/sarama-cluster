package cluster

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	DiscardCommit    = errors.New("sarama: commit discarded")
	NoCheckout       = errors.New("sarama: not checkout")
	RollbackCheckout = errors.New("sarama: rollback checkout")
)

// A ConsumerGroup operates on all partitions of a single topic. The goal is to ensure
// each topic message is consumed only once, no matter of the number of consumer instances within
// a cluster, as described in: http://kafka.apache.org/documentation.html#distributionimpl.
//
// The ConsumerGroup internally creates multiple Consumer instances. It uses Zookkeper
// and follows a simple consumer rebalancing algorithm which allows all the consumers
// in a group to come into consensus on which consumer is consuming which partitions. Each
// ConsumerGroup can 'claim' 0-n partitions and will consume their messages until another
// ConsumerGroup instance with the same name joins or leaves the cluster.
//
// Unlike stated in the Kafka documentation, consumer rebalancing is *only* triggered on each
// addition or removal of consumers within the same group, while the addition of broker nodes
// and/or partition *does currently not trigger* a rebalancing cycle.
type ConsumerGroup struct {
	id, name, topic string

	client *sarama.Client
	config *sarama.ConsumerConfig
	zoo    *ZK
	claims []PartitionConsumer

	zkchange <-chan zk.Event
	claimed  chan *PartitionConsumer
	notify   Notifier

	checkout, checkin, force, stopper, done chan bool
}

// NewConsumerGroup creates a new consumer group for a given topic.
//
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
func NewConsumerGroup(client *sarama.Client, zoo *ZK, name string, topic string, notify Notifier, config *sarama.ConsumerConfig) (group *ConsumerGroup, err error) {
	if config == nil {
		config = sarama.NewConsumerConfig()
	}

	if notify == nil {
		notify = &LogNotifier{Logger}
	}

	// Validate configuration
	if err = config.Validate(); err != nil {
		return
	} else if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	} else if name == "" {
		return nil, sarama.ConfigurationError("Empty name")
	}

	// Register consumer group
	if err = zoo.RegisterGroup(name); err != nil {
		return
	}

	// Init struct
	group = &ConsumerGroup{
		id:    NewGUID(name),
		name:  name,
		topic: topic,

		config: config,
		client: client,
		zoo:    zoo,
		claims: make([]PartitionConsumer, 0),
		notify: notify,

		stopper:  make(chan bool),
		done:     make(chan bool),
		checkout: make(chan bool),
		checkin:  make(chan bool),
		force:    make(chan bool),
		claimed:  make(chan *PartitionConsumer),
	}

	// Register itself with zookeeper
	if err = zoo.RegisterConsumer(group.name, group.id, group.topic); err != nil {
		return nil, err
	}

	go group.signalLoop()
	return group, nil
}

// Name exposes the group name
func (cg *ConsumerGroup) Name() string {
	return cg.name
}

// Topic exposes the group topic
func (cg *ConsumerGroup) Topic() string {
	return cg.topic
}

// Checkout applies a callback function to a single partition consumer.
// The latest consumer offset is automatically comitted to zookeeper if successful.
// The callback may return a DiscardCommit error to skip the commit silently.
// Returns an error if any, but may also return a NoCheckout error to indicate
// that no partition was available. You should add an artificial delay keep your CPU cool.
func (cg *ConsumerGroup) Checkout(callback func(*PartitionConsumer) error) error {
	cg.checkout <- true
	claimed := <-cg.claimed
	defer func() { cg.checkin <- true }()

	if claimed == nil {
		return NoCheckout
	}

	original := claimed.Offset()
	err := callback(claimed)
	if err == DiscardCommit {
		err = nil
	} else if err == RollbackCheckout {
		err = claimed.Rollback(original)
	} else if err == nil && claimed.offset > 0 {
		err = cg.Commit(claimed.partition, claimed.offset+1)
	}
	return err
}

// Process retrieves a bulk of events and applies a callback.
// The latest consumer offset is automatically comitted to zookeeper if successful.
// The callback may return a DiscardCommit error to skip the commit silently.
// Returns an error if any, but may also return a NoCheckout error to indicate
// that no partition was available. You should add an artificial delay keep your CPU cool.
func (cg *ConsumerGroup) Process(callback func(*EventBatch) error) error {
	return cg.Checkout(func(pc *PartitionConsumer) error {
		if batch := pc.Fetch(); batch != nil {

			// Try to reset offset on OffsetOutOfRange errors
			if batch.offsetIsOutOfRange() {
				cur, err := cg.Offset(pc.partition)
				if err != nil {
					return err
				}

				min, err := cg.client.GetOffset(cg.topic, pc.partition, sarama.EarliestOffset)
				if err != nil {
					return err
				}

				if cur < min {
					if err := cg.Commit(pc.partition, min); err != nil {
						return err
					}
				}

				cg.releaseClaims()
				cg.force <- true
				return fmt.Errorf("kafka: The requested offset is outside the range of offsets maintained by the server for %s-%d, current: %d, min: %d.", cg.topic, pc.partition, cur, min)
			}

			return callback(batch)
		}
		return NoCheckout
	})
}

// Commit manually commits an offset for a partition
func (cg *ConsumerGroup) Commit(partition int32, offset int64) error {
	return cg.zoo.Commit(cg.name, cg.topic, partition, offset)
}

// Offset manually retrives an offset for a partition
func (cg *ConsumerGroup) Offset(partition int32) (int64, error) {
	return cg.zoo.Offset(cg.name, cg.topic, partition)
}

// Claims returns the claimed partitions
func (cg *ConsumerGroup) Claims() []int32 {
	res := make([]int32, 0, len(cg.claims))
	for _, claim := range cg.claims {
		res = append(res, claim.partition)
	}
	return res
}

// Close closes the consumer group
func (cg *ConsumerGroup) Close() error {
	close(cg.stopper)
	<-cg.done
	return nil
}

// Background signal loop
func (cg *ConsumerGroup) signalLoop() {
	for {
		// If we have no zk handle, rebalance
		if cg.zkchange == nil {
			if err := cg.rebalance(); err != nil {
				cg.notify.RebalanceError(cg, err)
			}
		}

		// If rebalance failed, check if we had a stop signal, then try again
		if cg.zkchange == nil {
			select {
			case <-cg.stopper:
				cg.stop()
				return
			case <-time.After(time.Millisecond):
				// Continue
			}
			continue
		}

		// If rebalance worked, wait for a stop signal or a zookeeper change or a fetch-request
		select {
		case <-cg.stopper:
			cg.stop()
			return
		case <-cg.force:
			cg.zkchange = nil
		case <-cg.zkchange:
			cg.zkchange = nil
		case <-cg.checkout:
			cg.claimed <- cg.nextConsumer()
			<-cg.checkin
		}
	}
}

/**********************************************************************
 * PRIVATE
 **********************************************************************/

// Stops the consumer group
func (cg *ConsumerGroup) stop() {
	cg.releaseClaims()
	cg.zoo.DeleteConsumer(cg.name, cg.id)
	close(cg.done)
}

// Checkout a claimed partition consumer
func (cg *ConsumerGroup) nextConsumer() *PartitionConsumer {
	if len(cg.claims) < 1 {
		return nil
	}

	shift := cg.claims[0]
	cg.claims = append(cg.claims[1:], shift)
	return &shift
}

// Start a rebalance cycle
func (cg *ConsumerGroup) rebalance() (err error) {
	var cids []string
	var pids []int32
	cg.notify.RebalanceStart(cg)

	// Fetch a list of consumers and listen for changes
	if cids, cg.zkchange, err = cg.zoo.Consumers(cg.name); err != nil {
		cg.zkchange = nil
		return
	}

	// Fetch a list of partition IDs
	if pids, err = cg.client.Partitions(cg.topic); err != nil {
		cg.zkchange = nil
		return
	}

	// Get leaders for each partition ID
	parts := make(PartitionSlice, len(pids))
	for i, pid := range pids {
		var broker *sarama.Broker
		if broker, err = cg.client.Leader(cg.topic, pid); err != nil {
			cg.zkchange = nil
			return
		}
		parts[i] = Partition{ID: pid, Addr: broker.Addr()}
	}

	if err = cg.makeClaims(cids, parts); err != nil {
		cg.zkchange = nil
		cg.releaseClaims()
		return
	}
	return
}

func (cg *ConsumerGroup) makeClaims(cids []string, parts PartitionSlice) error {
	current := make(map[int32]bool, len(cg.claims))
	for _, pt := range cg.claims {
		current[pt.partition] = true
	}

	future := cg.claimRange(cids, parts)
	unchanged := len(current) == len(future)
	for _, pt := range future {
		unchanged = unchanged && current[pt.ID]
	}
	if unchanged {
		return nil
	}

	cg.releaseClaims()
	for _, part := range future {
		err := cg.zoo.Claim(cg.name, cg.topic, part.ID, cg.id)
		if err != nil {
			return err
		}

		offset, err := cg.Offset(part.ID)
		if err != nil {
			return err
		}

		pc, err := NewPartitionConsumer(cg.client, cg.config, cg.topic, cg.name, part.ID, offset)
		if err != nil {
			return err
		}

		cg.claims = append(cg.claims, *pc)
	}

	cg.notify.RebalanceOK(cg)
	return nil
}

// Determine the partititons number to claim
func (cg *ConsumerGroup) claimRange(cids []string, parts PartitionSlice) PartitionSlice {
	sort.Strings(cids)
	sort.Sort(parts)

	pos := sort.SearchStrings(cids, cg.id)
	cln := len(cids)
	if pos >= cln {
		return parts[:0]
	}

	n, i := float64(len(parts))/float64(cln), float64(pos)
	min := int(math.Floor(i*n + 0.5))
	max := int(math.Floor((i+1)*n + 0.5))
	return parts[min:max]
}

// Releases all claims
func (cg *ConsumerGroup) releaseClaims() {
	for _, pc := range cg.claims {
		pc.Close()
		cg.zoo.Release(cg.name, cg.topic, pc.partition, cg.id)
	}
	cg.claims = cg.claims[:0]
}
