package cluster

import (
	"github.com/Shopify/sarama"
)

// PartitionConsumer
type PartitionConsumer interface {
	// Topic returns the consumed topic name.
	Topic() string
	// Partition returns the consumed partition.
	Partition() int32

	// HighWaterMarkOffset returns the high water mark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64
	// Messages returns the read channel for the messages that are returned by
	// the broker.
	Messages() <-chan *sarama.ConsumerMessage

	// MarkMessage marks the provided message as consumed, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	MarkMessage(msg *sarama.ConsumerMessage, metadata string)

	// Done exposes a notification channel to indicate that a new rebalance cycle
	// is due. Once triggered, the consumer must finish its processing within
	// Config.Consumer.Group.Rebalance.Timeout before the topic/partition is re-assigned
	// to another group member.
	Done() <-chan struct{}
}

type partitionConsumer struct {
	topic     string
	partition int32

	*partitionOffsetManager
	sarama.ConsumerGroupSession
	sarama.PartitionConsumer
}

func (c *partitionConsumer) Topic() string    { return c.topic }
func (c *partitionConsumer) Partition() int32 { return c.partition }
func (c *partitionConsumer) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	if msg.Topic == c.topic && msg.Partition == c.partition {
		c.markOffset(msg.Offset+1, metadata)
	}
}
