package cluster

import (
	"github.com/Shopify/sarama"
)

// EventStream is an abstraction of a sarama.Consumer
type EventStream interface {
	Events() <-chan *sarama.ConsumerEvent
	Close() error
}

// EventBatch is a batch of events from a single topic/partition
type EventBatch struct {
	Topic     string
	Partition int32
	Events    []sarama.ConsumerEvent
}

// PartitionConsumer can consume a single partition of a single topic
type PartitionConsumer struct {
	stream    EventStream
	topic     string
	partition int32
	offset    int64
}

// NewPartitionConsumer creates a new partition consumer instance
func NewPartitionConsumer(group *ConsumerGroup, partition int32) (*PartitionConsumer, error) {
	config := sarama.ConsumerConfig{
		DefaultFetchSize: group.config.DefaultFetchSize,
		EventBufferSize:  group.config.EventBufferSize,
		MaxMessageSize:   group.config.MaxMessageSize,
		MaxWaitTime:      group.config.MaxWaitTime,
		MinFetchSize:     group.config.MinFetchSize,
		OffsetMethod:     sarama.OffsetMethodOldest,
	}

	offset, err := group.Offset(partition)
	if err != nil {
		return nil, err
	} else if offset > 0 {
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = offset
	}

	stream, err := sarama.NewConsumer(group.client, group.topic, partition, group.name, &config)
	if err != nil {
		return nil, err
	}

	return &PartitionConsumer{
		stream:    stream,
		topic:     group.topic,
		partition: partition,
	}, nil
}

// Fetch returns a batch of events
// WARNING: may return nil if not events are available
func (p *PartitionConsumer) Fetch() *EventBatch {
	events := p.stream.Events()
	evtlen := len(events)
	if evtlen < 1 {
		return nil
	}

	batch := &EventBatch{
		Topic:     p.topic,
		Partition: p.partition,
		Events:    make([]sarama.ConsumerEvent, evtlen),
	}
	for i := 0; i < evtlen; i++ {
		event := <-events
		batch.Events[i] = *event

		if event.Offset > p.offset {
			p.offset = event.Offset
		}
	}

	return batch
}

// Close closes a partition consumer
func (p *PartitionConsumer) Close() error {
	return p.stream.Close()
}
