package cluster

import "github.com/Shopify/sarama"

// EventStream is an abstraction of a sarama.Consumer
type EventStream interface {
	Events() <-chan *sarama.ConsumerEvent
	Close() error
}

// EventBatch is a batch of events from a single topic/partition
type EventBatch struct {
	Topic     string
	Partition int32
	Events    []*sarama.ConsumerEvent
}

// Returns true if starts with an OffsetOutOfRange error
func (b *EventBatch) offsetIsOutOfRange() bool {
	if b == nil || len(b.Events) < 1 {
		return false
	}

	err := b.Events[0].Err
	if err == nil {
		return false
	}

	kerr, ok := err.(sarama.KError)
	return ok && kerr == sarama.OffsetOutOfRange
}

// PartitionConsumer can consume a single partition of a single topic
type PartitionConsumer struct {
	stream    EventStream
	partition int32
	offset    int64
	client    *sarama.Client
	topic     string
	group     string
	config    *sarama.ConsumerConfig
}

// NewPartitionConsumer creates a new partition consumer instance
func NewPartitionConsumer(client *sarama.Client, config *sarama.ConsumerConfig, topic, group string, partition int32, offset int64) (*PartitionConsumer, error) {
	p := &PartitionConsumer{
		client:    client,
		config:    config,
		topic:     topic,
		group:     group,
		partition: partition,
	}

	if err := p.newStream(offset); err != nil {
		return nil, err
	}
	return p, nil
}

// Offset returns the current offset
func (p *PartitionConsumer) Offset() int64 {
	return p.offset
}

// Rollback rollbacks stream to given offset
func (p *PartitionConsumer) Rollback(offset int64) error {
	return p.newStream(offset)
}

// Fetch returns a batch of events
// WARNING: may return nil if no events are available
func (p *PartitionConsumer) Fetch() *EventBatch {
	events := p.stream.Events()
	evtlen := len(events)
	if evtlen < 1 {
		return nil
	}

	batch := &EventBatch{
		Topic:     p.topic,
		Partition: p.partition,
		Events:    make([]*sarama.ConsumerEvent, evtlen),
	}
	for i := 0; i < evtlen; i++ {
		event := <-events
		batch.Events[i] = event

		if event.Err == nil && event.Offset > p.offset {
			p.offset = event.Offset
		}
	}

	return batch
}

// Close closes a partition consumer
func (p *PartitionConsumer) Close() error {
	return p.stream.Close()
}

// PRIVATE
func (p *PartitionConsumer) newStream(offset int64) error {
	c := *p.config
	c.OffsetMethod = sarama.OffsetMethodOldest
	if offset > 0 {
		c.OffsetMethod = sarama.OffsetMethodManual
		c.OffsetValue = offset
	}
	var err error
	p.stream, err = sarama.NewConsumer(p.client, p.topic, p.partition, p.group, &c)
	p.offset = offset
	return err
}
