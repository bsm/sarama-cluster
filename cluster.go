package cluster

import "github.com/Shopify/sarama"

type none struct{}

// --------------------------------------------------------------------

// Handler instances are able to consume from PartitionConsumer instances.
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently.
// Ensure that all state is safely protected against race conditions.
type Handler interface {
	// ProcessPartition must start a consumer loop over Messages().
	ProcessPartition(PartitionConsumer) error
}

// HandlerFunc is a Handler function shortcut.
type HandlerFunc func(PartitionConsumer) error

// ProcessLoop implements the Handler interface.
func (f HandlerFunc) ProcessPartition(c PartitionConsumer) error { return f(c) }

// --------------------------------------------------------------------

// Claim is a notification issued by a consumer to indicate
// the claimed topics after a rebalance.
type Claim struct {
	// Current lists the currently claimed topics and partitions
	Current map[string][]int32
}

// --------------------------------------------------------------------

type PartitionConsumer interface {
	sarama.ConsumerGroupClaim

	// MarkMessage marks a message as consumed.
	MarkMessage(msg *sarama.ConsumerMessage, metadata string)
}

type partitionConsumer struct {
	sarama.ConsumerGroupClaim
	sess sarama.ConsumerGroupSession
}

func (pc *partitionConsumer) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	pc.sess.MarkMessage(msg, metadata)
}
