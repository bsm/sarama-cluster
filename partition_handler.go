package cluster

import "github.com/Shopify/sarama"

// PartitionHandler is a handler for subscribing to
// partition. It will receive messages for this partition.
type PartitionHandler interface {
	Messages() chan<- *sarama.ConsumerMessage
	Errors() chan<- error
	Close(error)
}

// PartitionHandlerFunc is a function to be called when a new partition for the group.
type PartitionHandlerFunc func(topic string, partition int32, offset int64, metadata string) PartitionHandler

// partitionHandler is a default implementation of the PartitionHandler
// interface.
type partitionHandler struct {
	messages chan<- *sarama.ConsumerMessage
	errors   chan<- error
}

// Messages implements the PartitionHandler interface.
func (gph *partitionHandler) Messages() chan<- *sarama.ConsumerMessage {
	return gph.messages
}

// Errors implements the PartitionHandler interface.
func (gph *partitionHandler) Errors() chan<- error {
	return gph.errors
}

// Close implements the PartitionHandler interface.
func (gph *partitionHandler) Close(_ error) {}
