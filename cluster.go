package cluster

import "github.com/Shopify/sarama"

type none struct{}

// --------------------------------------------------------------------

type handlerWrapper struct {
	sarama.ConsumerGroupHandler
	preSetup func(sarama.ConsumerGroupSession)
}

func (w *handlerWrapper) Setup(s sarama.ConsumerGroupSession) error {
	w.preSetup(s)
	return w.ConsumerGroupHandler.Setup(s)
}

// HandlerFunc is a sarama.ConsumerGroupHandler function shortcut.
type HandlerFunc func(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error

// ConsumeClaim implements sarama.ConsumerGroupHandler
func (f HandlerFunc) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	return f(s, c)
}

// Setup implements sarama.ConsumerGroupHandler
func (f HandlerFunc) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler
func (f HandlerFunc) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// --------------------------------------------------------------------

// Claim is a notification issued by a consumer to indicate
// the claimed topics after a rebalance.
type Claim struct {
	// Current lists the currently claimed topics and partitions
	Current map[string][]int32
}
