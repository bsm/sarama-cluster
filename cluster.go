package cluster

import (
	"io/ioutil"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Standard logger, set to ioutil.Discard by default
var Logger = log.New(ioutil.Discard, "[sarama/cluster]", log.LstdFlags)

// A notifier is an abstract event notification handler
// By default, sarama/cluster uses the LogNotifier which logs events to standard logger
type Notifier interface {
	RebalanceStart(*Consumer)
	RebalanceOK(*Consumer)
	RebalanceError(*Consumer, error)
	CommitError(*Consumer, error)
}

// Standard log notifier, writes to Logger
type LogNotifier struct{ *log.Logger }

func (n *LogNotifier) RebalanceStart(c *Consumer) {
	n.Printf("rebalancing %s", c.Group())
}

func (n *LogNotifier) RebalanceOK(c *Consumer) {
	n.Printf("rebalanced %s, claimed: %v", c.Group(), c.Claims())
}

func (n *LogNotifier) RebalanceError(c *Consumer, err error) {
	n.Printf("rebalancing %s failed: %s", c.Group(), err.Error())
}

func (n *LogNotifier) CommitError(c *Consumer, err error) {
	n.Printf("committing %s failed: %s", c.Group(), err.Error())
}

type ConsumerConfig struct {
	// Standard consumer configuration
	*sarama.ConsumerConfig

	// Set to true if you want to automatically ack
	// every event once it has been consumed through
	// the Consumer.Events() channel
	AutoAck bool

	// Enable automatic commits but setting this options.
	// Automatic commits will remain disabled if is set to < 10ms
	CommitEvery time.Duration

	// Notifier instance to handle info/error
	// notifications from the consumer
	// Default: *LogNotifier
	Notifier Notifier

	customID string
}

func (c *ConsumerConfig) normalize() {
	if c.ConsumerConfig == nil {
		c.ConsumerConfig = sarama.NewConsumerConfig()
	}
	if c.Notifier == nil {
		c.Notifier = &LogNotifier{Logger}
	}
	if c.CommitEvery < 10*time.Millisecond {
		c.CommitEvery = 0
	}
}
