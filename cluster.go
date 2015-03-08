package cluster

import (
	"io/ioutil"
	"log"
)

// Standard logger, set to ioutil.Discard by default
var Logger = log.New(ioutil.Discard, "[sarama/cluster]", log.LstdFlags)

// Notifier is an abstract event notification handler
// By default, sarama/cluster uses the LogNotifier which logs events to standard logger
type Notifier interface {
	RebalanceStart(*Consumer)
	RebalanceOK(*Consumer)
	RebalanceError(*Consumer, error)
	CommitError(*Consumer, error)
}

// LogNotifier is the standard log notifier, uses a Logger as the backend
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
