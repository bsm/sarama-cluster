package cluster

import (
	"io/ioutil"
	"log"
)

// Standard logger, set to ioutil.Discard by default
var Logger = log.New(ioutil.Discard, "[sarama/cluster]", log.LstdFlags)

// A notifier is an abstract event notification handler
// By default, sarama/cluster uses the LogNotifier which logs events to standard logger
type Notifier interface {
	RebalanceStart(*ConsumerGroup)
	RebalanceOK(*ConsumerGroup)
	RebalanceError(*ConsumerGroup, error)
}

// Standard log notifier, writes to Logger
type LogNotifier struct {
	*log.Logger
}

func (n *LogNotifier) RebalanceStart(cg *ConsumerGroup) {
	n.Printf("rebalancing %s", cg.Name())
}

func (n *LogNotifier) RebalanceOK(cg *ConsumerGroup) {
	n.Printf("rebalanced %s, claimed: %v", cg.Name(), cg.Claims())
}

func (n *LogNotifier) RebalanceError(cg *ConsumerGroup, err error) {
	n.Printf("rebalancing %s failed: %s", cg.Name(), err.Error())
}
