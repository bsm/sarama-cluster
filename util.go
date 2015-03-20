package cluster

import (
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

/* topic/partition combo */

type topicPartition struct {
	topic     string
	partition int32
}

func (tp topicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.topic, tp.partition)
}

/* claims map */

type claimsMap map[topicPartition]sarama.PartitionConsumer

// Names returns the claimed topic-partitions
func (c claimsMap) Names() []string {
	names := make([]string, 0, len(c))
	for tp, _ := range c {
		names = append(names, tp.String())
	}
	sort.Strings(names)
	return names
}

/* GUID generator */

// Global UID config
var cGUID struct {
	hostname string
	pid      int
	inc      uint32
}

// Init GUID configuration
func init() {
	cGUID.hostname, _ = os.Hostname()
	cGUID.pid = os.Getpid()
	cGUID.inc = 0xffffffff
	if cGUID.hostname == "" {
		cGUID.hostname = "localhost"
	}
}

// Create a new GUID
func newGUID(prefix string) string {
	return newGUIDAt(prefix, time.Now())
}

// Create a new GUID for a certain time
func newGUIDAt(prefix string, at time.Time) string {
	inc := atomic.AddUint32(&cGUID.inc, 1)
	uts := at.Unix()
	ins := fmt.Sprintf("%08x", inc)
	return fmt.Sprintf("%s:%s:%08x-%04x-%s-%s", prefix, cGUID.hostname, uts, cGUID.pid, ins[:4], ins[4:])
}
