package cluster

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Global UID config
var cGUID struct {
	hostname string
	pid      int
	inc      uint32
	sync.Mutex
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
func NewGUID(prefix string) string {
	return NewGUIDAt(prefix, time.Now())
}

// Create a new GUID for a certain time
func NewGUIDAt(prefix string, at time.Time) string {
	cGUID.Lock()
	defer cGUID.Unlock()

	cGUID.inc++
	return fmt.Sprintf("%s-%s-%d-%d-%d", prefix, cGUID.hostname, cGUID.pid, at.Unix(), cGUID.inc)
}

// Partition information
type Partition struct {
	ID   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int { return len(s) }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr == s[j].Addr {
		return s[i].ID < s[j].ID
	}
	return s[i].Addr < s[j].Addr
}
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
