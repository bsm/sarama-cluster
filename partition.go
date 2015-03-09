package cluster

import (
	"math"
	"sort"
)

// Partition information
type Partition struct {
	ID   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int      { return len(s) }
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr == s[j].Addr {
		return s[i].ID < s[j].ID
	}
	return s[i].Addr < s[j].Addr
}

// Select is an algorithm to distribute a subset of a partitions
// to one of the consumers
func (s PartitionSlice) Select(consumerID string, consumerIDs []string) PartitionSlice {
	sort.Strings(consumerIDs)
	sort.Sort(s)

	pos := sort.SearchStrings(consumerIDs, consumerID)
	cln := len(consumerIDs)
	if pos >= cln {
		return s[:0]
	}

	n, i := float64(len(s))/float64(cln), float64(pos)
	min := int(math.Floor(i*n + 0.5))
	max := int(math.Floor((i+1)*n + 0.5))
	return s[min:max]
}
