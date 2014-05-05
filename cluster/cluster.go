package cluster

const (
	REBALANCE_START uint8 = iota + 1
	REBALANCE_OK
	REBALANCE_ERROR
)

// COMMON TYPES

// Partition information
type Partition struct {
	Id   int32
	Addr string // Leader address
}

// A sortable slice of Partition structs
type PartitionSlice []Partition

func (s PartitionSlice) Len() int { return len(s) }
func (s PartitionSlice) Less(i, j int) bool {
	if s[i].Addr < s[j].Addr {
		return true
	}
	return s[i].Id < s[j].Id
}
func (s PartitionSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// A subscribable notification
type Notification struct {
	Type uint8
	Src  *ConsumerGroup
	Err  error
}
