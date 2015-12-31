package cluster

// Strategy for partition to consumer assignement
type Strategy string

const (
	// StrategyRange is the default and assigns partition ranges to consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 1, 2]
	//   C2: [3, 4, 5]
	StrategyRange Strategy = "range"

	// StrategyRoundRobin assigns partitions by alternating over consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 2, 4]
	//   C2: [1, 3, 5]
	StrategyRoundRobin Strategy = "roundrobin"
)

// --------------------------------------------------------------------

type none struct{}

type topicPartition struct {
	Topic     string
	Partition int32
}

type offsetInfo struct {
	Offset   int64
	Metadata string
}

func (i offsetInfo) NextOffset(fallback int64) int64 {
	if i.Offset > -1 {
		return i.Offset + 1
	}
	return fallback
}

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
