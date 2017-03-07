package cluster

import (
	"fmt"
	"sort"
)

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

	// StrategyStriped stripes partitions across consumers, distributing partitions
	// evenly across a set of consumers
	// Given topics with partitions:
	//    T1: [0, 1, 2, 3]
	//    T2: [0, 1]
	//    T3: [0, 1]
	//    T4: [0, 1]
	// And consumers C1-C4, balance would be:
	//    C1: T1: [0], T2: [0], T4: [0]
	//    C2: T1: [1], T2: [1], T4: [1]
	//    C3: T1: [2], T3: [0]
	//    C4: T2: [3], T3: [1]
	// Note that StrategyStriped expects all consumers to consume from the same set
	// of topics.  It will combine all members' subscribed topics into a single set and
	// distribute partitions to consumers evenly.
	StrategyStriped Strategy = "striped"
)

// Error instances are wrappers for internal errors with a context and
// may be returned through the consumer's Errors() channel
type Error struct {
	Ctx string
	error
}

// --------------------------------------------------------------------

type none struct{}

type topicPartition struct {
	Topic     string
	Partition int32
}

func (tp *topicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

type offsetInfo struct {
	Offset   int64
	Metadata string
}

func (i offsetInfo) NextOffset(fallback int64) int64 {
	if i.Offset > -1 {
		return i.Offset
	}
	return fallback
}

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p int32Slice) Diff(o int32Slice) (res []int32) {
	on := len(o)
	for _, x := range p {
		n := sort.Search(on, func(i int) bool { return o[i] >= x })
		if n < on && o[n] == x {
			continue
		}
		res = append(res, x)
	}
	return
}
