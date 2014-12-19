package cluster

import "github.com/Shopify/sarama"

type Claims map[int32]*sarama.Consumer

// PartitionIDs returns the associated partition IDs
func (c Claims) PartitionIDs() []int32 {
	ids := make(int32Slice, 0, len(c))
	for id, _ := range c {
		ids = append(ids, id)
	}
	return ids.Sorted()
}
