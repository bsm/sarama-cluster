package cluster

import (
	"sort"
	"sync"

	"github.com/Shopify/sarama"
)

type partitionConsumer struct {
	pcm sarama.PartitionConsumer

	state partitionState
	mutex sync.Mutex

	dying, dead chan none
}

func newPartitionConsumer(manager sarama.Consumer, topic string, partition int32, info offsetInfo, defaultOffset int64) (*partitionConsumer, error) {
	pcm, err := manager.ConsumePartition(topic, partition, info.NextOffset(defaultOffset))

	// Resume from default offset, if requested offset is out-of-range
	if err == sarama.ErrOffsetOutOfRange {
		info.Offset = -1
		pcm, err = manager.ConsumePartition(topic, partition, defaultOffset)
	}
	if err != nil {
		return nil, err
	}

	return &partitionConsumer{
		pcm:   pcm,
		state: partitionState{Info: info},

		dying: make(chan none),
		dead:  make(chan none),
	}, nil
}

func (c *partitionConsumer) Loop(messages chan<- *sarama.ConsumerMessage, errors chan<- error) {
	for {
		select {
		case msg := <-c.pcm.Messages():
			select {
			case messages <- msg:
			case <-c.dying:
				close(c.dead)
				return
			}
		case err := <-c.pcm.Errors():
			select {
			case errors <- err:
			case <-c.dying:
				close(c.dead)
				return
			}
		case <-c.dying:
			close(c.dead)
			return
		}
	}
}

func (c *partitionConsumer) Close() (err error) {
	close(c.dying)
	<-c.dead

	if e := c.pcm.Close(); e != nil {
		err = e
	}
	return
}

func (c *partitionConsumer) State() partitionState {
	if c == nil {
		return partitionState{}
	}

	c.mutex.Lock()
	state := c.state
	c.mutex.Unlock()

	return state
}

func (c *partitionConsumer) MarkCommitted(offset int64) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	if offset == c.state.Info.Offset {
		c.state.Dirty = false
	}
	c.mutex.Unlock()
}

func (c *partitionConsumer) MarkOffset(offset int64, metadata string) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	if offset > c.state.Info.Offset {
		c.state.Info.Offset = offset
		c.state.Info.Metadata = metadata
		c.state.Dirty = true
	}
	c.mutex.Unlock()
}

// --------------------------------------------------------------------

type partitionState struct {
	Info  offsetInfo
	Dirty bool
}

// --------------------------------------------------------------------

type partitionMap struct {
	data  map[topicPartition]*partitionConsumer
	mutex sync.RWMutex
}

func newPartitionMap() *partitionMap {
	return &partitionMap{
		data: make(map[topicPartition]*partitionConsumer),
	}
}

func (m *partitionMap) Fetch(topic string, partition int32) *partitionConsumer {
	m.mutex.RLock()
	pc, _ := m.data[topicPartition{topic, partition}]
	m.mutex.RUnlock()
	return pc
}

func (m *partitionMap) Store(topic string, partition int32, pc *partitionConsumer) {
	m.mutex.Lock()
	m.data[topicPartition{topic, partition}] = pc
	m.mutex.Unlock()
}

func (m *partitionMap) HasDirty() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for _, pc := range m.data {
		if state := pc.State(); state.Dirty {
			return true
		}
	}
	return false
}

func (m *partitionMap) Snapshot() map[topicPartition]partitionState {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	snap := make(map[topicPartition]partitionState, len(m.data))
	for tp, pc := range m.data {
		snap[tp] = pc.State()
	}
	return snap
}

func (m *partitionMap) Stop() (err error) {
	m.mutex.RLock()
	size := len(m.data)
	errs := make(chan error, size)
	for tp := range m.data {
		go func(p *partitionConsumer) { errs <- p.Close() }(m.data[tp])
	}
	m.mutex.RUnlock()

	for i := 0; i < size; i++ {
		if e := <-errs; e != nil {
			err = e
		}
	}
	return
}

func (m *partitionMap) Clear() {
	m.mutex.Lock()
	for tp := range m.data {
		delete(m.data, tp)
	}
	m.mutex.Unlock()
}

func (m *partitionMap) Info() map[string][]int32 {
	info := make(map[string][]int32)
	m.mutex.RLock()
	for tp := range m.data {
		info[tp.Topic] = append(info[tp.Topic], tp.Partition)
	}
	m.mutex.RUnlock()

	for topic := range info {
		sort.Sort(int32Slice(info[topic]))
	}
	return info
}

// Diffs map with current partitionMap, returns additions and removals
func Diff(current map[string][]int32, new map[string][]int32) (map[string][]int32, map[string][]int32) {
	additions := make(map[string][]int32)
	removals := make(map[string][]int32)

	for topic, parts := range current {
		if otherParts, ok := new[topic]; ok {
			rems := compare(parts, otherParts)

			if len(rems) > 0 {
				removals[topic] = rems
			}

			adds := compare(otherParts, parts)

			if len(adds) > 0 {
				additions[topic] = adds
			}
		} else {
			removals[topic] = new[topic]
		}
	}

	for topic, otherParts := range new {
		if _, ok := current[topic]; !ok {
			additions[topic] = otherParts
		}
	}

	return additions, removals
}

// http://stackoverflow.com/questions/23870102/compare-two-slices
func compare(X, Y []int32) []int32 {
	counts := make(map[int32]int)
	var total int32
	for _, val := range X {
		counts[val] += 1
		total += 1
	}
	for _, val := range Y {
		if count := counts[val]; count > 0 {
			counts[val] -= 1
			total -= 1
		}
	}
	difference := make([]int32, total)
	i := 0
	for val, count := range counts {
		for j := 0; j < count; j++ {
			difference[i] = val
			i++
		}
	}
	return difference
}
