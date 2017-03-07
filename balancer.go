package cluster

import (
	"fmt"
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

// Notification events are emitted by the consumers on rebalancing
type Notification struct {
	// Claimed contains topic/partitions that were claimed by this rebalance cycle
	Claimed map[string][]int32

	// Released contains topic/partitions that were released as part of this rebalance cycle
	Released map[string][]int32

	// Current are topic/partitions that are currently claimed to the consumer
	Current map[string][]int32
}

func newNotification(released map[string][]int32) *Notification {
	return &Notification{
		Claimed:  make(map[string][]int32),
		Released: released,
		Current:  make(map[string][]int32),
	}
}

func (n *Notification) claim(current map[string][]int32) {
	previous := n.Released
	for topic, partitions := range current {
		n.Claimed[topic] = int32Slice(partitions).Diff(int32Slice(previous[topic]))
	}
	for topic, partitions := range previous {
		n.Released[topic] = int32Slice(partitions).Diff(int32Slice(current[topic]))
	}
	n.Current = current
}

// --------------------------------------------------------------------

var (
	balancerStrategies map[Strategy]Balancer = map[Strategy]Balancer{
		StrategyRange:      rangeBalancer{},
		StrategyRoundRobin: roundRobinBalancer{},
		StrategyStriped:    stripedBalancer{},
	}
)

// RegisterBalancerStrategy allows for use of custom rebalance strategies.
func RegisterBalancerStrategy(name Strategy, b Balancer) {
	balancerStrategies[name] = b
}

// TopicInfoGroup represents a collection of topics and their consumers and partitions.
type TopicInfoGroup map[string]TopicInfo

// MemberIDs returns the unique set of member IDs in the TopicInfoGroup
func (ti TopicInfoGroup) MemberIDs() []string {
	ret := make([]string, 0, len(ti))
	seen := make(map[string]struct{})

	for _, topicInfo := range ti {
		for _, memberID := range topicInfo.MemberIDs {
			if _, ok := seen[memberID]; !ok {
				ret = append(ret, memberID)
				seen[memberID] = struct{}{}
			}
		}
	}

	return ret
}

// Partitions returns a map of topics with their respective partitions
func (ti TopicInfoGroup) Partitions() map[string][]int32 {
	ret := make(map[string][]int32)
	for topic, topicInfo := range ti {
		ret[topic] = topicInfo.Partitions
	}
	return ret
}

// --------------------------------------------------------------------

type partitionSorter []int32

func (ps partitionSorter) Len() int           { return len(ps) }
func (ps partitionSorter) Swap(i, j int)      { ps[i], ps[j] = ps[j], ps[i] }
func (ps partitionSorter) Less(i, j int) bool { return ps[i] < ps[j] }

// --------------------------------------------------------------------

// Balancer implements a method Rebalance that determines which topics/partitions each
// member of the group should consume.
type Balancer interface {
	// Rebalance takes a map of topic names with group member and partition information, and
	// returns a map of group members with a map of the topics/partitions they should consume.
	// Example input:
	// Topic1:
	//   MemberIDs: [Consumer1, Consumer2]
	//   Partitions: [0, 1]
	// Topic2:
	//   MemberIDs: [Consumer2, Consumer3]
	//   Partitions: [0, 1, 2, 3]
	// Example output (in this case, StrategyStriped):
	// Consumer1:
	//   Topic1: [0]
	//   Topic2: [1]
	// Consumer2:
	//   Topic1: [1]
	//   Topic2: [2]
	// Consumer3:
	//   Topic2: [0, 3]
	Rebalance(topics TopicInfoGroup) map[string]map[string][]int32
}

// TopicInfo contains the partitions a topic contains and the member IDs subscribed to it.
type TopicInfo struct {
	Topic      string
	Partitions []int32
	MemberIDs  []string
}

// RangeBalancer implements StrategyRange, balancing partitions in ranges to consumers.
type rangeBalancer struct{}

// Rebalance rebalances the consumer group using the range strategy.
func (rb rangeBalancer) Rebalance(topics TopicInfoGroup) map[string]map[string][]int32 {
	return rebalanceWithStrategy(topics, func(info TopicInfo) map[string][]int32 {
		sort.Strings(info.MemberIDs)

		mlen := len(info.MemberIDs)
		plen := len(info.Partitions)
		res := make(map[string][]int32, mlen)

		for pos, memberID := range info.MemberIDs {
			n, i := float64(plen)/float64(mlen), float64(pos)
			min := int(math.Floor(i*n + 0.5))
			max := int(math.Floor((i+1)*n + 0.5))
			sub := info.Partitions[min:max]
			if len(sub) > 0 {
				res[memberID] = sub
			}
		}
		return res
	})
}

// --------------------------------------------------------------------

// RoundRobinBalancer implements StrategyRoundRobin, balancing partitions alternating over consumers.
type roundRobinBalancer struct{}

// Rebalance rebalances the consumer group using the round robin strategy.
func (rr roundRobinBalancer) Rebalance(topics TopicInfoGroup) map[string]map[string][]int32 {
	return rebalanceWithStrategy(topics, func(info TopicInfo) map[string][]int32 {
		sort.Strings(info.MemberIDs)

		mlen := len(info.MemberIDs)
		res := make(map[string][]int32, mlen)
		for i, pnum := range info.Partitions {
			memberID := info.MemberIDs[i%mlen]
			res[memberID] = append(res[memberID], pnum)
		}
		return res
	})
}

func rebalanceWithStrategy(topics TopicInfoGroup,
	rebalanceFunc func(TopicInfo) map[string][]int32,
) map[string]map[string][]int32 {

	res := make(map[string]map[string][]int32, 1)
	for topic, info := range topics {
		for memberID, partitions := range rebalanceFunc(info) {
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][topic] = partitions
		}
	}
	return res
}

// --------------------------------------------------------------------

// StripedRebalancer implements StrategyStriped, balancing topics fairly
// across consumers
type stripedBalancer struct{}

// Rebalance rebalances the consumer group using the striped strategy.
func (sb stripedBalancer) Rebalance(topics TopicInfoGroup) map[string]map[string][]int32 {
	memberIDs := topics.MemberIDs()
	topicsAndPartitions := topics.Partitions()
	sort.Strings(memberIDs)

	sortedTopics := make([]string, 0, len(topics))
	for key, _ := range topics {
		sortedTopics = append(sortedTopics, key)
	}

	sort.Strings(sortedTopics)

	output := make(map[string]map[string][]int32)

	memberIdx := 0
	memLen := len(memberIDs)
	for _, topic := range sortedTopics {
		partitions := topicsAndPartitions[topic]
		sort.Sort(partitionSorter(partitions))
		for _, partition := range partitions {
			partition := partition
			memberID := memberIDs[memberIdx%memLen]
			if _, ok := output[memberID]; !ok {
				output[memberID] = make(map[string][]int32)
			}
			output[memberID][topic] = append(output[memberID][topic], partition)
			memberIdx++
		}
	}

	return output
}

// --------------------------------------------------------------------

type balancer struct {
	client sarama.Client
	topics TopicInfoGroup
}

func newBalancerFromMeta(client sarama.Client, members map[string]sarama.ConsumerGroupMemberMetadata) (*balancer, error) {
	balancer := newBalancer(client)
	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			if err := balancer.Topic(topic, memberID); err != nil {
				return nil, err
			}
		}
	}
	return balancer, nil
}

func newBalancer(client sarama.Client) *balancer {
	return &balancer{
		client: client,
		topics: make(TopicInfoGroup),
	}
}

func (r *balancer) Topic(name string, memberID string) error {
	topic, ok := r.topics[name]
	if !ok {
		nums, err := r.client.Partitions(name)
		if err != nil {
			return err
		}
		topic = TopicInfo{
			Topic:      name,
			Partitions: nums,
			MemberIDs:  make([]string, 0, 1),
		}
	}
	topic.MemberIDs = append(topic.MemberIDs, memberID)
	r.topics[name] = topic
	return nil
}

func (r *balancer) Perform(s Strategy) map[string]map[string][]int32 {
	if r == nil {
		return nil
	}

	strat, ok := balancerStrategies[s]
	if !ok {
		panic(fmt.Sprintf("invalid balancer strategy: %s", string(s)))
	}

	return strat.Rebalance(r.topics)
}
