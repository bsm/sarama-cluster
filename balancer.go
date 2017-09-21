package cluster

import (
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

type topicInfo struct {
	Partitions []int32
	MemberIDs  []string
}

func (info topicInfo) Perform(s Strategy) map[string][]int32 {
	if s == StrategyRoundRobin {
		return info.RoundRobin()
	}
	return info.Ranges()
}

func (info topicInfo) Ranges() map[string][]int32 {
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
}

func (info topicInfo) RoundRobin() map[string][]int32 {
	sort.Strings(info.MemberIDs)

	mlen := len(info.MemberIDs)
	res := make(map[string][]int32, mlen)
	for i, pnum := range info.Partitions {
		memberID := info.MemberIDs[i%mlen]
		res[memberID] = append(res[memberID], pnum)
	}
	return res
}

// --------------------------------------------------------------------

type balancer struct {
	client sarama.Client
	topics map[string]topicInfo
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
		topics: make(map[string]topicInfo),
	}
}

func (r *balancer) Topic(name string, memberID string) error {
	topic, ok := r.topics[name]
	if !ok {
		nums, err := r.client.Partitions(name)
		if err != nil {
			return err
		}
		topic = topicInfo{
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

	// Striped behaves slightly differently, hence why it is performed in its own function.
	if s == StrategyStriped || s == StrategyTopic {
		return r.performMultibalance(s)
	}

	res := make(map[string]map[string][]int32, 1)
	for topic, info := range r.topics {
		for memberID, partitions := range info.Perform(s) {
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][topic] = partitions
		}
	}
	return res
}

func (r *balancer) performMultibalance(strat Strategy) map[string]map[string][]int32 {
	members := make(map[string]struct{}, 1)
	topics := make(map[string][]int32, 1)
	for topic, info := range r.topics {
		topics[topic] = info.Partitions
		for _, memberID := range info.MemberIDs {
			members[memberID] = struct{}{}
		}
	}

	topicNames := make([]string, 0)
	for topic, _ := range topics {
		topicNames = append(topicNames, topic)
	}
	sort.Strings(topicNames)

	memberIDs := make([]string, 0)
	for memberID, _ := range members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	memberCount := len(memberIDs)
	res := make(map[string]map[string][]int32, 1)

	if strat == StrategyTopic {
		for memberIdx, topicName := range topicNames {
			partitions := topics[topicName]
			memberID := memberIDs[memberIdx%memberCount]
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][topicName] = append(res[memberID][topicName], partitions...)
		}
	} else { // StrategyStriped
		memberIdx := 0
		for _, topicName := range topicNames {
			partitions := topics[topicName]
			for _, partition := range partitions {
				memberID := memberIDs[memberIdx%memberCount]
				if _, ok := res[memberID]; !ok {
					res[memberID] = make(map[string][]int32, 1)
				}
				res[memberID][topicName] = append(res[memberID][topicName], partition)
				memberIdx++
			}
		}
	}

	return res
}
