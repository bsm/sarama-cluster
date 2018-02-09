package cluster

import (
	"container/ring"
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

// NotificationType defines the type of notification
type NotificationType uint8

// String describes the notification type
func (t NotificationType) String() string {
	switch t {
	case RebalanceStart:
		return "rebalance start"
	case RebalanceOK:
		return "rebalance OK"
	case RebalanceError:
		return "rebalance error"
	}
	return "unknown"
}

const (
	UnknownNotification NotificationType = iota
	RebalanceStart
	RebalanceOK
	RebalanceError
)

// Notification are state events emitted by the consumers on rebalance
type Notification struct {
	// Type exposes the notification type
	Type NotificationType

	// Claimed contains topic/partitions that were claimed by this rebalance cycle
	Claimed map[string][]int32

	// Released contains topic/partitions that were released as part of this rebalance cycle
	Released map[string][]int32

	// Current are topic/partitions that are currently claimed to the consumer
	Current map[string][]int32
}

func newNotification(current map[string][]int32) *Notification {
	return &Notification{
		Type:    RebalanceStart,
		Current: current,
	}
}

func (n *Notification) success(current map[string][]int32) *Notification {
	o := &Notification{
		Type:     RebalanceOK,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  current,
	}
	for topic, partitions := range current {
		o.Claimed[topic] = int32Slice(partitions).Diff(int32Slice(n.Current[topic]))
	}
	for topic, partitions := range n.Current {
		o.Released[topic] = int32Slice(partitions).Diff(int32Slice(current[topic]))
	}
	return o
}

// --------------------------------------------------------------------

type balancer struct {
	client    sarama.Client
	memberIDs []string
	topics    map[string]*topicInfo
}

type topicInfo struct {
	Partitions []int32
	MemberIDs  []string
}

func newBalancerFromMeta(client sarama.Client, members map[string]sarama.ConsumerGroupMemberMetadata) (*balancer, error) {
	balancer := &balancer{
		client:    client,
		memberIDs: make([]string, 0, len(members)),
		topics:    make(map[string]*topicInfo),
	}
	for memberID, meta := range members {
		balancer.memberIDs = append(balancer.memberIDs, memberID)
		for _, topic := range meta.Topics {
			if err := balancer.Topic(memberID, topic); err != nil {
				return nil, err
			}
		}
	}

	sort.Strings(balancer.memberIDs)
	return balancer, nil
}

func (r *balancer) Topic(memberID string, name string) error {
	info, ok := r.topics[name]
	if !ok {
		nums, err := r.client.Partitions(name)
		if err != nil {
			return err
		}

		r.topics[name] = &topicInfo{
			MemberIDs:  []string{memberID},
			Partitions: nums,
		}

		return nil
	}

	info.MemberIDs = append(info.MemberIDs, memberID)
	return nil
}

func (r *balancer) Perform(s Strategy) map[string]map[string][]int32 {
	switch s {
	case StrategyRoundRobin:
		return assignRoundRobin(r.memberIDs, r.topics)
	default:
		return assignRange(r.memberIDs, r.topics)
	}
}

func assignRange(_ []string, topics map[string]*topicInfo) map[string]map[string][]int32 {
	tlen := len(topics)
	res := make(map[string]map[string][]int32)

	for topic, info := range topics {
		mlen := len(info.MemberIDs)
		plen := len(info.Partitions)

		sort.Strings(info.MemberIDs)
		for pos, memberID := range info.MemberIDs {
			n, i := float64(plen)/float64(mlen), float64(pos)
			min := int(math.Floor(i*n + 0.5))
			max := int(math.Floor((i+1)*n + 0.5))
			sub := info.Partitions[min:max]
			if len(sub) <= 0 {
				continue
			}

			assigned, ok := res[memberID]
			if !ok {
				assigned = make(map[string][]int32, tlen)
				res[memberID] = assigned
			}
			assigned[topic] = sub
		}
	}

	return res
}

func assignRoundRobin(memberIDs []string, topics map[string]*topicInfo) map[string]map[string][]int32 {
	sort.Strings(memberIDs)

	r := ring.New(len(memberIDs))
	for i := 0; i < r.Len(); i++ {
		r.Value = memberIDs[i]
		r = r.Next()
	}

	isSubscribed := func(memberID string, topic string) bool {
		info, ok := topics[topic]
		if !ok {
			return false
		}

		for _, subscriber := range info.MemberIDs {
			if memberID == subscriber {
				return true
			}
		}

		return false
	}

	tlen := len(topics)
	res := make(map[string]map[string][]int32, r.Len())

	for topic, info := range topics {
		for i := range info.Partitions {
			for ; !isSubscribed(r.Value.(string), topic); r = r.Next() {
				continue
			}

			memberID := r.Value.(string)
			assigned, ok := res[memberID]
			if !ok {
				assigned = make(map[string][]int32, tlen)
				res[memberID] = assigned
			}
			assigned[topic] = append(assigned[topic], info.Partitions[i])
			r = r.Next()
		}
	}

	return res
}
