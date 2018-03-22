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

// Assignor is a function which returns specific partition assignments
// given the set of topic subscriptions of a given group.
type Assignor func(subs *Subscriptions, topics []*TopicPartitions) Assignments

// TopicPartitions identifies a topic and its partition IDs.
type TopicPartitions struct {
	Name       string
	Partitions []int32
}

// Subscriptions contains information about all members in a consumer
// group, and which topics they have subscribed to.
type Subscriptions struct {
	memberIDs   []string
	subscribers map[string][]string
}

// NewSubscriptions returns an empty set of subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		memberIDs:   []string{},
		subscribers: map[string][]string{},
	}
}

// Members returns the list of all member IDs in the group.
func (m *Subscriptions) Members() []string {
	return m.memberIDs
}

// AddSubscriber registers a member as subscribed to a topic.
// Returns self.
func (m *Subscriptions) AddSubscriber(memberID, topic string) *Subscriptions {
	seen := false
	for i := range m.memberIDs {
		if m.memberIDs[i] == memberID {
			seen = true
		}
	}

	if !seen {
		m.memberIDs = append(m.memberIDs, memberID)
	}

	m.subscribers[topic] = append(m.subscribers[topic], memberID)
	return m
}

// SubscribedMembers returns the full list of members subscribed
// to a topic.
func (m *Subscriptions) SubscribedMembers(topic string) []string {
	return m.subscribers[topic]
}

// IsSubscribed returns true if a member is subscribed to a topic.
func (m *Subscriptions) IsSubscribed(memberID, topic string) bool {
	subs, ok := m.subscribers[topic]
	if !ok {
		return false
	}

	for i := range subs {
		if subs[i] == memberID {
			return true
		}
	}

	return false
}

// Assignments is a mapping of member IDs to the topic partitions that they
// have been assigned.
type Assignments map[string]map[string][]int32

// NewAssignments returns an empty set of assignments.
func NewAssignments() Assignments {
	return map[string]map[string][]int32{}
}

// Assign adds a partition to the list of a member's assignments.
func (a Assignments) Assign(memberID, topic string, partition int32) {
	topics, ok := a[memberID]
	if !ok {
		topics = map[string][]int32{}
		a[memberID] = topics
	}

	topics[topic] = append(topics[topic], partition)
}

type balancer struct {
	client sarama.Client
	subs   *Subscriptions
	topics []*TopicPartitions
}

func newBalancerFromMeta(client sarama.Client, members map[string]sarama.ConsumerGroupMemberMetadata) (*balancer, error) {
	balancer := &balancer{
		client: client,
		subs:   NewSubscriptions(),
		topics: []*TopicPartitions{},
	}

	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			balancer.subs.AddSubscriber(memberID, topic)
			if err := balancer.AddTopic(topic); err != nil {
				return nil, err
			}
		}
	}

	return balancer, nil
}

func (r *balancer) AddTopic(name string) error {
	for i := range r.topics {
		if r.topics[i].Name == name {
			return nil
		}
	}

	nums, err := r.client.Partitions(name)
	if err != nil {
		return err
	}

	r.topics = append(r.topics, &TopicPartitions{
		Name:       name,
		Partitions: nums,
	})
	return nil
}

func (r *balancer) Perform(fn Assignor) Assignments {
	return fn(r.subs, r.topics)
}

// RangeAssignor assigns partitions to subscribed group members by
// dividing the number of partitions, per-topic, by the number of
// consumers to determine the number of partitions per consumer that
// should be assigned. If the value does not evenly divide, consumers
// lexicographically earlier will be assigned an extra partition.
func RangeAssignor(subs *Subscriptions, topics []*TopicPartitions) Assignments {
	assignments := NewAssignments()

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	for _, tp := range topics {
		members := subs.SubscribedMembers(tp.Name)
		mlen := len(members)
		plen := len(tp.Partitions)

		sort.Strings(members)
		for pos, memberID := range members {
			n, i := float64(plen)/float64(mlen), float64(pos)
			min := int(math.Floor(i*n + 0.5))
			max := int(math.Floor((i+1)*n + 0.5))
			sub := tp.Partitions[min:max]
			for i := range sub {
				assignments.Assign(memberID, tp.Name, sub[i])
			}
		}
	}

	return assignments
}

// RoundRobinAssignor assigns partitions by iterating through the
// list of group members and assigning one to each consumer until
// all partitions have been assigned. If a group member is not
// subscribed to a topic, the next subscribed member is assigned
// instead.
func RoundRobinAssignor(subs *Subscriptions, topics []*TopicPartitions) Assignments {
	assignments := NewAssignments()
	memberIDs := subs.Members()
	sort.Strings(memberIDs)

	r := ring.New(len(memberIDs))
	for i := 0; i < r.Len(); i++ {
		r.Value = memberIDs[i]
		r = r.Next()
	}

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	for _, tp := range topics {
		if len(subs.SubscribedMembers(tp.Name)) == 0 {
			continue
		}

		partitions := tp.Partitions
		for i := range partitions {
			for ; !subs.IsSubscribed(r.Value.(string), tp.Name); r = r.Next() {
				continue
			}

			assignments.Assign(r.Value.(string), tp.Name, partitions[i])
			r = r.Next()
		}
	}

	return assignments
}
