package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Notification", func() {

	It("should init and convert", func() {
		n := newNotification(map[string][]int32{
			"a": {1, 2, 3},
			"b": {4, 5},
			"c": {1, 2},
		})
		Expect(n).To(Equal(&Notification{
			Type:    RebalanceStart,
			Current: map[string][]int32{"a": {1, 2, 3}, "b": {4, 5}, "c": {1, 2}},
		}))

		o := n.success(map[string][]int32{
			"a": {3, 4},
			"b": {1, 2, 3, 4},
			"d": {3, 4},
		})
		Expect(o).To(Equal(&Notification{
			Type:     RebalanceOK,
			Claimed:  map[string][]int32{"a": {4}, "b": {1, 2, 3}, "d": {3, 4}},
			Released: map[string][]int32{"a": {1, 2}, "b": {5}, "c": {1, 2}},
			Current:  map[string][]int32{"a": {3, 4}, "b": {1, 2, 3, 4}, "d": {3, 4}},
		}))
	})

})

var _ = Describe("balancer", func() {
	var subject *balancer

	BeforeEach(func() {
		client := &mockClient{
			topics: map[string][]int32{
				"one":   {0, 1, 2, 3},
				"two":   {0, 1, 2},
				"three": {0, 1},
			},
		}

		var err error
		subject, err = newBalancerFromMeta(client, map[string]sarama.ConsumerGroupMemberMetadata{
			"b": {Topics: []string{"three", "one"}},
			"a": {Topics: []string{"one", "two"}},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should track topic partitions", func() {
		Expect(subject.topics).To(HaveLen(3))
	})

	It("should track group member subscriptions", func() {
		subs := subject.subs
		Expect(subs.Members()).To(ConsistOf([]string{"a", "b"}))
		Expect(subs.SubscribedMembers("one")).To(ConsistOf([]string{"a", "b"}))
		Expect(subs.SubscribedMembers("two")).To(Equal([]string{"a"}))
		Expect(subs.SubscribedMembers("three")).To(Equal([]string{"b"}))
	})

})

var _ = Describe("Assignors", func() {
	DescribeTable("Range",
		func(subs submap, topics topicparts, expected Assignments) {
			result := RangeAssignor(subs.Subscriptions(), topics.TopicPartitions())
			Expect(result).To(Equal(expected))
		},

		Entry("one consumer, no topics", submap{"m0": {"t0"}}, topicparts{}, Assignments{}),
		Entry("one consumer, no such topic", submap{"m0": {"t1"}}, topicparts{"t0": {0}}, Assignments{}),

		Entry("one consumer, one topic", submap{"m0": {"t0"}}, topicparts{"t0": {0, 1, 2}}, Assignments{
			"m0": {"t0": {0, 1, 2}},
		}),

		Entry("one consumer, two topics, one subscribed", submap{"m0": {"t0"}}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 1, 2}},
		}),

		Entry("one consumer, multiple topics", submap{"m0": {"t0", "t1"}}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 1, 2}, "t1": {0, 1, 2}},
		}),

		Entry("two consumers, one topic, one partition", submap{
			"m0": {"t0"},
			"m1": {"t0"},
		}, topicparts{
			"t0": {0},
		}, Assignments{
			"m0": {"t0": {0}},
		}),

		Entry("two consumers, one topic, two partitions", submap{
			"m0": {"t0"},
			"m1": {"t0"},
		}, topicparts{
			"t0": {0, 1},
		}, Assignments{
			"m0": {"t0": {0}},
			"m1": {"t0": {1}},
		}),

		Entry("multiple consumers, mixed topics", submap{
			"m0": {"t0"},
			"m1": {"t0", "t1"},
			"m2": {"t0"},
		}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1},
		}, Assignments{
			"m0": {"t0": {0}},
			"m1": {"t0": {1}, "t1": {0, 1}},
			"m2": {"t0": {2}},
		}),

		Entry("two consumers, two topics, six partitions", submap{
			"m0": {"t0", "t1"},
			"m1": {"t0", "t1"},
		}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 1}, "t1": {0, 1}},
			"m1": {"t0": {2}, "t1": {2}},
		}),

		Entry("heavily uneven partition counts", submap{
			"m0": {"t0", "t1", "t2"},
			"m1": {"t0", "t1", "t2"},
		}, topicparts{
			"t0": {0, 1, 2, 3, 4},
			"t1": {0, 1},
			"t2": {0},
		}, Assignments{
			"m0": {"t0": {0, 1, 2}, "t1": {0}, "t2": {0}},
			"m1": {"t0": {3, 4}, "t1": {1}},
		}),
	)

	DescribeTable("RoundRobin",
		func(subs submap, topics topicparts, expected Assignments) {
			result := RoundRobinAssignor(subs.Subscriptions(), topics.TopicPartitions())
			Expect(result).To(Equal(expected))
		},

		Entry("one consumer, no topics", submap{"m0": {"t0"}}, topicparts{}, Assignments{}),
		Entry("one consumer, no such topic", submap{"m0": {"t1"}}, topicparts{"t0": {0}}, Assignments{}),

		Entry("one consumer, one topic", submap{"m0": {"t0"}}, topicparts{"t0": {0, 1, 2}}, Assignments{
			"m0": {"t0": {0, 1, 2}},
		}),

		Entry("one consumer, two topics, one subscribed", submap{"m0": {"t0"}}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 1, 2}},
		}),

		Entry("one consumer, multiple topics", submap{"m0": {"t0", "t1"}}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 1, 2}, "t1": {0, 1, 2}},
		}),

		Entry("two consumers, one topic, two partitions", submap{
			"m0": {"t0"},
			"m1": {"t0"},
		}, topicparts{
			"t0": {0, 1},
		}, Assignments{
			"m0": {"t0": {0}},
			"m1": {"t0": {1}},
		}),

		Entry("two consumers, one topic, one partition", submap{
			"m0": {"t0"},
			"m1": {"t0"},
		}, topicparts{
			"t0": {0},
		}, Assignments{
			"m0": {"t0": {0}},
		}),

		Entry("multiple consumers, mixed topics", submap{
			"m0": {"t0"},
			"m1": {"t0", "t1"},
			"m2": {"t0"},
		}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1},
		}, Assignments{
			"m0": {"t0": {0}},
			"m1": {"t0": {1}, "t1": {0, 1}},
			"m2": {"t0": {2}},
		}),

		Entry("two consumers, two topics, six partitions", submap{
			"m0": {"t0", "t1"},
			"m1": {"t0", "t1"},
		}, topicparts{
			"t0": {0, 1, 2},
			"t1": {0, 1, 2},
		}, Assignments{
			"m0": {"t0": {0, 2}, "t1": {1}},
			"m1": {"t0": {1}, "t1": {0, 2}},
		}),

		Entry("heavily uneven partition counts", submap{
			"m0": {"t0", "t1", "t2"},
			"m1": {"t0", "t1", "t2"},
		}, topicparts{
			"t0": {0, 1, 2, 3, 4},
			"t1": {0, 1},
			"t2": {0},
		}, Assignments{
			"m0": {"t0": {0, 2, 4}, "t1": {1}},
			"m1": {"t0": {1, 3}, "t1": {0}, "t2": {0}},
		}),
	)
})

type submap map[string][]string
type topicparts map[string][]int32

func (s submap) Subscriptions() *Subscriptions {
	subs := NewSubscriptions()
	for memberID, topics := range s {
		for _, topic := range topics {
			subs.AddSubscriber(memberID, topic)
		}
	}
	return subs
}

func (t topicparts) TopicPartitions() []*TopicPartitions {
	tps := []*TopicPartitions{}
	for topic, partitions := range t {
		tps = append(tps, &TopicPartitions{Name: topic, Partitions: partitions})
	}
	return tps
}
