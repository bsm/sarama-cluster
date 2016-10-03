package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Notification", func() {

	It("should init and update", func() {
		n := newNotification(map[string][]int32{
			"a": {1, 2, 3},
			"b": {4, 5},
			"c": {1, 2},
		})
		n.claim(map[string][]int32{
			"a": {3, 4},
			"b": {1, 2, 3, 4},
			"d": {3, 4},
		})
		Expect(n).To(Equal(&Notification{
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
			"b": sarama.ConsumerGroupMemberMetadata{Topics: []string{"three", "one"}},
			"a": sarama.ConsumerGroupMemberMetadata{Topics: []string{"one", "two"}},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should parse from meta data", func() {
		Expect(subject.topics).To(HaveLen(3))
	})

	It("should perform", func() {
		Expect(subject.Perform(StrategyRange)).To(Equal(map[string]map[string][]int32{
			"a": {"one": {0, 1}, "two": {0, 1, 2}},
			"b": {"one": {2, 3}, "three": {0, 1}},
		}))

		Expect(subject.Perform(StrategyRoundRobin)).To(Equal(map[string]map[string][]int32{
			"a": {"one": {0, 2}, "two": {0, 1, 2}},
			"b": {"one": {1, 3}, "three": {0, 1}},
		}))

		// Topics are sorted alphabetically, hence why `three` is expected before `two`
		Expect(subject.Perform(StrategyStriped)).To(Equal(map[string]map[string][]int32{
			"a": {"one": {0, 2}, "two": {0, 2}, "three": {0}},
			"b": {"one": {1, 3}, "two": {1}, "three": {1}},
		}))
	})

})

var _ = Describe("topicInfo", func() {
	DescribeTable("Striped", func(memberIDs []string, topics map[string][]int32, expected map[string]map[string][]int32) {
		r := &balancer{
			topics: make(map[string]topicInfo),
		}

		for topicName, partitions := range topics {
			r.topics[topicName] = topicInfo{
				MemberIDs:  memberIDs,
				Partitions: partitions,
			}
		}

		res := r.performStripedBalance()
		Expect(res).To(Equal(expected))
	},

		Entry("More consumers than topics",
			[]string{"M1", "M2", "M3"},
			map[string][]int32{
				"T1": {0, 1},
			},
			map[string]map[string][]int32{
				"M1": {
					"T1": []int32{0},
				},
				"M2": {
					"T1": []int32{1},
				},
			}),

		Entry("Uneven number of consumers",
			[]string{"M1", "M2", "M3"},
			map[string][]int32{
				"T1": {0, 1},
				"T2": {0, 1},
			},
			map[string]map[string][]int32{
				"M1": {
					"T1": []int32{0},
					"T2": []int32{1},
				},
				"M2": {
					"T1": []int32{1},
				},
				"M3": {
					"T2": []int32{0},
				},
			}),

		Entry("Many small topics",
			[]string{"M1", "M2", "M3", "M4"},
			map[string][]int32{
				"T1": {0, 1},
				"T2": {0, 1},
				"T3": {0, 1},
				"T4": {0, 1},
				"T5": {0, 1},
				"T6": {0, 1},
				"T7": {0, 1},
				"T8": {0, 1},
			},
			map[string]map[string][]int32{
				"M1": {
					"T1": []int32{0},
					"T3": []int32{0},
					"T5": []int32{0},
					"T7": []int32{0},
				},
				"M2": {
					"T1": []int32{1},
					"T3": []int32{1},
					"T5": []int32{1},
					"T7": []int32{1},
				},
				"M3": {
					"T2": []int32{0},
					"T4": []int32{0},
					"T6": []int32{0},
					"T8": []int32{0},
				},
				"M4": {
					"T2": []int32{1},
					"T4": []int32{1},
					"T6": []int32{1},
					"T8": []int32{1},
				},
			}),

		Entry("Varied partition counts",
			[]string{"M1", "M2", "M3", "M4"},
			map[string][]int32{
				"T1": {0, 1},
				"T2": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				"T3": {0, 1, 2, 3},
				"T4": {0, 1},
			},
			map[string]map[string][]int32{
				"M1": {
					"T1": []int32{0},
					"T2": []int32{2, 6, 10},
					"T3": []int32{3},
				},
				"M2": {
					"T1": []int32{1},
					"T2": []int32{3, 7},
					"T3": []int32{0},
					"T4": []int32{0},
				},
				"M3": {
					"T2": []int32{0, 4, 8},
					"T3": []int32{1},
					"T4": []int32{1},
				},
				"M4": {
					"T2": []int32{1, 5, 9},
					"T3": []int32{2},
				},
			}),
	)

	DescribeTable("Ranges",
		func(memberIDs []string, partitions []int32, expected map[string][]int32) {
			info := topicInfo{MemberIDs: memberIDs, Partitions: partitions}
			Expect(info.Ranges()).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("member ID order", []string{"M3", "M1", "M2"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("more members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1}, map[string][]int32{
			"M1": {0}, "M3": {1},
		}),
		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, []int32{0}, map[string][]int32{
			"M2": {0},
		}),
		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2, 3}, map[string][]int32{
			"M1": {0}, "M2": {1, 2}, "M3": {3},
		}),
		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, []int32{0, 2, 4, 6, 8}, map[string][]int32{
			"M1": {0, 2}, "M2": {4}, "M3": {6, 8},
		}),
	)

	DescribeTable("RoundRobin",
		func(memberIDs []string, partitions []int32, expected map[string][]int32) {
			info := topicInfo{MemberIDs: memberIDs, Partitions: partitions}
			Expect(info.RoundRobin()).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("member ID order", []string{"M3", "M1", "M2"}, []int32{0, 1, 2}, map[string][]int32{
			"M1": {0}, "M2": {1}, "M3": {2},
		}),
		Entry("more members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1}, map[string][]int32{
			"M1": {0}, "M2": {1},
		}),
		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, []int32{0}, map[string][]int32{
			"M1": {0},
		}),
		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, []int32{0, 1, 2, 3}, map[string][]int32{
			"M1": {0, 3}, "M2": {1}, "M3": {2},
		}),
		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, []int32{0, 2, 4, 6, 8}, map[string][]int32{
			"M1": {0, 6}, "M2": {2, 8}, "M3": {4},
		}),
	)
})
