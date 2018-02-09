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

	It("should parse from meta data", func() {
		Expect(subject.memberIDs).To(Equal([]string{"a", "b"}))
		Expect(subject.topics).To(HaveLen(3))
	})

})

var _ = Describe("partition assignment", func() {

	DescribeTable("Ranges",
		func(memberIDs []string, topics map[string]*topicInfo, expected map[string]map[string][]int32) {
			assignments := assignRange(memberIDs, topics)
			Expect(assignments).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1}},
			"M3": {"t1": {2}},
		}),

		Entry("member ID order", []string{"M3", "M1", "M2"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1}},
			"M3": {"t1": {2}},
		}),

		Entry("more members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M3": {"t1": {1}},
		}),

		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M2": {"t1": {0}},
		}),

		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2, 3},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1, 2}},
			"M3": {"t1": {3}},
		}),

		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 2, 4, 6, 8},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0, 2}},
			"M2": {"t1": {4}},
			"M3": {"t1": {6, 8}},
		}),

		Entry("multiple topics", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
			"t2": &topicInfo{
				Partitions: []int32{0, 1},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
			"t3": &topicInfo{
				Partitions: []int32{0},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}, "t2": {0}},
			"M2": {"t1": {1}, "t3": {0}},
			"M3": {"t1": {2}, "t2": {1}},
		}),

		Entry("different subscriptions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
			"t2": &topicInfo{
				Partitions: []int32{0, 1},
				MemberIDs:  []string{"M2", "M3"},
			},
			"t3": &topicInfo{
				Partitions: []int32{0},
				MemberIDs:  []string{"M1"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}, "t3": {0}},
			"M2": {"t1": {1}, "t2": {0}},
			"M3": {"t1": {2}, "t2": {1}},
		}),
	)

	DescribeTable("RoundRobin",
		func(memberIDs []string, topics map[string]*topicInfo, expected map[string]map[string][]int32) {
			assignments := assignRoundRobin(memberIDs, topics)
			Expect(assignments).To(Equal(expected))
		},

		Entry("three members, three partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1}},
			"M3": {"t1": {2}},
		}),

		Entry("member ID order", []string{"M3", "M1", "M2"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1}},
			"M3": {"t1": {2}},
		}),

		Entry("more members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
			"M2": {"t1": {1}},
		}),

		Entry("far more members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0}},
		}),

		Entry("fewer members than partitions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2, 3},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0, 3}},
			"M2": {"t1": {1}},
			"M3": {"t1": {2}},
		}),

		Entry("uneven members/partitions ratio", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 2, 4, 6, 8},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {"t1": {0, 6}},
			"M2": {"t1": {2, 8}},
			"M3": {"t1": {4}},
		}),

		Entry("multiple topics", []string{"M1", "M2"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2"},
			},
			"t2": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M1", "M2"},
			},
		}, map[string]map[string][]int32{
			"M1": {
				"t1": {0, 2},
				"t2": {1},
			},
			"M2": {
				"t1": {1},
				"t2": {0, 2},
			},
		}),

		Entry("different subscriptions", []string{"M1", "M2", "M3"}, map[string]*topicInfo{
			"t1": &topicInfo{
				Partitions: []int32{0},
				MemberIDs:  []string{"M1", "M2", "M3"},
			},
			"t2": &topicInfo{
				Partitions: []int32{0, 1},
				MemberIDs:  []string{"M2", "M3"},
			},
			"t3": &topicInfo{
				Partitions: []int32{0, 1, 2},
				MemberIDs:  []string{"M3"},
			},
		}, map[string]map[string][]int32{
			"M1": {
				"t1": {0},
			},
			"M2": {
				"t2": {0},
			},
			"M3": {
				"t2": {1},
				"t3": {0, 1, 2},
			},
		}),
	)

})
