package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"gopkg.in/Shopify/sarama.v1"
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
				"topic1": {0, 1, 2, 3},
				"topic2": {0, 1, 2},
				"topic3": {0, 1},
			},
		}

		var err error
		subject, err = newBalancerFromMeta(client, map[string]sarama.ConsumerGroupMemberMetadata{
			"consumerB": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic3", "topic1"}},
			"consumerA": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2"}},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should panic if invalid balancer specified", func() {
		Ω(func() { subject.Perform(Strategy("some non-existant strategy")) }).Should(Panic())
	})

	It("should parse from meta data", func() {
		Expect(subject.topics).To(HaveLen(3))
	})

	It("should rebalance using the range strategy", func() {
		rb := &RangeBalancer{}
		Expect(rb.Rebalance(subject.topics)).To(Equal(map[string]map[string][]int32{
			"consumerA": {"topic1": {0, 1}, "topic2": {0, 1, 2}},
			"consumerB": {"topic1": {2, 3}, "topic3": {0, 1}},
		}))
	})

	It("should rebalance using the round robin strategy", func() {
		rrb := &RoundRobinBalancer{}
		Expect(rrb.Rebalance(subject.topics)).To(Equal(map[string]map[string][]int32{
			"consumerA": {"topic1": {0, 2}, "topic2": {0, 1, 2}},
			"consumerB": {"topic1": {1, 3}, "topic3": {0, 1}},
		}))
	})

	It("should rebalance using the striped strategy", func() {
		sb := &StripedBalancer{}
		Expect(sb.Rebalance(subject.topics)).To(Equal(map[string]map[string][]int32{
			"consumerA": {"topic1": {0, 2}, "topic2": {0, 2}, "topic3": {1}},
			"consumerB": {"topic1": {1, 3}, "topic2": {1}, "topic3": {0}},
		}))
	})

})

var _ = Describe("Striped balancer", func() {
	var subject *balancer

	BeforeEach(func() {
		client := &mockClient{
			topics: map[string][]int32{
				"topic1": {0, 1, 2, 3}, //c1 c2 c3 c4
				"topic2": {0, 1},       //c1 c2
				"topic3": {0, 1},       //c3 c4
				"topic4": {0, 1},       //c1 c2
				"topic5": {0, 1},       //c3 c4
				"topic6": {0, 1},       //c1 c2
				"topic7": {0, 1},       //c3 c4
				"topic8": {0, 1},       //c1 c2
			},
		}

		var err error
		subject, err = newBalancerFromMeta(client, map[string]sarama.ConsumerGroupMemberMetadata{
			"consumer1": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"}},
			"consumer2": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"}},
			"consumer3": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"}},
			"consumer4": sarama.ConsumerGroupMemberMetadata{Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5", "topic6", "topic7", "topic8"}},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should parse from meta data", func() {
		Expect(subject.topics).To(HaveLen(8))
	})

	It("should perform a striped rebalance", func() {
		Expect(subject.Perform(StrategyStriped)).To(Equal(map[string]map[string][]int32{
			"consumer1": {"topic1": {0}, "topic2": {0}, "topic4": {0}, "topic6": {0}, "topic8": {0}},
			"consumer2": {"topic1": {1}, "topic2": {1}, "topic4": {1}, "topic6": {1}, "topic8": {1}},
			"consumer3": {"topic1": {2}, "topic3": {0}, "topic5": {0}, "topic7": {0}},
			"consumer4": {"topic1": {3}, "topic3": {1}, "topic5": {1}, "topic7": {1}},
		}))
	})
})

var _ = Describe("topicInfo", func() {

	DescribeTable("Ranges",
		func(memberIDs []string, partitions []int32, expected map[string][]int32) {
			rb := &RangeBalancer{}
			info := TopicInfo{MemberIDs: memberIDs, Partitions: partitions}
			Expect(rb.rebalanceTopicInfo(info)).To(Equal(expected))
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
			info := TopicInfo{MemberIDs: memberIDs, Partitions: partitions}
			rrb := &RoundRobinBalancer{}
			Expect(rrb.rebalanceTopicInfo(info)).To(Equal(expected))
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
