package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("partitionConsumer", func() {
	var subject *partitionConsumer

	BeforeEach(func() {
		var err error
		subject, err = newPartitionConsumer(&mockConsumer{}, "topic", 0, offsetInfo{2000, "m3ta"}, sarama.OffsetOldest)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		close(subject.dead)
		Expect(subject.Close()).NotTo(HaveOccurred())
		Expect(subject.Close()).NotTo(HaveOccurred()) // test that consumer can be closed 2x
	})

	It("should set state", func() {
		Expect(subject.State()).To(Equal(partitionState{
			Processed: offsetInfo{2000, "m3ta"},
			Dirty:     false,
		}))
	})

	It("should recover from default offset if requested offset is out of bounds", func() {
		pc, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, offsetInfo{200, "m3ta"}, sarama.OffsetOldest)
		Expect(err).NotTo(HaveOccurred())
		defer pc.Close()
		close(pc.dead)

		state := pc.State()
		Expect(state.Processed.Offset).To(Equal(int64(-1)))
		Expect(state.Processed.Metadata).To(Equal("m3ta"))
	})

	It("should update state", func() {
		subject.MarkConsumed(2002)
		subject.MarkProcessed(2001, "met@") // should set state
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2002,
			Processed: offsetInfo{2001, "met@"},
			Dirty:     true,
		}))

		subject.MarkCommitted(2001) // should reset dirty status
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2002,
			Processed: offsetInfo{2001, "met@"},
			Dirty:     false,
		}))

		subject.MarkProcessed(2001, "me7a") // should not update state
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2002,
			Processed: offsetInfo{2001, "met@"},
			Dirty:     false,
		}))

		subject.MarkProcessed(2002, "me7a") // should bump state
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2002,
			Processed: offsetInfo{2002, "me7a"},
			Dirty:     true,
		}))

		subject.MarkCommitted(2001) // should not unset state
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2002,
			Processed: offsetInfo{2002, "me7a"},
			Dirty:     true,
		}))

		subject.MarkConsumed(2003) // should increment consumed status
		Expect(subject.State()).To(Equal(partitionState{
			Consumed:  2003,
			Processed: offsetInfo{2002, "me7a"},
			Dirty:     true,
		}))
	})

	It("should check if pending", func() {
		Expect(subject.State().Pending()).To(Equal(int64(0)))
		subject.MarkConsumed(2001)
		Expect(subject.State().Pending()).To(Equal(int64(1)))
		subject.MarkProcessed(2001, "")
		Expect(subject.State().Pending()).To(Equal(int64(0)))
		subject.MarkConsumed(2002)
		Expect(subject.State().Pending()).To(Equal(int64(1)))
	})

	It("should not fail when nil", func() {
		blank := (*partitionConsumer)(nil)
		Expect(func() {
			_ = blank.State()
			blank.MarkProcessed(2001, "met@")
			blank.MarkCommitted(2001)
		}).NotTo(Panic())
	})

})

var _ = Describe("partitionMap", func() {
	var subject *partitionMap

	BeforeEach(func() {
		subject = newPartitionMap()
	})

	It("should fetch/store", func() {
		Expect(subject.Fetch("topic", 0)).To(BeNil())

		pc, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, offsetInfo{2000, "m3ta"}, sarama.OffsetNewest)
		Expect(err).NotTo(HaveOccurred())

		subject.Store("topic", 0, pc)
		Expect(subject.Fetch("topic", 0)).To(Equal(pc))
		Expect(subject.Fetch("topic", 1)).To(BeNil())
		Expect(subject.Fetch("other", 0)).To(BeNil())
	})

	It("should return info", func() {
		pc0, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, offsetInfo{2000, "m3ta"}, sarama.OffsetNewest)
		Expect(err).NotTo(HaveOccurred())
		pc1, err := newPartitionConsumer(&mockConsumer{}, "topic", 1, offsetInfo{2000, "m3ta"}, sarama.OffsetNewest)
		Expect(err).NotTo(HaveOccurred())
		subject.Store("topic", 0, pc0)
		subject.Store("topic", 1, pc1)

		info := subject.Info()
		Expect(info).To(HaveLen(1))
		Expect(info).To(HaveKeyWithValue("topic", []int32{0, 1}))
	})

	It("should create snapshots", func() {
		pc0, err := newPartitionConsumer(&mockConsumer{}, "topic", 0, offsetInfo{2000, "m3ta"}, sarama.OffsetNewest)
		Expect(err).NotTo(HaveOccurred())
		pc1, err := newPartitionConsumer(&mockConsumer{}, "topic", 1, offsetInfo{2000, "m3ta"}, sarama.OffsetNewest)
		Expect(err).NotTo(HaveOccurred())

		subject.Store("topic", 0, pc0)
		subject.Store("topic", 1, pc1)
		subject.Fetch("topic", 1).MarkConsumed(2001)
		subject.Fetch("topic", 1).MarkProcessed(2001, "met@")
		subject.Fetch("topic", 1).MarkConsumed(2002)

		Expect(subject.Snapshot()).To(Equal(map[topicPartition]partitionState{
			topicPartition{"topic", 0}: {offsetInfo{2000, "m3ta"}, 0, false},
			topicPartition{"topic", 1}: {offsetInfo{2001, "met@"}, 2002, true},
		}))
		Expect(subject.Pending()).To(Equal(int64(1)))
	})

})
