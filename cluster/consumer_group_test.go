package cluster

import (
	"errors"
	"sort"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var mockError = errors.New("mock: error")

var _ = Describe("ConsumerGroup", func() {
	var testClaimRangeCases = []struct {
		id   string
		cids []string
		pids []int32
		exp  []int32
	}{
		{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{0}},
		{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{1}},
		{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{2}},

		{"N3", []string{"N1", "N2", "N3", "N4"}, []int32{0, 3, 2, 1}, []int32{2}},
		{"N3", []string{"N1", "N2", "N3", "N4"}, []int32{1, 3, 5, 7}, []int32{5}},

		{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{0}},
		{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{}},
		{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{1}},

		{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{0}},
		{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{1, 2}},
		{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{3}},

		{"N1", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{0, 2}},
		{"N2", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{4}},
		{"N3", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{6, 8}},

		{"N1", []string{"N1", "N2"}, []int32{0, 1, 2, 3, 4}, []int32{0, 1, 2}},
		{"N2", []string{"N1", "N2"}, []int32{0, 1, 2, 3, 4}, []int32{3, 4}},

		{"N1", []string{"N1", "N2", "N3"}, []int32{0}, []int32{}},
		{"N2", []string{"N1", "N2", "N3"}, []int32{0}, []int32{0}},
		{"N3", []string{"N1", "N2", "N3"}, []int32{0}, []int32{}},

		{"N1", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{0}},
		{"N2", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{1}},
		{"N3", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{}},
		{"N4", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{2}},
		{"N5", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{3}},

		{"N1", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{0, 1}},
		{"N2", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{2, 3, 4}},
		{"N3", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{5, 6}},
		{"N4", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{7, 8, 9}},
		{"N5", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{10, 11}},

		{"N9", []string{"N1", "N2"}, []int32{0, 1}, []int32{}},
	}

	Describe("instances", func() {
		var subject *ConsumerGroup

		var newCG = func() (*ConsumerGroup, error) {
			return NewConsumerGroup(testState.client, testState.zk, t_GROUP, t_TOPIC, testState.notifier, testConsumerConfig())
		}

		var runConsumerCycle = func(errors chan error, events chan int64, n int) {
			group, err := newCG()
			if err != nil {
				errors <- err
				return
			}
			defer group.Close()

			for i := 0; i < n; i++ {
				group.Process(func(b *EventBatch) error {
					for _, e := range b.Events {
						events <- int64(b.Partition)*1e6 + e.Offset
					}
					return nil
				})
			}
			errors <- nil
		}

		BeforeEach(func() {
			var err error
			subject, err = newCG()
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			if subject != nil {
				subject.Close()
				subject = nil
			}
		})

		It("can be created & closed", func() {
			lst, _, err := testState.zk.Consumers(t_GROUP)
			Expect(err).NotTo(HaveOccurred())
			Expect(lst).To(HaveLen(1))
			Expect(subject.Close()).NotTo(HaveOccurred())
			subject = nil
		})

		It("should claim partitions", func() {
			Eventually(func() []int32 {
				return subject.Claims()
			}, "5s").Should(Equal([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}))
		})

		It("should notify subscribed listener", func() {
			Eventually(func() []string {
				return testState.notifier.messages
			}, "5s").Should(HaveLen(2))
		})

		It("should release partitions & rebalance when new consumers join", func() {
			Eventually(func() []int32 {
				return subject.Claims()
			}, "5s").Should(Equal([]int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}))

			other, err := newCG()
			Expect(err).NotTo(HaveOccurred())
			defer other.Close()

			Eventually(func() []int32 {
				return subject.Claims()
			}, "5s").Should(Equal([]int32{0, 1, 2, 3, 4, 5}))

			Eventually(func() []int32 {
				return other.Claims()
			}, "5s").Should(Equal([]int32{6, 7, 8, 9, 10, 11}))

			third, err := newCG()
			Expect(err).NotTo(HaveOccurred())
			defer third.Close()

			Eventually(func() []int32 {
				return subject.Claims()
			}, "5s").Should(Equal([]int32{0, 1, 2, 3}))

			Eventually(func() []int32 {
				return other.Claims()
			}, "5s").Should(Equal([]int32{4, 5, 6, 7}))

			Eventually(func() []int32 {
				return third.Claims()
			}, "5s").Should(Equal([]int32{8, 9, 10, 11}))
		})

		It("should checkout individual consumers", func() {
			partition := int32(-1)
			p0, _ := subject.Offset(0)
			p1, _ := subject.Offset(1)

			err := subject.Checkout(func(c *PartitionConsumer) error { partition = c.partition; return DiscardCommit })
			Expect(err).NotTo(HaveOccurred())
			Expect(partition).To(Equal(int32(0)))
			num, _ := subject.Offset(0)
			Expect(num).To(Equal(p0))

			err = subject.Checkout(func(c *PartitionConsumer) error { partition = c.partition; return DiscardCommit })
			Expect(err).NotTo(HaveOccurred())
			Expect(partition).To(Equal(int32(1)))
			num, _ = subject.Offset(1)
			Expect(num).To(Equal(p1))
		})

		It("should process batches, and commit offsets", func() {
			was, _ := subject.Offset(0)

			var batch *EventBatch
			err := subject.Process(func(b *EventBatch) error { batch = b; return nil })
			Expect(err).NotTo(HaveOccurred())

			Expect(batch).NotTo(BeNil())
			Expect(batch.Events).To(HaveLen(16))

			now, _ := subject.Offset(0)
			Expect(now).To(Equal(was + 16))
		})

		It("should skip commits if requested", func() {
			was, _ := subject.Offset(0)
			err := subject.Process(func(b *EventBatch) error { return DiscardCommit })
			Expect(err).NotTo(HaveOccurred())

			now, _ := subject.Offset(0)
			Expect(now).To(Equal(was))
		})

		It("should propagate errors", func() {
			err := subject.Checkout(func(c *PartitionConsumer) error { return mockError })
			Expect(err).To(Equal(mockError))

			err = subject.Process(func(b *EventBatch) error { return mockError })
			Expect(err).To(Equal(mockError))
		})

		It("should consume uniquely across all consumers within a group", func() {
			errors := make(chan error, 5)
			events := make(chan int64, 1e5)
			go runConsumerCycle(errors, events, 24)
			go runConsumerCycle(errors, events, 96)
			go runConsumerCycle(errors, events, 2)
			go runConsumerCycle(errors, events, 48)
			go runConsumerCycle(errors, events, 24)

			for i := 0; i < 5; i++ {
				Expect(<-errors).NotTo(HaveOccurred())
			}
			consumed := make(map[int64]bool)
			for i := 0; i < len(events); i++ {
				evt := <-events
				Expect(consumed[evt]).To(BeFalse())
				consumed[evt] = true
			}
		})

	})

	It("should determine which partitions to claim", func() {
		for _, item := range testClaimRangeCases {
			group := &ConsumerGroup{id: item.id}
			parts := make(PartitionSlice, len(item.pids))
			for i, pid := range item.pids {
				parts[i] = Partition{ID: pid, Addr: "locahost:9092"}
			}
			sort.StringSlice(item.cids).Swap(0, len(item.cids)-1)
			parts.Swap(0, len(parts)-1)

			parts = group.claimRange(item.cids, parts)
			actual := make([]int32, len(parts))
			for i, part := range parts {
				actual[i] = part.ID
			}

			Expect(actual).To(Equal(item.exp), "Case: %+v", item)
		}
	})

})
