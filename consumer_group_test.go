package cluster

import (
	"errors"
	"os"
	"sort"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var mockError = errors.New("mock: error")

var _ = Describe("ConsumerGroup", func() {

	It("should determine which partitions to claim", func() {
		testCases := []struct {
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

		for _, item := range testCases {
			// Prepare
			group := &ConsumerGroup{id: item.id}
			parts := make(PartitionSlice, len(item.pids))
			for i, pid := range item.pids {
				parts[i] = Partition{ID: pid, Addr: "locahost:9092"}
			}

			// Pseudo-shuffle
			sort.StringSlice(item.cids).Swap(0, len(item.cids)-1)
			parts.Swap(0, len(parts)-1)

			// Claim and compare
			parts = group.claimRange(item.cids, parts)
			actual := make([]int32, len(parts))
			for i, part := range parts {
				actual[i] = part.ID
			}
			Expect(actual).To(Equal(item.exp), "Case: %+v", item)
		}
	})

	Describe("instances", func() {
		var subject *ConsumerGroup
		var client *sarama.Client
		var zk *ZK

		BeforeEach(func() {
			var err error

			client, err = newClient()
			Expect(err).NotTo(HaveOccurred())
			zk, err = NewZK([]string{"localhost:22181"}, 1e9)
			Expect(err).NotTo(HaveOccurred())
			subject, err = newCG(client, zk)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			if subject != nil {
				subject.Close()
				subject = nil
			}
			if zk != nil {
				zk.DeleteAll("/")
				zk.Close()
				zk = nil
			}
			if client != nil {
				client.Close()
				subject = nil
			}
		})

		It("can be created & closed", func() {
			lst, _, err := zk.Consumers(t_GROUP)
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

			other, err := newCG(client, zk)
			Expect(err).NotTo(HaveOccurred())
			defer other.Close()

			Eventually(func() []int32 {
				return subject.Claims()
			}, "5s").Should(Equal([]int32{0, 1, 2, 3, 4, 5}))

			Eventually(func() []int32 {
				return other.Claims()
			}, "5s").Should(Equal([]int32{6, 7, 8, 9, 10, 11}))

			third, err := newCG(client, zk)
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

		It("should rewind checkout if requested", func() {
			was, _ := subject.Offset(0)
			err := subject.Process(func(b *EventBatch) error {
				return RollbackCheckout
			})
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

	})

	Describe("fuzzing", func() {
		if os.Getenv("FUZZ") == "" {

			PIt("tests are disabled, please run with FUZZ=1")

		} else {

			It("should consume uniquely across all consumers within a group", func() {
				errors := make(chan error, 100)
				events := make(chan *fuzzingEvent, 1e6)

				go fuzzingTest("A", 3000, errors, events)
				go fuzzingTest("B", 100, errors, events)
				Eventually(func() int { return len(events) }, "20s").Should(BeNumerically(">", 500))

				go fuzzingTest("C", 600, errors, events)
				go fuzzingTest("D", 700, errors, events)
				go fuzzingTest("E", 800, errors, events)
				go fuzzingTest("F", 900, errors, events)
				go fuzzingTest("G", 1000, errors, events)
				go fuzzingTest("H", 1200, errors, events)
				go fuzzingTest("I", 1400, errors, events)
				go fuzzingTest("J", 2000, errors, events)
				Eventually(func() int { return len(events) }, "20s").Should(BeNumerically(">", 1000))

				go fuzzingTest("K", 9000, errors, events)
				go fuzzingTest("L", 3000, errors, events)
				go fuzzingTest("M", 6000, errors, events)
				go fuzzingTest("N", 5000, errors, events)
				go fuzzingTest("O", 10000, errors, events)
				Eventually(func() int { return len(events) }, "40s").Should(BeNumerically(">", 5000))

				go fuzzingTest("P", 10000, errors, events)
				Eventually(func() int { return len(events) }, "60s").Should(BeNumerically(">=", 10000))
				Eventually(func() int { return len(errors) }, "10s").Should(Equal(16))

				for len(errors) > 0 {
					Expect(<-errors).NotTo(HaveOccurred())
				}

				byID := make(map[int][]fuzzingEvent, len(events))
				for len(events) > 0 {
					evt := <-events
					eid := evt.ID()
					byID[eid] = append(byID[eid], *evt)
				}

				// Ensure each event was consumed only once
				for _, evts := range byID {
					Expect(evts).To(HaveLen(1))
				}

				// Ensure we have consumed 10k events
				Expect(len(byID)).To(Equal(10000))
			})
		}

	})
})

/*******************************************************************
 * TEST HELPERS
 *******************************************************************/

func newCG(client *sarama.Client, zk *ZK) (*ConsumerGroup, error) {
	return NewConsumerGroup(client, zk, t_GROUP, t_TOPIC, testState.notifier, testConsumerConfig())
}

type fuzzingEvent struct {
	Origin            string
	Partition, Offset int
}

func (e *fuzzingEvent) ID() int {
	return e.Partition*1e6 + e.Offset
}

func fuzzingTest(origin string, n int, errors chan error, events chan *fuzzingEvent) {
	client, err := newClient()
	if err != nil {
		errors <- err
		return
	}
	defer client.Close()

	zk, err := NewZK([]string{"localhost:22181"}, 1e9)
	if err != nil {
		errors <- err
		return
	}
	defer zk.Close()

	// logger := log.New(os.Stdout, "["+origin+"] ", 0)
	// notifier := LogNotifier{logger}
	group, err := NewConsumerGroup(client, zk, "sarama-cluster-fuzzing-test", t_TOPIC, nil, testConsumerConfig())
	if err != nil {
		errors <- err
		return
	}
	defer group.Close()

	for len(events) < n {
		err := group.Process(func(b *EventBatch) error {
			for _, evt := range b.Events {
				if evt.Err != nil {
					return evt.Err
				}
				events <- &fuzzingEvent{origin, int(b.Partition), int(evt.Offset)}
			}
			return nil
		})
		if err != nil && err != NoCheckout {
			errors <- err
			return
		}
	}
	errors <- nil
}
