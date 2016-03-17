package cluster

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {

	var newConsumer = func(group string) (*Consumer, error) {
		return NewConsumer(testKafkaAddrs, group, testTopics, nil)
	}

	var consume = func(consumerID, group string, max int, out chan *testConsumerMessage) {
		go func() {
			defer GinkgoRecover()

			cs, err := newConsumer(group)
			Expect(err).NotTo(HaveOccurred())
			defer cs.Close()
			cs.consumerID = consumerID

			for msg := range cs.Messages() {
				out <- &testConsumerMessage{*msg, consumerID}
				cs.MarkOffset(msg, "")

				if max--; max == 0 {
					return
				}
			}
		}()
	}

	It("should fail for invalid group id", func() {
		_, err := newConsumer("")
		Expect(err).To(HaveOccurred())

		_, err = newConsumer("foo:bar")
		Expect(err).To(HaveOccurred())
	})

	It("should init and share", func() {
		cs1, err := newConsumer(testGroup)
		Expect(err).NotTo(HaveOccurred())
		defer cs1.Close()

		Eventually(func() map[string][]int32 {
			return cs1.Subscriptions()
		}, "10s", "100ms").Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))

		cs2, err := newConsumer(testGroup)
		Expect(err).NotTo(HaveOccurred())
		defer cs2.Close()

		Eventually(func() map[string][]int32 {
			return cs2.Subscriptions()
		}, "10s", "100ms").Should(HaveLen(2))

		Eventually(func() map[string][]int32 {
			return cs1.Subscriptions()
		}).Should(HaveKeyWithValue("topic-a", HaveLen(2)))
		Eventually(func() map[string][]int32 {
			return cs1.Subscriptions()
		}).Should(HaveKeyWithValue("topic-b", HaveLen(2)))
		Eventually(func() map[string][]int32 {
			return cs2.Subscriptions()
		}).Should(HaveKeyWithValue("topic-a", HaveLen(2)))
		Eventually(func() map[string][]int32 {
			return cs2.Subscriptions()
		}).Should(HaveKeyWithValue("topic-b", HaveLen(2)))
	})

	It("should consume/commit/resume", func() {
		acc := make(chan *testConsumerMessage, 150000)
		consume("A", "fuzzing", 2000, acc)
		consume("B", "fuzzing", 2000, acc)
		consume("C", "fuzzing", 1000, acc)
		consume("D", "fuzzing", 100, acc)
		consume("E", "fuzzing", 200, acc)

		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(BeNumerically(">=", 5000))

		consume("F", "fuzzing", 300, acc)
		consume("G", "fuzzing", 400, acc)
		consume("H", "fuzzing", 1000, acc)
		consume("I", "fuzzing", 2000, acc)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(BeNumerically(">=", 8000))

		consume("J", "fuzzing", 1000, acc)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(BeNumerically(">=", 9000))

		consume("K", "fuzzing", 1000, acc)
		consume("L", "fuzzing", 3000, acc)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(BeNumerically(">=", 12000))

		consume("M", "fuzzing", 1000, acc)
		Expect(testSeed(5000)).NotTo(HaveOccurred())
		Eventually(func() int { return len(acc) }, "30s", "100ms").Should(BeNumerically(">=", 15000))

		close(acc)

		uniques := make(map[string][]string)
		for msg := range acc {
			key := fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
			uniques[key] = append(uniques[key], msg.ConsumerID)
		}
		Expect(uniques).To(HaveLen(15000))
	})

})
