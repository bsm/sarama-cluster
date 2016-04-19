package cluster

import (
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {

	var newConsumer = func(group string) (*Consumer, error) {
		config := NewConfig()
		config.Consumer.Return.Errors = true
		return NewConsumer(testKafkaAddrs, group, testTopics, config)
	}

	var newConsumerOf = func(group, topic string) (*Consumer, error) {
		config := NewConfig()
		config.Consumer.Return.Errors = true
		return NewConsumer(testKafkaAddrs, group, []string{topic}, config)
	}

	var subscriptionsOf = func(c *Consumer) GomegaAsyncAssertion {
		return Eventually(func() map[string][]int32 {
			return c.Subscriptions()
		}, "10s", "100ms")
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

	It("should init and share", func() {
		cs1, err := newConsumer(testGroup)
		Expect(err).NotTo(HaveOccurred())
		defer cs1.Close()

		subscriptionsOf(cs1).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))

		cs2, err := newConsumer(testGroup)
		Expect(err).NotTo(HaveOccurred())
		defer cs2.Close()

		subscriptionsOf(cs1).Should(HaveLen(2))
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-a", HaveLen(2)))
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-b", HaveLen(2)))

		subscriptionsOf(cs2).Should(HaveLen(2))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-a", HaveLen(2)))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-b", HaveLen(2)))
	})

	It("should allow more consumers than partitions", func() {
		cs1, err := newConsumerOf(testGroup, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs1.Close()
		cs2, err := newConsumerOf(testGroup, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs2.Close()
		cs3, err := newConsumerOf(testGroup, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs3.Close()
		cs4, err := newConsumerOf(testGroup, "topic-a")
		Expect(err).NotTo(HaveOccurred())

		// start 4 consumers, one for each partition
		subscriptionsOf(cs1).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		subscriptionsOf(cs2).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		subscriptionsOf(cs3).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		subscriptionsOf(cs4).Should(HaveKeyWithValue("topic-a", HaveLen(1)))

		// add a 5th consumer
		cs5, err := newConsumerOf(testGroup, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer cs5.Close()

		// wait for rebalance, make sure no errors occurred
		Eventually(func() bool { return atomic.LoadInt32(&cs5.consuming) == 1 }, "10s", "100ms").Should(BeTrue())
		time.Sleep(time.Second)
		Expect(cs1.Errors()).ShouldNot(Receive())
		Expect(cs2.Errors()).ShouldNot(Receive())
		Expect(cs3.Errors()).ShouldNot(Receive())
		Expect(cs4.Errors()).ShouldNot(Receive())
		Expect(cs5.Errors()).ShouldNot(Receive())

		// close 4th, make sure the 5th takes over
		cs4.Close()
		Eventually(func() bool { return atomic.LoadInt32(&cs4.consuming) == 1 }, "10s", "100ms").Should(BeFalse())
		subscriptionsOf(cs4).Should(BeEmpty())
		subscriptionsOf(cs5).Should(HaveKeyWithValue("topic-a", HaveLen(1)))

		// there should still be no errors
		Expect(cs1.Errors()).ShouldNot(Receive())
		Expect(cs2.Errors()).ShouldNot(Receive())
		Expect(cs3.Errors()).ShouldNot(Receive())
		Expect(cs4.Errors()).ShouldNot(Receive())
		Expect(cs5.Errors()).ShouldNot(Receive())
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
