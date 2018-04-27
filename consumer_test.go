package cluster_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {

	newConsumerProcess := func(clientID, groupID string, topics []string, handler cluster.Handler) (cluster.Consumer, error) {
		config := sarama.NewConfig()
		config.ClientID = clientID
		config.Version = sarama.V1_0_0_0
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		return cluster.NewConsumer(testKafkaBrokers, groupID, topics, config, handler)
	}

	newConsumer := func(clientID, groupID string, topics ...string) (cluster.Consumer, error) {
		return newConsumerProcess(clientID, groupID, topics, cluster.HandlerFunc(func(pc cluster.PartitionConsumer) error {
			<-pc.Done()
			return nil
		}))
	}

	claimsOf := func(c cluster.Consumer) GomegaAsyncAssertion {
		return Eventually(func() map[string][]int32 {
			select {
			case claim := <-c.Claims():
				if claim != nil {
					return claim.Topics
				}
			default:
			}
			return nil
		}, "5s", "50ms")
	}

	It("should init and share", func() {
		groupID := newTestConsumerGroupID()

		// start M1
		m1, err := newConsumer("M1", groupID, testTopics...)
		Expect(err).NotTo(HaveOccurred())
		defer m1.Close()

		// M1 should consume all 8 partitions
		claimsOf(m1).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))

		// start M2
		m2, err := newConsumer("M2", groupID, testTopics...)
		Expect(err).NotTo(HaveOccurred())
		defer m2.Close()

		// M1 and M2 should consume 4 partitions each
		claimsOf(m1).Should(SatisfyAll(
			HaveLen(2),
			HaveKeyWithValue("topic-a", HaveLen(2)),
			HaveKeyWithValue("topic-b", HaveLen(2)),
		))
		claimsOf(m2).Should(SatisfyAll(
			HaveLen(2),
			HaveKeyWithValue("topic-a", HaveLen(2)),
			HaveKeyWithValue("topic-b", HaveLen(2)),
		))

		// Check that no errors were happening
		Expect(m1.Errors()).ShouldNot(Receive())
		Expect(m2.Errors()).ShouldNot(Receive())

		// shutdown M1, now M2 should consume all 8 partitions
		Expect(m1.Close()).To(Succeed())
		claimsOf(m2).Should(Equal(map[string][]int32{
			"topic-a": {0, 1, 2, 3},
			"topic-b": {0, 1, 2, 3},
		}))

		// shutdown M2
		Expect(m2.Close()).To(Succeed())
	})

	It("should allow more consumers than partitions", func() {
		groupID := newTestConsumerGroupID()

		m1, err := newConsumer("M1", groupID, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer m1.Close()

		m2, err := newConsumer("M2", groupID, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer m2.Close()

		m3, err := newConsumer("M3", groupID, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer m3.Close()

		m4, err := newConsumer("M4", groupID, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer m4.Close()

		// start 4 consumers, one for each partition
		claimsOf(m1).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m2).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m3).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m4).Should(HaveKeyWithValue("topic-a", HaveLen(1)))

		// add a 5th consumer
		m5, err := newConsumer("M5", groupID, "topic-a")
		Expect(err).NotTo(HaveOccurred())
		defer m5.Close()

		// make sure no errors have occurred
		Expect(m1.Errors()).ShouldNot(Receive())
		Expect(m2.Errors()).ShouldNot(Receive())
		Expect(m3.Errors()).ShouldNot(Receive())
		Expect(m4.Errors()).ShouldNot(Receive())
		Expect(m5.Errors()).ShouldNot(Receive())

		// close 4th, make sure the 5th takes over
		Expect(m4.Close()).To(Succeed())
		claimsOf(m1).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m2).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m3).Should(HaveKeyWithValue("topic-a", HaveLen(1)))
		claimsOf(m4).Should(BeEmpty())
		claimsOf(m5).Should(HaveKeyWithValue("topic-a", HaveLen(1)))

		// there should still be no errors
		Expect(m1.Close()).To(Succeed())
		Expect(m2.Close()).To(Succeed())
		Expect(m3.Close()).To(Succeed())
		Expect(m4.Close()).To(Succeed())
		Expect(m5.Close()).To(Succeed())
	})

	It("should allow change topics", func() {
		groupID := newTestConsumerGroupID()

		m1, err := newConsumer("M1", groupID, testTopics...)
		Expect(err).NotTo(HaveOccurred())
		defer m1.Close()

		m2, err := newConsumer("M2", groupID, testTopics...)
		Expect(err).NotTo(HaveOccurred())
		defer m2.Close()

		// M1 and M2 should consume 4 partitions each
		claimsOf(m1).Should(SatisfyAll(
			HaveLen(2),
			HaveKeyWithValue("topic-a", HaveLen(2)),
			HaveKeyWithValue("topic-b", HaveLen(2)),
		))
		claimsOf(m2).Should(SatisfyAll(
			HaveLen(2),
			HaveKeyWithValue("topic-a", HaveLen(2)),
			HaveKeyWithValue("topic-b", HaveLen(2)),
		))

		// Claims should be redistributed after topic change
		m1.SetTopics("topic-a")
		claimsOf(m1).Should(SatisfyAll(
			HaveLen(1),
			HaveKeyWithValue("topic-a", HaveLen(2)),
		))
		claimsOf(m2).Should(SatisfyAll(
			HaveLen(2),
			HaveKeyWithValue("topic-a", HaveLen(2)),
			HaveKeyWithValue("topic-b", HaveLen(4)),
		))

		// should not error
		Expect(m1.Close()).To(Succeed())
		Expect(m2.Close()).To(Succeed())
	})

	It("should allow close to be called multiple times", func() {
		groupID := newTestConsumerGroupID()

		m1, err := newConsumer("M1", groupID, testTopics...)
		Expect(err).NotTo(HaveOccurred())
		Expect(m1.Close()).To(Succeed())
		Expect(m1.Close()).To(Succeed())
	})

	It("should consume/commit/resume", func() {
		acc := make(chan *testConsumerMessage, 20000)
		accMu := sync.Mutex{}
		accLen := func() int {
			accMu.Lock()
			n := len(acc)
			accMu.Unlock()
			return n
		}
		groupID := newTestConsumerGroupID()
		consume := func(clientID string, max int32) {
			defer GinkgoRecover()

			member, err := newConsumerProcess(clientID, groupID, testTopics, cluster.HandlerFunc(func(pc cluster.PartitionConsumer) error {
				for msg := range pc.Messages() {
					if atomic.AddInt32(&max, -1) < 0 {
						break
					}

					if msg.Offset%50 == 0 {
						time.Sleep(100 * time.Millisecond)
					}

					accMu.Lock()
					acc <- &testConsumerMessage{*msg, clientID}
					accMu.Unlock()
					pc.MarkMessage(msg, "")
				}
				return nil
			}))
			Expect(err).NotTo(HaveOccurred())

			// wait for `max` messages to be consumed
			ticker := time.NewTicker(10 * time.Millisecond)
			for range ticker.C {
				if atomic.LoadInt32(&max) <= 0 {
					break
				}
			}
			ticker.Stop()

			// close consumer
			Expect(member.Close()).To(Succeed())
		}

		go consume("M01", 1500)
		go consume("M02", 3000)
		go consume("M03", 1500)
		go consume("M04", 200)
		go consume("M05", 100)
		Eventually(accLen, "30s", "50ms").Should(BeNumerically(">=", 3000))

		go consume("M06", 300)
		go consume("M07", 400)
		go consume("M08", 500)
		go consume("M09", 2000)
		Eventually(accLen, "30s", "50ms").Should(BeNumerically(">=", 8000))

		go consume("M10", 1000)
		Eventually(accLen, "30s", "50ms").Should(BeNumerically(">=", 10000))

		go consume("M11", 1000)
		go consume("M12", 2500)
		Eventually(accLen, "30s", "50ms").Should(BeNumerically(">=", 12000))

		go consume("M13", 1000)
		Eventually(accLen, "30s", "50ms").Should(BeNumerically(">=", 15000))

		accMu.Lock()
		close(acc)
		accMu.Unlock()

		uniques := make(map[string][]string)
		for msg := range acc {
			key := fmt.Sprintf("%s-%d:%d", msg.Topic, msg.Partition, msg.Offset)
			uniques[key] = append(uniques[key], msg.ClientID)
		}
		Expect(uniques).To(HaveLen(15000))
	})

})
