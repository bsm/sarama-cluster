package cluster

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var subject *Consumer

	BeforeEach(func() {
		var err error

		subject, err = newConsumer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if subject != nil {
			subject.Close()
			subject = nil
		}
	})

	It("should normalize config", func() {
		config := &Config{DefaultOffsetMode: sarama.OffsetNewest}
		config.normalize()
		Expect(config.DefaultOffsetMode).To(Equal(sarama.OffsetNewest))
		config = &Config{DefaultOffsetMode: sarama.OffsetOldest}
		config.normalize()
		Expect(config.DefaultOffsetMode).To(Equal(sarama.OffsetOldest))
		config = &Config{DefaultOffsetMode: -100, CommitEvery: time.Millisecond}
		config.normalize()
		Expect(config.DefaultOffsetMode).To(Equal(sarama.OffsetOldest))
		Expect(config.Notifier).ToNot(BeNil())
		Expect(config.CommitEvery).To(Equal(time.Duration(0)))
		Expect(config.ZKSessionTimeout).To(Equal(time.Second))
	})

	It("can be created & closed", func() {
		lst, _, err := subject.zoo.Consumers(tGroup)
		Expect(err).NotTo(HaveOccurred())
		Expect(lst).To(HaveLen(1))
		Expect(subject.Close()).NotTo(HaveOccurred())
		subject = nil
	})

	It("should claim partitions", func() {
		Eventually(func() []string { return subject.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-0",
			"sarama-cluster-topic-a-1",
			"sarama-cluster-topic-a-2",
			"sarama-cluster-topic-a-3",
			"sarama-cluster-topic-b-0",
			"sarama-cluster-topic-b-1",
			"sarama-cluster-topic-b-2",
			"sarama-cluster-topic-b-3",
			"sarama-cluster-topic-b-4",
			"sarama-cluster-topic-b-5",
		}))
	})

	It("should notify subscribed listener", func() {
		notifier := &mockNotifier{messages: make([]string, 0)}
		consumer, err := newConsumer(nil, &Config{Notifier: notifier})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		Eventually(func() []string {
			return notifier.Messages()
		}, "5s").Should(HaveLen(2))
	})

	It("should release partitions & rebalance when new consumers join", func() {
		Eventually(func() []string { return subject.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-0",
			"sarama-cluster-topic-a-1",
			"sarama-cluster-topic-a-2",
			"sarama-cluster-topic-a-3",
			"sarama-cluster-topic-b-0",
			"sarama-cluster-topic-b-1",
			"sarama-cluster-topic-b-2",
			"sarama-cluster-topic-b-3",
			"sarama-cluster-topic-b-4",
			"sarama-cluster-topic-b-5",
		}))

		second, err := newConsumer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		Eventually(func() []string { return subject.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-0",
			"sarama-cluster-topic-a-1",
			"sarama-cluster-topic-b-0",
			"sarama-cluster-topic-b-1",
			"sarama-cluster-topic-b-2",
		}))
		Eventually(func() []string { return second.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-2",
			"sarama-cluster-topic-a-3",
			"sarama-cluster-topic-b-3",
			"sarama-cluster-topic-b-4",
			"sarama-cluster-topic-b-5",
		}))

		third, err := newConsumer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		defer third.Close()

		Eventually(func() []string { return subject.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-0",
			"sarama-cluster-topic-b-0",
			"sarama-cluster-topic-b-1",
		}))
		Eventually(func() []string { return second.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-1",
			"sarama-cluster-topic-a-2",
			"sarama-cluster-topic-b-2",
			"sarama-cluster-topic-b-3",
		}))
		Eventually(func() []string { return third.Claims() }, "5s").Should(Equal([]string{
			"sarama-cluster-topic-a-3",
			"sarama-cluster-topic-b-4",
			"sarama-cluster-topic-b-5",
		}))
	})

	It("should process messages", func() {
		res := make(map[topicPartition]int)
		cnt := 0
		for evt := range subject.Messages() {
			if cnt++; cnt > 5000 {
				break
			}
			res[topicPartition{evt.Topic, evt.Partition}]++
		}
		Expect(len(res)).To(BeNumerically(">=", 6))
	})

	It("should auto-ack if requested", func() {
		consumer, err := newConsumer(nil, &Config{AutoAck: true})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		cnt := 0
		for _ = range consumer.Messages() {
			if cnt++; cnt > 10 {
				break
			}
		}
		Eventually(func() map[topicPartition]int64 {
			return consumer.resetAcked()
		}).ShouldNot(BeEmpty())
	})

	It("should use default offset if requested", func() {
		Expect(subject.offset(topicPartition{tTopicA, 1})).To(Equal(sarama.OffsetOldest))

		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 1, Offset: 17})
		Expect(subject.Commit()).NotTo(HaveOccurred())
		Expect(subject.offset(topicPartition{tTopicA, 1})).To(Equal(int64(18)))
	})

	It("should auto-commit if requested", func() {
		consumer, err := newConsumer(nil, &Config{AutoAck: true, CommitEvery: 10 * time.Millisecond})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		cnt := 0
		for _ = range consumer.Messages() {
			if cnt++; cnt > 99 {
				break
			}
		}
		Eventually(func() int64 {
			n1, _ := consumer.Offset(tTopicA, 0)
			n2, _ := consumer.Offset(tTopicA, 1)
			n3, _ := consumer.Offset(tTopicA, 2)
			n4, _ := consumer.Offset(tTopicA, 3)
			return n1 + n2 + n3 + n4
		}).Should(Equal(int64(100)))
	})

	It("should ack processed messages", func() {
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 1, Offset: 17})
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 2, Offset: 15})
		Expect(subject.Commit()).NotTo(HaveOccurred())

		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 2, Offset: 0})
		Expect(subject.Commit()).NotTo(HaveOccurred())

		off1, err := subject.Offset(tTopicA, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(off1).To(Equal(int64(18)))

		off2, err := subject.Offset(tTopicA, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(off2).To(Equal(int64(16)))
	})

	It("should allow to commit manually/periodically", func() {
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 1, Offset: 27})
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 2, Offset: 25})
		Expect(subject.Commit()).NotTo(HaveOccurred())
		Expect(subject.Commit()).NotTo(HaveOccurred())

		off1, err := subject.Offset(tTopicA, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(off1).To(Equal(int64(28)))

		off2, err := subject.Offset(tTopicA, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(off2).To(Equal(int64(26)))

		off3, err := subject.Offset(tTopicA, 3)
		Expect(err).NotTo(HaveOccurred())
		Expect(off3).To(Equal(int64(0)))
	})

	It("should auto-commit on close/rebalance", func() {
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 1, Offset: 37})
		subject.Ack(&sarama.ConsumerMessage{Topic: tTopicA, Partition: 2, Offset: 35})

		second, err := newConsumer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		Eventually(func() []string {
			return subject.Claims()
		}, "10s").Should(HaveLen(5))

		off1, err := subject.Offset(tTopicA, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(off1).To(Equal(int64(38)))

		off2, err := subject.Offset(tTopicA, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(off2).To(Equal(int64(36)))
	})

	It("should gracefully recover from offset-out-range errors", func() {
		if testing.Short() {
			return
		}

		Eventually(func() []string {
			entries, _ := filepath.Glob("/tmp/sarama-cluster-test/kafka/sarama-cluster-topic-x-0/*.deleted")
			return entries
		}, "30s", "1s").ShouldNot(BeEmpty())

		truncated, err := NewConsumer(tKafkaAddrs, tZKAddrs, tGroupX, []string{tTopicX}, nil)
		Expect(err).NotTo(HaveOccurred())
		defer truncated.Close()

		var msg *sarama.ConsumerMessage
		Eventually(truncated.Messages(), "5s").Should(Receive(&msg))
		Expect(msg.Offset).To(BeNumerically(">", 0))
	})

})
