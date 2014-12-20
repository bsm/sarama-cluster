package cluster

import (
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var subject *Consumer
	var client *sarama.Client
	var newConsumer = func() (*Consumer, error) {
		return NewConsumer(client, t_ZK_ADDRS, t_GROUP, t_TOPIC, nil)
	}

	BeforeEach(func() {
		var err error

		client, err = newClient()
		Expect(err).NotTo(HaveOccurred())

		subject, err = newConsumer()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if subject != nil {
			subject.Close()
			subject = nil
		}
		if client != nil {
			client.Close()
			subject = nil
		}
	})

	It("can be created & closed", func() {
		lst, _, err := subject.zoo.Consumers(t_GROUP)
		Expect(err).NotTo(HaveOccurred())
		Expect(lst).To(HaveLen(1))
		Expect(subject.Close()).NotTo(HaveOccurred())
		subject = nil
	})

	It("should claim partitions", func() {
		Eventually(func() []int32 {
			return subject.Claims()
		}, "5s").Should(ConsistOf([]int32{0, 1, 2, 3}))
	})

	It("should notify subscribed listener", func() {
		notifier := &mockNotifier{messages: make([]string, 0)}
		consumer, err := NewConsumer(client, t_ZK_ADDRS, t_GROUP, t_TOPIC, &ConsumerConfig{
			Notifier: notifier,
		})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		Eventually(func() []string {
			return notifier.Messages()
		}, "5s").Should(HaveLen(2))
	})

	It("should release partitions & rebalance when new consumers join", func() {
		Eventually(func() []int32 {
			return subject.Claims()
		}, "5s").Should(ConsistOf([]int32{0, 1, 2, 3}))

		second, err := newConsumer()
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		Eventually(func() []int32 {
			return subject.Claims()
		}, "5s").Should(ConsistOf([]int32{0, 1}))
		Eventually(func() []int32 {
			return second.Claims()
		}, "5s").Should(ConsistOf([]int32{2, 3}))

		third, err := newConsumer()
		Expect(err).NotTo(HaveOccurred())
		defer third.Close()

		Eventually(func() []int32 {
			return subject.Claims()
		}, "5s").Should(ConsistOf([]int32{0}))
		Eventually(func() []int32 {
			return second.Claims()
		}, "5s").Should(ConsistOf([]int32{1, 2}))
		Eventually(func() []int32 {
			return third.Claims()
		}, "5s").Should(ConsistOf([]int32{3}))
	})

	It("should process events", func() {
		res := make(map[int32]int)
		cnt := 0
		for evt := range subject.Events() {
			if cnt++; cnt > 800 {
				break
			}
			res[evt.Partition]++
		}
		Expect(len(res)).To(BeNumerically(">=", 3))
	})

	It("should auto-ack if requested", func() {
		consumer, err := NewConsumer(client, t_ZK_ADDRS, t_GROUP, t_TOPIC, &ConsumerConfig{
			AutoAck: true,
		})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		cnt := 0
		for _ = range consumer.Events() {
			if cnt++; cnt > 10 {
				break
			}
		}
		Eventually(func() map[int32]int64 {
			return consumer.resetAcked()
		}).ShouldNot(BeEmpty())
	})

	It("should auto-commit if requested", func() {
		consumer, err := NewConsumer(client, t_ZK_ADDRS, t_GROUP, t_TOPIC, &ConsumerConfig{
			AutoAck:     true,
			CommitEvery: 10 * time.Millisecond,
		})
		Expect(err).NotTo(HaveOccurred())
		defer consumer.Close()

		cnt := 0
		for _ = range consumer.Events() {
			if cnt++; cnt > 99 {
				break
			}
		}
		Eventually(func() int64 {
			n1, _ := consumer.Offset(0)
			n2, _ := consumer.Offset(1)
			n3, _ := consumer.Offset(2)
			n4, _ := consumer.Offset(3)
			return n1 + n2 + n3 + n4
		}).Should(Equal(int64(100)))
	})

	It("should ack processed events", func() {
		subject.Ack(&sarama.ConsumerEvent{Partition: 1, Offset: 17})
		subject.Ack(&sarama.ConsumerEvent{Partition: 2, Offset: 15})
		Expect(subject.acked).To(Equal(map[int32]int64{1: 17, 2: 15}))

		subject.Ack(&sarama.ConsumerEvent{Partition: 2, Offset: 0})
		Expect(subject.acked).To(Equal(map[int32]int64{1: 17, 2: 15}))
	})

	It("should allow to commit manually/periodically", func() {
		subject.Ack(&sarama.ConsumerEvent{Partition: 1, Offset: 27})
		subject.Ack(&sarama.ConsumerEvent{Partition: 2, Offset: 25})
		Expect(subject.Commit()).NotTo(HaveOccurred())
		Expect(subject.acked).To(Equal(map[int32]int64{}))

		off1, err := subject.Offset(1)
		Expect(err).NotTo(HaveOccurred())
		Expect(off1).To(Equal(int64(28)))

		off2, err := subject.Offset(2)
		Expect(err).NotTo(HaveOccurred())
		Expect(off2).To(Equal(int64(26)))

		off3, err := subject.Offset(3)
		Expect(err).NotTo(HaveOccurred())
		Expect(off3).To(Equal(int64(0)))
	})

	It("should auto-commit on close/rebalance", func() {
		subject.Ack(&sarama.ConsumerEvent{Partition: 1, Offset: 37})
		subject.Ack(&sarama.ConsumerEvent{Partition: 2, Offset: 35})
		Expect(subject.acked).To(HaveLen(2))

		second, err := newConsumer()
		Expect(err).NotTo(HaveOccurred())
		defer second.Close()

		Eventually(func() []int32 {
			return subject.Claims()
		}, "10s").Should(HaveLen(2))
		Expect(subject.acked).To(BeEmpty())

		off1, err := subject.Offset(1)
		Expect(err).NotTo(HaveOccurred())
		Expect(off1).To(Equal(int64(38)))

		off2, err := subject.Offset(2)
		Expect(err).NotTo(HaveOccurred())
		Expect(off2).To(Equal(int64(36)))
	})
})
