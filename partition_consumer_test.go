package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionConsumer", func() {
	var client *sarama.Client
	var subject *PartitionConsumer

	BeforeEach(func() {
		var err error

		client, err = newClient()
		Expect(err).NotTo(HaveOccurred())
		subject, err = NewPartitionConsumer(client, testConsumerConfig(), t_TOPIC, t_GROUP, 3, 0)
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

	nextBatch := func() *EventBatch {
		Eventually(func() int { return len(subject.stream.Events()) }).Should(BeNumerically(">", 10))
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
		return batch
	}

	It("should fetch batches of events", func() {
		batch := nextBatch()
		Expect(batch.Topic).To(Equal(t_TOPIC))
		Expect(batch.Partition).To(Equal(int32(3)))
		Expect(len(batch.Events)).To(BeNumerically(">", 10))
	})

	It("should rollback to given offset", func() {
		// Consume something to get real offset
		nextBatch()

		// Consume the batch to be rolled back to
		was := subject.Offset()
		Expect(was).To(BeNumerically(">", 0))
		b2 := nextBatch()
		Expect(b2.Events[0].Offset).To(Equal(was + 1))

		// Rollback
		subject.Rollback(was)
		Expect(subject.Offset()).To(Equal(was))

		// Confirm consumption starts from previous offset
		b3 := nextBatch()
		Expect(b3.Events[0].Offset).To(Equal(was))
	})

	It("should close consumer", func() {
		Expect(subject.Close()).To(BeNil())
		subject = nil
	})

})
