package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fuzzing", func() {

	var fuzz = func(id string, n int, errors chan error, messages chan *sarama.ConsumerMessage) {
		consumer, err := newConsumer(&Config{customID: id})
		if err != nil {
			errors <- err
			return
		}
		defer consumer.Close()

		go func() {
			for msg := range consumer.Errors() {
				errors <- msg.Err
			}
		}()

		cnt := 0
		for msg := range consumer.Messages() {
			msg.Key = []byte("PROCESSED BY " + id)
			messages <- msg
			consumer.Ack(msg)
			if cnt++; cnt >= n {
				errors <- nil
				return
			}
		}
	}

	It("should consume uniquely across all consumers within a group", func() {
		units := tN / 100
		errors := make(chan error, 100)
		messages := make(chan *sarama.ConsumerMessage, tN+100)

		go fuzz("A", 2*units, errors, messages)
		go fuzz("B", 3*units, errors, messages)
		Eventually(func() int { return len(messages) }, "10s").Should(BeNumerically(">=", 5*units))

		go fuzz("C", 22*units, errors, messages)
		go fuzz("D", 10*units, errors, messages)
		go fuzz("E", 1*units, errors, messages)
		go fuzz("F", 12*units, errors, messages)
		go fuzz("G", 30*units, errors, messages)
		go fuzz("H", 2*units, errors, messages)
		go fuzz("I", 3*units, errors, messages)
		go fuzz("J", 1*units, errors, messages)
		go fuzz("K", 1*units, errors, messages)
		go fuzz("L", 1*units, errors, messages)
		go fuzz("M", 2*units, errors, messages)
		go fuzz("N", 10*units, errors, messages)
		Eventually(func() int { return len(messages) }, "30s").Should(BeNumerically(">=", 100*units))
		Eventually(func() int { return len(errors) }, "10s").Should(Equal(14))

		for len(errors) > 0 {
			Expect(<-errors).NotTo(HaveOccurred())
		}

		byID := make(map[int64][]*sarama.ConsumerMessage, len(messages))
		for len(messages) > 0 {
			msg := <-messages
			uid := int64(msg.Partition)*1e9 + msg.Offset
			byID[uid] = append(byID[uid], msg)
		}

		// Ensure each event was consumed only once
		for _, msgs := range byID {
			Expect(msgs).To(HaveLen(1))
		}
	})

})
