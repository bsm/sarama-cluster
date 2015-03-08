package cluster

import (
	"runtime"

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

	var numGoroutine int

	BeforeEach(func() {
		numGoroutine = runtime.NumGoroutine()
	})

	AfterEach(func() {
		Expect(runtime.NumGoroutine()).To(BeNumerically("~", numGoroutine, 15))
	})

	It("should consume uniquely across all consumers within a group", func() {
		errors := make(chan error, 100)
		messages := make(chan *sarama.ConsumerMessage, 1e6)

		go fuzz("A", 200, errors, messages)
		go fuzz("B", 300, errors, messages)
		Eventually(func() int { return len(messages) }, "10s").Should(BeNumerically(">=", 500))

		go fuzz("C", 2700, errors, messages)
		go fuzz("D", 1000, errors, messages)
		go fuzz("E", 100, errors, messages)
		go fuzz("F", 1200, errors, messages)
		go fuzz("G", 4000, errors, messages)
		go fuzz("H", 200, errors, messages)
		go fuzz("I", 300, errors, messages)
		Eventually(func() int { return len(messages) }, "30s").Should(BeNumerically(">=", 10000))
		Eventually(func() int { return len(errors) }, "30s").Should(Equal(9))

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
