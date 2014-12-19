package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fuzzing", func() {

	var fuzz = func(client *sarama.Client, id string, n int, errors chan error, events chan *sarama.ConsumerEvent) {
		consumer, err := NewConsumerWithID(client, []string{"localhost:22181"},
			"sarama-cluster-fuzzing-test", t_TOPIC, id, nil,
			sarama.NewConsumerConfig(),
		)
		if err != nil {
			errors <- err
			return
		}
		defer consumer.Close()

		cnt := 0
		for evt := range consumer.Events() {
			if evt.Err != nil {
				errors <- evt.Err
				return
			}

			evt.Key = []byte("PROCESSED BY " + id)
			events <- evt
			consumer.Ack(evt)
			if cnt++; cnt >= n {
				errors <- nil
				return
			}
		}
	}

	It("should consume uniquely across all consumers within a group", func() {
		errors := make(chan error, 100)
		events := make(chan *sarama.ConsumerEvent, 1e6)

		client, err := newClient()
		Expect(err).NotTo(HaveOccurred())
		defer client.Close()

		go fuzz(client, "A", 200, errors, events)
		go fuzz(client, "B", 300, errors, events)
		Eventually(func() int { return len(events) }, "10s").Should(BeNumerically(">=", 500))

		go fuzz(client, "C", 2700, errors, events)
		go fuzz(client, "D", 1000, errors, events)
		go fuzz(client, "E", 100, errors, events)
		go fuzz(client, "F", 1200, errors, events)
		go fuzz(client, "G", 4000, errors, events)
		go fuzz(client, "H", 200, errors, events)
		go fuzz(client, "I", 300, errors, events)
		Eventually(func() int { return len(events) }, "30s").Should(BeNumerically(">=", 10000))
		Eventually(func() int { return len(errors) }, "30s").Should(Equal(9))

		for len(errors) > 0 {
			Expect(<-errors).NotTo(HaveOccurred())
		}

		byID := make(map[int64][]*sarama.ConsumerEvent, len(events))
		for len(events) > 0 {
			evt := <-events
			uid := int64(evt.Partition)*1e9 + evt.Offset
			byID[uid] = append(byID[uid], evt)
		}

		// Ensure each event was consumed only once
		for _, evts := range byID {
			Expect(evts).To(HaveLen(1))
		}
	})

})
