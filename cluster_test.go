package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	testTopics       = []string{"topic-a", "topic-b"}
	testKafkaBrokers = []string{"127.0.0.1:29091", "127.0.0.1:29092", "127.0.0.1:29093"}
)

func newTestConsumerGroupID() string {
	return fmt.Sprintf("test_sarama_cluster_%d", time.Now().UnixNano())
}

// --------------------------------------------------------------------

var _ = BeforeSuite(func() {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Net.DialTimeout = time.Second
	config.Net.ReadTimeout = 2 * time.Second
	config.Net.WriteTimeout = 2 * time.Second

	client, err := sarama.NewClient(testKafkaBrokers, config)
	Expect(err).NotTo(HaveOccurred())
	defer client.Close()

	// Check brokers and topic
	Expect(client.Brokers()).To(HaveLen(3))
	for _, topic := range testTopics {
		Expect(client.Topics()).To(ContainElement(topic))
	}

	// Seed partitions
	value := sarama.ByteEncoder([]byte("testdata"))
	producer, err := sarama.NewAsyncProducerFromClient(client)
	Expect(err).NotTo(HaveOccurred())

	for _, topic := range testTopics {
		Expect(client.Partitions(topic)).To(HaveLen(4), "for topic %q", topic)

		max := int64(0)
		for partition := int32(0); partition < 4; partition++ {
			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			Expect(err).NotTo(HaveOccurred(), "for topic %q", topic)

			if offset > max {
				max = offset
			}
		}

		for i := max; i < 21000; i++ {
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: value}
		}
	}
	Expect(producer.Close()).To(Succeed())
})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sarama/cluster")
}

type testConsumerMessage struct {
	sarama.ConsumerMessage
	ClientID string
}
