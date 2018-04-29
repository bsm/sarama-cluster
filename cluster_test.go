package cluster_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
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

func newConsumerProcess(clientID, groupID string, topics []string, handler cluster.Handler) (cluster.Consumer, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return cluster.NewConsumer(testKafkaBrokers, groupID, topics, config, handler)
}

func newConsumer(clientID, groupID string, topics ...string) (cluster.Consumer, error) {
	return newConsumerProcess(clientID, groupID, topics, nil)
}

func withinFiveSec(v interface{}) GomegaAsyncAssertion {
	return Eventually(v, "5s", "50ms")
}

func claimsOf(c cluster.Consumer) GomegaAsyncAssertion {
	return withinFiveSec(func() map[string][]int32 {
		select {
		case claim := <-c.Claims():
			if claim != nil {
				return claim.Current
			}
		default:
		}
		return nil
	})
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

type countingHandler struct{ sessions, messages int32 }

func (h *countingHandler) ProcessLoop(pc cluster.PartitionConsumer) error {
	atomic.AddInt32(&h.sessions, 1)
	defer atomic.AddInt32(&h.sessions, -1)

	// start seemingly endless loop
	for range pc.Messages() {
		atomic.AddInt32(&h.messages, 1)
	}
	return nil
}

func (h *countingHandler) NumSessions() int { return int(atomic.LoadInt32(&h.sessions)) }
func (h *countingHandler) NumMessages() int { return int(atomic.LoadInt32(&h.messages)) }
