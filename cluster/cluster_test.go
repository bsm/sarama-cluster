package cluster

import (
	"fmt"

	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionSlice", func() {

	It("should sort correctly", func() {
		p1 := Partition{Addr: "host1:9093", Id: 1}
		p2 := Partition{Addr: "host1:9092", Id: 2}
		p3 := Partition{Addr: "host2:9092", Id: 3}
		p4 := Partition{Addr: "host2:9093", Id: 4}
		p5 := Partition{Addr: "host1:9092", Id: 5}

		slice := PartitionSlice{p1, p2, p3, p4, p5}
		sort.Sort(slice)
		Expect(slice).To(BeEquivalentTo(PartitionSlice{p2, p5, p1, p3, p4}))
	})

})

/*********************************************************************
 * TEST HOOK
 *********************************************************************/

var _ = BeforeSuite(func() {
	client, err := sarama.NewClient("sarama-cluster-client", []string{"127.0.0.1:29092"}, &clientConfig)
	Expect(err).NotTo(HaveOccurred())
	defer client.Close()

	producer, err := sarama.NewProducer(client, &sarama.ProducerConfig{
		Partitioner:                sarama.NewHashPartitioner(),
		MaxBufferedBytes:           1024 * 1024,
		MaxBufferTime:              time.Second,
		BackPressureThresholdBytes: 10 * 1024 * 1024,
	})
	Expect(err).NotTo(HaveOccurred())
	defer producer.Close()

	for i := 0; i < 1000; i++ {
		Eventually(func() error {
			return producer.SendMessage(tnT, nil, sarama.ByteEncoder([]byte("PLAINDATA")))
		}).ShouldNot(HaveOccurred(), "50ms")
	}
})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeEach(func() {
		tnN = &mockNotifier{msgs: make([]string, 0)}
	})
	RunSpecs(t, "sarama/cluster")
}

/*******************************************************************
 * TEST HELPERS
 *******************************************************************/

var tnG = "sarama-cluster-group"
var tnT = "sarama-cluster-topic"
var tnN *mockNotifier
var clientConfig = sarama.ClientConfig{
	MetadataRetries: 30,
	WaitForElection: time.Second,
}
var consumerConfig = sarama.ConsumerConfig{
	MaxWaitTime:     600 * time.Millisecond,
	EventBufferSize: 10,
}

type mockNotifier struct{ msgs []string }

func (n *mockNotifier) RebalanceStart(cg *ConsumerGroup) {
	n.msgs = append(n.msgs, fmt.Sprintf("rebalance start %s", cg.Name()))
}
func (n *mockNotifier) RebalanceOK(cg *ConsumerGroup) {
	n.msgs = append(n.msgs, fmt.Sprintf("rebalance ok %s", cg.Name()))
}
func (n *mockNotifier) RebalanceError(cg *ConsumerGroup, err error) {
	n.msgs = append(n.msgs, fmt.Sprintf("rebalance error %s: %s", cg.Name(), err.Error()))
}
