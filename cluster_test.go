package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionSlice", func() {

	It("should sort correctly", func() {
		p1 := Partition{Addr: "host1:9093", ID: 1}
		p2 := Partition{Addr: "host1:9092", ID: 2}
		p3 := Partition{Addr: "host2:9092", ID: 3}
		p4 := Partition{Addr: "host3:9091", ID: 4}
		p5 := Partition{Addr: "host2:9093", ID: 5}
		p6 := Partition{Addr: "host1:9092", ID: 6}

		slice := PartitionSlice{p1, p2, p3, p4, p5, p6}
		sort.Sort(slice)
		Expect(slice).To(BeEquivalentTo(PartitionSlice{p2, p6, p1, p3, p5, p4}))
	})

})

/*********************************************************************
 * TEST HOOK
 *********************************************************************/

const (
	t_KAFKA_VERSION = "kafka_2.10-0.8.1.1"
	t_CLIENT        = "sarama-cluster-client"
	t_TOPIC         = "sarama-cluster-topic"
	t_GROUP         = "sarama-cluster-group"
	t_DIR           = "/tmp/sarama-cluster-test"
)

var (
	t_ZK_ADDRS = []string{"localhost:22181"}
)

var _ = BeforeSuite(func() {
	runner := testDir(t_KAFKA_VERSION, "bin", "kafka-run-class.sh")
	testState.zookeeper = exec.Command(runner, "-name", "zookeeper", "org.apache.zookeeper.server.ZooKeeperServerMain", testDir("zookeeper.properties"))
	testState.kafka = exec.Command(runner, "-name", "kafkaServer", "kafka.Kafka", testDir("server.properties"))
	testState.kafka.Env = []string{"KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"}

	// Create Dir
	Expect(os.MkdirAll(t_DIR, 0775)).NotTo(HaveOccurred())

	// Start ZK & Kafka
	Expect(testState.zookeeper.Start()).NotTo(HaveOccurred())
	Expect(testState.kafka.Start()).NotTo(HaveOccurred())

	// Wait for client
	var client *sarama.Client
	Eventually(func() error {
		var err error
		client, err = newClient()
		return err
	}, "10s", "1s").ShouldNot(HaveOccurred())
	defer client.Close()

	Eventually(func() error {
		_, err := client.Partitions(t_TOPIC)
		return err
	}, "10s", "1s").ShouldNot(HaveOccurred())

	// Seed messages
	Expect(seedMessages(client, 10000)).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if testState.kafka != nil {
		testState.kafka.Process.Kill()
	}
	if testState.zookeeper != nil {
		testState.zookeeper.Process.Kill()
	}
	Expect(os.RemoveAll(t_DIR)).NotTo(HaveOccurred())
})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	AfterEach(func() {
		zk, err := NewZK(t_ZK_ADDRS, time.Second)
		Expect(err).NotTo(HaveOccurred())

		zk.DeleteAll("/consumers/" + t_GROUP)
		zk.Close()
	})
	RunSpecs(t, "sarama/cluster")
}

/*******************************************************************
 * TEST HELPERS
 *******************************************************************/

var testState struct{ kafka, zookeeper *exec.Cmd }

func newClient() (*sarama.Client, error) {
	return sarama.NewClient(t_CLIENT, []string{"127.0.0.1:29092"}, nil)
}

func testDir(tokens ...string) string {
	_, filename, _, _ := runtime.Caller(1)
	tokens = append([]string{path.Dir(filename), "_test"}, tokens...)
	return path.Join(tokens...)
}

func seedMessages(client *sarama.Client, count int) error {
	producer, err := sarama.NewSimpleProducer(client, t_TOPIC, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < count; i++ {
		kv := sarama.StringEncoder(fmt.Sprintf("PLAINDATA-%08d", i))
		err := producer.SendMessage(kv, kv)
		if err != nil {
			return err
		}
	}
	return nil
}

type mockNotifier struct{ messages []string }

func (n *mockNotifier) RebalanceStart(c *Consumer) {
	n.messages = append(n.messages, "REBALANCE START")
}
func (n *mockNotifier) RebalanceOK(c *Consumer) {
	n.messages = append(n.messages, "REBALANCE OK")
}
func (n *mockNotifier) RebalanceError(c *Consumer, err error) {
	n.messages = append(n.messages, "REBALANCE ERROR")
}
func (n *mockNotifier) CommitError(c *Consumer, err error) {
	n.messages = append(n.messages, "COMMIT ERROR")
}
