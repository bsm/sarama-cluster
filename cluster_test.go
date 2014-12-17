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

var _ = Describe("GUID", func() {

	BeforeEach(func() {
		cGUID.hostname = "testhost"
		cGUID.pid = 20100
	})

	AfterEach(func() {
		cGUID.inc = 0
	})

	It("should create GUIDs", func() {
		cGUID.inc = 0xffffffff
		Expect(NewGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
		Expect(NewGUIDAt("prefix", time.Unix(1414141414, 0))).To(Equal("prefix-testhost-20100-1414141414-1"))
	})

	It("should increment correctly", func() {
		cGUID.inc = 0xffffffff - 1
		Expect(NewGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-4294967295"))
		Expect(NewGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
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

var _ = BeforeSuite(func() {
	runner := testDir(t_KAFKA_VERSION, "bin", "kafka-run-class.sh")
	testState.zookeeper = exec.Command(runner, "-name", "zookeeper", "org.apache.zookeeper.server.ZooKeeperServerMain", testDir("zookeeper.properties"))
	testState.kafka = exec.Command(runner, "-name", "kafkaServer", "kafka.Kafka", testDir("server.properties"))
	testState.kafka.Env = []string{"KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"}

	// Create Dir
	Expect(os.MkdirAll(t_DIR, 0775)).NotTo(HaveOccurred())

	// Start ZK
	Expect(testState.zookeeper.Start()).NotTo(HaveOccurred())
	Eventually(func() *os.Process {
		return testState.zookeeper.Process
	}).ShouldNot(BeNil())

	// Start Kafka
	Expect(testState.kafka.Start()).NotTo(HaveOccurred())
	Eventually(func() *os.Process {
		return testState.kafka.Process
	}).ShouldNot(BeNil())

	// Create partition
	time.Sleep(1 * time.Second)
	err := exec.Command(testDir(t_KAFKA_VERSION, "bin", "kafka-topics.sh"),
		"--create",
		"--topic", t_TOPIC,
		"--zookeeper", "127.0.0.1:22181",
		"--partitions", "12",
		"--replication-factor", "1",
	).Run()
	Expect(err).NotTo(HaveOccurred())

	// Create and wait for client
	client, err := newClient()
	Expect(err).NotTo(HaveOccurred())
	defer client.Close()

	Eventually(func() error {
		_, err := client.Partitions(t_TOPIC)
		return err
	}).ShouldNot(HaveOccurred(), "120s")

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
	BeforeEach(func() {
		testState.notifier = &mockNotifier{messages: make([]string, 0)}
	})
	RunSpecs(t, "sarama/cluster")
}

/*******************************************************************
 * TEST HELPERS
 *******************************************************************/

var testState struct {
	kafka, zookeeper *exec.Cmd
	notifier         *mockNotifier
}

func newClient() (*sarama.Client, error) {
	return sarama.NewClient(t_CLIENT, []string{"127.0.0.1:29092"}, sarama.NewClientConfig())
}

func testDir(tokens ...string) string {
	_, filename, _, _ := runtime.Caller(1)
	tokens = append([]string{path.Dir(filename), "test"}, tokens...)
	return path.Join(tokens...)
}

func testConsumerConfig() *sarama.ConsumerConfig {
	return sarama.NewConsumerConfig()
}

func seedMessages(client *sarama.Client, count int) error {
	producer, err := sarama.NewSimpleProducer(client, t_TOPIC, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < count; i++ {
		err := producer.SendMessage(nil, sarama.StringEncoder(fmt.Sprintf("PLAINDATA-%08d", i)))
		if err != nil {
			return err
		}
	}
	return nil
}

type mockNotifier struct{ messages []string }

func (n *mockNotifier) RebalanceStart(cg *ConsumerGroup) {
	n.messages = append(n.messages, fmt.Sprintf("rebalance start %s", cg.Name()))
}
func (n *mockNotifier) RebalanceOK(cg *ConsumerGroup) {
	n.messages = append(n.messages, fmt.Sprintf("rebalance ok %s", cg.Name()))
}
func (n *mockNotifier) RebalanceError(cg *ConsumerGroup, err error) {
	n.messages = append(n.messages, fmt.Sprintf("rebalance error %s: %s", cg.Name(), err.Error()))
}
