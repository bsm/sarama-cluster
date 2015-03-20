package cluster

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("claimsMap", func() {

	It("should extract names", func() {
		claims := claimsMap{
			topicPartition{tTopicA, 5}: nil,
			topicPartition{tTopicB, 2}: nil,
			topicPartition{tTopicA, 1}: nil,
		}
		Expect(claims.Names()).To(Equal([]string{
			"sarama-cluster-topic-a-1",
			"sarama-cluster-topic-a-5",
			"sarama-cluster-topic-b-2",
		}))
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
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix:testhost:4e44cb31-4e84-0000-0000"))
		Expect(newGUIDAt("prefix", time.Unix(1414141414, 0))).To(Equal("prefix:testhost:544a15e6-4e84-0000-0001"))
	})

	It("should increment correctly", func() {
		cGUID.inc = 0xffffffff - 1
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix:testhost:4e44cb31-4e84-ffff-ffff"))
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix:testhost:4e44cb31-4e84-0000-0000"))
	})
})
