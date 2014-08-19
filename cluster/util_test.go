package cluster

import (
	"sort"
	"time"

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
