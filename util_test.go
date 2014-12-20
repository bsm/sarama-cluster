package cluster

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Claims", func() {

	It("should extract partition IDs", func() {
		claims := Claims{5: nil, 2: nil, 1: nil}
		Expect(claims.PartitionIDs()).To(Equal([]int32{1, 2, 5}))
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
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
		Expect(newGUIDAt("prefix", time.Unix(1414141414, 0))).To(Equal("prefix-testhost-20100-1414141414-1"))
	})

	It("should increment correctly", func() {
		cGUID.inc = 0xffffffff - 1
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-4294967295"))
		Expect(newGUIDAt("prefix", time.Unix(1313131313, 0))).To(Equal("prefix-testhost-20100-1313131313-0"))
	})
})

var _ = Describe("int32Slice", func() {

	It("should sort", func() {
		slice := int32Slice{5, 1, 2}
		Expect(slice.Sorted()).To(Equal([]int32{1, 2, 5}))
	})

})
