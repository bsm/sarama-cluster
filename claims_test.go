package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Claims", func() {

	It("should extract partition IDs", func() {
		claims := Claims{5: nil, 2: nil, 1: nil}
		Expect(claims.PartitionIDs()).To(Equal([]int32{1, 2, 5}))
	})

})
