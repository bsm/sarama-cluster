package cluster

import (
	"sort"

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

	It("should compare with claims", func() {
		p1 := Partition{Addr: "host1:9093", ID: 1}
		p2 := Partition{Addr: "host1:9092", ID: 2}
		slice := PartitionSlice{p1, p2}

		Expect(slice.ConsistOf(Claims{2: nil, 1: nil})).To(BeTrue())
		Expect(slice.ConsistOf(Claims{1: nil, 2: nil})).To(BeTrue())
		Expect(slice.ConsistOf(Claims{1: nil, 2: nil, 3: nil})).To(BeFalse())
		Expect(slice.ConsistOf(Claims{1: nil})).To(BeFalse())
		Expect(slice.ConsistOf(Claims{7: nil, 8: nil})).To(BeFalse())
	})

	It("should determine which partitions to select", func() {
		testCases := []struct {
			one string
			all []string
			ids []int32
			exp []int32
		}{
			{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{0}},
			{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{1}},
			{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1, 2}, []int32{2}},

			{"N3", []string{"N1", "N2", "N3", "N4"}, []int32{0, 3, 2, 1}, []int32{2}},
			{"N3", []string{"N1", "N2", "N3", "N4"}, []int32{1, 3, 5, 7}, []int32{5}},

			{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{0}},
			{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{}},
			{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1}, []int32{1}},

			{"N1", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{0}},
			{"N2", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{1, 2}},
			{"N3", []string{"N1", "N2", "N3"}, []int32{0, 1, 2, 3}, []int32{3}},

			{"N1", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{0, 2}},
			{"N2", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{4}},
			{"N3", []string{"N1", "N2", "N3"}, []int32{0, 2, 4, 6, 8}, []int32{6, 8}},

			{"N1", []string{"N1", "N2"}, []int32{0, 1, 2, 3, 4}, []int32{0, 1, 2}},
			{"N2", []string{"N1", "N2"}, []int32{0, 1, 2, 3, 4}, []int32{3, 4}},

			{"N1", []string{"N1", "N2", "N3"}, []int32{0}, []int32{}},
			{"N2", []string{"N1", "N2", "N3"}, []int32{0}, []int32{0}},
			{"N3", []string{"N1", "N2", "N3"}, []int32{0}, []int32{}},

			{"N1", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{0}},
			{"N2", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{1}},
			{"N3", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{}},
			{"N4", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{2}},
			{"N5", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3}, []int32{3}},

			{"N1", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{0, 1}},
			{"N2", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{2, 3, 4}},
			{"N3", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{5, 6}},
			{"N4", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{7, 8, 9}},
			{"N5", []string{"N1", "N2", "N3", "N4", "N5"}, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, []int32{10, 11}},

			{"N9", []string{"N1", "N2"}, []int32{0, 1}, []int32{}},
		}

		for _, item := range testCases {
			slice := make(PartitionSlice, len(item.ids))
			for i, id := range item.ids {
				slice[i] = Partition{ID: id, Addr: "locahost:9092"}
			}

			// Pseudo-shuffle
			sort.StringSlice(item.all).Swap(0, len(item.all)-1)
			slice.Swap(0, len(slice)-1)

			// Claim and compare
			slice = slice.Select(item.one, item.all)
			actual := make([]int32, len(slice))
			for i, item := range slice {
				actual[i] = item.ID
			}
			Expect(actual).To(Equal(item.exp), "Case: %+v", item)
		}
	})

})
