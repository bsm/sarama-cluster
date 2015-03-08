package cluster

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/samuel/go-zookeeper/zk"
)

var _ = Describe("ZK", func() {
	var subject *ZK

	BeforeEach(func() {
		var err error
		subject, err = NewZK(tZKAddrs, time.Second)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if subject != nil {
			subject.Close()
		}
	})

	It("should connect to ZooKeeper", func() {
		Expect(subject).To(BeAssignableToTypeOf(&ZK{}))
	})

	Describe("high-level API", func() {

		var checkOwner = func(num string) string {
			val, _, _ := subject.Get("/consumers/" + tGroup + "/owners/" + tTopic + "/" + num)
			return string(val)
		}

		It("should return consumers within a group", func() {
			Expect(subject.RegisterGroup(tGroup)).To(BeNil())

			sub, _, err := subject.Consumers(tGroup)
			Expect(err).NotTo(HaveOccurred())
			Expect(sub).To(BeEmpty())

			err = subject.Create("/consumers/"+tGroup+"/ids/consumer-c", []byte{'C'}, true)
			Expect(err).NotTo(HaveOccurred())
			err = subject.Create("/consumers/"+tGroup+"/ids/consumer-a", []byte{'A'}, true)
			Expect(err).NotTo(HaveOccurred())
			err = subject.Create("/consumers/"+tGroup+"/ids/consumer-b", []byte{'B'}, true)
			Expect(err).NotTo(HaveOccurred())

			sub, _, err = subject.Consumers(tGroup)
			Expect(err).NotTo(HaveOccurred())
			Expect(sub).To(Equal([]string{"consumer-a", "consumer-b", "consumer-c"}))
		})

		It("should register groups", func() {
			ok, err := subject.Exists("/consumers/" + tGroup + "/ids")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			err = subject.RegisterGroup(tGroup)
			Expect(err).NotTo(HaveOccurred())

			ok, err = subject.Exists("/consumers/" + tGroup + "/ids")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
		})

		It("should register consumers (ephemeral) ", func() {
			Expect(subject.RegisterGroup(tGroup)).To(BeNil())

			other, err := NewZK(tZKAddrs, 1e9)
			Expect(err).NotTo(HaveOccurred())

			strs, watch, err := subject.Consumers(tGroup)
			Expect(err).NotTo(HaveOccurred())
			Expect(strs).To(BeEmpty())

			err = subject.RegisterConsumer(tGroup, "consumer-b", "topic")
			Expect(err).NotTo(HaveOccurred())
			err = other.RegisterConsumer(tGroup, "consumer-a", "topic")
			Expect(err).NotTo(HaveOccurred())
			err = subject.RegisterConsumer(tGroup, "consumer-b", "topic")
			Expect(err).To(Equal(zk.ErrNodeExists))
			Expect((<-watch).Type).To(Equal(zk.EventNodeChildrenChanged))

			strs, watch, err = subject.Consumers(tGroup)
			Expect(err).NotTo(HaveOccurred())
			Expect(strs).To(Equal([]string{"consumer-a", "consumer-b"}))

			other.Close()
			Expect((<-watch).Type).To(Equal(zk.EventNodeChildrenChanged))

			strs, _, err = subject.Consumers(tGroup)
			Expect(err).NotTo(HaveOccurred())
			Expect(strs).To(Equal([]string{"consumer-b"}))

			val, _, err := subject.Get("/consumers/" + tGroup + "/ids/consumer-b")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(val)).To(ContainSubstring(`"subscription":{"topic":1}`))
		})

		It("should claim partitions (ephemeral)", func() {
			Expect(subject.Claim(tGroup, tTopic, 0, "consumer-a")).To(BeNil())
			Expect(checkOwner("0")).To(Equal(`consumer-a`))
		})

		It("should wait with claim until available", func() {
			Expect(subject.Claim(tGroup, tTopic, 1, "consumer-b")).To(BeNil())
			go func() {
				subject.Claim(tGroup, tTopic, 1, "consumer-c")
			}()
			Expect(checkOwner("1")).To(Equal(`consumer-b`))
			Expect(subject.Release(tGroup, tTopic, 1, "consumer-b")).To(BeNil())
			Eventually(func() string { return checkOwner("1") }).Should(Equal(`consumer-c`))
		})

		It("should release partitions", func() {
			Expect(subject.Release(tGroup, tTopic, 0, "consumer-a")).To(BeNil())

			Expect(subject.Claim(tGroup, tTopic, 0, "consumer-a")).To(BeNil())
			Expect(subject.Release(tGroup, tTopic, 0, "consumer-a")).To(BeNil())

			Expect(subject.Claim(tGroup, tTopic, 0, "consumer-a")).To(BeNil())
			Expect(subject.Release(tGroup, tTopic, 0, "consumer-b")).To(Equal(zk.ErrNotLocked))
		})

		It("should retrieve offsets", func() {
			offset, err := subject.Offset(tGroup, tTopic, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset).To(Equal(int64(0)))

			err = subject.Create("/consumers/"+tGroup+"/offsets/"+tTopic+"/0", []byte("14798"), false)
			Expect(err).NotTo(HaveOccurred())

			offset, err = subject.Offset(tGroup, tTopic, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset).To(Equal(int64(14798)))
		})

		It("should commit offsets", func() {
			Expect(subject.Commit(tGroup, tTopic, 0, 999)).To(BeNil())

			val, stat, err := subject.Get("/consumers/" + tGroup + "/offsets/" + tTopic + "/0")
			Expect(err).NotTo(HaveOccurred())
			Expect(string(val)).To(Equal(`999`))
			Expect(stat.Version).To(Equal(int32(0)))

			Expect(subject.Commit(tGroup, tTopic, 0, 2999)).To(BeNil())
			offset, err := subject.Offset(tGroup, tTopic, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset).To(Equal(int64(2999)))
		})

	})

	Describe("low-level API", func() {

		It("should check path existence", func() {
			ok, err := subject.Exists("/consumers/" + tGroup + "/ids")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		})

		It("should create dirs recursively", func() {
			ok, _ := subject.Exists("/consumers/" + tGroup + "/ids")
			Expect(ok).To(BeFalse())

			err := subject.MkdirAll("/consumers/" + tGroup + "/ids")
			Expect(err).NotTo(HaveOccurred())
			err = subject.MkdirAll("/consumers/" + tGroup + "/ids")
			Expect(err).NotTo(HaveOccurred())

			ok, _ = subject.Exists("/consumers/" + tGroup + "/ids")
			Expect(ok).To(BeTrue())
		})

		It("should create entries", func() {
			err := subject.Create("/consumers/"+tGroup+"/ids/x", []byte{'X'}, false)
			Expect(err).NotTo(HaveOccurred())
			err = subject.Create("/consumers/"+tGroup+"/ids/x", []byte{'Y'}, false)
			Expect(err).To(Equal(zk.ErrNodeExists))
		})

		It("should create ephemeral entries", func() {
			other, err := NewZK(tZKAddrs, 1e9)
			Expect(err).NotTo(HaveOccurred())
			err = other.Create("/consumers/"+tGroup+"/ids/x", []byte{'X'}, true)
			Expect(err).NotTo(HaveOccurred())

			ok, _ := subject.Exists("/consumers/" + tGroup + "/ids/x")
			Expect(ok).To(BeTrue())

			other.Close()
			ok, _ = subject.Exists("/consumers/" + tGroup + "/ids/x")
			Expect(ok).To(BeFalse())
		})

	})
})
