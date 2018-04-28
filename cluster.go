package cluster

type none struct{}

// Handler instances are able to consume from PartitionConsumer instances.
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently.
// Ensure that all state is safely protected against race conditions.
type Handler interface {
	// ProcessLoop must start a consumer loop of PartitionConsumer's Messages()
	// while listening to the Done() channel. Once triggered, the Handler must
	// finish its processing loop and exit within Config.Consumer.Group.Rebalance.Timeout
	// as the topic/partition may be re-assigned to another group member.
	ProcessLoop(PartitionConsumer) error
}

// HandlerFunc is a Handler function shortcut.
type HandlerFunc func(PartitionConsumer) error

// ProcessLoop implements the Handler interface.
func (f HandlerFunc) ProcessLoop(c PartitionConsumer) error { return f(c) }

// Claim is a notification issued by a consumer to indicate
// the claimed topics after a rebalance.
type Claim struct {
	// Topics of claimed topics and partitions
	Topics map[string][]int32
}

var noopHandler = HandlerFunc(func(pc PartitionConsumer) error {
	<-pc.Done()
	return nil
})
