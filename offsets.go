package cluster

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type sessionOffsetManager struct {
	partitions   map[topicPartition]*partitionOffsetManager
	errorHandler func(error)

	groupID string
	session sarama.ConsumerGroupSession
	client  sarama.Client
	config  *sarama.Config
	broker  *sarama.Broker

	dying, dead chan none
	stopOnce    sync.Once
}

func newSessionOffsetManager(session sarama.ConsumerGroupSession, groupID string, client sarama.Client, errorHandler func(error)) (*sessionOffsetManager, error) {
	som := &sessionOffsetManager{
		partitions:   make(map[topicPartition]*partitionOffsetManager),
		errorHandler: errorHandler,

		groupID: groupID,
		session: session,
		client:  client,
		config:  client.Config(),

		dying: make(chan none),
		dead:  make(chan none),
	}
	if err := som.selectBroker(); err != nil {
		return nil, err
	}
	if err := som.fetchInitialOffsets(som.config.Metadata.Retry.Max); err != nil {
		return nil, err
	}
	go som.mainLoop()
	return som, nil
}

func (m *sessionOffsetManager) Get(topic string, partition int32) *partitionOffsetManager {
	key := topicPartition{topic: topic, partition: partition}
	return m.partitions[key]
}

func (m *sessionOffsetManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.dying)
	})
	<-m.dead
}

func (m *sessionOffsetManager) mainLoop() {
	defer close(m.dead)

	ticker := time.NewTicker(m.config.Consumer.Offsets.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.flushToBroker(m.config.Metadata.Retry.Max); err != nil {
				m.handleError(err)
			}
		case <-m.dying:
			if err := m.flushToBroker(m.config.Metadata.Retry.Max); err != nil {
				m.handleError(err)
			}
			return
		}
	}
}

func (m *sessionOffsetManager) handleError(err error) {
	if m.errorHandler != nil {
		m.errorHandler(err)
	}
}

func (m *sessionOffsetManager) flushToBroker(retries int) error {
	// build request
	req := &sarama.OffsetCommitRequest{
		Version:                 2,
		RetentionTime:           -1,
		ConsumerGroup:           m.groupID,
		ConsumerID:              m.session.MemberID(),
		ConsumerGroupGeneration: m.session.GenerationID(),
	}
	if ms := int64(m.config.Consumer.Offsets.Retention / time.Millisecond); ms != 0 {
		req.RetentionTime = ms
	}

	// add blocks + build snapshot
	snap := make(map[topicPartition]partitionOffsetState, len(m.partitions))
	for tp, pom := range m.partitions {
		pom.lock.Lock()
		if pom.dirty {
			snap[tp] = partitionOffsetState{offset: pom.offset, metadata: pom.metadata}
			req.AddBlock(tp.topic, tp.partition, pom.offset, 0, pom.metadata)
		}
		pom.lock.Unlock()
	}
	if len(snap) == 0 {
		return nil
	}

	// issue request, get response
	res, err := m.broker.CommitOffset(req)
	if err != nil {
		if retries <= 0 {
			return err
		}
		return m.flushToBroker(retries - 1)
	}

	// update all successful commits
	for tp, state := range snap {
		if res.Errors[tp.topic] != nil && res.Errors[tp.topic][tp.partition] == sarama.ErrNoError {
			delete(res.Errors[tp.topic], tp.partition)

			if pom, ok := m.partitions[tp]; ok {
				pom.updateCommitted(state.offset, state.metadata)
			}
		}
	}

	// return first of any remaining errors
	for _, perrors := range res.Errors {
		for _, err := range perrors {
			switch err {
			case sarama.ErrNotLeaderForPartition, sarama.ErrLeaderNotAvailable,
				sarama.ErrConsumerCoordinatorNotAvailable, sarama.ErrNotCoordinatorForConsumer:
				if err := m.selectBroker(); err != nil {
					return err
				}
			case sarama.ErrOffsetsLoadInProgress:
				time.Sleep(m.config.Metadata.Retry.Backoff)
			case sarama.ErrUnknownMemberId:
				return err
			}

			if retries <= 0 {
				return err
			}
			return m.flushToBroker(retries - 1)
		}
	}
	return nil
}

func (m *sessionOffsetManager) selectBroker() error {
	m.broker = nil

	err := m.client.RefreshCoordinator(m.groupID)
	if err != nil {
		return err
	}

	broker, err := m.client.Coordinator(m.groupID)
	if err != nil {
		return err
	}

	m.broker = broker
	return nil
}

func (m *sessionOffsetManager) fetchInitialOffsets(retries int) error {
	claims := m.session.Claims()

	// build request
	req := new(sarama.OffsetFetchRequest)
	req.Version = 1
	req.ConsumerGroup = m.groupID
	for topic, partitions := range claims {
		for _, partition := range partitions {
			req.AddPartition(topic, partition)
		}
	}

	// issue request, fetch response
	res, err := m.broker.FetchOffset(req)
	if err != nil {
		return err
	}

	// check for each partition
	for topic, partitions := range claims {
		for _, partition := range partitions {
			block := res.GetBlock(topic, partition)
			if block == nil {
				return sarama.ErrIncompleteResponse
			}

			switch block.Err {
			case sarama.ErrNoError:
				key := topicPartition{
					topic:     topic,
					partition: partition,
				}
				m.partitions[key] = &partitionOffsetManager{
					partitionOffsetState: partitionOffsetState{
						offset:   block.Offset,
						metadata: block.Metadata,
					},
				}
			case sarama.ErrNotCoordinatorForConsumer:
				if retries <= 0 {
					return block.Err
				}
				if err := m.selectBroker(); err != nil {
					return err
				}
				return m.fetchInitialOffsets(retries - 1)
			case sarama.ErrOffsetsLoadInProgress:
				if retries <= 0 {
					return block.Err
				}
				time.Sleep(m.config.Metadata.Retry.Backoff)
				return m.fetchInitialOffsets(retries - 1)
			default:
				return block.Err
			}
		}
	}
	return nil
}

// --------------------------------------------------------------------

type topicPartition struct {
	topic     string
	partition int32
}

type partitionOffsetState struct {
	offset   int64
	metadata string
}

type partitionOffsetManager struct {
	partitionOffsetState
	dirty bool
	lock  sync.Mutex
}

func (o *partitionOffsetManager) nextOffset(fallback int64) int64 {
	o.lock.Lock()
	offset := o.offset
	o.lock.Unlock()

	if offset < 0 {
		return fallback
	}
	return offset
}

func (o *partitionOffsetManager) updateCommitted(offset int64, metadata string) {
	o.lock.Lock()
	if o.offset == offset && o.metadata == metadata {
		o.dirty = false
	}
	o.lock.Unlock()
}

func (o *partitionOffsetManager) reset() {
	o.lock.Lock()
	o.offset = -1
	o.metadata = ""
	o.dirty = false
	o.lock.Unlock()
}

func (o *partitionOffsetManager) markOffset(offset int64, metadata string) {
	o.lock.Lock()
	if offset > o.offset {
		o.offset = offset
		o.metadata = metadata
		o.dirty = true
	}
	o.lock.Unlock()
}
