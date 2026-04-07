package infocenter

import (
	"sync"
	"sync/atomic"
)

type TopicManager struct {
	mu         sync.RWMutex
	globalID   atomic.Int64
	bufferSize int
	topics     map[string]*Topic
}

func NewTopicManager(bufferSize int) *TopicManager {
	return &TopicManager{
		bufferSize: bufferSize,
		topics:     make(map[string]*Topic),
	}
}

func (m *TopicManager) Subscribe(topicName string) *Subscriber {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, ok := m.topics[topicName]
	if !ok {
		topic = &Topic{subscribers: make(map[*Subscriber]struct{})}
		m.topics[topicName] = topic
	}

	sub := &Subscriber{ch: make(chan Message, m.bufferSize)}
	topic.addSubscriber(sub)

	return sub
}

func (m *TopicManager) Unsubscribe(topicName string, sub *Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, ok := m.topics[topicName]
	if !ok {
		return
	}

	if topic.removeSubscriber(sub) {
		delete(m.topics, topicName)
	}
}

func (m *TopicManager) Publish(topicName, data string) {
	id := m.globalID.Add(1)

	m.mu.RLock()
	topic := m.topics[topicName]
	m.mu.RUnlock()

	if topic != nil {
		topic.broadcast(Message{ID: id, Data: data})
	}
}
