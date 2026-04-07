package infocenter

import (
	"sync"
	"testing"
	"time"
)

func (m *TopicManager) topicExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.topics[name]
	return ok
}

func (m *TopicManager) subscriberCount(name string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	topic, ok := m.topics[name]
	if !ok {
		return 0
	}
	return len(topic.subscribers)
}

func TestSubscribeAndPublish(t *testing.T) {
	m := NewTopicManager(16)
	sub := m.Subscribe("news")
	defer m.Unsubscribe("news", sub)

	m.Publish("news", "hello")

	select {
	case msg := <-sub.ch:
		if msg.Data != "hello" {
			t.Fatalf("expected 'hello', got %q", msg.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestGlobalIDAlwaysIncrements(t *testing.T) {
	m := NewTopicManager(16)

	m.Publish("x", "msg1")
	m.Publish("x", "msg2")

	sub := m.Subscribe("x")
	defer m.Unsubscribe("x", sub)

	m.Publish("x", "msg3")

	select {
	case msg := <-sub.ch:
		if msg.ID != 3 {
			t.Fatalf("expected ID 3, got %d", msg.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestTopicCleanedUpWhenEmpty(t *testing.T) {
	m := NewTopicManager(16)
	sub := m.Subscribe("tmp")
	m.Unsubscribe("tmp", sub)

	if m.topicExists("tmp") {
		t.Fatal("topic should have been deleted after last unsubscribe")
	}
}

func TestTopicsAreIsolated(t *testing.T) {
	m := NewTopicManager(16)

	subA := m.Subscribe("a")
	subB := m.Subscribe("b")
	defer m.Unsubscribe("a", subA)
	defer m.Unsubscribe("b", subB)

	m.Publish("a", "for-a")

	select {
	case msg := <-subA.ch:
		if msg.Data != "for-a" {
			t.Fatalf("expected 'for-a', got %q", msg.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case msg := <-subB.ch:
		t.Fatalf("subB should not receive message, got %q", msg.Data)
	default:
	}
}

func TestMultipleSubscribersReceiveBroadcast(t *testing.T) {
	m := NewTopicManager(16)

	sub1 := m.Subscribe("news")
	sub2 := m.Subscribe("news")
	defer m.Unsubscribe("news", sub1)
	defer m.Unsubscribe("news", sub2)

	m.Publish("news", "broadcast")

	for i, sub := range []*Subscriber{sub1, sub2} {
		select {
		case msg := <-sub.ch:
			if msg.Data != "broadcast" {
				t.Fatalf("sub%d: expected 'broadcast', got %q", i+1, msg.Data)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub%d: timed out waiting for message", i+1)
		}
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	m := NewTopicManager(16)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			sub := m.Subscribe("topic")
			m.Publish("topic", "data")
			m.Unsubscribe("topic", sub)
		})
	}
	wg.Wait()
}
