package infocenter

import (
	"sync"
	"sync/atomic"
)

type Message struct {
	ID   int64
	Data string
}

type Subscriber struct {
	ch      chan Message
	dropped atomic.Int64
}

func (s *Subscriber) Dropped() int64 {
	return s.dropped.Load()
}

type Topic struct {
	mu          sync.RWMutex
	subscribers map[*Subscriber]struct{}
}

func (t *Topic) addSubscriber(sub *Subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers[sub] = struct{}{}
}

func (t *Topic) removeSubscriber(sub *Subscriber) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.subscribers, sub)

	return len(t.subscribers) == 0
}

func (t *Topic) broadcast(message Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for s := range t.subscribers {
		select {
		case s.ch <- message:
		default:
			s.dropped.Add(1)
		}
	}
}
