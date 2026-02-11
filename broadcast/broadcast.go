package broadcast

import (
	"sync"
)

type Broadcaster[T any] struct {
	mu        sync.RWMutex
	listeners map[chan T]struct{}
}

func NewBroadcaster[T any]() *Broadcaster[T] {
	return &Broadcaster[T]{
		listeners: make(map[chan T]struct{}),
	}
}

func (b *Broadcaster[T]) Subscribe(buffer int) chan T {
	ch := make(chan T, buffer)

	b.mu.Lock()
	b.listeners[ch] = struct{}{}
	b.mu.Unlock()

	return ch
}

func (b *Broadcaster[T]) Unsubscribe(ch chan T) {
	b.mu.Lock()
	delete(b.listeners, ch)
	close(ch)
	b.mu.Unlock()
}

func (b *Broadcaster[T]) Broadcast(msg T) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.listeners {
		select {
		case ch <- msg:
		default:
			// listener is slow — drop message
		}
	}
}
