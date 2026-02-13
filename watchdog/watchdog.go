package watchdog

import (
	"sync"
	"time"
)

// Watchdog emits a single bark on Bark if it is not Pet() before the timeout.
// After barking once, it stops permanently.
type Watchdog struct {
	Bark <-chan struct{} // receive-only

	barkCh chan struct{}
	petCh  chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}

	once sync.Once
}

// NewWatchdog creates and starts a watchdog with the given timeout.
// You must call Pet() before the timeout elapses or it will bark once.
func NewWatchdog(timeout time.Duration) *Watchdog {
	if timeout <= 0 {
		timeout = time.Millisecond
	}

	barkCh := make(chan struct{}, 1)

	w := &Watchdog{
		Bark:   barkCh,
		barkCh: barkCh,
		petCh:  make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go w.loop(timeout)
	return w
}

// Pet resets the watchdog deadline.
// Safe for concurrent use.
func (w *Watchdog) Pet() {
	select {
	case w.petCh <- struct{}{}:
	default:
	}
}

// Stop terminates the watchdog goroutine. Safe to call multiple times.
func (w *Watchdog) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
		<-w.doneCh
	})
}

func (w *Watchdog) loop(timeout time.Duration) {
	defer close(w.doneCh)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-w.stopCh:
			return

		case <-w.petCh:
			// Drain extra pets
			for {
				select {
				case <-w.petCh:
				default:
					goto drained
				}
			}
		drained:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)

		case <-timer.C:
			// Bark once (non-blocking)
			select {
			case w.barkCh <- struct{}{}:
			default:
			}
			return // stop permanently after first bark
		}
	}
}
