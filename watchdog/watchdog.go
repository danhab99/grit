package watchdog

import (
	"sync"
	"time"
)

// Watchdog emits a bark on Bark if it is not Pet() before the timeout.
// After the first bark, it continues barking every timeout until Pet() is called again.
type Watchdog struct {
	Bark <-chan struct{} // receive-only for users

	barkCh chan struct{}
	petCh  chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}

	once sync.Once
}

// NewWatchdog creates and starts a watchdog with the given timeout.
// You must call Pet() before the timeout elapses, otherwise it will bark on Bark.
func NewWatchdog(timeout time.Duration) *Watchdog {
	if timeout <= 0 {
		// A non-positive timeout is almost always a bug; force something usable.
		timeout = time.Millisecond
	}

	barkCh := make(chan struct{}, 1) // buffer 1 so a single bark can be observed even if not immediately read

	w := &Watchdog{
		Bark:   barkCh,
		barkCh: barkCh,
		petCh:  make(chan struct{}, 1), // coalesce pets
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go w.loop(timeout)
	return w
}

// Pet resets the watchdog deadline.
// Safe to call from multiple goroutines.
func (w *Watchdog) Pet() {
	// Coalesce multiple pets; we only need to know "at least one happened".
	select {
	case w.petCh <- struct{}{}:
	default:
	}
}

// Stop terminates the watchdog goroutine. Idempotent.
func (w *Watchdog) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
		<-w.doneCh
	})
}

func (w *Watchdog) loop(timeout time.Duration) {
	defer close(w.doneCh)

	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			// Drain if needed (rare, but correct).
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-w.stopCh:
			return

		case <-w.petCh:
			// Drain extra pets so we don't immediately "re-pet" after reset.
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
			// Non-blocking bark: if consumer isn't reading and buffer is full, drop.
			select {
			case w.barkCh <- struct{}{}:
			default:
			}
			// Keep barking every timeout until Pet() happens.
			timer.Reset(timeout)
		}
	}
}
