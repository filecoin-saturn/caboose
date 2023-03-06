package stoker

import (
	"sync/atomic"
	"time"
)

type writeAheadSynchronizer struct {
	max     int
	min     int
	current atomic.Int32
	notify  chan struct{}
}

func NewWAS(max, min int) *writeAheadSynchronizer {
	return &writeAheadSynchronizer{
		max:     max,
		min:     min,
		current: atomic.Int32{},
		notify:  make(chan struct{}, 1),
	}
}

// requst to add 1 to the queue, potentially blocking until that's cokay
func (w *writeAheadSynchronizer) ReqAdd() {
	n := w.current.Add(1)
	if n >= int32(w.max) {
		w.wait()
	}
}

func (w *writeAheadSynchronizer) wait() {
	done := false
	t := time.NewTimer(15 * time.Millisecond)
	for {
		select {
		case <-t.C:
		case <-w.notify:
		}

		curr := w.current.Load()
		if curr < int32(w.min) {
			done = true
		}

		if !t.Stop() {
			// Exhaust expired timer's chan.
			select {
			case <-t.C:
			default:
			}
		}
		if done {
			return
		}
		t.Reset(15 * time.Millisecond)
	}
}

// decrement 1 from the queue, potentially unblocking writers
func (w *writeAheadSynchronizer) Dec() {
	curr := w.current.Add(-1)
	if curr < 0 {
		// todo: okay that not atomic? i think we're okay leaking in this direction
		w.current.Store(0)
	}
	if curr <= int32(w.min) {
		select {
		case w.notify <- struct{}{}:
		default:
		}
	}
}
