package stm

import (
	"sync"
	"time"
)

// notifier is a broadcast condition used to wake transactions that called
// Txn.Retry after every committed change. It maintains a monotonic epoch;
// waiters block until the epoch advances past the value they observed.
type notifier struct {
	mu    sync.Mutex
	cv    *sync.Cond
	epoch uint64
}

func newNotifier() *notifier {
	n := &notifier{}
	n.cv = sync.NewCond(&n.mu)
	return n
}

// snapshotEpoch returns the current change epoch.
func (n *notifier) snapshotEpoch() uint64 {
	n.mu.Lock()
	v := n.epoch
	n.mu.Unlock()
	return v
}

// waitUntilChanged blocks until a commit newer than epoch occurs or the
// timeout elapses. True is returned when woken by a change.
func (n *notifier) waitUntilChanged(epoch uint64, timeout time.Duration) bool {
	var timedOut bool
	timer := time.AfterFunc(timeout, func() {
		n.mu.Lock()
		timedOut = true
		n.cv.Broadcast()
		n.mu.Unlock()
	})
	defer timer.Stop()

	n.mu.Lock()
	for n.epoch <= epoch && !timedOut {
		n.cv.Wait()
	}
	changed := n.epoch > epoch
	n.mu.Unlock()
	return changed
}

// notifyChanged advances the epoch and wakes all waiting transactions.
func (n *notifier) notifyChanged() {
	n.mu.Lock()
	n.epoch++
	n.cv.Broadcast()
	n.mu.Unlock()
}
