package stm

import "errors"

// Tx is an in-progress transaction.
//
// All reads are consistent with the snapshot captured at Begin (read
// committed + first-committer-wins validation): the observed version of every
// read key is recorded, and writes are buffered locally. Nothing another
// transaction does is visible inside an open Tx, and nothing this Tx writes
// is visible to anyone until Commit succeeds. On conflict, abort or retry the
// buffered writes are simply discarded -- they can never partially apply.
type Tx struct {
	store    *Store
	readVer  uint64            // global version captured at Begin
	observed map[string]uint64 // version of every key observed (read or write base)
	writes   map[string]writeEntry
	done     bool
}

// writeEntry is one buffered local mutation.
type writeEntry struct {
	value  []byte
	delete bool
}

// ErrTxRetry is the sentinel returned by Retry. Atomically recognizes it and
// re-runs the transaction body after the watched state changes.
var ErrTxRetry = errors.New("stm: transaction retry requested")

// Begin opens a new transaction against the store.
func (s *Store) Begin() *Tx {
	ver := s.currentVersion()
	s.registerBegin(ver)
	return &Tx{
		store:    s,
		readVer:  ver,
		observed: make(map[string]uint64),
		writes:   make(map[string]writeEntry),
	}
}

// finish marks the transaction closed and releases its snapshot
// registration. It is idempotent.
func (t *Tx) finish() {
	if t.done {
		return
	}
	t.done = true
	t.store.releaseBegin(t.readVer)
}

// Get returns a defensive copy of the value for key, or ErrNotFound if the
// key does not exist. Reads inside one transaction see one consistent
// snapshot: local writes are visible to the transaction itself.
func (t *Tx) Get(key string) ([]byte, error) {
	if t.done {
		return nil, ErrTxClosed
	}
	if w, ok := t.writes[key]; ok {
		if w.delete {
			return nil, ErrNotFound
		}
		return append([]byte(nil), w.value...), nil
	}
	value, ver, exists, deleted := t.store.snapshotRead(key, t.readVer)
	// Record the version the decision was based on, even for absent keys
	// (version 0). A concurrent insert/delete/update is then detected.
	t.observed[key] = ver
	if !exists || deleted {
		return nil, ErrNotFound
	}
	return value, nil
}

// Put buffers value for key. Nothing is shared until commit. If the key was
// never read in this transaction, its current committed version is recorded
// as the write base so blind write/write conflicts are still detected.
func (t *Tx) Put(key string, value []byte) error {
	if t.done {
		return ErrTxClosed
	}
	if _, seen := t.observed[key]; !seen {
		t.observed[key] = t.store.keyVersion(key)
	}
	t.writes[key] = writeEntry{value: append([]byte(nil), value...)}
	return nil
}

// Delete buffers a deletion of key (idempotent if the key is absent).
func (t *Tx) Delete(key string) error {
	if t.done {
		return ErrTxClosed
	}
	if _, seen := t.observed[key]; !seen {
		t.observed[key] = t.store.keyVersion(key)
	}
	t.writes[key] = writeEntry{delete: true}
	return nil
}

// ReadVersion reports the snapshot version the transaction started at.
func (t *Tx) ReadVersion() uint64 { return t.readVer }

// Retry requests a conditional retry: the transaction rolls back (buffered
// writes are discarded) and Atomically blocks until some other transaction
// commits, then re-executes the body. It returns ErrTxRetry; callers normally
// `return tx.Retry()`.
//
// The block is generation-based: commits that happen before the wait starts
// do not count, the first commit after the wait starts wakes the waiter, so
// there are no lost wakeups and no busy loop.
func (t *Tx) Retry() error {
	if t.done {
		return ErrTxClosed
	}
	t.finish()
	return ErrTxRetry
}

// Abort performs an explicit rollback: buffered writes are dropped and err is
// propagated as the transaction result (a non-nil err must be supplied). The
// data store is left exactly as before the transaction began.
func (t *Tx) Abort(err error) error {
	if err == nil {
		err = ErrAborted
	}
	t.finish()
	return err
}

// Commit validates the transaction against all concurrent commits and, if no
// observed key changed, atomically persists and applies the buffered writes.
// On conflict the local writes are discarded unchanged and ErrConflict is
// returned; no reader can ever observe a partial write set.
func (t *Tx) Commit() error {
	if t.done {
		return ErrTxClosed
	}
	defer t.finish()
	return t.store.tryCommit(t)
}
