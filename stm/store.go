package stm

import (
	"sort"
	"sync"
	"sync/atomic"
)

// versionedValue is one historical revision of a shared object.
type versionedValue struct {
	value   []byte
	version uint64 // version of the transaction that wrote it
	deleted bool
}

// object is the version history of one shared object (revisions ascending).
type object struct {
	versions []versionedValue
}

// atVersion returns the newest revision with version <= at (snapshot read).
// ok=false means the key did not exist yet in that snapshot.
func (o *object) atVersion(at uint64) (versionedValue, bool) {
	i := sort.Search(len(o.versions), func(i int) bool {
		return o.versions[i].version > at
	})
	if i == 0 {
		return versionedValue{}, false
	}
	return o.versions[i-1], true
}

func (o *object) latest() versionedValue { return o.versions[len(o.versions)-1] }

// Store holds shared objects and coordinates transactions.
//
// Concurrency model: transaction bodies run fully in parallel and touch no
// shared state (reads take a short RLock, writes are buffered). Only Commit
// takes the exclusive lock, validates the read/write sets, appends the commit
// record to the WAL and applies it -- all as one atomic step. Consequently a
// long-running transaction never blocks short ones, and of several
// transactions writing the same key exactly one can win the commit race.
type Store struct {
	mu      sync.RWMutex
	objects map[string]*object
	clock   uint64

	cfg Config
	wal *wal

	// activeMu guards beginVersions: the snapshot versions of all currently
	// open transactions. It drives history GC (a revision is only removable
	// when no live transaction's snapshot can still see it).
	activeMu      sync.Mutex
	beginVersions []uint64

	// wakeGen + retryCond implement Retry(): every successful commit bumps
	// the generation and broadcasts; retriers wake on the first commit after
	// they started waiting (no lost wakeups, no busy spinning).
	retryMu   sync.Mutex
	retryCond *sync.Cond
	wakeGen   uint64

	closed atomic.Bool

	// beforeCommitAttempt, when set, runs inside Atomically after the body
	// returns nil and before commit is attempted. It is a deterministic test
	// seam (e.g. force the read set to go stale); production code leaves it
	// nil. attempt is the 0-based attempt number.
	beforeCommitAttempt func(attempt int)
}

// New creates a Store, recovering committed state from the WAL when
// Config.WALPath is set. Replay is deterministic: records are applied in
// on-disk order and yields identical state on every restart.
func New(cfg Config) (*Store, error) {
	c := cfg.withDefaults()
	s := &Store{
		objects: make(map[string]*object),
		cfg:     c,
	}
	s.retryCond = sync.NewCond(&s.retryMu)
	// recoverFromWAL opens the WAL (if configured), replays durable commits
	// and truncates any torn tail before the store serves transactions.
	if err := s.recoverFromWAL(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close flushes and releases the WAL and wakes any Retry() waiters. After
// Close the store must not be used.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.retryMu.Lock()
	s.retryCond.Broadcast()
	s.retryMu.Unlock()
	return s.wal.Close()
}

func (s *Store) registerBegin(ver uint64) {
	s.activeMu.Lock()
	s.beginVersions = append(s.beginVersions, ver)
	s.activeMu.Unlock()
}

// releaseBegin removes one previously registered begin version.
func (s *Store) releaseBegin(ver uint64) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	vs := s.beginVersions
	for i, v := range vs {
		if v == ver {
			vs[i] = vs[len(vs)-1]
			s.beginVersions = vs[:len(vs)-1]
			return
		}
	}
}

// oldestSnapshot is the minimum begin version among live transactions, or the
// current clock when there are none. Called with s.mu held (during commit);
// the value is a conservative GC floor.
func (s *Store) oldestSnapshot() uint64 {
	s.activeMu.Lock()
	min := s.clock
	for _, v := range s.beginVersions {
		if v < min {
			min = v
		}
	}
	s.activeMu.Unlock()
	return min
}

// currentVersion returns the global commit version.
func (s *Store) currentVersion() uint64 {
	s.mu.RLock()
	v := s.clock
	s.mu.RUnlock()
	return v
}

// keyVersion returns the current committed version of key: 0 if the key has
// never existed, or the version of the latest write (including a delete).
func (s *Store) keyVersion(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if o, ok := s.objects[key]; ok {
		return o.latest().version
	}
	return 0
}

// snapshotRead returns the value visible to a transaction whose snapshot is
// atVersion. The returned byte slice is a defensive copy.
//   - !exists: key did not exist yet in that snapshot
//   - exists && deleted: key existed but was deleted (revision recorded)
//   - exists && !deleted: value is valid
func (s *Store) snapshotRead(key string, atVersion uint64) (value []byte, version uint64, exists, deleted bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.objects[key]
	if !ok {
		return nil, 0, false, false
	}
	rev, ok := o.atVersion(atVersion)
	if !ok {
		return nil, 0, false, false
	}
	if rev.deleted {
		return nil, rev.version, true, true
	}
	return append([]byte(nil), rev.value...), rev.version, true, false
}

// tryCommit validates t against current state and, if valid, durably commits
// it. It returns ErrConflict when validation fails; the caller's buffered
// writes are discarded untouched in that case.
//
// The whole validate -> WAL append -> apply sequence runs under the exclusive
// lock, so the durable order and the in-memory order can never diverge.
func (s *Store) tryCommit(t *Tx) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validation: every key the transaction observed (read or used as the
	// base of a write) must still carry the exact version it observed. Any
	// concurrent commit touching one of those keys bumps the version and is
	// therefore detected -- including blind write/write conflicts, because
	// Put registers the key's base version in the validated set.
	for key, observed := range t.observed {
		cur := uint64(0)
		if o, ok := s.objects[key]; ok {
			cur = o.latest().version
		}
		if cur != observed {
			return ErrConflict
		}
	}

	// Read-only transaction: validation is all it needs; do not burn a
	// version bump or an fsync on a commit that changes nothing.
	if len(t.writes) == 0 {
		return nil
	}

	newVersion := s.clock + 1
	entries := make([]walEntry, 0, len(t.writes))
	for key, w := range t.writes {
		entries = append(entries, walEntry{
			Key:    key,
			Value:  append([]byte(nil), w.value...),
			Delete: w.delete,
		})
	}
	rec := walRecord{Version: newVersion, Entries: entries}

	// Durability first: once append+fsync returns, the commit survives a
	// crash. Applying to memory afterwards means a crash between the two is
	// indistinguishable from a crash before the commit from the outside
	// (recovery redoes it), and there is no window where uncommitted data is
	// visible in memory.
	if err := s.wal.append(rec); err != nil {
		return err
	}

	for key, w := range t.writes {
		o := s.objects[key]
		if o == nil {
			o = &object{}
			s.objects[key] = o
		}
		o.versions = append(o.versions, versionedValue{
			value:   append([]byte(nil), w.value...),
			version: newVersion,
			deleted: w.delete,
		})
	}
	s.clock = newVersion

	// Garbage-collect revisions that no live snapshot can read, for the keys
	// this commit touched (only those grew).
	floor := s.oldestSnapshot()
	for key := range t.writes {
		s.objects[key].gc(floor)
	}

	// Wake conditional-retry waiters: broadcast after the state is visible.
	s.signalWaiters()
	return nil
}

// gc drops revisions strictly older than floor, keeping the newest revision
// with version <= floor so a snapshot at floor still resolves correctly.
func (o *object) gc(floor uint64) {
	keep := sort.Search(len(o.versions), func(i int) bool {
		return o.versions[i].version > floor
	})
	if keep <= 1 {
		return // keep the floor-visible revision (index keep-1) and everything newer
	}
	o.versions = append([]versionedValue(nil), o.versions[keep-1:]...)
}

// signalWaiters bumps the wake generation and broadcasts to Retry() waiters.
// Called with s.mu held; retryMu is a leaf lock (never acquired before mu in
// the opposite order elsewhere), so the nesting is deadlock-free.
func (s *Store) signalWaiters() {
	s.retryMu.Lock()
	s.wakeGen++
	s.retryCond.Broadcast()
	s.retryMu.Unlock()
}
