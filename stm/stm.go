package stm

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// STM is a local software transactional memory service.
//
// It provides optimistic, multi-object transactions with first-committer-wins
// conflict detection. Multiple goroutines may call Do concurrently.
type STM struct {
	cfg Config
	mu  sync.Mutex // guards db, epoch, closed
	db  *store

	// commitMu serializes the validate -> durable-append -> apply critical
	// section so that committed order is a single total order and matches the
	// order of records in the WAL.
	commitMu sync.Mutex

	txnSeq uint64
	notify *notifier
	log    *wal

	// beforeCommit, when set, runs inside the commit critical section before
	// validation. It is intended for tests (e.g. forcing a deterministic
	// conflict); production code leaves it unset.
	beforeCommit atomic.Pointer[func()]

	closed bool
}

// New creates an in-memory (non-persistent) STM with DefaultConfig.
func New() *STM {
	s, err := Open(DefaultConfig())
	if err != nil {
		panic(err) // default config is always valid
	}
	return s
}

// Open creates an STM with the given configuration. When DataDir is non-empty
// the WAL is opened (created if missing) and all committed transactions found
// in it are replayed before Open returns, so restarted processes observe
// exactly the committed state.
func Open(cfg Config) (*STM, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &STM{
		cfg:    cfg,
		db:     newStore(),
		notify: newNotifier(),
	}
	if cfg.DataDir != "" {
		w, entries, err := openWAL(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		s.log = w
		var maxID uint64
		s.mu.Lock()
		for _, e := range entries {
			s.db.apply(e.sets, e.deletes)
			if e.txnID > maxID {
				maxID = e.txnID
			}
		}
		s.txnSeq = maxID
		s.mu.Unlock()
	}
	return s, nil
}

// readCommitted returns the committed entry for key and marks key as read.
func (s *STM) readCommitted(key string, readKeys map[string]struct{}) *entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	readKeys[key] = struct{}{}
	return s.db.load(key)
}

// writeBase records the base version of a key the first time a transaction
// stages a write to a key it has never read (a blind write). Read keys are
// covered by the begin snapshot check in validate.
func (s *STM) writeBase(key string, readKeys map[string]struct{}, writeVer map[string]uint64) {
	if _, read := readKeys[key]; read {
		return
	}
	if _, tracked := writeVer[key]; tracked {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	writeVer[key] = versionOf(s.db.load(key))
}

// Do executes fn inside a transaction with automatic conflict detection and
// retry.
//
// Semantics:
//   - If fn returns nil the transaction commits: all staged writes become
//     visible atomically and, when persistence is enabled, durably.
//   - If fn returns a non-nil error (other than the control-flow panics below)
//     the transaction is rolled back and that error is returned; staged
//     writes are discarded.
//   - t.Retry() abandons the attempt and re-runs fn only after some other
//     transaction commits; no staged write leaks.
//   - t.Abort() / t.AbortWith(err) rolls back, returning ErrAborted or err.
//   - A validation failure causes a bounded number of automatic retries with
//     full-jitter exponential backoff; exhaustion returns ErrConflict.
func (s *STM) Do(fn func(*Txn) error) (err error) {
	if s.isClosed() {
		return ErrClosed
	}

	var attempts int
	for {
		if s.isClosed() {
			return ErrClosed
		}
		txn := s.beginTxn()
		waitEpoch := s.notify.snapshotEpoch()

		runErr, panicked := s.runBody(fn, txn)
		if panicked != nil {
			return panicked // unexpected panic: propagate, nothing was applied
		}

		switch {
		case runErr == ErrRetry:
			// Conditional retry: block until a commit changes the state or the
			// bounded wait elapses, then re-run and consume another unit.
			if !s.consumeRetry(&attempts) {
				return s.conflictExhausted("", attempts, ErrRetry)
			}
			s.notify.waitUntilChanged(waitEpoch, s.cfg.RetryWaitMax)
			if s.isClosed() {
				return ErrClosed
			}
			continue
		case runErr != nil:
			return runErr // explicit rollback / user error
		}

		conflictKey, committed, err := s.tryCommit(txn)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
		if s.cfg.Policy == PolicyAbort {
			return &ConflictError{Key: conflictKey, Attempts: attempts + 1, Cause: ErrConflict}
		}
		if !s.consumeRetry(&attempts) {
			return s.conflictExhausted(conflictKey, attempts, ErrConflict)
		}
		s.backoff(attempts)
	}
}

func (s *STM) runBody(fn func(*Txn) error, txn *Txn) (err error, unexpectedPanic error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case abortPanic:
				err = v.cause
				if err == nil {
					err = ErrAborted
				}
			case error:
				if isErrRetry(v) {
					err = ErrRetry
					return
				}
				unexpectedPanic = v
			default:
				unexpectedPanic = fmt.Errorf("stm: panic in transaction body: %v", r)
			}
		}
	}()
	return fn(txn), nil
}

func isErrRetry(err error) bool { return err == ErrRetry }

// beginTxn allocates a fresh transaction with a unique id and the current
// global commit clock as its snapshot water mark. Nothing from a previous
// attempt is reused, so discarded writes can never leak.
func (s *STM) beginTxn() *Txn {
	s.mu.Lock()
	s.txnSeq++
	id := s.txnSeq
	beginVer := s.db.ver
	s.mu.Unlock()
	return newTxn(s, id, beginVer)
}

// consumeRetry reports whether another attempt is allowed.
func (s *STM) consumeRetry(attempts *int) bool {
	if *attempts >= s.cfg.MaxRetries {
		return false
	}
	*attempts++
	return true
}

func (s *STM) conflictExhausted(key string, attempts int, cause error) error {
	return &ConflictError{Key: key, Attempts: attempts, Cause: cause}
}

// tryCommit runs the validate -> WAL append -> apply protocol. It returns
// committed=false with the first conflicting key when validation fails.
func (s *STM) tryCommit(t *Txn) (string, bool, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if hook := s.beforeCommit.Load(); hook != nil {
		(*hook)()
	}

	s.mu.Lock()
	if key, ok := s.validate(t); !ok {
		s.mu.Unlock()
		return key, false, nil
	}
	s.mu.Unlock()

	// Durability point: the record is on disk (and fsynced when configured)
	// BEFORE the in-memory state moves, so a crash either has the whole commit
	// or none of it.
	if s.log != nil {
		if err := s.log.appendCommit(t.id, t.writes, t.deletes, s.cfg.Fsync); err != nil {
			return "", false, err
		}
	}

	s.mu.Lock()
	// No other committer can interleave (commitMu), so re-validation would be
	// redundant; apply in the same total order as the WAL.
	s.db.apply(t.writes, t.deletes)
	s.mu.Unlock()
	s.notify.notifyChanged()
	return "", true, nil
}

// validate enforces snapshot isolation at commit:
//
//   - For every read key, the committed version stamped on that key must not
//     be newer than the transaction's begin snapshot (beginVer). A version
//     > beginVer means a concurrent transaction wrote it after this
//     transaction's snapshot, so what it read is stale.
//   - For every blind-written key (never read), its current version must equal
//     the base captured when the write was staged.
//
// Must run under s.mu.
func (s *STM) validate(t *Txn) (string, bool) {
	for key := range t.readKeys {
		if versionOf(s.db.load(key)) > t.beginVer {
			return key, false
		}
	}
	for key, base := range t.writeVer {
		if versionOf(s.db.load(key)) != base {
			return key, false
		}
	}
	return "", true
}

// backoff sleeps a bounded full-jitter exponential delay. Each transaction
// starts the curve anew on its first conflict, so short transactions are not
// penalized by contention created by long ones.
func (s *STM) backoff(attempt int) {
	time.Sleep(backoffDelay(attempt, s.cfg.RetryBackoffMin, s.cfg.RetryBackoffMax))
}

// backoffDelay computes one full-jitter exponential delay. attempt is 1-based.
// The range is [min/2, min(2^(attempt-1)*min, max)].
func backoffDelay(attempt int, min, max time.Duration) time.Duration {
	if min <= 0 && max <= 0 {
		return 0
	}
	upper := min
	for i := 0; i < attempt-1 && upper < max; i++ {
		upper *= 2
	}
	if upper > max {
		upper = max
	}
	lower := min / 2
	if upper <= lower {
		return upper
	}
	return lower + time.Duration(rand.Uint64N(uint64(upper-lower+1)))
}

// Snapshot returns a point-in-time deep copy of all committed key/values.
func (s *STM) Snapshot() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Snapshot()
}

func (s *STM) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close shuts the STM down. Waiting transactions are woken and fail with
// ErrClosed; the WAL is flushed and closed.
func (s *STM) Close() error {
	s.commitMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.commitMu.Unlock()
		return ErrClosed
	}
	s.closed = true
	w := s.log
	s.mu.Unlock()
	s.commitMu.Unlock()

	s.notify.notifyChanged() // wake Retry waiters so they observe closing
	if w != nil {
		return w.Close()
	}
	return nil
}

func versionOf(e *entry) uint64 {
	if e == nil {
		return 0
	}
	return e.version
}
