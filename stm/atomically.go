package stm

import (
	"errors"
	"fmt"
	"time"
)

// TxFunc is the transaction body executed by Atomically.
//
//   - return nil            -> attempt to commit
//   - return tx.Retry()     -> conditional retry: block until another
//     transaction commits, then run the body again
//   - return tx.Abort(err)  -> explicit rollback; err is returned
//   - return any other err  -> rollback; the error is returned as-is
type TxFunc func(tx *Tx) error

// Atomically runs fn inside a transaction.
//
// Conflicts (ErrConflict) are resolved according to Config.Policy: with
// PolicyRetry the body re-executes after a jittered exponential backoff, at
// most Config.MaxRetries times; with PolicyAbort it fails immediately. A
// Retry() request instead blocks efficiently until a concurrent commit
// changes the store (no busy-wait) and does not count against the conflict
// retry budget. All outcomes preserve atomicity: buffered writes of a failed
// attempt are discarded before the next attempt.
func (s *Store) Atomically(fn TxFunc) error {
	attempt := 0
	for {
		tx := s.Begin()
		genAtBegin := s.currentWakeGen()

		err := fn(tx)
		switch {
		case err == nil:
			if s.beforeCommitAttempt != nil {
				s.beforeCommitAttempt(attempt)
			}
			err = tx.Commit()
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrConflict) {
				return err
			}
			// conflict -> fall through to policy handling below

		case errors.Is(err, ErrTxRetry):
			// Conditional retry: block until the first commit after this
			// attempt started waiting, then re-run. The transaction's
			// buffered writes were never applied.
			if s.closed.Load() {
				return ErrTxClosed
			}
			s.waitForWake(genAtBegin)
			continue

		default:
			// Explicit abort or arbitrary error: nothing was committed.
			tx.finish()
			return err
		}

		// Conflict resolution.
		if s.cfg.Policy == PolicyAbort {
			return ErrConflict
		}
		if attempt >= s.cfg.MaxRetries {
			return fmt.Errorf("%w: gave up after %d retries", ErrRetryExhausted, attempt)
		}
		if s.closed.Load() {
			return ErrTxClosed
		}
		select {
		case <-time.After(s.cfg.nextBackoff(attempt)):
		}
		attempt++
	}
}

// currentWakeGen returns the current commit generation.
func (s *Store) currentWakeGen() uint64 {
	s.retryMu.Lock()
	g := s.wakeGen
	s.retryMu.Unlock()
	return g
}

// waitForWake blocks until the wake generation advances past gen (i.e. until a
// transaction commits after the caller captured gen), or the store closes.
func (s *Store) waitForWake(gen uint64) {
	s.retryMu.Lock()
	for s.wakeGen == gen && !s.closed.Load() {
		s.retryCond.Wait()
	}
	s.retryMu.Unlock()
}
