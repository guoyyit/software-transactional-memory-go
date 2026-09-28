package stm

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the STM.
var (
	// ErrAborted is returned when a transaction is explicitly rolled back
	// via Txn.Abort or by returning ErrAborted from the transaction body.
	ErrAborted = errors.New("stm: transaction aborted")
	// ErrRetry is used inside transaction bodies via Txn.Retry to request a
	// conditional retry; it never escapes a successful Do call.
	ErrRetry = errors.New("stm: transaction retry requested")
	// ErrConflict is returned when a transaction cannot commit after the
	// configured maximum number of retries.
	ErrConflict = errors.New("stm: conflict, retries exhausted")
	// ErrNotFound is returned by Txn.Get (strict variant) when a key has no
	// committed value.
	ErrNotFound = errors.New("stm: key not found")
	// ErrClosed is returned when the STM has been closed.
	ErrClosed = errors.New("stm: closed")
)

// ConflictError describes a failed commit due to a read/write conflict.
type ConflictError struct {
	Key      string
	Attempts int
	Cause    error
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("stm: conflict on key %q after %d attempt(s): %v", e.Key, e.Attempts, e.Cause)
}
func (e *ConflictError) Unwrap() error { return e.Cause }
