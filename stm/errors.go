package stm

import "errors"

// Errors that may be returned by transaction execution.
var (
	// ErrConflict means the transaction's read set was invalidated by a
	// concurrent committer; the caller may retry.
	ErrConflict = errors.New("stm: transaction conflict")
	// ErrAborted is returned when a transaction is explicitly rolled back.
	ErrAborted = errors.New("stm: transaction aborted")
	// ErrRetryExhausted is returned when the retry limit is exceeded.
	ErrRetryExhausted = errors.New("stm: retry limit exhausted")
	// ErrNotFound is returned when reading a key that was never committed.
	ErrNotFound = errors.New("stm: key not found")
	// ErrTxClosed is returned when using a transaction after it finished.
	ErrTxClosed = errors.New("stm: transaction closed")
	// ErrCorruptLog is returned when the WAL fails checksum validation.
	ErrCorruptLog = errors.New("stm: corrupt transaction log")
)
