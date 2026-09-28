package stm

import (
	"errors"
	"fmt"
	"time"
)

// ConflictPolicy selects how conflicting transactions are resolved.
type ConflictPolicy int

const (
	// PolicyRetry retries the losing transaction with backoff.
	PolicyRetry ConflictPolicy = iota
	// PolicyAbort immediately aborts the losing transaction with a conflict.
	PolicyAbort
)

// Config configures an STM instance.
type Config struct {
	// MaxRetries bounds the number of times a transaction is re-executed
	// because of a conflict or a Txn.Retry request.
	MaxRetries int
	// RetryBackoffMin/Max bound the full-jitter exponential backoff used
	// after validation conflicts.
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
	// RetryWaitMax bounds how long one Txn.Retry() parks waiting for a
	// relevant commit. On timeout the body re-runs and another retry unit is
	// consumed, bounding resources even if the condition never appears.
	// Zero defaults to 5s.
	RetryWaitMax time.Duration
	// Policy determines what happens on a detected conflict.
	Policy ConflictPolicy
	// DataDir, when non-empty, enables WAL persistence and crash recovery.
	DataDir string
	// Fsync controls whether every commit is fsynced before acknowledgement.
	Fsync bool
}

// DefaultConfig returns a Config with sane defaults and no persistence.
func DefaultConfig() Config {
	return Config{
		MaxRetries:      32,
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 100 * time.Millisecond,
		RetryWaitMax:    5 * time.Second,
		Policy:          PolicyRetry,
		DataDir:         "",
		Fsync:           true,
	}
}

// Validate checks the configuration and normalizes unset values.
func (c *Config) Validate() error {
	if c.MaxRetries < 0 {
		return errors.New("stm: MaxRetries must be >= 0")
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 32
	}
	if c.RetryBackoffMin < 0 || c.RetryBackoffMax < 0 {
		return errors.New("stm: backoff durations must be >= 0")
	}
	if c.RetryBackoffMin == 0 {
		c.RetryBackoffMin = time.Millisecond
	}
	if c.RetryBackoffMax == 0 {
		c.RetryBackoffMax = 100 * time.Millisecond
	}
	if c.RetryBackoffMax < c.RetryBackoffMin {
		return errors.New("stm: RetryBackoffMax must be >= RetryBackoffMin")
	}
	if c.RetryWaitMax < 0 {
		return errors.New("stm: RetryWaitMax must be >= 0")
	}
	if c.RetryWaitMax == 0 {
		c.RetryWaitMax = 5 * time.Second
	}
	switch c.Policy {
	case PolicyRetry, PolicyAbort:
	default:
		return fmt.Errorf("stm: unknown conflict policy %d", c.Policy)
	}
	return nil
}
