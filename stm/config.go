package stm

import (
	"math/rand"
	"time"
)

// ConflictPolicy decides what happens when two transactions conflict.
type ConflictPolicy int

const (
	// PolicyRetry retries the losing transaction with backoff.
	PolicyRetry ConflictPolicy = iota
	// PolicyAbort aborts the losing transaction immediately with ErrConflict.
	PolicyAbort
)

// Config configures a Store: retry limits, backoff and conflict policy.
//
// The zero value is not usable directly; use DefaultConfig or New, which
// fills unset fields with defaults.
type Config struct {
	// MaxRetries is the maximum number of automatic retries performed by
	// Atomically after a conflict (0 => a default value is applied; to run a
	// body exactly once use Begin/Commit directly).
	MaxRetries int
	// BaseBackoff is the initial delay before the first retry.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential backoff delay.
	MaxBackoff time.Duration
	// BackoffFactor multiplies the delay after each retry (>=1).
	BackoffFactor float64
	// Policy selects retry vs immediate abort on conflict.
	Policy ConflictPolicy
	// WALPath, when non-empty, enables durable commit logging and recovery.
	WALPath string
}

// DefaultConfig returns sensible defaults: up to 16 retries, exponential
// backoff from 1ms capped at 100ms with jitter, retry-based conflict policy.
func DefaultConfig() Config {
	return Config{
		MaxRetries:    16,
		BaseBackoff:   1 * time.Millisecond,
		MaxBackoff:    100 * time.Millisecond,
		BackoffFactor: 2,
		Policy:        PolicyRetry,
	}
}

// withDefaults returns c with every unset field replaced by the default.
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxRetries > 0 {
		d.MaxRetries = c.MaxRetries
	}
	if c.BaseBackoff > 0 {
		d.BaseBackoff = c.BaseBackoff
	}
	if c.MaxBackoff > 0 {
		d.MaxBackoff = c.MaxBackoff
	}
	if c.BackoffFactor > 1 {
		d.BackoffFactor = c.BackoffFactor
	}
	if d.MaxBackoff < d.BaseBackoff {
		d.MaxBackoff = d.BaseBackoff
	}
	d.Policy = c.Policy
	d.WALPath = c.WALPath
	return d
}

// nextBackoff computes the delay before retry attempt n (0-based).
// It grows exponentially, is capped at MaxBackoff and carries up to 25%
// random jitter. The jitter decorrelates competing transactions so that
// short transactions are not repeatedly beaten to the lock by long ones and
// no busy retry loop can burn CPU under sustained contention.
func (c Config) nextBackoff(n int) time.Duration {
	backoff := float64(c.BaseBackoff)
	factor := c.BackoffFactor
	if factor < 1 {
		factor = 1
	}
	for i := 0; i < n; i++ {
		backoff *= factor
		if backoff >= float64(c.MaxBackoff) {
			backoff = float64(c.MaxBackoff)
			break
		}
	}
	jitter := 1.0 + rand.Float64()*0.25 // [1.0, 1.25)
	d := time.Duration(backoff * jitter)
	if d > c.MaxBackoff*5/4 {
		d = c.MaxBackoff * 5 / 4
	}
	return d
}
