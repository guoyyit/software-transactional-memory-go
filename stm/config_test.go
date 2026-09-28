package stm

import (
	"testing"
	"time"
)

func TestBackoffMonotonicAndCapped(t *testing.T) {
	c := Config{
		BaseBackoff:   1 * time.Millisecond,
		MaxBackoff:    100 * time.Millisecond,
		BackoffFactor: 2,
	}
	prev := time.Duration(0)
	for n := 0; n < 10; n++ {
		d := c.nextBackoff(n)
		// Jitter ceiling is 1.25x; never below base.
		if d < 1*time.Millisecond {
			t.Fatalf("backoff %d below base", d)
		}
		// Up to the cap, expected delay (jitter aside) doubles.
		if n < 7 && d < prev && prev < 100*time.Millisecond {
			t.Fatalf("backoff shrank before cap: %d -> %d", prev, d)
		}
		if d > 125*time.Millisecond {
			t.Fatalf("backoff %d above jittered cap", d)
		}
		prev = d
	}
}

func TestDefaultsFilled(t *testing.T) {
	c := Config{}.withDefaults()
	if c.MaxRetries <= 0 || c.BaseBackoff <= 0 || c.MaxBackoff < c.BaseBackoff || c.BackoffFactor < 1 {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.Policy != PolicyRetry {
		t.Fatal("default policy should be retry")
	}
}

func TestCustomConfigPreserved(t *testing.T) {
	c := Config{
		MaxRetries:    3,
		BaseBackoff:   5 * time.Millisecond,
		MaxBackoff:    9 * time.Millisecond,
		BackoffFactor: 1.5,
		Policy:        PolicyAbort,
	}.withDefaults()
	if c.MaxRetries != 3 || c.BaseBackoff != 5*time.Millisecond ||
		c.MaxBackoff != 9*time.Millisecond || c.BackoffFactor != 1.5 || c.Policy != PolicyAbort {
		t.Fatalf("custom config altered: %+v", c)
	}
}
