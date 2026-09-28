package stm

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func getKV(t *testing.T, s *Store, key string) string {
	t.Helper()
	var v string
	if err := s.Atomically(func(tx *Tx) error {
		b, err := tx.Get(key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		v = string(b)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBasicCommitAndRead(t *testing.T) {
	s, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Atomically(func(tx *Tx) error {
		return tx.Put("counter", []byte("10"))
	}); err != nil {
		t.Fatal(err)
	}
	if got := getKV(t, s, "counter"); got != "10" {
		t.Fatalf("got %q want 10", got)
	}

	// Read-your-writes inside one transaction.
	if err := s.Atomically(func(tx *Tx) error {
		if err := tx.Put("k", []byte("a")); err != nil {
			return err
		}
		v, err := tx.Get("k")
		if err != nil || string(v) != "a" {
			t.Fatalf("read-your-writes failed: %q %v", v, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	s, _ := New(DefaultConfig())
	defer s.Close()

	_ = s.Atomically(func(tx *Tx) error { return tx.Put("k", []byte("v")) })
	_ = s.Atomically(func(tx *Tx) error { return tx.Delete("k") })
	if got := getKV(t, s, "k"); got != "" {
		t.Fatalf("expected deleted key, got %q", got)
	}
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("k", []byte("v2")) })
	if got := getKV(t, s, "k"); got != "v2" {
		t.Fatalf("expected v2, got %q", got)
	}
}

func TestAbortLeaksNoWrites(t *testing.T) {
	s, _ := New(DefaultConfig())
	defer s.Close()

	myErr := errors.New("business rule failed")
	err := s.Atomically(func(tx *Tx) error {
		if err := tx.Put("a", []byte("1")); err != nil {
			return err
		}
		if err := tx.Put("b", []byte("2")); err != nil {
			return err
		}
		return tx.Abort(myErr)
	})
	if !errors.Is(err, myErr) {
		t.Fatalf("want myErr, got %v", err)
	}
	if getKV(t, s, "a") != "" || getKV(t, s, "b") != "" {
		t.Fatal("aborted transaction leaked writes")
	}
}

func TestSnapshotIsolationDoesNotSeeConcurrentCommit(t *testing.T) {
	s, _ := New(DefaultConfig())
	defer s.Close()
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("x", []byte("old")) })

	tx := s.Begin()
	// Concurrent committer changes x after tx started.
	_ = s.Atomically(func(tx2 *Tx) error { return tx2.Put("x", []byte("new")) })

	v, err := tx.Get("x")
	if err != nil || string(v) != "old" {
		t.Fatalf("tx must keep reading its snapshot, got %q err=%v", v, err)
	}
	// But commit must be rejected: the observed value no longer holds.
	if err := tx.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict on stale commit, got %v", err)
	}
	// Rejected writes must not appear.
	if getKV(t, s, "x") != "new" {
		t.Fatal("concurrent committed value changed by failed tx")
	}
}

func TestConditionalRetry(t *testing.T) {
	cfg := DefaultConfig()
	s, _ := New(cfg)
	defer s.Close()

	var tries int64
	done := make(chan struct{})
	go func() {
		_ = s.Atomically(func(tx *Tx) error {
			atomic.AddInt64(&tries, 1)
			_, err := tx.Get("ready")
			if errors.Is(err, ErrNotFound) {
				return tx.Retry() // wait until "ready" appears
			}
			return err
		})
		close(done)
	}()

	// While not ready, the waiter must have run once and blocked.
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt64(&tries) != 1 {
		t.Fatalf("expected 1 body execution while blocked, got %d", tries)
	}
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("ready", []byte("1")) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("conditional retry never woke up after commit")
	}
	if atomic.LoadInt64(&tries) != 2 {
		t.Fatalf("expected 2 executions, got %d", tries)
	}
}

func TestRetryDoesNotConsumeRetryBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 1
	s, _ := New(cfg)
	defer s.Close()

	var tries int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Atomically(func(tx *Tx) error {
			if atomic.AddInt64(&tries, 1) >= 5 {
				return nil
			}
			return tx.Retry()
		})
	}()
	// Unblock four times; budget of 1 must not interfere.
	for i := 0; i < 4; i++ {
		time.Sleep(10 * time.Millisecond)
		_ = s.Atomically(func(tx *Tx) error { return tx.Put("tick", []byte("x")) })
	}
	wg.Wait()
}
