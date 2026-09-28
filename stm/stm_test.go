package stm

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testSTM(t *testing.T) *STM {
	t.Helper()
	cfg := DefaultConfig()
	cfg.RetryBackoffMin = 0
	cfg.RetryBackoffMax = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestBasicCommitAndRead(t *testing.T) {
	s := testSTM(t)

	if err := s.Do(func(tx *Txn) error {
		tx.Set("a", []byte("hello"))
		tx.Set("b", []byte("world"))
		return nil
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if err := s.Do(func(tx *Txn) error {
		v, err := tx.Get("a")
		if err != nil || string(v) != "hello" {
			t.Fatalf("get a = %q, %v", v, err)
		}
		if _, ok := tx.GetOK("missing"); ok {
			t.Fatal("missing key should not exist")
		}
		// own staged write is visible inside the txn
		tx.Set("a", []byte("changed"))
		v, _ = tx.Get("a")
		if string(v) != "changed" {
			t.Fatalf("own write not visible: %q", v)
		}
		return errors.New("rollback")
	}); !errors.Is(err, errors.New("rollback")) && err.Error() != "rollback" {
		t.Fatalf("expected rollback error, got %v", err)
	}

	// rolled-back write must not be visible
	if v := s.Snapshot()["a"]; string(v) != "hello" {
		t.Fatalf("rollback leaked: a=%q", v)
	}
}

func TestAbortDoesNotLeak(t *testing.T) {
	s := testSTM(t)
	_ = s.Do(func(tx *Txn) error { tx.Set("k", []byte("v0")); return nil })

	err := s.Do(func(tx *Txn) error {
		tx.Set("k", []byte("v1"))
		tx.Abort()
		return nil
	})
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("want ErrAborted, got %v", err)
	}
	err = s.Do(func(tx *Txn) error {
		tx.Set("k", []byte("v2"))
		tx.AbortWith(errors.New("nope"))
		return nil
	})
	if err == nil || err.Error() != "nope" {
		t.Fatalf("want custom abort cause, got %v", err)
	}
	if got := string(s.Snapshot()["k"]); got != "v0" {
		t.Fatalf("abort leaked writes: k=%q", got)
	}
}

func TestDeleteSemantics(t *testing.T) {
	s := testSTM(t)
	_ = s.Do(func(tx *Txn) error { tx.Set("x", []byte("1")); return nil })
	_ = s.Do(func(tx *Txn) error { tx.Delete("x"); return nil })
	_ = s.Do(func(tx *Txn) error {
		if _, ok := tx.GetOK("x"); ok {
			t.Fatal("deleted key appears present")
		}
		if _, err := tx.Get("x"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound after delete, got %v", err)
		}
		tx.Set("x", []byte("2")) // resurrect inside same txn
		if v, _ := tx.GetOK("x"); string(v) != "2" {
			t.Fatalf("resurrect failed: %q", v)
		}
		return nil
	})
	if got := string(s.Snapshot()["x"]); got != "2" {
		t.Fatalf("x=%q", got)
	}
}

func TestConditionalRetry(t *testing.T) {
	s := testSTM(t)
	var attempts int64
	done := make(chan struct{})

	// waiter retries until "ready" appears
	go func() {
		_ = s.Do(func(tx *Txn) error {
			n := atomic.AddInt64(&attempts, 1)
			if n > 100 {
				t.Errorf("retry loop too many times: %d", n)
				return errors.New("stop")
			}
			if _, ok := tx.GetOK("ready"); !ok {
				tx.Retry() // blocks until another txn commits
			}
			v, _ := tx.Get("ready")
			if string(v) != "go" {
				return fmt.Errorf("bad ready value %q", v)
			}
			return nil
		})
		close(done)
	}()

	// Give the waiter time to park, then commit the condition.
	time.Sleep(30 * time.Millisecond)
	if n := atomic.LoadInt64(&attempts); n != 1 {
		t.Fatalf("retry should park, attempts=%d", n)
	}
	if err := s.Do(func(tx *Txn) error {
		tx.Set("unrelated", []byte("x")) // first commit must NOT spuriously wake logic incorrectly; it will wake
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	// unrelated commit re-runs body, which retries again
	if n := atomic.LoadInt64(&attempts); n < 2 {
		t.Fatalf("expected re-run after unrelated commit, attempts=%d", n)
	}
	_ = s.Do(func(tx *Txn) error { tx.Set("ready", []byte("go")); return nil })

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("conditional retry waiter never completed")
	}
}

func TestRetryBudgetCountsConditional(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryBackoffMin = 0
	cfg.RetryBackoffMax = 0
	cfg.RetryWaitMax = 5 * time.Millisecond
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Do(func(tx *Txn) error {
		if _, ok := tx.GetOK("never"); !ok {
			tx.Retry()
		}
		return nil
	})
	if !errors.Is(err, ErrConflict) {
		var ce *ConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("want conflict error, got %T %v", err, err)
		}
	}
}

func TestConcurrentWritersSingleWinnerPerCommit(t *testing.T) {
	s := testSTM(t)
	// initial value
	_ = s.Do(func(tx *Txn) error { tx.Set("counter", []byte("0")); return nil })

	var wg sync.WaitGroup
	var success, conflict int64
	// All goroutines first read the same committed version, then race to write
	// after a barrier. With PolicyAbort (no automatic retry), first-committer-
	// wins means exactly one of the conflicting writers may commit.
	s.cfg.Policy = PolicyAbort
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Do(func(tx *Txn) error {
				if _, err := tx.Get("counter"); err != nil {
					return err
				}
				<-start
				tx.Set("counter", []byte(fmt.Sprintf("w%d", i)))
				return nil
			})
			if err == nil {
				atomic.AddInt64(&success, 1)
			} else {
				var ce *ConflictError
				if errors.As(err, &ce) {
					atomic.AddInt64(&conflict, 1)
					return
				}
				t.Errorf("unexpected err: %v", err)
			}
		}(i)
	}
	time.Sleep(30 * time.Millisecond) // let everyone stage their read
	close(start)
	wg.Wait()
	if success != 1 {
		t.Fatalf("expected exactly 1 winner, got success=%d conflict=%d", success, conflict)
	}
	if conflict != 49 {
		t.Fatalf("expected 49 conflicts, got %d", conflict)
	}
	if v := s.Snapshot()["counter"]; len(v) == 0 || v[0] != 'w' {
		t.Fatalf("bad final value %q", v)
	}
}

func TestConcurrentIncrementAllCommitWithRetry(t *testing.T) {
	s := testSTM(t)
	const n = 100
	_ = s.Do(func(tx *Txn) error { tx.Set("c", []byte{0}); return nil })

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Do(func(tx *Txn) error {
				v, _ := tx.Get("c")
				tx.Set("c", []byte{v[0] + 1})
				return nil
			})
			if err != nil {
				t.Errorf("increment failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := s.Snapshot()["c"][0]; int(got) != n {
		t.Fatalf("lost updates: counter=%d want %d", got, n)
	}
}

func TestReadWriteConflictAbortsCommit(t *testing.T) {
	s := testSTM(t)
	_ = s.Do(func(tx *Txn) error { tx.Set("a", []byte("0")); tx.Set("b", []byte("0")); return nil })

	// T1 reads a, waits, then writes b. T2 commits a meanwhile -> T1's commit
	// must fail validation (even though T1 writes a different key).
	t1started := make(chan struct{})
	var startedOnce sync.Once
	t1done := make(chan error, 1)
	go func() {
		t1done <- s.Do(func(tx *Txn) error {
			va, _ := tx.Get("a")
			firstAttempt := false
			startedOnce.Do(func() {
				firstAttempt = true
				close(t1started)
			})
			if firstAttempt {
				time.Sleep(80 * time.Millisecond) // let T2 commit
			}
			if string(va) != "0" && string(va) != "1" {
				t.Error("read value unexpected")
			}
			tx.Set("b", []byte("from-t1"))
			return nil
		})
	}()

	<-t1started
	time.Sleep(20 * time.Millisecond)
	if err := s.Do(func(tx *Txn) error {
		tx.Set("a", []byte("1"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-t1done:
		if err != nil {
			t.Fatalf("T1 should have retried and committed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("T1 hung")
	}
	// after retry T1 reads a=1 and commits b
	snap := s.Snapshot()
	if string(snap["a"]) != "1" || string(snap["b"]) != "from-t1" {
		t.Fatalf("unexpected final state: a=%q b=%q", snap["a"], snap["b"])
	}
}
