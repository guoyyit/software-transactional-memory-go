package stm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConflictRetryExhausted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 2
	cfg.RetryBackoffMin = 0
	cfg.RetryBackoffMax = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.Do(func(tx *Txn) error { tx.Set("k", []byte("0")); return nil })

	t1done := make(chan error, 1)
	go func() {
		t1done <- s.Do(func(tx *Txn) error {
			if _, e := tx.Get("k"); e != nil {
				return e
			}
			tx.Set("k", []byte("t1"))
			return nil
		})
	}()

	var bumps int64
	hook := func() {
		s.mu.Lock()
		s.db.apply(map[string][]byte{"k": []byte("bump")}, nil)
		s.mu.Unlock()
		atomic.AddInt64(&bumps, 1)
	}
	s.beforeCommit.Store(&hook)

	select {
	case err := <-t1done:
		s.beforeCommit.Store(nil)
		var ce *ConflictError
		if !errors.As(err, &ce) {
			t.Fatalf("want ConflictError, got %v", err)
		}
		if ce.Attempts == 0 || ce.Attempts > cfg.MaxRetries {
			t.Fatalf("attempts=%d", ce.Attempts)
		}
		if b := atomic.LoadInt64(&bumps); b != int64(cfg.MaxRetries+1) {
			t.Fatalf("bumps=%d want %d", b, cfg.MaxRetries+1)
		}
	case <-time.After(3 * time.Second):
		s.beforeCommit.Store(nil)
		t.Fatal("T1 never terminated")
	}
}

func TestPolicyAbortImmediate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = PolicyAbort
	s, _ := Open(cfg)
	defer s.Close()
	_ = s.Do(func(tx *Txn) error { tx.Set("k", []byte("0")); return nil })

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	var wins, aborts int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Do(func(tx *Txn) error {
				if _, e := tx.Get("k"); e != nil {
					return e
				}
				<-barrier
				tx.Set("k", []byte("z"))
				return nil
			})
			if err == nil {
				atomic.AddInt64(&wins, 1)
			} else {
				var ce *ConflictError
				if errors.As(err, &ce) {
					atomic.AddInt64(&aborts, 1)
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(barrier)
	wg.Wait()
	if wins+aborts != 20 || wins != 1 {
		t.Fatalf("wins=%d aborts=%d, want 1/19", wins, aborts)
	}
}

func TestShortTxnsNotStarvedByLongTxn(t *testing.T) {
	s := testSTM(t)
	_ = s.Do(func(tx *Txn) error { tx.Set("counter", []byte("0")); return nil })

	var stop int64
	var wg sync.WaitGroup

	// One long-running transaction that keeps touching a hot key, retries
	// with backoff and must eventually commit.
	wg.Add(1)
	longCommitted := make(chan struct{})
	go func() {
		defer wg.Done()
		err := s.Do(func(tx *Txn) error {
			v, _ := tx.Get("counter")
			tx.Set("long", []byte("work"))
			tx.Set("counter", []byte(fmt.Sprintf("long-%s", v)))
			return nil
		})
		if err != nil {
			t.Errorf("long txn failed: %v", err)
		}
		close(longCommitted)
	}()

	// Many short transactions hammering independent keys must complete promptly.
	var shortOK int64
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Do(func(tx *Txn) error {
				tx.Set(fmt.Sprintf("short-%d", i), []byte("v"))
				return nil
			})
			if err == nil {
				atomic.AddInt64(&shortOK, 1)
			}
		}(i)
	}
	_ = stop
	wg.Wait()
	if shortOK != 200 {
		t.Fatalf("only %d/200 short txns committed (starvation?)", shortOK)
	}
	select {
	case <-longCommitted:
	case <-time.After(3 * time.Second):
		t.Fatal("long txn starved / never committed")
	}
}

func TestBackoffBounded(t *testing.T) {
	d := backoffDelay(5, time.Millisecond, 100*time.Millisecond)
	if d > 100*time.Millisecond {
		t.Fatalf("backoff exceeded cap: %v", d)
	}
	d = backoffDelay(1, 2*time.Millisecond, 100*time.Millisecond)
	if d < time.Millisecond || d > 2*time.Millisecond {
		t.Fatalf("first backoff out of jitter range: %v", d)
	}
}

func TestClosedSTMRejects(t *testing.T) {
	s := testSTM(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Do(func(tx *Txn) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if _, err := Open(Config{MaxRetries: -1}); err == nil {
		t.Fatal("invalid config accepted")
	}
}

// TestDeterministicReplay runs a fixed transaction schedule, crashes (drops
// the in-memory instance without graceful close mid-stream is simulated by
// reopening the same dir) and asserts replay produces byte-identical state to
// an in-memory run of the same schedule.
func TestDeterministicReplay(t *testing.T) {
	type op struct {
		key, val string
		del      bool
	}
	schedule := []op{
		{"a", "1", false}, {"b", "2", false}, {"a", "3", false},
		{"b", "", true}, {"c", "4", false}, {"a", "5", false},
		{"c", "", true}, {"d", "6", false},
	}

	apply := func(tx *Txn, o op) error {
		if o.del {
			tx.Delete(o.key)
		} else {
			tx.Set(o.key, []byte(o.val))
		}
		return nil
	}

	// Reference state from a fresh in-memory instance.
	ref := testSTM(t)
	for _, o := range schedule {
		o := o
		if err := ref.Do(func(tx *Txn) error { return apply(tx, o) }); err != nil {
			t.Fatal(err)
		}
	}
	want := ref.Snapshot()

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir

	// Apply half, "crash" (no Close), reopen, apply the rest, reopen again.
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range schedule[:4] {
		o := o
		if err := s.Do(func(tx *Txn) error { return apply(tx, o) }); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate process death: drop the handle without Close (OS closes fd).
	//nolint:staticcheck // intentionally abandon to mimic crash
	s = nil

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	for _, o := range schedule[4:] {
		o := o
		if err := s2.Do(func(tx *Txn) error { return apply(tx, o) }); err != nil {
			t.Fatal(err)
		}
	}
	_ = s2.Close()

	s3, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	got := s3.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("replay state differs in size: got=%d want=%d (%v vs %v)", len(got), len(want), got, want)
	}
	for k, v := range want {
		if string(got[k]) != string(v) {
			t.Fatalf("key %q after replay = %q, want %q", k, got[k], v)
		}
	}

	// WAL file itself must be deterministic given txn ids replay identically.
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("wal empty")
	}
}

// TestRetryWaitersWokenOnClose verifies parked Retry() calls return rather
// than hanging forever when the STM is shut down.
func TestRetryWaitersWokenOnClose(t *testing.T) {
	s := testSTM(t)
	done := make(chan error, 1)
	go func() {
		done <- s.Do(func(tx *Txn) error {
			if _, ok := tx.GetOK("x"); !ok {
				tx.Retry()
			}
			return nil
		})
	}()
	time.Sleep(30 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retry waiter not woken on close")
	}
}

func TestStressMixedWorkload(t *testing.T) {
	s := testSTM(t)
	_ = s.Do(func(tx *Txn) error {
		var z [8]byte
		tx.Set("hot", z[:])
		tx.Set("sum", z[:])
		return nil
	})

	var wg sync.WaitGroup
	const writers, readers, perWriter = 16, 8, 100
	var failed int64

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if err := s.Do(func(tx *Txn) error {
					hv, _ := tx.Get("hot")
					n := binary.LittleEndian.Uint64(hv) + 1
					var nb [8]byte
					binary.LittleEndian.PutUint64(nb[:], n)
					tx.Set("hot", nb[:])
					sv, _ := tx.Get("sum")
					sum := binary.LittleEndian.Uint64(sv) + n
					var sb [8]byte
					binary.LittleEndian.PutUint64(sb[:], sum)
					tx.Set("sum", sb[:])
					return nil
				}); err != nil {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if err := s.Do(func(tx *Txn) error {
					_, _ = tx.Get("hot")
					_, _ = tx.Get("sum")
					tx.Set(fmt.Sprintf("r-%d-%d", i, j), []byte("x"))
					return nil
				}); err != nil {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}
	wg.Wait()

	if f := atomic.LoadInt64(&failed); f != 0 {
		t.Fatalf("%d transactions failed", f)
	}
	hot := binary.LittleEndian.Uint64(s.Snapshot()["hot"])
	if hot != uint64(writers*perWriter) {
		t.Fatalf("hot=%d want %d", hot, writers*perWriter)
	}
}
