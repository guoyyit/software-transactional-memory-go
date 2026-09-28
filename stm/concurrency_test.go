package stm

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many transactions write the SAME key concurrently with a read-modify-write
// body. Exactly one attempt per increment can win; the rest must retry, and
// the final value must reflect every successful transaction -- proving
// conflict detection plus automatic retry lose no updates.
func TestConcurrentRMWSameKeyNoLostUpdate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 500
	s, _ := New(cfg)
	defer s.Close()
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("n", []byte("0")) })

	const writers = 32
	const perWriter = 50
	var wg sync.WaitGroup
	var conflicts int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				err := s.Atomically(func(tx *Tx) error {
					b, err := tx.Get("n")
					if err != nil {
						return err
					}
					n, _ := strconv.Atoi(string(b))
					return tx.Put("n", []byte(strconv.Itoa(n+1)))
				})
				if err != nil {
					if errors.Is(err, ErrConflict) {
						atomic.AddInt64(&conflicts, 1)
					}
					i-- // retry this iteration ourselves if aborted-policy style
					if errors.Is(err, ErrRetryExhausted) {
						t.Errorf("retry exhausted: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	final := getKV(t, s, "n")
	want := writers * perWriter
	if final != strconv.Itoa(want) {
		t.Fatalf("lost updates: got %s want %d (conflicts=%d)", final, want, conflicts)
	}
}

// Blind (write-only) transactions on the same key must still serialize:
// concurrent puts race, exactly one wins per commit round; losers get a
// definite conflict/retry result, never silent both-apply.
func TestConcurrentBlindWritesSingleWinner(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 100
	s, _ := New(cfg)
	defer s.Close()

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Atomically(func(tx *Tx) error {
				return tx.Put("same", []byte(fmt.Sprintf("w%d", i)))
			})
		}(i)
	}
	wg.Wait()

	// Every transaction ultimately commits (serialized via retries), and the
	// key equals exactly one writer's value: a whole write set can never be
	// half-applied, so the result cannot be a mix.
	for i, e := range errs {
		if e != nil {
			t.Fatalf("writer %d failed: %v", i, e)
		}
	}
	v := getKV(t, s, "same")
	if len(v) < 2 || v[0] != 'w' {
		t.Fatalf("unexpected final value %q", v)
	}
}

// Readers must always observe a consistent value (no torn intermediate
// state) while many writers commit multi-key updates atomically.
func TestAtomicMultiKeyVisibility(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 200
	s, _ := New(cfg)
	defer s.Close()

	const pairs = 10
	for i := 0; i < pairs; i++ {
		k := fmt.Sprintf("k%d", i)
		_ = s.Atomically(func(tx *Tx) error { return tx.Put(k, []byte("0")) })
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writers update all keys to the same version number in one tx.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 1; ; v++ {
			select {
			case <-stop:
				return
			default:
			}
			err := s.Atomically(func(tx *Tx) error {
				for i := 0; i < pairs; i++ {
					if err := tx.Put(fmt.Sprintf("k%d", i), []byte(strconv.Itoa(v))); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("writer: %v", err)
				return
			}
		}
	}()
	// Readers see one snapshot: all keys equal.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.Atomically(func(tx *Tx) error {
					var first string
					for i := 0; i < pairs; i++ {
						b, err := tx.Get(fmt.Sprintf("k%d", i))
						if err != nil {
							return err
						}
						if first == "" {
							first = string(b)
						} else if string(b) != first {
							t.Errorf("inconsistent snapshot: k0=%s k%d=%s", first, i, b)
						}
					}
					return nil
				})
				runtime.Gosched()
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Retry cap: a deterministic conflict injected before every commit attempt
// forces the transaction to retry exactly MaxRetries+1 times and then return
// ErrRetryExhausted; its buffered writes must never apply.
func TestRetryLimitExhausted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 2
	cfg.BaseBackoff = time.Microsecond
	s, _ := New(cfg)
	defer s.Close()
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("hot", []byte("0")) })

	// Adversarial committer racing every single attempt (uses Begin/Commit
	// directly so it does not re-enter the Atomically hook).
	s.beforeCommitAttempt = func(attempt int) {
		t2 := s.Begin()
		b, err := t2.Get("hot")
		if err != nil {
			t2.Abort(err)
			return
		}
		n, _ := strconv.Atoi(string(b))
		_ = t2.Put("hot", []byte(strconv.Itoa(n+1)))
		if err := t2.Commit(); err != nil {
			t.Errorf("adversarial commit failed: %v", err)
		}
	}

	var attempts int64
	err := s.Atomically(func(tx *Tx) error {
		atomic.AddInt64(&attempts, 1)
		if _, err := tx.Get("hot"); err != nil {
			return err
		}
		return tx.Put("victim-marker", []byte("x"))
	})
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("want ErrRetryExhausted, got %v", err)
	}
	if got := atomic.LoadInt64(&attempts); got != 3 {
		t.Fatalf("want 1 initial + 2 retry attempts, got %d", got)
	}
	if getKV(t, s, "victim-marker") != "" {
		t.Fatal("exhausted transaction leaked buffered writes")
	}
}

// PolicyAbort: the first conflict is reported immediately, no retries.
func TestPolicyAbortReturnsConflictImmediately(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Policy = PolicyAbort
	s, _ := New(cfg)
	defer s.Close()
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("k", []byte("a")) })

	tx := s.Begin()
	_, _ = tx.Get("k")
	_ = s.Atomically(func(tx2 *Tx) error { return tx2.Put("k", []byte("b")) })
	if err := tx.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

// Short transactions must not starve while long transactions run: a long
// reader-writer loops while short putters keep committing. We assert a high
// fraction of short txs succeed in bounded time.
func TestShortTxNotStarvedByLongTx(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 100
	s, _ := New(cfg)
	defer s.Close()
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("g", []byte("0")) })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// "Long" transactions: heavy local work touching a disjoint key each
	// round plus a read of the contended key.
	for l := 0; l < 4; l++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.Atomically(func(tx *Tx) error {
					_, _ = tx.Get("g")
					end := i + 200
					for ; i < end; i++ {
						_ = tx.Put(fmt.Sprintf("long-%d", i), []byte("x"))
					}
					return nil
				})
			}
		}()
	}
	var shortOK, shortFail int64
	for sh := 0; sh < 4; sh++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				err := s.Atomically(func(tx *Tx) error {
					b, err := tx.Get("g")
					if err != nil {
						return err
					}
					n, _ := strconv.Atoi(string(b))
					return tx.Put("g", []byte(strconv.Itoa(n+1)))
				})
				if err == nil {
					atomic.AddInt64(&shortOK, 1)
				} else {
					atomic.AddInt64(&shortFail, 1)
				}
			}
		}()
	}
	// Give the mix a bounded run, then stop producers and wait for exit.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&shortOK) < 50 {
		t.Fatalf("short txs appear starved: ok=%d fail=%d", shortOK, shortFail)
	}
}
