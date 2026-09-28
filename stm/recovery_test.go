package stm

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Committed transactions survive a simulated crash (process restart): only
// the WAL exists on disk, memory is rebuilt from scratch.
func TestRecoveryCommittedOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	cfg := DefaultConfig()
	cfg.WALPath = path

	func() {
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Atomically(func(tx *Tx) error {
			if err := tx.Put("a", []byte("1")); err != nil {
				return err
			}
			return tx.Put("b", []byte("2"))
		}); err != nil {
			t.Fatal(err)
		}
		// A transaction that never commits: simulate a crash mid-flight by
		// dropping the store without committing.
		tx := s.Begin()
		_ = tx.Put("a", []byte("999"))
		_ = tx.Put("ghost", []byte("partial"))
		_ = tx.Abort(ErrAborted)
		// Simulate crash: close without any special shutdown commit.
		_ = s.Close()
	}()

	// Restart.
	s2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if got := getKV(t, s2, "a"); got != "1" {
		t.Fatalf("a = %q, want committed 1", got)
	}
	if got := getKV(t, s2, "b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
	if got := getKV(t, s2, "ghost"); got != "" {
		t.Fatalf("uncommitted/aborted write survived crash: %q", got)
	}
	// Store keeps serving transactions after recovery.
	if err := s2.Atomically(func(tx *Tx) error { return tx.Put("c", []byte("3")) }); err != nil {
		t.Fatal(err)
	}
	if got := getKV(t, s2, "c"); got != "3" {
		t.Fatalf("post-recovery commit failed: %q", got)
	}
}

// New commits after recovery continue the global version sequence; restart
// again and every value must remain identical (deterministic replay).
func TestRecoveryVersionSequenceContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	cfg := DefaultConfig()
	cfg.WALPath = path

	s, _ := New(cfg)
	for i := 1; i <= 5; i++ {
		v := i
		_ = s.Atomically(func(tx *Tx) error {
			return tx.Put("seq", []byte(strconv.Itoa(v)))
		})
	}
	_ = s.Close()

	s2, _ := New(cfg)
	// Conflicting after recovery: two txs, one wins, loser retries. Verifies
	// the clock resumed (otherwise versions would collide).
	for i := 0; i < 3; i++ {
		if err := s2.Atomically(func(tx *Tx) error {
			b, err := tx.Get("seq")
			if err != nil {
				return err
			}
			n, _ := strconv.Atoi(string(b))
			return tx.Put("seq", []byte(strconv.Itoa(n+1)))
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = s2.Close()

	s3, _ := New(cfg)
	defer s3.Close()
	if got := getKV(t, s3, "seq"); got != "8" {
		t.Fatalf("seq = %q, want 8 after deterministic replay", got)
	}
}

// Deterministic replay: take one WAL, recover from it twice into separate
// stores; all keys/values and the global clock must be identical.
func TestDeterministicReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	cfg := DefaultConfig()
	cfg.WALPath = path

	s, _ := New(cfg)
	for i := 0; i < 20; i++ {
		v := i
		var err error
		if v%3 == 0 {
			err = s.Atomically(func(tx *Tx) error {
				return tx.Put("even", []byte(strconv.Itoa(v)))
			})
		} else {
			err = s.Atomically(func(tx *Tx) error {
				if err := tx.Put("k"+strconv.Itoa(v), []byte(strconv.Itoa(v))); err != nil {
					return err
				}
				if v%5 == 1 {
					return tx.Delete("k" + strconv.Itoa(v-1))
				}
				return nil
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()

	snap := func() map[string]string {
		st, _ := New(cfg)
		defer st.Close()
		out := make(map[string]string)
		st.mu.RLock()
		for k, o := range st.objects {
			latest := o.latest()
			if !latest.deleted {
				out[k] = string(latest.value)
			} else {
				out[k] = "<deleted>"
			}
		}
		out["__clock__"] = strconv.FormatUint(st.clock, 10)
		st.mu.RUnlock()
		return out
	}
	a, b := snap(), snap()
	if len(a) != len(b) {
		t.Fatalf("replay divergence: %d vs %d keys", len(a), len(b))
	}
	for k := range a {
		if a[k] != b[k] {
			t.Fatalf("deterministic replay mismatch on %q: %q vs %q", k, a[k], b[k])
		}
	}
}

// A torn final frame (crash during append) is discarded on recovery and the
// file is physically trimmed; the next commit appends right after the last
// good frame and survives a further restart.
func TestRecoveryTrimsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	cfg := DefaultConfig()
	cfg.WALPath = path

	s, _ := New(cfg)
	_ = s.Atomically(func(tx *Tx) error { return tx.Put("good", []byte("v1")) })
	_ = s.Close()

	info, _ := os.Stat(path)
	cleanSize := info.Size()

	// Append a torn frame: header claiming 64 bytes followed by 6 garbage
	// bytes, as if the process died mid-write.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.Write([]byte{0x00, 0x00, 0x00, 0x40, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06})
	_ = f.Close()

	s2, _ := New(cfg)
	if got := getKV(t, s2, "good"); got != "v1" {
		t.Fatalf("good commit lost: %q", got)
	}
	_ = s2.Atomically(func(tx *Tx) error { return tx.Put("after", []byte("v2")) })
	_ = s2.Close()

	if info, _ := os.Stat(path); info.Size() < cleanSize {
		t.Fatal("WAL shrank below last-good size")
	}

	// Restart once more: both commits present, torn frame never resurfaced.
	s3, _ := New(cfg)
	defer s3.Close()
	if got := getKV(t, s3, "good"); got != "v1" {
		t.Fatalf("good = %q", got)
	}
	if got := getKV(t, s3, "after"); got != "v2" {
		t.Fatalf("after = %q", got)
	}
}

// Every commit is durable before Atomically returns: kill the process
// model (close) immediately after return and re-open; data must be there.
func TestDurableBeforeReturn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	cfg := DefaultConfig()
	cfg.WALPath = path
	for round := 0; round < 10; round++ {
		s, _ := New(cfg)
		err := s.Atomically(func(tx *Tx) error {
			return tx.Put("round", []byte(strconv.Itoa(round)))
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Close() // "crash" immediately
	}
	s, _ := New(cfg)
	defer s.Close()
	if got := getKV(t, s, "round"); got != "9" {
		t.Fatalf("durable value = %q, want 9", got)
	}
}
