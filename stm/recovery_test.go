package stm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitPersistAndRecover(t *testing.T) {
	dir := t.TempDir()

	cfg := DefaultConfig()
	cfg.DataDir = dir
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Do(func(tx *Txn) error {
			tx.Set(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
			if i == 3 {
				tx.Delete("k0") // delete of k0 ... note k0 committed at i==0
			}
			return nil
		}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	// Uncommitted write: aborted and must not survive.
	_ = s.Do(func(tx *Txn) error {
		tx.Set("ghost", []byte("nope"))
		tx.Abort()
		return nil
	})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: recover committed state.
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	snap := s2.Snapshot()
	if _, ok := snap["k0"]; ok {
		t.Fatal("deleted k0 recovered as present")
	}
	if string(snap["k9"]) != "v9" {
		t.Fatalf("k9=%q", snap["k9"])
	}
	if string(snap["k1"]) != "v1" {
		t.Fatalf("k1=%q", snap["k1"])
	}
	if _, ok := snap["ghost"]; ok {
		t.Fatal("aborted transaction leaked into recovery")
	}

	// New commits after recovery append after the replayed tail.
	if err := s2.Do(func(tx *Txn) error { tx.Set("after", []byte("1")); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if string(s3.Snapshot()["after"]) != "1" {
		t.Fatal("post-recovery commit lost")
	}
	if string(s3.Snapshot()["k9"]) != "v9" {
		t.Fatal("recovered state lost after second restart")
	}
}

func TestTornTailTruncation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Do(func(tx *Txn) error { tx.Set("a", []byte("1")); return nil })
	_ = s.Do(func(tx *Txn) error { tx.Set("b", []byte("2")); return nil })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "wal.log")
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-write: append a partial frame.
	torn := append(append([]byte{}, good...), []byte{'S', 'T', 'M', 'L', opCommit, 0, 0, 0, 0, 20, 0, 0, 0}...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("recovery with torn tail should succeed: %v", err)
	}
	defer s2.Close()
	snap := s2.Snapshot()
	if string(snap["a"]) != "1" || string(snap["b"]) != "2" {
		t.Fatalf("committed data lost after torn-tail recovery: %v", snap)
	}

	// A commit after recovery must append a clean frame (tail was truncated).
	if err := s2.Do(func(tx *Txn) error { tx.Set("c", []byte("3")); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if string(s3.Snapshot()["c"]) != "3" {
		t.Fatal("commit after torn-tail recovery lost on next restart")
	}
}

func TestEmptyValuesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Do(func(tx *Txn) error {
		tx.Set("empty", []byte{})
		tx.Set("full", []byte("x"))
		return nil
	})
	_ = s.Close()

	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v, ok := s2.Snapshot()["empty"]
	if !ok || len(v) != 0 {
		t.Fatalf("empty value not recovered: ok=%v len=%d", ok, len(v))
	}
}

func TestNoFsyncStillRecoversFromCleanClose(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Fsync = false
	s, _ := Open(cfg)
	_ = s.Do(func(tx *Txn) error { tx.Set("z", []byte("q")); return nil })
	_ = s.Close()
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if string(s2.Snapshot()["z"]) != "q" {
		t.Fatal("write lost without fsync after clean close")
	}
}

func TestWALDecodeCRCRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	s, _ := Open(cfg)
	_ = s.Do(func(tx *Txn) error { tx.Set("a", []byte("1")); return nil })
	_ = s.Close()

	path := filepath.Join(dir, "wal.log")
	raw, _ := os.ReadFile(path)
	// Flip a payload byte (payload begins after 13-byte header).
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(cfg)
	if err == nil {
		s2.Close()
		t.Fatal("expected CRC-detectable corruption to be reported")
	}
	if !errors.Is(err, errCorruptWAL) {
		t.Fatalf("want errCorruptWAL, got %v", err)
	}
}
