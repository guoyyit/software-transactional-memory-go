package stm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	recs := []walRecord{
		{Version: 1, Entries: []walEntry{{Key: "a", Value: []byte("hello")}}},
		{Version: 2, Entries: []walEntry{{Key: "b", Value: []byte("world")}, {Key: "a", Delete: true}}},
	}
	for _, r := range recs {
		if err := l.append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var got []walRecord
	l2, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l2.replay(func(r walRecord) error { got = append(got, r); return nil }); err != nil {
		t.Fatal(err)
	}
	l2.Close()
	if len(got) != 2 || got[0].Version != 1 || got[1].Version != 2 {
		t.Fatalf("unexpected replay: %+v", got)
	}
	if string(got[0].Entries[0].Value) != "hello" {
		t.Fatalf("bad value: %q", got[0].Entries[0].Value)
	}
	if !got[1].Entries[1].Delete || got[1].Entries[1].Value != nil {
		t.Fatalf("bad delete entry: %+v", got[1].Entries[1])
	}
}

func TestWALTornTailIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l, _ := openWAL(path)
	if err := l.append(walRecord{Version: 1, Entries: []walEntry{{Key: "k", Value: []byte("v1")}}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Append garbage bytes simulating a torn final write.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0x99, 0x88}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, _ := openWAL(path)
	var n int
	if _, err := l2.replay(func(r walRecord) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	l2.Close()
	if n != 1 {
		t.Fatalf("expected 1 intact record, got %d", n)
	}
}
