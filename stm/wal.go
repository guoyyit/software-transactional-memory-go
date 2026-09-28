package stm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// walFrame layout (little endian):
//
//	[4]byte magic "STML"
//	[1]byte kind (opCommit)
//	[4]byte CRC32 IEEE over (kind || payload)
//	[4]byte payload length N
//	[N]byte payload
//
// Commit payload:
//
//	[8]byte txn id
//	[4]byte number of sets  M
//	M * ([4]byte keylen, key, [4]byte vallen, value)   vallen may be 0
//	[4]byte number of deletes D
//	D * ([4]byte keylen, key)
const (
	walFrameHeader = 4 + 1 + 4 + 4
	walMaxPayload  = 256 * 1024 * 1024

	// opCommit is the only WAL record kind: a transaction commit.
	opCommit byte = 1
)

var walMagic = [4]byte{'S', 'T', 'M', 'L'}

var (
	errCorruptWAL = errors.New("stm: corrupt wal record (checksum mismatch)")
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// walEntry is one decoded commit record from the WAL.
type walEntry struct {
	txnID   uint64
	sets    map[string][]byte
	deletes map[string]bool
}

// wal is an append-only transaction log backed by a single file.
type wal struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// openWAL opens (creating if needed) the log in dir and replays it.
// A torn tail from a crash is truncated. Entries are returned in order.
func openWAL(dir string) (*wal, []walEntry, error) {
	if dir == "" {
		return nil, nil, errors.New("stm: wal requires a non-empty data dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("stm: create data dir: %w", err)
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("stm: open wal: %w", err)
	}

	raw, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("stm: read wal: %w", err)
	}

	var entries []walEntry
	valid, err := decodeWAL(raw, &entries)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if valid < int64(len(raw)) {
		// Drop a torn tail left by a crashed process.
		if err := f.Truncate(valid); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("stm: truncate torn wal: %w", err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("stm: seek wal: %w", err)
	}
	return &wal{f: f, path: path}, entries, nil
}

// appendCommit durably appends one committed transaction record.
func (w *wal) appendCommit(id uint64, sets map[string][]byte, deletes map[string]bool, fsync bool) error {
	payload, err := encodeCommit(id, sets, deletes)
	if err != nil {
		return err
	}
	frame := make([]byte, walFrameHeader+len(payload))
	copy(frame[0:4], walMagic[:])
	frame[4] = opCommit
	binary.LittleEndian.PutUint32(frame[5:9], crc32.Checksum(append([]byte{opCommit}, payload...), crcTable))
	binary.LittleEndian.PutUint32(frame[9:13], uint32(len(payload)))
	copy(frame[13:], payload)

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("stm: write wal: %w", err)
	}
	if fsync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("stm: fsync wal: %w", err)
		}
	}
	return nil
}

// Close releases the log file.
func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func encodeCommit(id uint64, sets map[string][]byte, deletes map[string]bool) ([]byte, error) {
	var b []byte
	var tmp [4]byte
	b = binary.LittleEndian.AppendUint64(b, id)

	binary.LittleEndian.PutUint32(tmp[:], uint32(len(sets)))
	b = append(b, tmp[:]...)
	for k, v := range sets {
		if len(k) > 0xffffffff || len(v) > 0xffffffff {
			return nil, errors.New("stm: wal key/value too large")
		}
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(k)))
		b = append(b, tmp[:]...)
		b = append(b, k...)
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(v)))
		b = append(b, tmp[:]...)
		b = append(b, v...)
	}

	binary.LittleEndian.PutUint32(tmp[:], uint32(len(deletes)))
	b = append(b, tmp[:]...)
	for k := range deletes {
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(k)))
		b = append(b, tmp[:]...)
		b = append(b, k...)
	}
	return b, nil
}

// decodeWAL parses frames and reports the number of bytes belonging to
// complete, valid frames. Garbage or a partial frame at the tail ends parsing.
func decodeWAL(raw []byte, entries *[]walEntry) (int64, error) {
	pos := 0
	for pos < len(raw) {
		if len(raw)-pos < walFrameHeader {
			break
		}
		hdr := raw[pos : pos+walFrameHeader]
		if string(hdr[0:4]) != string(walMagic[:]) {
			return 0, fmt.Errorf("stm: wal magic mismatch at offset %d", pos)
		}
		if hdr[4] != opCommit {
			return 0, fmt.Errorf("stm: unknown wal record kind %d at offset %d", hdr[4], pos)
		}
		wantCRC := binary.LittleEndian.Uint32(hdr[5:9])
		n := int(binary.LittleEndian.Uint32(hdr[9:13]))
		if n > walMaxPayload {
			return 0, fmt.Errorf("stm: wal frame too large at offset %d", pos)
		}
		if len(raw)-pos-walFrameHeader < n {
			break // torn tail
		}
		payload := raw[pos+walFrameHeader : pos+walFrameHeader+n]
		if crc32.Checksum(append([]byte{opCommit}, payload...), crcTable) != wantCRC {
			return 0, errCorruptWAL // full-length frame with bad CRC: real corruption
		}
		e, err := decodeCommit(payload)
		if err != nil {
			return 0, fmt.Errorf("stm: wal decode at offset %d: %w", pos, err)
		}
		*entries = append(*entries, e)
		pos += walFrameHeader + n
	}
	return int64(pos), nil
}

func decodeCommit(p []byte) (walEntry, error) {
	var e walEntry
	if len(p) < 12 {
		return e, errors.New("commit record too short")
	}
	e.txnID = binary.LittleEndian.Uint64(p[0:8])
	pos := 8

	m := int(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	e.sets = make(map[string][]byte, m)
	for i := 0; i < m; i++ {
		k, v, n, err := readKV(p, pos)
		if err != nil {
			return e, err
		}
		e.sets[k] = v
		pos = n
	}

	if len(p)-pos < 4 {
		return e, errors.New("commit record truncated at delete count")
	}
	d := int(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	e.deletes = make(map[string]bool, d)
	for i := 0; i < d; i++ {
		if len(p)-pos < 4 {
			return e, errors.New("commit record truncated at delete key")
		}
		kl := int(binary.LittleEndian.Uint32(p[pos : pos+4]))
		pos += 4
		if len(p)-pos < kl {
			return e, errors.New("commit record truncated at delete key bytes")
		}
		e.deletes[string(p[pos:pos+kl])] = true
		pos += kl
	}
	if pos != len(p) {
		return e, errors.New("commit record has trailing bytes")
	}
	return e, nil
}

func readKV(p []byte, pos int) (key string, val []byte, next int, err error) {
	if len(p)-pos < 4 {
		return "", nil, 0, errors.New("commit record truncated at key length")
	}
	kl := int(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	if len(p)-pos < kl {
		return "", nil, 0, errors.New("commit record truncated at key")
	}
	key = string(p[pos : pos+kl])
	pos += kl
	if len(p)-pos < 4 {
		return "", nil, 0, errors.New("commit record truncated at value length")
	}
	vl := int(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	if len(p)-pos < vl {
		return "", nil, 0, errors.New("commit record truncated at value")
	}
	val = make([]byte, vl)
	copy(val, p[pos:pos+vl])
	pos += vl
	return key, val, pos, nil
}
