package stm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// On-disk layout (all integers big-endian):
//
//   frame:   [ payloadLen uint32 ][ payload ... ]
//   payload: [ magic uint32 ][ rectype uint8 ][ version uint64 ]
//            [ entryCount uint32 ] entries... [ crc32 uint32 ]
//   entry:   [ keyLen uint32 ][ key bytes ][ valueLen int32 ]
//            [ value bytes ][ delete uint8 ]
//
// A frame is only written for a fully committed transaction. The CRC covers
// every payload byte except the CRC itself. Crash tearing shows up either as a
// short frame (payloadLen bytes unavailable) or a CRC mismatch; recovery
// stops at the first such frame, so all earlier committed transactions
// survive and no partial transaction is ever applied.

const (
	walMagic     uint32 = 0x5354_4d57 // "STMW"
	walRecCommit uint8  = 1
)

// walRecord is one committed transaction: new version + written entries.
type walRecord struct {
	Version uint64
	Entries []walEntry
}

// walEntry is one key mutation inside a committed transaction.
type walEntry struct {
	Key    string
	Value  []byte
	Delete bool
}

type wal struct {
	f    *os.File
	path string
}

// openWAL opens (creating if needed) the WAL at path for append.
func openWAL(path string) (*wal, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, path: path}, nil
}

// append writes one commit record and fsyncs it before returning: after a
// successful append the transaction is durable.
func (l *wal) append(rec walRecord) error {
	if l == nil {
		return nil
	}
	payload, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := l.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := l.f.Write(payload); err != nil {
		return err
	}
	return l.f.Sync()
}

// Sync flushes any buffered writes to stable storage.
func (l *wal) Sync() error {
	if l == nil {
		return nil
	}
	return l.f.Sync()
}

// Close releases the log file.
func (l *wal) Close() error {
	if l == nil {
		return nil
	}
	return l.f.Close()
}

// replay visits every intact frame in order. A truncated tail (torn write)
// or a checksum mismatch terminates replay cleanly; an earlier hard I/O error
// is returned. It returns the byte offset immediately after the last fully
// validated frame, which recovery uses to discard a torn tail.
func (l *wal) replay(visit func(walRecord) error) (int64, error) {
	if l == nil {
		return 0, nil
	}
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var hdr [4]byte
	var validEnd int64
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return validEnd, nil // clean end or torn length prefix
			}
			return validEnd, err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 || n > 64*1024*1024 {
			return validEnd, nil // implausible frame: treat as torn tail
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			// Torn frame body: all previous frames are already applied.
			return validEnd, nil
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			// Corrupt frame: stop replay at the last good commit.
			return validEnd, nil
		}
		if err := visit(rec); err != nil {
			return validEnd, err
		}
		validEnd += int64(4 + len(payload))
	}
}

func encodeRecord(rec walRecord) ([]byte, error) {
	buf := make([]byte, 0, 32)
	putU32 := func(v uint32) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], v)
		buf = append(buf, b[:]...)
	}
	putU64 := func(v uint64) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		buf = append(buf, b[:]...)
	}
	putS32 := func(v int32) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(v))
		buf = append(buf, b[:]...)
	}

	putU32(walMagic)
	buf = append(buf, walRecCommit)
	putU64(rec.Version)
	putU32(uint32(len(rec.Entries)))
	for _, e := range rec.Entries {
		if len(e.Key) > 0xffffffff {
			return nil, errors.New("stm: wal key too large")
		}
		putU32(uint32(len(e.Key)))
		buf = append(buf, e.Key...)
		if e.Delete {
			putS32(-1)
		} else {
			if len(e.Value) > 0x7fffffff {
				return nil, errors.New("stm: wal value too large")
			}
			putS32(int32(len(e.Value)))
			buf = append(buf, e.Value...)
		}
		if e.Delete {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	sum := crc32.ChecksumIEEE(buf)
	putU32(sum)
	return buf, nil
}

func decodeRecord(payload []byte) (walRecord, error) {
	var rec walRecord
	if len(payload) < 4+1+8+4+4 {
		return rec, ErrCorruptLog
	}
	got := binary.BigEndian.Uint32(payload[len(payload)-4:])
	want := crc32.ChecksumIEEE(payload[:len(payload)-4])
	if got != want {
		return rec, ErrCorruptLog
	}
	p := payload
	if binary.BigEndian.Uint32(p[:4]) != walMagic {
		return rec, ErrCorruptLog
	}
	p = p[4:]
	if p[0] != walRecCommit {
		return rec, ErrCorruptLog
	}
	p = p[1:]
	rec.Version = binary.BigEndian.Uint64(p[:8])
	p = p[8:]
	n := binary.BigEndian.Uint32(p[:4])
	p = p[4:]
	for i := uint32(0); i < n; i++ {
		if len(p) < 4 {
			return rec, ErrCorruptLog
		}
		kl := int(binary.BigEndian.Uint32(p[:4]))
		p = p[4:]
		if kl > len(p) {
			return rec, ErrCorruptLog
		}
		e := walEntry{Key: string(p[:kl])}
		p = p[kl:]
		if len(p) < 4 {
			return rec, ErrCorruptLog
		}
		vl := int(int32(binary.BigEndian.Uint32(p[:4])))
		p = p[4:]
		if vl == -1 {
			e.Delete = true
			e.Value = nil
		} else {
			if vl < 0 || vl > len(p) {
				return rec, ErrCorruptLog
			}
			e.Value = append([]byte(nil), p[:vl]...)
			p = p[vl:]
		}
		if len(p) < 1 {
			return rec, ErrCorruptLog
		}
		if p[0] == 1 {
			e.Delete = true
		}
		p = p[1:]
		rec.Entries = append(rec.Entries, e)
	}
	if len(p) != 4 { // only CRC should remain
		return rec, ErrCorruptLog
	}
	return rec, nil
}
