package stm

import "os"

// recoverFromWAL rebuilds in-memory state from the durable log.
//
// Rules implemented:
//   - Records are applied strictly in on-disk order, so replay is
//     deterministic: the same log always yields the same state.
//   - Only complete, CRC-valid frames are applied. A torn/corrupt tail left
//     by a crash mid-append is ignored and physically removed, so later
//     commits append directly after the last good one.
//   - Every applied record is a whole committed transaction; an uncommitted
//     transaction never reached the log and therefore leaves no partial
//     writes behind.
//   - The global clock is advanced to the highest committed version, so
//     post-recovery commits continue the version sequence.
func (s *Store) recoverFromWAL() error {
	if s.cfg.WALPath == "" {
		return nil
	}
	// Replay before opening the append handle: the replay both reads and
	// truncates the file.
	l, err := openWAL(s.cfg.WALPath)
	if err != nil {
		return err
	}

	s.mu.Lock()
	var maxVersion uint64
	validEnd, err := l.replay(func(rec walRecord) error {
		for _, e := range rec.Entries {
			o := s.objects[e.Key]
			if o == nil {
				o = &object{}
				s.objects[e.Key] = o
			}
			o.versions = append(o.versions, versionedValue{
				value:   append([]byte(nil), e.Value...),
				version: rec.Version,
				deleted: e.Delete,
			})
		}
		if rec.Version > maxVersion {
			maxVersion = rec.Version
		}
		return nil
	})
	s.clock = maxVersion
	s.mu.Unlock()
	if err != nil {
		l.Close()
		return err
	}

	// Cut off any torn tail so future appends follow the last valid frame.
	if err := os.Truncate(s.cfg.WALPath, validEnd); err != nil {
		l.Close()
		return err
	}
	if err := l.Sync(); err != nil {
		l.Close()
		return err
	}
	s.wal = l
	return nil
}
