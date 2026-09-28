package stm

// entry is one versioned committed object.
type entry struct {
	value   []byte
	present bool // false when the key is committed as deleted
	exists  bool // false when the key was never committed
	version uint64
}

// store holds the committed state. All methods must be invoked while
// STM.mu is held.
type store struct {
	data map[string]*entry
	ver  uint64 // monotonic global commit clock (start snapshot water mark)
}

func newStore() *store {
	return &store{data: make(map[string]*entry)}
}

// load returns the committed entry for key, or nil if the key has never been
// written. Callers must not mutate the returned value.
func (s *store) load(key string) *entry {
	return s.data[key]
}

// apply commits one transaction's writes atomically. Every touched key gets a
// fresh, strictly increasing version.
func (s *store) apply(sets map[string][]byte, deletes map[string]bool) {
	for k, v := range sets {
		s.ver++
		cp := make([]byte, len(v))
		copy(cp, v)
		s.data[k] = &entry{value: cp, present: true, exists: true, version: s.ver}
	}
	for k := range deletes {
		if _, ok := sets[k]; ok {
			continue // a later set in the same commit wins
		}
		s.ver++
		s.data[k] = &entry{present: false, exists: true, version: s.ver}
	}
}

// Snapshot returns a deep copy of all currently present key/values.
func (s *store) Snapshot() map[string][]byte {
	out := make(map[string][]byte, len(s.data))
	for k, e := range s.data {
		if e.exists && e.present {
			cp := make([]byte, len(e.value))
			copy(cp, e.value)
			out[k] = cp
		}
	}
	return out
}
