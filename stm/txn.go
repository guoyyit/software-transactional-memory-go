package stm

// abortPanic is raised by Txn.Abort and recovered by STM.Do; it never escapes
// the package as a panic.
type abortPanic struct{ cause error }

// Txn is the transactional read/write handle passed to a transaction body.
//
// Reads see the transaction's own staged writes first; otherwise they see the
// latest committed state. Every key read or written is tracked with the
// per-key version observed at that moment; commit re-checks those versions.
type Txn struct {
	stm      *STM
	id       uint64
	beginVer uint64 // global commit clock when the transaction started

	readKeys map[string]struct{} // keys observed from the committed store
	writeVer map[string]uint64   // base version of blind-written keys
	writes   map[string][]byte
	deletes  map[string]bool
}

func newTxn(s *STM, id uint64, beginVer uint64) *Txn {
	return &Txn{
		stm:      s,
		id:       id,
		beginVer: beginVer,
		readKeys: make(map[string]struct{}),
		writeVer: make(map[string]uint64),
		writes:   make(map[string][]byte),
		deletes:  make(map[string]bool),
	}
}

// Get returns the value of key within the transaction. Missing keys return
// (nil, ErrNotFound). The returned slice must not be modified.
func (t *Txn) Get(key string) ([]byte, error) {
	v, ok := t.GetOK(key)
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// GetOK returns (value, true) if key currently exists in the transaction's
// view, (nil, false) otherwise. Deleted keys and never-written keys report
// false.
func (t *Txn) GetOK(key string) ([]byte, bool) {
	// Own staged write wins (a Set after a Delete in the same txn resurrects).
	if v, ok := t.writes[key]; ok {
		return v, true
	}
	if t.deletes[key] {
		t.observe(key)
		return nil, false
	}
	e := t.stm.readCommitted(key, t.readKeys)
	if e == nil || !e.exists || !e.present {
		return nil, false
	}
	return e.value, true
}

// observe marks key as read (used when the answer is derived purely from local
// delete staging) so commit-time validation still covers it.
func (t *Txn) observe(key string) {
	if _, ok := t.readKeys[key]; ok {
		return
	}
	t.stm.readCommitted(key, t.readKeys)
}

// Set stages a write. It is only visible to others after a successful commit.
// The value is copied so later caller mutation cannot affect the transaction.
func (t *Txn) Set(key string, value []byte) {
	t.stm.writeBase(key, t.readKeys, t.writeVer)
	cp := make([]byte, len(value))
	copy(cp, value)
	t.writes[key] = cp
	delete(t.deletes, key)
}

// Delete stages a deletion of key.
func (t *Txn) Delete(key string) {
	t.stm.writeBase(key, t.readKeys, t.writeVer)
	delete(t.writes, key)
	t.deletes[key] = true
}

// Retry requests a conditional retry: the body is re-executed after some other
// transaction commits. It always panics with ErrRetry and never returns.
func (t *Txn) Retry() {
	panic(ErrRetry)
}

// Abort rolls the transaction back immediately with ErrAbort. Nothing staged
// by this transaction becomes visible. It always panics and never returns.
func (t *Txn) Abort() {
	panic(abortPanic{cause: ErrAborted})
}

// AbortWith rolls the transaction back with the supplied cause.
func (t *Txn) AbortWith(cause error) {
	panic(abortPanic{cause: cause})
}
