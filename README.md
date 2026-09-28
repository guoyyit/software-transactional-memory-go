# Software Transactional Memory (Go)

A local, in-process Software Transactional Memory service. Multiple concurrent
tasks read and write shared memory objects through transactions; the STM
performs optimistic conflict detection, deterministic retry/abort handling,
optional conditional blocking retry, and write-ahead-log durability with
crash recovery.

## Quick start

```go
import "github.com/guoyyit/software-transactional-memory-go/stm"

store, err := stm.New(stm.DefaultConfig())
if err != nil { log.Fatal(err) }
defer store.Close()

err = store.Atomically(func(tx *stm.Tx) error {
    b, err := tx.Get("counter")
    if errors.Is(err, stm.ErrNotFound) {
        b = []byte("0")
    } else if err != nil {
        return err
    }
    n, _ := strconv.Atoi(string(b))
    return tx.Put("counter", []byte(strconv.Itoa(n+1))) // atomic RMW
})
```

Lower-level control is available with `Begin` / `tx.Get` / `tx.Put` /
`tx.Delete` / `tx.Commit` / `tx.Abort` / `tx.Retry`. A runnable demo lives in
`cmd/stm-demo` (run it twice against the same `--dir` to see recovery).

## Consistency semantics

- **Snapshot reads (repeatable read).** Each transaction captures the global
  commit version at `Begin`. Reads resolve against a multi-versioned history
  and always see the snapshot taken at begin; concurrent commits are invisible
  inside the running transaction, and repeated reads of a key return the same
  value.
- **First-committer-wins validation.** A transaction records the version of
  every key it observes (reads *and* the base version of every written key).
  At commit, under a single exclusive critical section, every observed
  version is compared with the current committed version. If any observed key
  was changed by a concurrent committer — updated, created, or deleted —
  commit fails with `stm.ErrConflict` and **no** buffered write is applied.
- **Atomic visibility.** Writes are buffered locally during the body. Commit
  is one critical section that validates, appends to the WAL, and applies the
  whole write set. Readers can therefore never observe a partial write set,
  and uncommitted writes are never visible to other transactions.
- **Write/write conflicts included.** Even a blind `Put` registers the key's
  base version, so two transactions writing the same key cannot both commit:
  exactly one wins; the others get a definite `ErrConflict` and, under
  `PolicyRetry`, are retried.
- **Explicit rollback.** `tx.Abort(err)` discards all buffered writes and
  returns `err`; the store is unchanged. Returning any error from an
  `Atomically` body has the same effect.
- **Conditional retry.** `tx.Retry()` discards buffered writes and blocks the
  transaction efficiently (a condition-variable broadcast on commit — no
  polling) until another transaction commits, then re-executes the body.
  Conditional retries do **not** consume the conflict-retry budget.
- **Durability and crash recovery.** When `Config.WALPath` is set, every
  mutating commit appends one length-framed, CRC32-checksummed record and
  `fsync`s before the commit returns. In-memory state is applied only after
  the durable append. On restart the log is replayed in on-disk order
  (deterministic: identical log ⇒ identical state); complete valid records
  survive, a torn/corrupt trailing frame from a crash mid-append is ignored
  and physically trimmed, and uncommitted transactions never reached the log
  so leave no partial writes. The global version clock is restored from the
  log, so post-recovery commits continue the sequence.

### Isolation level

Snapshot reads + first-committer-wins gives snapshot isolation (no dirty
reads, no non-repeatable reads, no lost updates). Two transactions that
disjointly write keys never conflict; transactions writing the same key
serialize.

## Conflict policy, retries and backoff

`Atomically` runs the body and handles results:

| Body result / commit result | Behavior |
| --- | --- |
| commit succeeds | returns `nil` |
| `ErrConflict`, policy `PolicyRetry` | wait backoff, rerun, up to `MaxRetries` |
| `ErrConflict`, policy `PolicyAbort` | return `ErrConflict` immediately |
| retries exhausted | wrap `ErrRetryExhausted` (use `errors.Is`) |
| `tx.Retry()` sentinel | block until next commit, then rerun |
| any other error / `tx.Abort(err)` | roll back and return the error |

Backoff is exponential (`BackoffFactor` per retry), capped at `MaxBackoff`,
with up to 25% random jitter. The jitter decorrelates competing transactions
so short transactions are not repeatedly beaten by long ones and contention
collapses into a busy retry storm. Transaction bodies never hold global
locks, so long transactions never block short ones.

## Configuration

```go
type Config struct {
    MaxRetries    int           // max automatic conflict retries (default 16)
    BaseBackoff   time.Duration // first retry delay (default 1ms)
    MaxBackoff    time.Duration // backoff cap (default 100ms)
    BackoffFactor float64       // exponential growth, >= 1 (default 2)
    Policy        ConflictPolicy // PolicyRetry (default) | PolicyAbort
    WALPath       string        // set to enable durable logging + recovery
}
```

Zero/negative-valued fields are replaced by defaults in `New`; explicitly set
fields are preserved.

## Errors

| Error | Meaning |
| --- | --- |
| `ErrConflict` | observed set changed; retry or abort per policy |
| `ErrAborted` | explicit rollback without a specific cause |
| `ErrRetryExhausted` | retry cap reached (wrapped) |
| `ErrNotFound` | key absent in the transaction's snapshot |
| `ErrTxClosed` | transaction used after commit/abort/retry |
| `ErrCorruptLog` | WAL record failed validation during decode |

## Tests

```sh
go test -race ./...
```

Coverage includes:

- basic commit/read-your-writes, delete/recreate, explicit abort isolation;
- snapshot isolation versus a concurrent committer;
- concurrent read-modify-write on one key (no lost updates), blind
  write/write single-winner serialization, atomic multi-key visibility;
- retry cap (`ErrRetryExhausted`) with a deterministic conflict seam and
  immediate-abort policy;
- conditional retry wake-up and retry-budget independence;
- long-vs-short transaction mix (no starvation);
- WAL round-trip and torn-tail handling;
- crash recovery (committed-only visibility), version-sequence continuation,
  deterministic replay, torn-tail trimming, and durability-before-return.
