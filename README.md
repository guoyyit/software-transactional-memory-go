# Software Transactional Memory (Go)

一个进程内的本地软件事务内存（STM）服务。多个并发 goroutine 可以通过事务
安全地读写共享的键/值对象；系统自动进行乐观冲突检测、有界重试、显式回滚、
条件重试，并可选地通过预写日志（WAL）做事务持久化与崩溃恢复。

## 快速开始

```go
import "github.com/guoyyit/software-transactional-memory-go/stm"

db := stm.New()                 // 纯内存实例
defer db.Close()

err := db.Do(func(tx *stm.Txn) error {
    v, err := tx.Get("counter") // 读已提交数据
    if err != nil {
        return err              // 返回任意 error => 回滚
    }
    tx.Set("counter", v+1)      // 暂存写，提交前对外不可见
    return nil                  // 返回 nil => 提交
})
```

开启持久化：

```go
db, err := stm.Open(stm.Config{
    DataDir: "./data", // 非空即启用 WAL；Open 时自动重放恢复
    Fsync:   true,     // 每条提交在应答前 fsync
})
```

## 事务 API

| 方法 | 语义 |
| --- | --- |
| `Do(fn func(*Txn) error) error` | 以事务执行 `fn`；`nil` 返回值提交，非 `nil` 返回值回滚 |
| `tx.Get(key) ([]byte, error)` | 读值；不存在返回 `ErrNotFound` |
| `tx.GetOK(key) ([]byte, bool)` | 读值，区分“不存在/已删除”与“存在但值为空” |
| `tx.Set(key, value)` | 暂存写（值会被拷贝），仅提交后可见 |
| `tx.Delete(key)` | 暂存删除 |
| `tx.Retry()` | 条件重试：挂起，直到有其它事务提交后重新执行 |
| `tx.Abort()` / `tx.AbortWith(err)` | 显式回滚，不提交任何暂存写 |

## 一致性与冲突语义

- **快照读 + 提交时全量校验（optimistic validation）**。每个键带有单调递增的
  版本号。事务首次读某键时记录其版本；提交临界区内重新校验所有读过的键，
  任何一个键在此期间被并发事务改过，本次提交即失败。
- **first-committer-wins**。`validate → WAL 追加 → 内存应用` 在单一全局
  提交临界区内完成，因此提交顺序是一个全序，且与 WAL 中记录顺序一致。
  同一对象被多个事务并发写入时只有一个提交成功，其余得到 `ConflictError`
  （`PolicyAbort`）或自动重试（`PolicyRetry`）。
- **写写冲突同样检测**：暂存写记录其基线版本，若基线在提交前被其它事务改变
  （例如出现写偏序 write skew），提交失败。
- **原子可见性，无部分生效**。暂存写只存在于事务私有结构中；提交时一次性
  应用到共享存储，回滚/中止/冲突时直接丢弃，绝不泄露。
- **原子性等级**：进程内视角为可线性化（linearizable）；启用 WAL + `Fsync`
  后，已应答的提交在操作系统崩溃/掉电后仍在（依赖 `fsync` 语义）。
- `Retry()` 是 STM 风格的条件重试：事务体被放弃并在任意提交后重跑（重新
  读取最新状态）；等待受 `RetryWaitMax` 与 `MaxRetries` 双重约束，条件永远
  不满足时最终返回 `ErrConflict`/`ConflictError`，不会无限占用。
- 关闭 STM 时会唤醒所有停在 `Retry()` 上的事务，它们返回 `ErrClosed`。

## 持久化与崩溃恢复

- 每个已提交事务写一条 WAL 记录，格式为：定长帧头（magic `STML`、类型、
  CRC32、payload 长度）+ payload（事务 ID、set 列表、delete 列表）。
- **先落盘后改内存**：记录追加（并按需 `fsync`）成功后才更新内存状态，
  因此崩溃时“有整条记录 ⇔ 已提交”。
- 恢复时顺序重放全部有效记录；末尾的撕裂帧（崩溃时写到一半）被截断丢弃；
- 已提交事务的写入完整重放；未提交事务没有任何记录，不会留下部分写入。
- 帧载荷受 CRC32(IEEE) 保护，检测到完整但校验失败的帧会返回
  `errCorruptWAL` 而不是悄悄接受脏数据。
- 重放后继续提交会正确追加在日志尾部；事务 ID 在重放后继续单调递增。

## 冲突策略与退避（防饿死 / 防资源耗尽）

- `PolicyRetry`（默认）：冲突后用**全抖动指数退避**重试，等待区间为
  `[min/2, min(2^(n-1)·min, max)]`，上限由 `RetryBackoffMax` 封顶，
  重试次数由 `MaxRetries` 封顶；耗尽后返回 `*ConflictError`（其 `Unwrap`
  链到 `ErrConflict`）。
- `PolicyAbort`：首次冲突立即返回，不做自动重试，适合上层自行调度。
- 指数退避在每个事务的第一次冲突时重新起算，长事务造成的争用不会让短事务
  继承一个很大的退避值；`Retry()` 的等待也有 `RetryWaitMax` 上限。两者共同
  保证短事务不会被长期饿死，也不会因无限紧密重试而耗尽 CPU。

## 配置项（`stm.Config`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `MaxRetries` | `32` | 冲突重试与条件重试的总上限（≥0；0 用默认值） |
| `RetryBackoffMin` | `1ms` | 冲突退避区间下限 |
| `RetryBackoffMax` | `100ms` | 冲突退避区间上限（指数封顶） |
| `RetryWaitMax` | `5s` | 单次 `Txn.Retry()` 最长挂起时间，超时重跑并消耗一次预算 |
| `Policy` | `PolicyRetry` | `PolicyRetry` 或 `PolicyAbort` |
| `DataDir` | `""` | 非空则启用 WAL 持久化与崩溃恢复（文件 `wal.log`） |
| `Fsync` | `true` | 每条提交是否 `fsync` 后才算成功 |

## 测试

```bash
go test ./...        # 功能测试
go test -race ./...  # 竞态检测
```

覆盖内容：基本提交/读己之写、显式 Abort/错误回滚不泄露、删除与复活、
条件重试唤醒与预算耗尽、读写冲突、写写单胜者（50 路竞争仅 1 个成功）、
高并发自增无丢失更新（100 路）、`PolicyAbort` 立即中止、长短事务防饿死、
WAL 提交持久化与重启恢复、撕裂尾截断、CRC 损坏检测、空值往返、
确定性重放（与内存参照实例逐字节一致）、关闭唤醒等待者。

## 限制

- 单机、单进程、单 WAL 文件；不做日志压缩/快照（大数据集下日志会增长）。
- 值为 `[]byte`；大值整体复制与整体落盘。
- 不提供跨进程/网络复制；持久化保证依赖本地文件系统的 `fsync`。
