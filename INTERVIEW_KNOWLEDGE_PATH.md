# TinyKV 面试知识路径 - 完整技术指南

## 架构总览

TinyKV 是一个教学级分布式 KV 存储系统，对标 TiDB 生态的 TiKV + PD，包含四大核心模块：

```
Client (TinySQL)
    |  gRPC (kvrpcpb)
    v
TinyKV Server (kv/server/)
    |
    +-- Storage Layer
    |   +-- StandaloneStorage (Project1)
    |   +-- RaftStorage (Project2-3)
    |       |
    |       +-- Raftstore (kv/raftstore/)
    |           |  驱动 Raft 共识
    |           v
    |       Raft Module (raft/)
    |
    +-- Transaction Layer (Project4)
    |   +-- MVCC (kv/transaction/mvcc/)
    |   +-- Percolator 2PC
    |
TinyScheduler (scheduler/)
    |  类似 PD，负责集群管理和负载均衡
    +-- Region Heartbeat 处理
    +-- Balance Leader/Region 调度
```

---

## 第一部分：存储引擎基础 (Project 1)

### 1.1 Badger 引擎
- LSM-Tree 架构的 KV 存储（类似 RocksDB）
- Column Family 通过 key 前缀模拟：`${cf}_${key}`
- 三个 CF: `CfDefault`(用户数据), `CfLock`(事务锁), `CfWrite`(提交记录)

### 1.2 Storage 接口设计
```go
type Storage interface {
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
type StorageReader interface {
    GetCF(cf string, key []byte) ([]byte, error)
    IterCF(cf string) engine_util.DBIterator
    Close()
}
```

### 1.3 面试要点
- **为什么用 Column Family？** 逻辑隔离不同类型的数据（锁、值、写入记录），扫描效率更高
- **Badger vs RocksDB？** Badger 是 Go 原生，LSM-Tree with value log separation

---

## 第二部分：Raft 共识算法 (Project 2A)

### 2.1 核心数据结构

```go
type Raft struct {
    Term    uint64          // 当前任期
    Vote    uint64          // 投票给谁
    Lead    uint64          // 当前 leader
    State   StateType       // Follower/Candidate/Leader
    RaftLog *RaftLog        // 日志管理
    Prs     map[uint64]*Progress  // 每个节点的复制进度 {Match, Next}
    votes   map[uint64]bool       // 选举投票记录

    electionTimeout     int    // 选举超时基数
    heartbeatTimeout    int    // 心跳间隔
    randomizedTimeout   int    // 随机化选举超时 [election, 2*election)
    electionElapsed     int    // 选举计时器
    heartbeatElapsed    int    // 心跳计时器

    leadTransferee uint64     // Leader Transfer 目标
    PendingConfIndex uint64   // 防止并发配置变更
}
```

### 2.2 Leader 选举算法

**流程：**
1. Follower 选举超时 → 变为 Candidate
2. Term++, 投票给自己
3. 发送 RequestVote{Term, LastIndex, LogTerm} 给所有节点
4. 收到多数票 → 变为 Leader
5. Leader 立即追加一个 no-op entry（防止提交前任 term 的 entry）

**投票规则：**
- 每个 term 只能投一票
- 候选人的日志必须 >= 投票者（比较 LastLogTerm，相同则比 LastLogIndex）
- 收到更高 term 的消息 → 变回 Follower

**随机化超时：** `randomizedTimeout = electionTimeout + rand(0, electionTimeout)`
- 避免活锁（多个节点同时发起选举）

### 2.3 日志复制算法

**Leader 维护每个 Follower 的进度：**
```go
type Progress struct {
    Match uint64  // 已确认复制到的最大 index
    Next  uint64  // 下次发送的起始 index
}
```

**AppendEntries 流程：**
1. Leader 发送 {prevLogIndex, prevLogTerm, Entries[], Commit}
2. Follower 检查：prevLogIndex 处的 term 是否 == prevLogTerm
3. 匹配 → 追加日志，更新 committed
4. 不匹配 → 拒绝，Leader 递减 Next 重试（日志回溯）

**Commit 规则（关键！）：**
```go
func maybeCommit():
    1. 收集所有节点的 Match 值
    2. 排序找中位数 N（多数派复制到的位置）
    3. 只有 term[N] == currentTerm 时才提交
    // 不能提交前任 term 的 entry！
    // 这是 Raft 与 Paxos 的关键区别
```

### 2.4 RaftLog 结构

```
snapshot/first.....applied....committed....stabled.....last
--------|------------------------------------------------|
                      log entries

- applied: 已应用到状态机的最大 index
- committed: 已被多数派确认的最大 index
- stabled: 已持久化到存储的最大 index
- 不变量: applied <= committed <= stabled <= last
```

**关键函数：**
- `unstableEntries()`: stabled 之后的 entries（需要持久化）
- `nextEnts()`: applied+1 到 committed 的 entries（需要应用到状态机）
- `maybeCompact()`: 丢弃已被 snapshot 覆盖的 entries

### 2.5 RawNode 接口 - 上层应用驱动模式

```go
// 应用层循环：
for {
    rn.Tick()                    // 推进逻辑时钟
    if rn.HasReady() {
        rd := rn.Ready()         // 获取所有变更
        persist(rd.Entries)       // 持久化日志
        send(rd.Messages)        // 发送网络消息
        apply(rd.CommittedEntries)// 应用到状态机
        rn.Advance(rd)           // 通知 Raft 已处理完
    }
}
```

**Ready 结构：**
```go
type Ready struct {
    SoftState        *SoftState       // Leader/State 变化
    HardState        pb.HardState     // Term/Vote/Commit 变化
    Entries          []pb.Entry       // 待持久化的日志
    CommittedEntries []pb.Entry       // 待应用的已提交日志
    Snapshot         pb.Snapshot      // 待安装的快照
    Messages         []pb.Message     // 待发送的消息
}
```

### 2.6 面试高频问题

**Q: 为什么 Leader 当选后要追加 no-op entry？**
A: 防止"幽灵日志"问题。新 Leader 可能有前任 term 的未提交 entry，但 Raft 只能通过当前 term 的 entry 来间接提交前任 term 的 entry。no-op 确保了当前 term 有 entry 可以推进 commit。

**Q: Raft 的日志一致性是如何保证的？**
A: 通过 Log Matching Property：(1) 如果两个节点日志中相同 index 处 term 相同，则该 entry 相同；(2) 如果两个节点日志中相同 index 处 term 相同，则该 index 之前的所有 entry 都相同。AppendEntries 的 prevLogTerm 检查保证了这一点。

**Q: 为什么不能直接提交前任 term 的 entry？**
A: 见 Raft 论文 Figure 8。如果直接提交前任 term 的 entry，可能出现已提交的 entry 被覆盖的情况，违反 State Machine Safety。

---

## 第三部分：Raft KV 层 (Project 2B)

### 3.1 整体架构

```
Client → gRPC → RaftstoreRouter → router.send(regionID, MsgRaftCmd)
                                         |
                                    peerSender channel (buf: 40960)
                                         |
                                    raftWorker.run() [单线程]
                                         |
                                    peerMsgHandler
                                    ├── proposeRaftCommand() → Raft.Propose()
                                    └── HandleRaftReady()
                                        ├── SaveReadyState() [持久化]
                                        ├── Send(messages)   [网络]
                                        └── apply entries    [状态机]
```

### 3.2 关键数据持久化

**两个 Badger 实例：**

| 引擎 | 存储内容 | Key 格式 |
|-------|---------|---------|
| **RaftDB** | Raft 日志 + RaftLocalState | RaftLogKey(regionID, index) |
| **KVDB** | 用户数据 + RegionLocalState + RaftApplyState | 用户 key / ApplyStateKey(regionID) |

**三个关键元数据：**
```
RaftLocalState:  {HardState{Term, Vote, Commit}, LastIndex, LastTerm}
RaftApplyState:  {AppliedIndex, TruncatedState{Index, Term}}
RegionLocalState: {Region{Id, StartKey, EndKey, Peers[], RegionEpoch}, State}
```

### 3.3 Proposal 匹配机制

```go
type proposal struct {
    index uint64            // 期望的日志 index
    term  uint64            // 期望的 term
    cb    *message.Callback // 回调函数
}
```

**Proposal 提交时：** 记录 (nextIndex, currentTerm, callback)
**Entry 应用时：** 按 (term, index) 匹配 proposal，命中则回调

**Stale 检测：**
- entry.Term > proposal.term → 旧 proposal 被新 leader 覆盖 → ErrStaleCommand
- entry.Index > proposal.index → 同理

### 3.4 面试要点

**Q: 读操作也需要走 Raft 吗？**
A: 在 TinyKV 中是的（ReadIndex 优化未实现）。Get 请求也作为 entry 提交到 Raft，确保线性一致性。真实系统中通常用 ReadIndex 或 LeaseRead 优化。

**Q: 为什么 raftWorker 是单线程的？**
A: 所有 peer 的消息在同一个 goroutine 中处理，避免了锁竞争。通过 channel 实现无锁消息传递。

---

## 第四部分：快照机制 (Project 2C)

### 4.1 快照触发条件

```
定期 tick → onRaftGCLogTick()
    → 如果 appliedIndex - truncatedIndex > RaftLogGcCountLimit
    → 提议 CompactLogRequest{CompactIndex, CompactTerm}
    → 日志截断 + 调度 raftlog-gc worker 清理旧日志
```

### 4.2 快照生成与发送

```
Leader 检测到 follower.Next < firstIndex (日志已截断)
    → 调用 PeerStorage.Snapshot()
    → 异步发送 RegionTaskGen 到 region worker
    → 返回 ErrSnapshotTemporarilyUnavailable (Raft 会重试)
    → 下次 Ready() 获取到快照后发送 MsgSnapshot
```

### 4.3 快照应用流程

```
Follower 收到 MsgSnapshot
    → checkSnapshot() 验证 (peer在列表中、无重叠region、文件存在)
    → Raft 层: 丢弃被快照覆盖的 entries, 更新 applied/committed/stabled
    → Raftstore 层: ApplySnapshot()
        → 清除旧 metadata
        → 清除不在新 region 范围内的数据
        → 更新 RaftLocalState, RaftApplyState, RegionLocalState
        → 发送 RegionTaskApply (阻塞等待完成)
    → 更新 storeMeta (regionRanges B-Tree)
```

### 4.4 面试要点

**Q: 为什么快照生成是异步的？**
A: 快照需要扫描整个 region 数据，可能很慢。异步生成避免阻塞 Raft 主流程。通过 `ErrSnapshotTemporarilyUnavailable` 让 Raft 下次重试。

---

## 第五部分：Multi-Raft (Project 3A/3B)

### 5.1 Leader Transfer

```
1. Leader 收到 MsgTransferLeader{Target}
2. 检查 target 日志是否最新 (Match == LastIndex)
3. 如果不是 → 先发 MsgAppend 帮 target 追赶
4. 如果是 → 发 MsgTimeoutNow 给 target
5. Target 收到后立即发起选举（不等超时）
6. 超时机制: 2*electionTimeout 后取消 transfer
```

**关键区别：** Leader Transfer 不走 Raft 日志，直接调用 `RawNode.TransferLeader()`

### 5.2 Configuration Change (成员变更)

```go
// 提议配置变更
RawNode.ProposeConfChange(ConfChange{ChangeType, NodeId, Context})
    → 创建 EntryConfChange 类型的 entry
    → 通过 Raft 复制和提交

// 应用配置变更
entry committed → applyConfChangeEntry()
    → RawNode.ApplyConfChange(cc)  // 更新 Raft 内部 Prs
    → 更新 Region.Peers + RegionEpoch.ConfVer++
    → 持久化 RegionLocalState
    → 如果移除自己 → destroyPeer()
```

**安全性：** 一次只变更一个节点（非 Joint Consensus），通过 PendingConfIndex 防止并发变更

### 5.3 Region Split

```
SplitCheckTick → 扫描 region 大小 → 超过阈值找 split key
    → MsgSplitRegion → 请求 Scheduler 分配新 Region/Peer ID
    → 提议 AdminRequest{Split} 通过 Raft

Split 应用：
    原 Region: [startKey, endKey) → [startKey, splitKey)
    新 Region: [splitKey, endKey)  (新创建的 Raft Group)
    RegionEpoch.Version++
```

### 5.4 RegionEpoch 机制

```go
type RegionEpoch struct {
    ConfVer uint64  // 每次 AddPeer/RemovePeer 递增
    Version uint64  // 每次 Split/Merge 递增
}
```

**用途：** 检测过期请求。客户端请求的 epoch 必须与 region 当前 epoch 匹配。

### 5.5 面试要点

**Q: 为什么不用 Joint Consensus？**
A: TinyKV 简化了实现，一次只变更一个节点。Joint Consensus 允许一次变更多个节点但实现复杂。单步变更的安全性由 "一次最多一个变更" 保证。

**Q: Split 后新 Region 的副本如何创建？**
A: Split 的 followers 在 apply split entry 时本地创建新 peer。如果 follower 不在（比如网络分区），Leader 会发送 heartbeat，store 发现未知 region 后通过 `replicatePeer()` 创建空 peer，然后通过 snapshot 同步数据。

---

## 第六部分：Scheduler 负载均衡 (Project 3C)

### 6.1 processRegionHeartbeat

```go
func processRegionHeartbeat(region) error:
    1. 获取已有 region（按 ID）
    2. 版本比较（防止过期心跳）:
       - newVersion < existVersion → reject
       - newVersion == existVersion && newConfVer < existConfVer → reject
    3. 对新 region，检查 key range 重叠
    4. PutRegion 更新 region 信息
    5. updateStoreStatus 更新每个 store 的统计
```

### 6.2 Balance Region 调度器

```
Schedule(cluster):
    1. 收集所有健康 store (Up && DownTime < MaxStoreDownTime)
    2. 按 RegionSize 降序排列
    3. 从最大 store 开始, 选择一个 region:
       - 优先 pending region → follower → leader
       - 跳过副本数不足的 region (< MaxReplicas)
    4. 从最小 store 开始找 target (不能已有该 region 的 peer)
    5. 检查差距足够大: source.RegionSize - target.RegionSize >= 2 * region.ApproximateSize
    6. CreateMovePeerOperator(source → target)
```

### 6.3 面试要点

**Q: 为什么需要容忍度检查 (2*regionSize)？**
A: 防止振荡。如果差距太小，移动后可能马上需要反向移动，导致无限循环。

**Q: Scheduler 如何保证调度安全？**
A: 通过 RegionEpoch 检查。Operator 创建时记录 epoch，执行时如果 epoch 不匹配（region 已被 split 或配置变更），operator 会被取消。

---

## 第七部分：分布式事务 (Project 4)

### 7.1 MVCC 多版本并发控制

**三个 Column Family 的协作：**

| CF | Key 格式 | Value | 用途 |
|----|---------|-------|------|
| CfDefault | `EncodeKey(userKey, startTS)` | 实际数据 | 存储所有版本的值 |
| CfLock | `userKey`（原始key） | Lock{Primary, Ts, Ttl, Kind} | 事务锁（每个key最多一把） |
| CfWrite | `EncodeKey(userKey, commitTS)` | Write{StartTS, Kind} | 提交记录 |

**Key 编码（核心！）：**
```go
func EncodeKey(key []byte, ts uint64) []byte {
    encodedKey := codec.EncodeBytes(key)           // 保序编码
    newKey := append(encodedKey, make([]byte, 8)...)
    binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)  // 取反！
    return newKey
}
```
- `^ts` 使 **时间戳降序排列**（最新版本排在最前面）
- Seek(`EncodeKey(key, startTS)`) 直接定位到 <= startTS 的最新版本

### 7.2 Percolator 两阶段提交

**Phase 1: Prewrite（上锁 + 写数据）**
```
对每个 mutation:
    1. 检查写冲突: MostRecentWrite(key).commitTs > startTs → WriteConflict
    2. 检查锁冲突: GetLock(key) 存在 → KeyLocked
    3. PutValue(key, value)  → CfDefault[key+startTs] = value
    4. PutLock(key, Lock{Primary, startTs, TTL, Kind}) → CfLock[key] = lock
```

**Phase 2: Commit（提交 + 解锁）**
```
对每个 key:
    1. GetLock(key) 检查锁
    2. lock == nil:
       - 检查 CurrentWrite: 已提交 → 幂等返回; 已回滚 → 错误
       - 无 write 记录 → prewrite 丢失，skip (no-op)
    3. lock.Ts != startTs → 别的事务的锁，错误
    4. PutWrite(key, commitTs, Write{startTs, kind}) → CfWrite[key+commitTs]
    5. DeleteLock(key) → 删除 CfLock[key]
```

### 7.3 读操作 (KvGet)

```
1. GetLock(key): 如果 lock.Ts < readTs → 被阻塞（返回 Locked 错误）
2. Seek CfWrite at EncodeKey(key, readTs)
3. 找到第一个 commitTs <= readTs 的 Write 记录
4. 如果 Write.Kind == Delete → 返回 NotFound
5. 否则用 Write.StartTS 从 CfDefault 读取实际值
```

### 7.4 扫描操作 (KvScan)

```go
type Scanner struct {
    Txn  *MvccTxn
    Iter DBIterator  // 遍历 CfWrite
}

func Next() (key, value, error):
    1. Seek 到 EncodeKey(currentKey, startTS)
    2. 解码 userKey, 跳过同一 key 的旧版本
    3. 根据 Write.StartTS 从 CfDefault 读值
    4. 推进到下一个不同的 userKey
```

### 7.5 事务清理机制

**CheckTxnStatus（锁超时检测）：**
```
1. 检查 primary key 的 write 和 lock
2. 已提交 → 返回 commitTs
3. 已回滚 → 返回 NoAction
4. lock 存在 → 检查 TTL 是否过期
   - 过期: 清除锁+值, 写入 rollback, 返回 TTLExpireRollback
   - 未过期: 返回 NoAction
5. 锁不存在且无 write → 写入 rollback (防止迟到的 prewrite)
```

**BatchRollback（批量回滚）：**
```
1. 检查每个 key 是否已被提交 → 如果是，拒绝回滚
2. DeleteLock + DeleteValue + PutWrite(Rollback)
```

**ResolveLock（批量解决锁）：**
```
1. 扫描 CfLock 找到所有 lock.Ts == startTs 的锁
2. commitVersion == 0 → 全部 rollback
3. commitVersion > 0 → 全部 commit
```

### 7.6 Latch 机制

```go
type Latches struct {
    Locks map[string]*sync.Mutex
}
```
- 内存级互斥锁，防止同一进程内对同一 key 的并发操作
- 与 MVCC 锁不同：Latch 是本地的短期锁；MVCC Lock 是持久化的分布式锁

### 7.7 面试高频问题

**Q: Percolator 的 Primary Key 有什么作用？**
A: Primary Key 是事务的"真相之源"。commit 只需写 primary key 的 write 记录，其他 key（secondary）可以异步提交。检查事务状态时只需检查 primary key 的 lock/write 即可确定整个事务的状态。

**Q: 为什么 CfWrite 的 key 用 commitTS 而不是 startTS？**
A: 读操作需要找到 <= readTS 的最新 commit。如果用 startTS 编码，无法高效定位最新的已提交版本。用 commitTS 可以直接 seek 到 <= readTS 的第一个 write 记录。

**Q: Snapshot Isolation vs Serializable？**
A: SI 允许 Write Skew 异常（两个事务基于同一快照各写不同 key，但语义上冲突）。TinyKV 实现的是 SI，不是 Serializable。

**Q: 如果 Client 在 Prewrite 成功但 Commit 之前崩溃怎么办？**
A: 其他事务遇到锁时会调用 CheckTxnStatus 检查 primary key。如果锁的 TTL 过期，会主动回滚。如果 primary 已经被提交了，会继续完成 commit。

**Q: CfLock 的 key 为什么不需要带时间戳？**
A: 每个 key 同时只能被一个事务锁定。锁内部已经包含了 startTS，所以不需要在 key 中编码时间戳。

---

## 第八部分：系统设计面试题

### 8.1 完整的写请求路径

```
1. Client → gRPC → Server.KvPrewrite()
2. Server 获取 Latch → 获取 Reader → 创建 MvccTxn
3. 冲突检测 (write conflict + lock conflict)
4. PutValue + PutLock → 写入 Badger (通过 RaftStorage)
5. RaftStorage → Raftstore → Raft Propose
6. Raft 复制到多数派 → Commit
7. HandleRaftReady → Apply committed entries → 写入 KVDB
8. 回调 Client → Prewrite 成功
9. Client → Server.KvCommit()
10. PutWrite + DeleteLock → Commit 成功
```

### 8.2 Region 分裂流程

```
Region [a, z) 超过大小阈值
    → SplitChecker 找到 split key = "m"
    → 提议 Split{splitKey="m", newRegionID, newPeerIDs}
    → Raft 复制和提交
    → Apply: 原 Region [a, m), 新 Region [m, z)
    → 两个独立的 Raft Group 并行运行
    → 通知 Scheduler 更新路由表
```

### 8.3 故障恢复

```
节点重启:
    1. 从 KVDB 加载 RegionLocalState (所有 region 信息)
    2. 从 RaftDB 加载 RaftLocalState (HardState)
    3. 从 KVDB 加载 RaftApplyState (已应用到哪里)
    4. 重建 RaftLog: 从 stabled 开始, applied 之后的重新应用
    5. 通过 Raft 心跳重新加入集群
```

### 8.4 线性一致性保证

```
写入: Raft 保证多数派确认 → 写入不会丢失
读取: 也走 Raft → 保证读到最新值
    (优化: ReadIndex - Leader 确认自己仍是 Leader 后本地读)
    (优化: LeaseRead - 在租约期内直接本地读)
```

---

## 第九部分：关键代码文件索引

| 模块 | 文件 | 核心内容 |
|------|------|---------|
| Raft | `raft/raft.go` | 选举、日志复制、心跳、快照 |
| Raft | `raft/log.go` | RaftLog 管理、unstable/committed/applied |
| Raft | `raft/rawnode.go` | Ready 接口、Advance、ProposeConfChange |
| Raftstore | `kv/raftstore/peer_msg_handler.go` | 消息处理、entry 应用、split/confchange |
| Raftstore | `kv/raftstore/peer_storage.go` | 持久化、快照生成与应用 |
| Raftstore | `kv/raftstore/raft_worker.go` | 单线程消息循环 |
| MVCC | `kv/transaction/mvcc/transaction.go` | MvccTxn、EncodeKey、GetValue |
| MVCC | `kv/transaction/mvcc/scanner.go` | 范围扫描 |
| Server | `kv/server/server.go` | KvGet/KvPrewrite/KvCommit/KvScan |
| Scheduler | `scheduler/server/cluster.go` | processRegionHeartbeat |
| Scheduler | `scheduler/server/schedulers/balance_region.go` | Balance Region 调度 |

---

## 第十部分：论文与参考

| 主题 | 论文/资料 |
|------|---------|
| Raft | [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) |
| Percolator | [Large-scale Incremental Processing Using Distributed Transactions and Notifications](https://research.google/pubs/pub36726/) |
| Spanner | [Spanner: Google's Globally-Distributed Database](https://research.google/pubs/pub39966/) |
| TiKV 设计 | [TiKV Deep Dive](https://tikv.org/deep-dive/distributed-transaction/introduction/) |
| LSM-Tree | [The Log-Structured Merge-Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf) |
