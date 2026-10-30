# journal3z: the 3-zone journal (design)

Status: **design, phase 1 (no AWS, no engine change)**. Branch `journal3z`. Code of this phase: `journal3z/`
(interface + local backend, protocol core, simulator, spikes, formal model). Companion documents:

- `docs/journal3z-libraries.md`: library vs custom, criteria (fixed before measuring), spikes, numbers.
- `docs/journal3z-sim.md`: deterministic simulator, invariants, negative controls, step counts.
- `docs/journal3z-walbench-cycle1.md`: the unexplained "connection refused" cycle of the walbench quorum run.
- `docs/journal3z-tla.md`: the TLA+ model and what TLC checked.
- `docs/journal3z-soak-plan.md`: plan for the next phase (72 h AWS soak, not executed, not authorized).
- `docs/journal3z-report.md`: final report of this phase.

Every number in this document is marked **measured** (with where), **estimated** (with how), or **not verified**.

---

## 0. Summary of decisions

| # | decision | why (short) |
|---|---|---|
| D1 | **Protocol = Raft log replication with the leader chosen outside the group** (terms = epochs taken from the database lease; "election" = a Paxos phase 1 over a majority that returns the most up-to-date log; commit only by counting entries of the writer's own epoch, through a barrier entry). Not an invented protocol: every rule is a rule of Raft (Ongaro & Ousterhout 2014) or of Paxos/Viewstamped Replication (Lamport 2001; Liskov & Cowling 2012), and reconfiguration follows Vertical Paxos (Lamport, Malkhi, Zhou 2009). | Safety must not depend on clocks; the database already has one writer chosen by an S3 lease, so a second, independent leader election (Raft's) would be a second source of truth to reconcile. |
| D2 | **Fencing token = the epoch = the lease `seq` written by the conditional PUT that made this server the owner.** A journal node keeps `promised[db]` durably and refuses anything older. | The lease `seq` already increases on every conditional write (round3 `lease.rs`), so two owners never share an epoch and a later owner always has a larger one. |
| D3 | **Layout: per-database logical logs, multiplexed on one shared physical journal per node, one connection per writer–node pair, no per-database heartbeats.** Rejected: one consensus group per database (heartbeats, elections and fsyncs per database), and one consensus group per server (per-database fencing, hot move, trim and erase become whole-group operations). | §6. |
| D4 | **Journal key = (database name, incarnation)**, not the name alone. | The lease object (and its `seq`) is deleted with the database (`drop_database`, erase); a recreated database would restart at `seq` 1 and be refused (or worse, see §5.9). |
| D5 | **Trim only below the database's S3 marker** (the `applied` index read back from object storage, never "what I sent"). The marker is the journal index of the last record contained in the uploaded state. | Invariant (d). |
| D6 | **Recovery reads only what is above the S3 marker, from any 2 of the 3 nodes that answer** (never a fixed pair). | The walbench cycle-1 failure (§12, `docs/journal3z-walbench-cycle1.md`). |
| D7 | **Membership changes only at an upload point** (a database moves to a new set of 3 nodes right after its state up to index `k` is in S3; nothing is copied between nodes). | Node replacement needs no state transfer and no joint consensus; the S3 upload that happens every 30–60 s anyway is the state transfer. |
| D8 | Library or custom: **decided only after the measurements** of `docs/journal3z-libraries.md` (criteria fixed there before measuring). | §11 is filled in after the spikes. |

---

## 1. Goal and promise

Write path: write → journal → acknowledge → batched upload to S3 → trim.

Promise (what "acknowledged" means): **an acknowledged write is never lost on any single failure** — one server
process, one disk, one machine, or one whole availability zone — and **another server can take over a database
without waiting for the failed zone to come back**. Not promised: survival of two simultaneous permanent disk losses
among the three journal nodes of a database within one upload interval (§9).

Setting: three journal nodes, one per AZ (eu-west-1 AZ ids az1, az2, az3). Acknowledge when 2 of 3 have the record
durable (`fdatasync` returned). One writer per database at a time (lease + fencing). A database that goes to sleep is
flushed to S3 first, and then occupies no journal space (only a 32-byte fence record per node, §5.9).

## 2. Roles and state

**Writer** — the engine server process that holds the database's lease. Exactly one *believes* it is the writer most of
the time; because lease expiry is decided by clocks, two may believe it for a while (frozen process, clock skew). The
protocol makes the older one harmless: it can no longer have a write acknowledged (§5.3).

**Journal node** (3 per database, one per AZ) — a passive acceptor and store. It does not run the database, does not
elect anyone, and does not talk to the other nodes. Normally one journal node runs inside each engine server process
(or next to it on the same machine), so the writer's own node is one of the three and an acknowledgement needs one
remote round trip plus the local fsync. A node serves many databases and many writers through **one physical journal
file set** (§6).

**Lease store** — the object `tenants/{db}/_lease` in S3, written only with conditional PUT (create-if-absent,
update-if-ETag-matches). It is the configuration master in the Vertical Paxos sense. Fields used by the journal (engine
change, not made here): `seq` (exists), `incarnation` (new, D4), `journal: {members: [node ids], cfg: u64}` (new).

**Object store** — the database itself (SlateDB in S3). Its state carries the marker `sys/journal = {index, epoch}`:
the journal index of the last record contained in that state (round3 already writes `sys/local-log = {log, seq}` in
every batch; same idea). SlateDB fences its own older writers when a new writer opens the database (manifest writer
epoch); the protocol relies on this (§5.2 step 2).

### 2.1 Per-database state on a journal node (durable unless stated)

| field | meaning |
|---|---|
| `promised` | highest epoch this node has promised to (fence). Never decreases. |
| `log` | entries `(index, epoch, payload)`, contiguous from `trim + 1` to `last` |
| `trim`, `trim_epoch` | everything ≤ `trim` is known to be in S3 and was deleted (or may be). |
| `erased` | the database was erased: refuse everything (fence = ∞). |
| (volatile) `commit_hint` | highest index the writer said is committed (only for monitoring). |

An entry's `epoch` is the epoch of the writer that **created** it (Raft keeps the original term when an entry is
copied by a later leader; we do the same).

### 2.2 Per-database state in the writer (volatile)

`epoch e`, `members[3]`, `applied` (the S3 marker index as last read or written by this writer), `next` (next index),
`buf` (entries `> applied` not yet uploaded: needed for catch-up of lagging nodes), `match[n]` (highest index known
durable and matching on node n), `commit` (highest index committed, §5.4), `barrier` (index of this epoch's barrier).

## 3. Messages

All messages for one writer–node pair travel on one connection and are batched (§6.2). Every message carries the
database key `(name, incarnation)`; below `d` stands for it.

| message | from → to | content | node action |
|---|---|---|---|
| `FENCE(d, e, applied)` | writer → node | epoch, S3 marker | §5.2 |
| `PROMISE(d, e, last, last_epoch, trim)` | node → writer | | |
| `REJECT(d, promised)` | node → writer | the node's higher promise | |
| `READ(d, e, from, to)` | writer → node | | returns entries `[from, to]` if `promised ≤ e` |
| `ENTRIES(d, e, [entries])` | node → writer | | |
| `APPEND(d, e, applied, prev_index, prev_epoch, [entries], commit)` | writer → node | | §5.3 |
| `ACK(d, e, match)` | node → writer | highest index known to match the writer's log, durable | |
| `MISMATCH(d, e, last)` | node → writer | the node's last index (after applying `applied`) | |
| `TRIM(d, e, k)` | writer → node | `k` ≤ S3 marker | delete entries ≤ k (§5.6) |
| `RETIRE(d, k)` | writer → old member | the database left this node's set at k | §5.8 |
| `ERASE(d)` | writer/control → node | | §5.10 |

## 4. Invariants (what the simulator and the TLA+ model check)

- **(a) Durability.** While at most one journal node of a database has lost its disk (and any number may be down or
  restarting with their disks intact), every acknowledged write of the database is either in the database's S3 state
  (index ≤ marker, same bytes) or contained, at its index and with its bytes, in the log that **recovery would choose
  from every majority of the surviving nodes** (§5.2 rule R3).
- **(b) Fencing.** (b1) No node appends an entry whose epoch is lower than the epoch it has promised. (b2) Two
  acknowledged writes of a database never have the same index with different bytes. (b3) Every write acknowledged by a
  writer of epoch `e1` has an index lower than the barrier of every writer `e2 > e1` that committed its barrier: an
  older writer's acknowledgements are all ordered before a newer writer's (at most one writer is "current").
- **(c) Agreement.** (c1) If two nodes hold an entry with the same index and epoch, the bytes are equal (Raft Log
  Matching, checked on the bytes). (c2) For every committed index `i` created in epoch `E`, no node holds at `i` an
  entry of an epoch greater than `E` (a later writer always carried the committed entry; Leader Completeness).
- **(d) Trim safety.** A node deletes entries ≤ k only when the database's S3 marker is ≥ k at that moment; the S3
  state at every index ≤ marker equals the committed entry when the index was committed.

## 5. Protocol

### 5.1 Epochs and the lease

The writer of database `d` is the holder of `tenants/{d}/_lease`. When a server acquires the lease (create, take over
an expired lease, or re-acquire its own), the conditional PUT writes `seq + 1`; that value is the server's **epoch**
`e` for `d` until it loses or releases the lease. Renewals also bump `seq` but do **not** change the epoch the server
uses (they could: a higher epoch from the same server only fences itself). Two servers never obtain the same `seq`
(conditional PUT on one object is linearizable in S3), and a later acquisition always gets a larger one. **Safety
never depends on clocks**: clocks only decide when a server *may try* to take over (`expiresMs + skew`).

### 5.2 Opening a database (becoming the writer) — Paxos phase 1 + Raft log choice

Order (each step finishes before the next):

1. **Lease.** Acquire the lease → epoch `e`, members `M` (from the lease record).
2. **Object store.** Open the SlateDB writer (this fences older SlateDB writers of `d`: their later uploads fail) and
   read the marker → `applied`. From now on no older writer can move the S3 state.
3. **Fence.** Send `FENCE(d, e, applied)` to the 3 members. A node with `erased` replies `REJECT(∞)`. A node with
   `promised > e` replies `REJECT(promised)`. Otherwise it first trims below `applied` (a fact: the S3 marker only
   grows), sets `promised = max(promised, e)`, makes that durable (fdatasync; one fsync covers every queued record),
   and replies `PROMISE(d, e, last, last_epoch, trim)`.
   - Any `REJECT` with a higher epoch: this server is stale → it abandons the database (round3 `abandon`).
   - Wait for 2 `PROMISE`s (any 2; retry the third forever in the background). If fewer than 2 nodes answer within
     the open timeout, the open fails with a retryable error (unavailable, not unsafe).
4. **Choose (rule R3).** Among the promising nodes that have entries above `applied`, choose the one with the largest
   `(last_epoch, last)` (lexicographic: epoch first). If none has entries above `applied`, the recovered log is empty.
   `READ` the entries `(applied, last]` from the chosen node (the read is refused if the node promised a higher epoch).
   That is the recovered log `L`. Proof sketch (Raft §5.4.3 restricted to indices above `applied`): an entry committed
   at index `i > applied` is on a majority, which intersects the 2 promisers; entry epochs never decrease along a log,
   and every later writer copied it, so the most up-to-date log among the promisers contains it. Nothing at an index
   `≤ applied` can be needed: it is in S3.
5. **Barrier.** Append a `BARRIER` entry at `b = max(applied, last(L)) + 1` with epoch `e`, after `L`, and replicate
   it (§5.3). `L` is committed when the barrier is (Raft: a leader commits entries of earlier terms only indirectly, by
   committing one of its own — the "Figure 8" rule). The writer replays `L` into its memtable immediately (the engine's
   replay is idempotent through the marker), but acknowledges no new write before the barrier is committed (acks are in
   index order, so this is automatic).

S3 can be **ahead** of the journal: an uploaded state may include records that were never acknowledged (the memtable
holds applied-but-not-yet-acknowledged writes when the upload runs). Then `applied > last(L)`, the barrier goes at
`applied + 1`, and those records count as "maybe applied" (exactly like a write whose acknowledgement was lost on the
network). The barrier's index is above the marker, so no index can mean two different things.

### 5.3 Appending (normal operation) — Raft AppendEntries with an epoch fence

The writer assigns `index = next++` under the database's lock (as round3 assigns `seq`), keeps the entry in `buf`, and
sends `APPEND(d, e, applied, prev_index, prev_epoch, entries, commit)` to the 3 members (batched with other databases).

Node, in this order:
1. If `erased` or `e < promised`: `REJECT(promised)`. **This is the fence.** If `e > promised`: `promised = e` (an
   append from a higher epoch implies the promise, as in Raft; it is made durable with the entries).
2. Apply the trim hint: entries ≤ `applied` may be deleted (S3 has them).
3. Consistency check: the entry at `prev_index` must have `prev_epoch`, or `prev_index ≤ trim` (anything at or below
   the trim point matches: it is in S3, and S3 is one agreed history). Otherwise `MISMATCH(last)`.
4. For each entry: same index and same epoch already present → skip; same index, different epoch → delete it and
   everything after it (the diverged tail, never committed); then append. Write all records of the batch, one
   `fdatasync`, then `ACK(d, e, prev_index + len(entries))`.
   - fdatasync error: the node **stops** (exits). It never acknowledges after a failed fsync; on restart it rebuilds
     its state from what is on disk (fsyncgate: after an error the page cache can no longer be trusted).
   - Disk full (ENOSPC on write): reply nothing for that batch (the writer times it out) and raise an alarm.

Writer: on `ACK(m)`: `match[n] = max(match[n], m)`. `commit` = the largest index `i` with `entry(i).epoch == e` and
`match ≥ i` on 2 of the 3 members (Raft's rule; the local node counts like any other). Every write with index ≤
`commit` is acknowledged to its client, in index order. On `MISMATCH(last)`: resend from `max(last, applied) + 1` with
the matching `prev` (catch-up, §5.5). On `REJECT(p > e)`: the writer is stale → stop writing, fail every
unacknowledged write with a retryable "not the owner" error, abandon the database.

### 5.4 What "acknowledged" means

A write is acknowledged only when (1) it and every earlier index are committed, and (2) commit counted acks that the
nodes sent **after** fdatasync returned. With the writer's own node as one of the three, the cost is one round trip to
the nearest other AZ plus one local and one remote fsync in parallel. walbench measured that path at p99 1.98 ms
(1 KB, concurrency 8, real AWS; no epochs).

### 5.5 Catch-up of a node that was down, slow or partitioned

There is no special protocol: the writer keeps sending `APPEND`s; the node answers `MISMATCH(last)`; the writer resends
from `max(last, applied) + 1`. Only entries **above the S3 marker** are ever sent (the trim hint lets the node skip the
rest), so the volume is bounded by the database's unflushed data (≤ upload interval, ≤ per-database quota). The entries
come from the writer's `buf`. A node returning with a **diverged tail** (entries of an old epoch that were never
committed) gets it deleted by step 4 of §5.3 the first time the current writer's entries reach those indices; until
then those entries are harmless: rule R3 ranks them below any log of a later epoch.

### 5.6 Upload and trim (only after the upload is confirmed)

Every 30–60 s (round3 `--local-log-upload-secs`), or earlier when the database's unflushed bytes exceed its quota
(§7), the writer flushes the database to S3. The uploaded state contains every record applied so far, with the marker
`{index: k, epoch}` where `k` is the last index applied into it. When SlateDB confirms the upload is durable:
`applied = k`, drop `buf` up to `k`, and send `TRIM(d, e, k)` (also piggybacked as the `applied` field of later
appends). A node deletes entries ≤ k. **Trim is only ever driven by a value read back from, or confirmed by, S3**; a
stale writer's trim is harmless because S3's marker only grows.

### 5.7 Sleep, wake, hot move

- **Sleep.** Stop accepting writes, wait until `commit == next - 1`, upload (`applied = commit`), `TRIM(commit)`,
  release the lease. The journal now holds nothing for the database except its fence record.
- **Wake.** Normal open (§5.2): fence, nothing above `applied`, barrier at `applied + 1`. One round trip after the lease
  and the SlateDB open.
- **Hot move A → B** (planned). A: stop accepting writes for the database, wait for commit, release the lease. B:
  acquire (epoch higher), open (§5.2). An upload by A before release is optional (it shortens B's replay). If A is
  frozen instead of cooperative, B takes over after expiry and the fence does the rest. Unavailability (estimated, not
  measured): A's drain (≤ one round trip) + 2 conditional PUTs to S3 (round3 measured lease CAS latency as part of
  takeovers; not split out) + SlateDB open + one fence round trip ≈ 0.1–0.5 s.

### 5.8 Membership: replacing a node (Vertical Paxos, at an upload point)

The journal configuration of a database (`members`, `cfg`) lives in its lease record. To move the database from `M` to
`M'` (for example when a node's disk is lost and a new node `n4` is created with a **new identity**):

1. The writer (epoch `e`) stops new appends for the database, waits until everything appended is committed on `M`
   (`commit == next - 1`), and uploads: `applied = k = commit`.
2. Conditional PUT of the lease: `members = M'`, `cfg + 1`, `seq + 1`. The writer's new epoch is `e' = seq`.
3. Open on `M'` (§5.2 from step 3, with `applied = k`): fence `M'`, barrier at `k + 1`.
4. Best effort: `RETIRE(d, k)` to the members of `M` not in `M'` (they delete the database's entries; they keep the
   fence record).

Why this is safe: after step 1 the only writer able to append on `M` is this one (older epochs were fenced on a
majority of `M` when it opened), and it never appends on `M` again after step 2. Everything ≤ k is in S3, so `M'`
needs no copy. A crash between steps leaves either the old configuration (step 2 not done: normal recovery on `M`) or
the new one with `applied = k` (step 2 done). **A node that lost its disk never comes back under its old identity**:
it would have forgotten its promises and could help a stale writer reach a majority (the simulator's `forget_fence`
negative control shows exactly this loss).

While a database still lists a dead node, it runs on 2 of 3 and tolerates no further failure. Time to restore full
tolerance after a disk loss = time to create the new node + one forced upload per affected database (estimated: the
uploads are parallel PUTs, about 1,000 databases in a few seconds at round3's measured upload rates; not verified).

### 5.9 The fence record, incarnations, and why a node never forgets a fence

A node keeps, per database key, `{promised, trim, trim_epoch, erased}` (about 32 bytes + the key) **even when the
database has no entries** (asleep). Forgetting it would let a stale writer whose entries are all ≤ an old trim point
re-seed an empty node and reach a majority with another unfenced node; its "acknowledged" write would then sit below
the newer writer's S3 marker and be ignored forever (lost). The table is small (1 M databases ≈ 50 MB on disk) and is
checkpointed at each segment roll. It is deleted only for erased incarnations (where `erased = true` is kept for a
retention period, then dropped together with the incarnation).

Incarnation (D4): `drop_database` and erase delete `tenants/{db}/` including `_lease`, so the next database with the
same name starts again at `seq = 1`. With the key `(name, incarnation)` (a random 64-bit id created with the database
and stored in the lease record) the new database is a new journal key; the old key's fence record stays until it is
dropped.

### 5.10 Erase (GDPR deletion of one customer)

`ERASE(d)`: each node sets `erased`, deletes the database's index entries, and records an `ERASE` record. The bytes
still sit in shared segment files; they are physically removed when those segments are deleted or **rewritten** (§6.3).
Bound: a segment is closed at 64 MB or after 10 minutes, whichever first, and a segment holding an erased record is
rewritten (live records copied forward, then the file deleted and the directory fsynced) within 15 minutes. So: erased
from the journal API immediately, physically gone from journal disks ≤ 15 min (a design target, not verified).
Optional stronger variant: per-database encryption keys (crypto-shredding); not proposed for phase 2.

### 5.11 Node restart, recovery and the walbench lesson

A node restarts by loading the fence checkpoint and scanning its segments (a torn frame at the end of the last segment
is ignored: it was never acknowledged, because the ack is sent after fdatasync). It then serves normally; writers catch
it up (§5.5). Recovery by a writer reads **any 2 of the 3** (D6). The walbench harness read a fixed random pair and
failed when that pair contained a node that was still restarting: that is cycle 1 (§12).

## 6. Layout: one consensus group per database, or one shared journal per server?

### 6.1 Options

| | (A) one group per database | (B) one group per server | (C) **chosen**: per-database logical logs on one shared journal per node |
|---|---|---|---|
| leader | Raft election per database | Raft election per server stream | the lease holder (no election) |
| idle network | heartbeats × databases (1,000 dbs × 2 followers × 10/s = 20,000 msg/s; openraft measured in libraries doc) | heartbeats × servers | zero per database; one keepalive per connection |
| fsync | per group unless the library shares storage | one group commit per node | one group commit per node, all databases |
| per-database fencing (hot move of ONE database) | natural | not natural: the fence is the whole group's term; moving one database out needs a fence inside the state machine | natural (`promised[d]`) |
| per-database trim | natural | the group log is compacted only up to the slowest database (a database that cannot upload pins everybody's log) | natural; disk segments are pinned by the slowest database but can be rewritten (§6.3) |
| restore/replay one database | its own log | scan the shared log, filter | per-database index on the node |
| erase one database | delete its log | rewrite the shared log | erase flag + segment rewrite (§5.10) |
| noisy neighbour | isolated, but each group pays fixed costs | one hot database delays the group's commits | shared fsync and connection; per-database quotas (§7) |
| two notions of "the owner" | yes: Raft leader vs lease holder | yes | no |

(C) is what Apache BookKeeper does at the storage level (one journal per bookie shared by all ledgers, per-ledger
fencing on recovery, leadership from outside), with Raft's log rules instead of BookKeeper's ledger closing.

### 6.2 Batching

One connection per writer–node pair. The writer coalesces every queued `APPEND`/`TRIM`/`FENCE` for that node into one
frame; the node writes all of them to its active segment and calls fdatasync once, then answers one frame with one
`ACK` per database. This is round3's group fsync, extended across the network.

### 6.3 Physical format on a node

Segments `seg-NNNNNNNNNNNN.j3z` (64 MB or 10 minutes). Frame: `len u32 | crc32c u32 | kind u8 | key | body`, kinds
`PROMISE`, `ENTRIES(first_index, entries[epoch, len, payload])`, `TRUNCATE(from)`, `TRIM(k, epoch)`, `ERASE`. In
memory: per database `{promised, trim, last, last_epoch, entries: Vec<(index, epoch, segment, offset)>}`. A segment is
deleted when none of its live records remain (all trimmed, truncated, retired or erased) — the same low-water idea as
round3 `LocalLog::trim`. A segment older than 15 minutes that is still pinned is rewritten.

## 7. Quotas and noisy neighbours

- Per database, in the writer: unflushed bytes soft cap 32 MB (forces an upload), hard cap 128 MB (new writes get a
  retryable "write quota" error until an upload succeeds). This also bounds `buf` (writer memory) and catch-up volume.
- Per server: total unflushed bytes cap (1 GB); when reached, the databases with the most unflushed bytes upload first.
- Per node: per-writer disk quota; a node at 90 % disk refuses `APPEND`s of the writer with the most data on it and
  raises an alarm; writers then rely on the other two nodes.
- Batches are filled round-robin across databases (one database cannot fill a whole frame while others wait).

## 8. Behaviour under specific conditions

- **Partition.** A writer that cannot reach 2 members cannot commit; its writes wait (bounded by the request timeout,
  then a retryable error). If it stays cut off from the journal for `T_cut` (proposal 5 s) while it can still renew the
  lease, it **releases** the lease so that a server on the majority side can take over. If it is cut off from S3 too,
  the lease expires by itself. Asymmetric partitions (A→n reaches, n→A does not) look the same to the writer: no ACK.
  A minority side can never commit (2 of 3 needed). Nodes never need to talk to each other.
- **Disk full** on one node: that node stops acknowledging; writes continue on the other two. On two nodes: writes for
  databases on those nodes are unavailable; nothing acknowledged is lost.
- **fsync error:** the node exits (§5.3). Equivalent to a node crash; it is caught up after restart.
- **Slow disk** on one node: commit uses the 2 fastest; a slow node only lags (latency impact only if two are slow).
- **Clock skew:** cannot break (a)–(d) (no rule uses a clock). Skew larger than the lease allowance (1.5 s in round3)
  makes a takeover happen while the old owner still believes it holds the lease: the old owner's later writes are
  rejected by the fence (availability blip for its in-flight writes; the simulator injects skews up to 10 s).
- **S3 unavailable:** acknowledgements continue (they do not touch S3) until the per-database hard cap; trims stop;
  **takeovers stop** (they need the lease CAS and the SlateDB open). Wakes of sleeping databases stop.
- **Frozen writer resumes** after another took over: its next `APPEND` gets `REJECT`; it abandons the database. Any
  of its acknowledgements that were already justified before the fence are ordered before the new writer's barrier
  (invariant b3).

## 9. Failure matrix

Assumptions: 3 nodes in 3 AZs; each engine server also runs one journal node; lease TTL `T` (round3 default 30 s,
proposed 10 s in journal mode, because a too-early takeover is now harmless) + skew allowance 1.5 s; upload interval
`U` (30–60 s). "Unavailable" = writes to the affected databases fail or wait. Times are **estimated** from the
protocol (lease TTL + 1–3 round trips + a SlateDB open of ≈0.1–1 s), except where marked measured.

### 9.1 Single failures

| failure | acknowledged writes | unflushed but unacknowledged | unavailability |
|---|---|---|---|
| journal node process crash (kill -9), disk intact | none lost | may be lost or kept | none; latency rises while the writer's own node is the one down (walbench, measured: p99 7.9 ms with node 0 down vs 3.1 ms) |
| journal node disk lost | none lost | may be lost | none; tolerance reduced until the databases move to a new node (§5.8) |
| journal node returns stale / with diverged tail | none lost | — | none (caught up, §5.5) |
| writer process crash (kill -9) | none lost | may be lost | its databases: `T` + 1.5 s + open (≈ 11.5–12.5 s with T=10 s; 31.5–32.5 s with T=30 s) |
| writer machine lost (its journal node goes too) | none lost (the other two nodes hold them) | may be lost | same as writer crash |
| whole AZ lost (node + writers in that AZ) | none lost | may be lost | databases written in that AZ: as writer crash; others: none |
| frozen writer (GC, VM pause) | none lost | its in-flight writes fail (retryable) | its databases: until takeover (`T` + 1.5 s) or it resumes |
| writer partitioned from journal majority | none lost | wait / fail | `T_cut` + takeover (≈ 5 s + open, if it can release; else `T` + 1.5 s) |
| partition isolating one node | none lost | — | none |
| asymmetric partition | none lost | — | as the equivalent symmetric case |
| message delay / drop / duplication / reordering | none lost | — | latency only |
| slow disk on one node | none lost | — | none |
| fsync error on one node | none lost | — | as node crash |
| disk full on one node | none lost | — | none |
| clock skew (any size) | none lost | — | possible early takeover (seconds) |
| S3 outage | none lost | — | none for acks until quotas; takeovers and wakes impossible until S3 returns |
| lease store wrongly lets two servers in | none lost (fencing) | the older one's in-flight writes fail | seconds |

### 9.2 Double failures

| failures | acknowledged writes | unavailability |
|---|---|---|
| two journal node processes down (disks intact) | none lost | writes to databases on those nodes: until one returns (no majority) |
| one node down + writer crash | none lost | takeover as writer crash (2 nodes are enough) |
| two journal node **disks lost** within one upload interval | **writes acknowledged since each database's last upload may be lost** (≤ U seconds of writes; the third node may still hold some of them: recoverable by a manual, unsafe "read from one node" tool, not guaranteed) | until operator action |
| one disk lost, then a second before the databases moved to the replacement node | as above, only for databases not yet moved | |
| AZ lost + one more node crash (disk intact) | none lost | writes unavailable until a node returns |
| AZ lost + one more node disk lost | may lose ≤ U s of acknowledged writes (as two disks lost) | |
| writer crash + its journal node disk lost (same machine) | none lost (single machine = single failure) | as writer crash |
| S3 outage + writer crash | none lost | its databases: until S3 returns (no takeover without S3) |
| S3 outage + two nodes down | none lost (if the nodes return) | all writes on those nodes |
| two writers believe they own a database (skew/freeze) + node crash | none lost | blip |

## 10. Engine-facing interface

The trait lives in `journal3z/api/src/lib.rs` (with a single-node local implementation, `LocalJournal`, and tests).
Excerpt:

```rust
pub struct DbKey { pub name: String, pub incarnation: u64 }
pub struct Epoch(pub u64);          // from the lease (seq of the acquiring conditional PUT)
pub struct Index(pub u64);          // per database, from 1, contiguous

pub enum JournalError {
    Fenced { promised: u64 },       // another writer owns the database: abandon it (round3 `abandon`)
    Unavailable(String),            // no quorum / timeout: retryable, nothing was acknowledged
    Quota,                          // per-database unflushed cap: retryable after an upload
    Erased,
    Io(String),
}

pub struct Opened {
    pub replay: Vec<(Index, Bytes)>,  // records above `applied`, to apply again (idempotent via the marker)
    pub next: Index,                  // index of the next append
}

#[async_trait]
pub trait Journal: Send + Sync {
    /// Identity written into the database marker (`sys/journal`), like round3's LOG_ID.
    fn id(&self) -> &str;
    /// Become the writer of `db` at `epoch`. `applied` = index in the database's S3 marker. Fences older writers.
    async fn open(&self, db: &DbKey, epoch: Epoch, applied: Index) -> Result<Opened, JournalError>;
    /// Append one record. The index is assigned now (call under the database's lock, as round3 `next_seq`);
    /// `durable` resolves when the record and every earlier one of the database are acknowledged.
    fn append(&self, db: &DbKey, epoch: Epoch, payload: Bytes) -> Result<Appended, JournalError>;
    /// The database state up to `index` is durable in object storage (confirmed). Allows trim.
    async fn uploaded(&self, db: &DbKey, epoch: Epoch, index: Index) -> Result<(), JournalError>;
    /// Everything is uploaded and the database goes to sleep (or is moved): release in-memory state.
    async fn close(&self, db: &DbKey, epoch: Epoch) -> Result<(), JournalError>;
    /// The database is dropped / erased: nothing of it may ever be replayed.
    async fn erase(&self, db: &DbKey) -> Result<(), JournalError>;
    /// Databases with records not yet uploaded (startup: which databases must be opened and replayed).
    async fn pending(&self) -> Result<Vec<(DbKey, Index)>, JournalError>;
}
```

Mapping to round3's local log (`mongo-server/src/locallog.rs` on `round3`, read-only here):

| round3 | trait |
|---|---|
| `next_seq()` + `append(seq, db, ops)` → oneshot after fsync | `append(db, epoch, payload)` → `Appended { index, durable }` |
| marker `sys/local-log = {log, seq}` in every batch | marker `sys/journal = {journal id, epoch, index}` in every batch |
| global `seq` per process | per-database `index` (a shared log can still number globally inside, as the local backend does) |
| `has_pending(db)` | `pending()` |
| `closed(db)` (clean close uploaded everything) | `close(db, epoch)` |
| `append_drop(db)` (restart must not replay) | `erase(db)` (and `Fenced` on `abandon`) |
| uploader → `trim()` by low water | `uploaded(db, epoch, index)` → trim |
| `recover`: records with `seq > marker`, conflict if marker names another log | `open(..., applied)` returns `replay` = records with `index > applied`; a marker naming another journal id is the engine's conflict case, unchanged |
| torn last record ignored | same (both backends) |
| one fsync for every database | same (local: one file; 3-zone: one frame per node) |

The local backend keeps round3's semantics exactly: one process, one shared segment directory, group fsync, torn tail
ignored, per-database replay above the marker, trim below the uploaded index; epochs are checked in-process (a single
node cannot fence another machine; that is what the 3-zone backend adds).

## 11. Library or custom (summary; details and numbers in `docs/journal3z-libraries.md`)

No custom protocol exists today: the walbench "quorum" is a speed benchmark (no epochs, no fencing, catch-up from the
writer's memory only, recovery = union of a fixed pair). This phase specifies the protocol (this document), checks it
(simulator + TLA+), and measures 3 libraries and a spike of the custom write path against fixed criteria.

## 12. The walbench "connection refused" cycle (summary)

Harness problem, not a protocol or recovery-data problem. Cycle 1 of `crash-quorum` (seed 5) chose read pair `0,2`
(replayed exactly: all 200 recorded cycles reproduce), and node 0 was then still inside the 15 s restart delay of the
`quorumtest` scenario that had SIGKILLed it at t = 45.0 s; the crash test started right after that scenario
(07:02:33) without waiting for node 0. Details and evidence: `docs/journal3z-walbench-cycle1.md`. Design consequence:
D6.

## 13. References

- D. Ongaro, J. Ousterhout. In Search of an Understandable Consensus Algorithm (Raft). USENIX ATC 2014.
- L. Lamport. Paxos Made Simple. 2001.
- B. Liskov, J. Cowling. Viewstamped Replication Revisited. MIT-CSAIL-TR-2012-021.
- L. Lamport, D. Malkhi, L. Zhou. Vertical Paxos and Primary-Backup Replication. PODC 2009.
- F. Junqueira, I. Kelly, B. Reed. Durability with BookKeeper. ACM SIGOPS OSR 2013.
- PostgreSQL "fsyncgate" (2018): after an fsync error the page cache cannot be trusted.
