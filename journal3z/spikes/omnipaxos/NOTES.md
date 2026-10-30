# OmniPaxos spike: notes

Crates used: `omnipaxos = "=0.2.4"` (released 2026-10-18) and `omnipaxos_storage = "=0.2.4"` (`MemoryStorage`, used for
reads). Checked on 2026-10-30, by reading the 0.2.4 source in `~/.cargo/registry` and a clone of
`github.com/haraldng/omnipaxos`. Criteria: `docs/journal3z-libraries.md`. All numbers here are smoke tests on a loaded
4-vCPU container (load average 3–5 from other benchmarks). They show that the modes work and are not results.

## How to run

```
cargo build --release
./target/release/spike-omnipaxos --mode latency [--delay 0.5-1.0 --conc 8 --n 20000 --warmup 2000 --dir D --out F]
./target/release/spike-omnipaxos --mode idle --groups 1000 --secs 10 --dir D
./target/release/spike-omnipaxos --mode fence --dir D       # C5 scenarios, JSON
./target/release/spike-omnipaxos --mode trim  --dir D       # C6 scenario, JSON (extra mode)
```
Environment knobs (defaults in brackets): `OMNI_TICK_MS` [100], `OMNI_ELECTION_TICKS` [1, so one BLE heartbeat round
every 100 ms], `OMNI_RESEND_TICKS` [10], `OMNI_BLE=off` (sets `election_tick_timeout = u64::MAX`, so BLE never runs a
round and node 0 takes leadership with `try_become_leader()`). `fence` and `trim` always run with BLE off.

## Set-up

- There are 3 servers per group (pids 1, 2, 3 are harness nodes 0, 1, 2). Each server has its own tokio task, which
  calls `tick()` every 100 ms. After each step, the task takes the storage writes and calls `take_outgoing_messages`.
  A step is one queued event, or up to 256 events drained from the queue, or one tick. Messages are never held until
  the next tick. Every message between servers goes through `DelayLine::send`. Messages are not serialized: their
  sizes are estimated. The file `LogFile` is shared by all groups on a node.
- **Node 0 as leader.** `Ballot` is ordered by `(n, priority, pid)`, and `ServerConfig.leader_priority = 1` on pid 1
  (the others are 0), so with BLE on, node 0 wins the first round. With BLE off, node 0 calls `try_become_leader()`.
  In both modes, start-up takes about 0.1 s.
- **No acknowledgement before fdatasync.** The `Storage` trait is synchronous and has no "durable later" notion.
  `storage.rs` delegates to `MemoryStorage` and records every write. `SetDecidedIndex` is recorded lazily: it is
  written with the next real write, so a decide alone never costs an fsync. After each step, `server.rs` writes that
  step's records to the node's `LogFile` (one write plus fdatasync, with group commit). Every message of that step
  that can depend on them (Promise, Accepted, AcceptSync, Prepare, NotAccepted, ProposalForward, ...) is sent only
  from the LogFile callback. Later messages from the same server queue behind them, so none of them overtakes a
  held message. There is one exception, for latency: a leader's AcceptDecide and Decide, and the BLE heartbeats,
  leave at once. A Raft leader likewise sends entries in parallel with its own fsync.
  - OmniPaxos counts the leader's own vote at append time (`set_accepted_idx(self.pid, ..)` in `leader.rs`), before
    the vote is durable. To compensate, the driver withholds an incoming `Accepted(k)` from the leader until the
    leader's own file is fdatasync'd up to `k` (the `Durable` event). A decision (`is_chosen`, only in
    `handle_accepted`) therefore always counts 2 durable copies: either the leader plus one follower, or two followers.
  - The client acknowledgement is `decided_idx >= get_accepted_idx()` read right after `append` on node 0. This is
    valid only on the leader in the accept phase with `batch_size = 1`.
  - The simpler option would have been a blocking `fdatasync` inside the storage calls. That is what the crate's
    own RocksDB storage does (`sync_writes = true`). It puts one fsync on the leader's critical path before any
    AcceptDecide leaves, and it gives no group commit across steps.

## Smoke results (measured, small, noisy)

| mode | result |
|---|---|
| latency 0.5–1.0 ms, conc 8, n 3000, BLE on | p50 2.04 ms, p99 3.14 ms, 3,850 writes/s, 3.99 msgs/write, 332 µs CPU/write (209 µs excl. net), HWM 7.9 MB. A proto run just afterwards with the same small n gave p99 10.1 ms and 2,004/s; the machine was too loaded to compare the two. |
| latency 0.5–1.0, BLE off | p99 7.8 ms, 2,988/s (load average 5.4) |
| latency 1–2 ms | p99 8.9 ms, 1,901/s |
| idle, BLE on (100 ms round) | 1 group: 120 msg/s, 1.0 % CPU; 100: 11,995 msg/s, 7.3 %; **1000: 118,136 msg/s, 63 % CPU (42 % excl. net), 84 MB RSS** |
| idle, BLE off | 1000 groups: **0 msg/s**, 7.9 % CPU (tick 100 ms: 3,000 instances × 10 ticks/s), 66 MB; with `OMNI_TICK_MS=1000`: 0.9 % CPU |

BLE costs 12 messages per group per round: each of the 3 servers sends a HeartbeatRequest to its 2 peers and gets 2
replies. That cost cannot be lowered except by lengthening the round, because BLE is the failure detector. N = 1000
works in both modes.

## C5: fencing from an external lease

API facts (0.2.4):
- There is no call that sets a ballot. `try_become_leader()` sets `n = promise.n + 1` (with the own priority and pid).
  `set_priority(p)` changes only the tie-breaker, because `n` is compared first. `Ballot.n` is a `u32`, while the
  lease `seq` is a `u64`.
- BLE is an independent election. It runs while `election_tick_timeout` is finite, and `check_takeover` then raises
  `n` by itself. It can only be disabled indirectly, with a timeout of `u64::MAX`, and leadership then comes only
  from `try_become_leader`.
- On a server that is not the leader, `append()` returns `Ok(())` and forwards the entry to the leader
  (`ProposalForward`), which commits it. The caller gets no index, and no signal that it is no longer the leader.
- A fenced leader receives `NotAccepted`, which only marks the peer as `lost_promise`. The leader keeps its role and
  its promise until it receives the new leader's Prepare, so a partitioned stale writer's appends just hang. That is safe,
  because they are never decided, but the writer gets no error.
- Reconfiguration (`reconfigure`, StopSign) changes membership, not leadership. It does not help with fencing.

Spike (`--mode fence`, BLE off):
- **S1 (native).** The lease moves to node 1, which calls `try_become_leader()` while node 0 is appending.
  - The append in flight was decided at index 4. This is legitimate: it was accepted before the fence, as with an
    old-epoch entry kept by D1's barrier.
  - Node 0's later append returned `Ok(())`, was forwarded, and **was committed by the new leader at index 5**. The
    stale writer wrote after the fence.
- **S2 (native).** Node 0, now stale, calls `try_become_leader()` again: it gets `n = 3` and **takes leadership back
  from the newer lease holder**. Its append is acknowledged and decided. The new writer's append is forwarded to it.
- **S3 (workaround).** This sets the ballot number to the lease epoch. It forges a `Prepare{n: epoch-1, pid: self}`
  from the node to itself, so the promise becomes `epoch - 1`, then calls `try_become_leader()`, which gives
  `n = epoch`. It refuses locally when `promise.n >= epoch`, and appends only as leader in the accept phase with
  `promise.n == epoch`, so nothing is ever forwarded.
  - Epoch 9 on node 0, then epoch 10 on node 1, with node 1 → node 0 cut. Node 0's append that was in flight was
    decided at index 9 (accepted before the fence).
  - Node 0's later append was accepted locally but **never decided**: nodes 1 and 2 had promised 10.
  - After reconnection, node 0's undecided entry was replaced by the epoch-10 barrier, and a stale re-open at
    epoch 9 was refused.

**Verdict as measured: FAIL with the API as it is. PARTIAL only through a forged internal message and local guards.**
That is not supported API, and it depends on the internals of `handle_prepare` and `try_become_leader`.

## C6: trim to an external index

API facts:
- `trim(Some(i))` is accepted only on the leader (`NotCurrentLeader` otherwise), and only if `i <=` the minimum index
  accepted by **all** servers as known to this leader (`leader_state.accepted_indexes`). Otherwise it returns
  `NotAllDecided(min)`. The minimum is also bounded by the leader's decided index.
- Followers apply `Compaction::Trim(i)` with `try_trim`, which silently does nothing if `i` is above their own
  decided index.
- `accepted_indexes` restart at 0 with every new leader. From the source (not run): after a leader change, trim is
  refused until every follower has replied in the new term.
- `snapshot(Some(i), local_only = true)` compacts one node by itself. It needs `Entry::Snapshot` with
  `use_snapshots() = true` (with `NoSnapshot`, it panics in `create`). Once snapshots are on, `create_log_sync`
  sends a lagging node a (delta) snapshot of the decided entries instead of the entries themselves. That is the
  state-machine transfer D5–D7 do not want.

Spike (`--mode trim`):
- All servers up: `trim(Some(50))` gives Ok, and the compacted index is 50 on every node.
- Node 2 cut off, 100 more writes decided (200 in total): `trim(Some(150))` gives **`Err(NotAllDecided(100))`**.
  `trim(None)` trims to 100 on nodes 0 and 1. `trim` on a follower gives `Err(NotCurrentLeader(1))`.

**Verdict as measured: FAIL.** One node that is down pins the group's log, whatever the S3 marker says. The only
alternative is the snapshot path, with state transfer.

## C7: maturity (checked 2026-10-30)

- crates.io versions:
  - 0.1.0 (2023-05-05), 0.2.0 (2023-06-27), 0.2.1 (2023-07-04), 0.2.2 (2023-11-06).
  - Then nothing for almost 3 years, followed by 0.2.3 (2026-09-24) and 0.2.4 (2026-10-18).
  - Total downloads 33,116 (5,265 recent), of which 27,057 are for 0.2.2.
  - It is still 0.x, and the README says "in-development replicated log library".
- Reverse dependencies on crates.io: 6. Three are the project's own crates (`omnipaxos_storage`, `omnipaxos_ui`,
  `omnipaxos_runtime`); the other three are `tsoracle-driver-paxos`, `tsoracle-paxos-toolkit` and
  `tsoracle-standalone`. **Production users: not verified** (none found).
- Repository commits per year: 40 in 2021, 17 in 2022, 78 in 2023, 7 in 2024, 4 in 2025, 11 in 2026. About 119 of
  the 157 commits are by the main author, Harald Ng. GitHub stars and issues: not verified (the API is not reachable
  from this session).
- Origin: KTH research. EuroSys'23 paper "Omni-Paxos: Breaking the Barriers of Partial Connectivity" (linked in the
  README).
- No CHANGELOG. From the git log:
  - 0.2.3 broke the API compared with 0.2.2:
    - `u64` became `usize`.
    - `outgoing_messages()` became `take_outgoing_messages(&mut Vec)`.
    - `get_current_leader()` now returns `(NodeId, bool)`.
    - New: `try_become_leader`, `get_accepted_idx`, the atomic storage API (`write_atomically`), the flush-batch
      timer, and the async runtime crate. RocksDB storage moved behind a feature flag.
  - 0.2.4 changed only internal storage in the core crate (11 lines): the decided index can no longer decrease in
    `sync_log`. The other work in that release went to the runtime crate and a Maelstrom test.

## C8: adapter size and what production would still need

`wc -l src/*.rs`: `storage.rs` 234 (the Storage adapter), `server.rs` 374 (driver: durability ordering, Accepted
gating, network, ticks), `main.rs` 290 (modes and scenarios). Total 898.

Still missing for production:
1. A real on-disk log: index, CRC per record (a torn write inside a `write_atomically` record goes undetected),
   recovery that rebuilds promise, accepted round, entries, decided and compacted indexes, and physical trim with
   segment deletion. The `Storage` reads (`get_entries`, `get_suffix`) are synchronous, so they must be served from
   memory or block.
2. Serialization of messages (the `serde` feature plus a codec). A transport that calls `reconnected(pid)` after
   any loss, because follower sequence numbers depend on it.
3. Handling of fdatasync errors (stop the node).
4. The C5 workaround (forged Prepare plus guards) and a C6 workaround (snapshot path or a custom trim). Neither is
   API.
5. For many databases: one OmniPaxos instance per database, ticking. Each needs its own task or a ticker, and there
   is no shared fsync across instances unless built as here.

## Problems and surprises

- The leader counts its own vote before storage is durable. A synchronous `Storage` that writes asynchronously is
  therefore unsafe unless the driver adds gating, as here.
- `ServerConfig.buffer_size` defaults to 100,000 and is passed to `Vec::with_capacity` for each instance's outgoing
  buffer. I set it to 1024 and did not measure the default.
- A naive acknowledgement ("decided ≥ my index") on a deposed leader can acknowledge someone else's entry at that
  index. Production code must check the entry's identity. This did not happen in the runs.
- The decided index is persisted lazily in this adapter. This is safe because it is relearned from the leader.
- Not verified: crash and restart recovery (no reader for the file); behaviour under message loss; reconfiguration.
