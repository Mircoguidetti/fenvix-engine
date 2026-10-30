# openraft spike — notes

- Crate: **openraft 0.9.25** (crates.io, published 2026-07-28; latest stable line), feature `storage-v2` only
  (no `serde`: nothing is serialized, the delay line carries Rust values). Built and run on 2026-10-30, rustc 1.97.
- API facts come from reading the 0.9.25 source in `~/.cargo/registry` (file:line given). 0.10 facts come from reading
  the `0.10.0-alpha.37` crate source. Nothing about 0.10 was run.
- The numbers below are **smoke tests only** (small `--n`, `--secs 3`), run on the shared 4-vCPU container while other
  benchmarks were running (load average 3.5–4). The coordinator's sequential runs are the reference.

## What the spike does

| file | role |
|---|---|
| `src/store.rs` | `RaftLogStorage` + `RaftLogReader` on the harness `LogFile`, plus a trivial `RaftStateMachine` and `RaftSnapshotBuilder`. The state machine records the last applied log id. Its "snapshot" is only a pointer, `s3-marker:<index>`. |
| `src/net.rs` | `RaftNetworkFactory`/`RaftNetwork`. Every RPC between different nodes goes request → `DelayLine::send(from,to,..)` → `raft.<rpc>()` on the target → response → `DelayLine::send(to,from,..)`. For `fence`/`trim` it also has: per-node cut switches (partition), and an optional **epoch guard**. The guard is application code: each RPC carries the sender's lease epoch, and a receiver drops anything below the highest epoch it has seen. |
| `src/main.rs` | modes `latency`, `latency_batched` (application batching layer), `idle`, `fence`, `trim` |

- **Durability.** One `LogFile` per node, shared by every group on that node (the "shared journal" variant, used in
  every mode). `append` writes one framed record per entry and calls `LogFlushed::log_io_completed(Ok(()))` only from
  the `LogFile` callback, which runs after write + fdatasync. `save_vote`, `truncate` and `purge` each wait for their
  record's fdatasync before returning. Entries are readable in memory at once, so the leader can replicate while its own
  fsync runs; Raft allows this, and openraft counts the leader only after its flush.
- **Leader.** `initialize(members)` runs only on node 0, and node 0 elects itself at once (`engine_impl.rs:210`). The
  other nodes have no membership, so they cannot campaign. The run waits until node 0 is `Leader` and both followers
  report `current_leader == 0`.
- **Config:** openraft defaults. `heartbeat_interval` 50 ms; election timeout 150–300 ms; leader lease =
  `election_timeout_max` = 300 ms; `snapshot_policy` `LogsSinceLast(5000)`; `max_in_snapshot_log_to_keep` 1000;
  `max_payload_entries` 300. Exceptions: `fence` sets `enable_elect=false` (no independent election), and `trim` sets
  `SnapshotPolicy::Never`.
  - **Observed:** heartbeats actually go out every ~75 ms. The tick period is `heartbeat_interval*3/2`
    (`raft/mod.rs:249`), and a heartbeat is sent on the first tick after `next_heartbeat`.

Commands (release build: `cargo build --release`):

```
./target/release/spike-openraft --mode latency --n 20000 --warmup 2000 --conc 8 --dir <dir>
J3Z_INFLIGHT=1 ./target/release/spike-openraft --mode latency_batched --n 20000 --warmup 2000 --conc 8 --dir <dir>
./target/release/spike-openraft --mode idle --groups 1000 --secs 10 --dir <dir>      # also 1, 100
./target/release/spike-openraft --mode fence --dir <dir>
./target/release/spike-openraft --mode trim --dir <dir>
```

## Smoke results (measured, under concurrent load; not the reference numbers)

**Latency**, `--n 3000 --warmup 300`, conc 8, 1 KB, delay 0.5–1.0 ms:

| run | p50 | p95 | p99 | writes/s | CPU per write (excl. net) | HWM | messages per write |
|---|---|---|---|---|---|---|---|
| 1 | 6.0 ms | 31.9 ms | 32.4 ms | 776 | 274 µs | 13.7 MB | 1.23 |
| 2 | 7.7 ms | 23.1 ms | 29.5 ms | 823 | 268 µs | — | — |

For comparison, `proto` with the same arguments in the same minutes: p99 9.5 ms, 2,126 writes/s, 114 µs.

**The leader fsyncs once per entry.** Node 0 made 3,304 fdatasyncs for 3,301 entries; the followers made about 630
each (they batch). Two causes in the source:
- In 0.9.25 every `client_write` becomes its own one-entry append (`core/raft_core.rs:497`, `vec![entry]`).
- `RaftCore::append_to_log` **awaits the flush callback inline** (`core/raft_core.rs:726-729`) before the core
  handles the next command.

So the `LogFlushed` callback API exists, but in 0.9 the leader gets no I/O pipelining and no group commit, and
throughput is about 1 / fsync time. 0.10.0-alpha.37 changes this: "Submit IO request, do not wait for the response"
(`core/raft_core.rs:2421`). Not measured.

### `--mode latency_batched` (added on request, for fairness with `proto`/custom, which batch)

Everything is the same as `latency`: the same `LogFile` durability, the same delay line, node 0 as leader. The only
addition is a batching layer in front of `client_write` on node 0 (`batcher()` in `main.rs`):
- One task takes every write queued while earlier `client_write` calls are in flight, up to 64 writes / 256 KB per
  batch.
- It submits the batch as **one** entry. The payload is the writes' `frame(payload)`s concatenated, so the same bytes
  reach the file.
- It resolves every writer when that entry's `client_write` returns (committed and applied).
- The number of concurrent `client_write` calls is set by `J3Z_INFLIGHT` (default **1**). openraft 0.9 accepts
  concurrent `client_write` calls: they queue on the core's channel and each becomes its own append and fsync.

Smoke results, `--n 3000 --warmup 300`, conc 8, run under concurrent load:

| variant | p50 | p95 | p99 | writes/s | CPU per write (excl. net) | writes per entry | fsyncs per node |
|---|---|---|---|---|---|---|---|
| inflight 1 (default) | 7.4 ms | 8.6 ms | 10.0 ms | 1,198 | 200 µs | 4.6 | 718 / 717 / 716 |
| inflight 1, repeat | 7.4 ms | 8.6 ms | 9.5 ms | 1,186 | 213 µs | 4.6 | — |
| inflight 2 | 5.9 ms | 8.8 ms | 9.9 ms | 1,313 | 218 µs | 3.0 | 1,100 / 836 / 799 |
| inflight 4 | 5.1 ms | 8.5 ms | 9.5 ms | 1,465 | 254 µs | 1.9 | 1,766 / 753 / 708 |

`proto` with the same arguments, in the same minute as the repeat run: p50 2.9 ms, p99 5.3 ms, 2,667 writes/s, 117 µs.

What batching changes and what it doesn't:
- It removes the tail from serialized leader fsyncs: p99 drops from ~30 ms to ~10 ms, and throughput rises about 1.5×.
- At inflight 1, one commit cycle (`client_write` to return) takes ~3.8 ms. That is about 2.5× the network round trip
  plus one fsync.
- More in-flight calls do not help much: each `client_write` is still its own leader append and fsync, and the 0.9 core
  waits on each one.

Command: `J3Z_INFLIGHT=1 ./target/release/spike-openraft --mode latency_batched --n 20000 --warmup 2000 --conc 8 --dir <dir>`

**Idle**, `--secs 3`:

| groups | Raft instances | start-up (build + elect) | messages/s | CPU, % of one core (excl. net) | RSS |
|---|---|---|---|---|---|
| 1 | 3 | 0.006 s | 52 | 0.9 % (0.5 %) | 6 MB |
| 100 | 300 | 0.095 s | 5,197 | 13.2 % (9.2 %) | 30 MB |
| 1,000 | 3,000 | 1.31 s | 52,195 | 116 % (86 %) | 237 MB |

- 1,000 groups start fine, and all 1,000 node-0 leaders still hold at the end.
- The traffic is heartbeats: ~13/s × 2 followers × request+response = 52 messages/s per group, linear in groups.
- C4 limits are ≤ 1,000 messages/s and ≤ 5 % CPU. At 1,000 groups this run is about 52× over the message limit and
  about 23× over the CPU limit.
- Heartbeats do not touch the disk.

## C5 — fencing tied to an external lease

### API facts (0.9.25)

- **The term is internal.** `elect()` uses `new_term = vote.term + 1` (`engine/engine_impl.rs:228`).
  - `trigger().elect()` takes no term (`raft/trigger.rs:45`), and on a leader it is a no-op.
  - No public API sets the term or the leader directly.
  - There is no `transfer_leader` in 0.9. 0.10 adds `trigger().transfer_leader(to)`, but only the *current* leader can
    issue it.
- **Election restriction.** A voter refuses a candidate whose `last_log_id` is smaller than its own
  (`engine_impl.rs:322`). It also refuses while its leader lease is valid: the vote is committed and
  `now <= vote_utime + leader_lease`, where followers renew `vote_utime` on every heartbeat (`vote_handler/mod.rs:113`)
  (`engine_impl.rs:305`).
- **The only way to inject a term from outside:** call `Raft::vote(VoteRequest(Vote(epoch-1, self)))` on the node
  itself, then `elect()`. This was tried, and the effect is not exact.
- `enable_elect=false` turns off timer-driven elections, so leadership changes only by an explicit `trigger().elect()`.
- 0.10 alpha makes `Term`/`LeaderId`/`Vote` pluggable types. The new term is still `vote.term().next()`
  (`engine_impl.rs:282` in 0.10), so 0.10 still has no external term.

### Spike scenarios (`--mode fence`, `enable_elect=false`; output is one JSON line)

| | scenario | result |
|---|---|---|
| S1 | Lease moves to node 1; old leader node 0 is alive and connected | `elect()` on node 1 won in 7 ms (154 ms in another run, at T2). The stale write on node 0 got `ForwardToLeader`; node 1's write committed. |
| S2 | Leader node 1 is cut with a write in flight; lease moves to node 2 (epoch 30); term injection tried | The self-vote was refused once (lease), then granted at T29. The final term was **31, not 30**: each retry adds 1. The in-flight entry (idx 5, T2) was overwritten by T31 after the heal, and its client got `ForwardToLeader`. Node 2's write committed. |
| S3 | **No guard.** Legit holder node 2 (epoch 30) is cut briefly; stale node 0 (epoch 10) calls `elect()` | **The stale node won (T34, 305 ms) and committed a write.** After the heal, the legit holder is a follower and its write fails. openraft orders writers by its own term, not by the lease. |
| S4 | **Epoch guard on.** Node 2 is leader at epoch 40; node 2 is cut; stale node 0 calls `elect()` 14 times | Every vote was refused by the guard (14 RPCs dropped), and the stale write was not committed. On the heal, the legit leader saw the stale candidate's higher term and stepped down: its next write was **not committed within 2 s**. It had to re-elect (T50) before writes committed again. |
| S5 | Guard on. Lease moves to **node 0, which is behind** (it missed 5 committed writes); the old holder is gone | **Node 0 can never become leader:** 20 attempts in 3 s, all refused by node 1's log check. With elections off, no one else leads, so the database is unavailable until the lease moves to an up-to-date node. |

### Reading (verdict is the coordinator's call)

openraft alone does **not** meet C5:
- The fencing token is openraft's own term. Any node can out-term the lease holder (S3), and the term cannot be set to
  the lease epoch.
- With an application epoch guard in the transport (S4), stale writers are fenced. But two gaps remain:
  - A lagging lease holder cannot be elected (S5). The design's rule R3 avoids this by reading the best log from the 2
    promisers; Raft requires the candidate's own log to be up to date.
  - A fenced stale candidate still disrupts the legit leader once.

At best this is **partial**, and only with protocol code written around the library.

## C6 — trim to an external index

### API facts (0.9.25)

- Purge happens only below a snapshot. `trigger().purge_log(upto)` clamps `upto` to `snapshot_last_log_id`, and does
  nothing at all without a snapshot (`engine_impl.rs:609-638`).
- `SnapshotPolicy` is `LogsSinceLast(n)` (default 5000) or `Never` (manual `trigger().snapshot()`).
- Automatic purge keeps `max_in_snapshot_log_to_keep` (default 1000) entries; a manual `purge_log` ignores that setting.
- The snapshot is whatever the application's `RaftSnapshotBuilder` returns, and its `meta.last_log_id` may be any
  applied log id. The spike builds it **at the S3 marker k**, with data `s3-marker:k` and no state.
- A follower behind the purge point gets the snapshot through `install_snapshot` (chunked; or `full_snapshot` with
  `generic-snapshot-data`). The application's `install_snapshot` must accept a pointer, i.e. "state is in S3 up to k".
- Purge is per Raft instance: the application calls it on each of the 3 replicas.
- A leader may delay a purge while a replication task is reading those entries (`raft/trigger.rs` doc).

### Spike (`--mode trim`)

- 200 writes, then marker k=151 on all 3 nodes: snapshot at 151 → `purge_log(151)` → every log is now 152..201.
- Then node 2 is cut, 100 more writes, k=291 on nodes 0 and 1. The leader's purge took 104 ms with the follower down.
- After the heal, the leader sent node 2 the pointer snapshot: **2 InstallSnapshot RPCs, 154 bytes**. Node 2 installed
  it and caught up, with log 292..302.

### Reading

Trim to an external index **works** without moving state-machine data, but only through openraft's snapshot ritual:
- a builder that returns a pointer,
- per-node trigger calls,
- a pointer "install" on lagging followers.

That is pass with ceremony, or partial if the criterion is read strictly. The spike sets `last_membership` to the
current membership, which is wrong if a membership change falls between k and the applied index. A production adapter
must track membership per index.

### Surprise seen in `trim`

The cut follower, with elections on (the default), campaigned while isolated. When it came back, its higher term made
the leader step down and re-elect: the term went from 1 to 5 and a blank entry appeared at index 302. 0.9 has no
pre-vote. 0.10's `trigger().elect(pre_vote)` suggests pre-vote exists there (not verified).

## C7 — maturity (crates.io API, fetched 2026-10-30)

- **Stable line:** 0.9.x, from 0.9.0 (2024-03-11) to **0.9.25 (2026-07-28)**. 2026 releases so far: 0.9.22 (04-08),
  0.9.23 (04-20), 0.9.24 (04-21), 0.9.25 (07-28).
- **Next line:** 0.10.0 is **alpha only**. 24 alphas are on crates.io, from alpha.14 (2026-02-28) to alpha.37
  (2026-10-10); `max_version` is 0.10.0-alpha.37. 1.0 does not exist.
- **Downloads:** 2,131,372 total, 964,821 recent (90 days); 100 reverse dependencies.
- **Production use:** the README names Databend's meta-service as its user ("Who use it": Databend). Not verified beyond
  the README.
- **Repository:** github.com/databendlabs/openraft. Activity could not be checked: GitHub API access is not enabled in
  this session (**not verified**).
- **Upgrade cost:** the 0.9 → 0.10 API changes are large (type config, error and storage traits), so starting on 0.9
  means a migration later. This is from reading the 0.10 source; the extent is **not verified**.

## C8 — adapter size

`wc -l src/*.rs`: `main.rs` 416, `net.rs` 179, `store.rs` 243 = **838**. The batcher in `main.rs` is about 60 lines.

- The adapter proper (storage + network) is `store.rs` + `net.rs` = **422** lines.
- `main.rs` is the test driver and the scenarios.

Still needed for production:
1. Restart recovery: replay the file into the in-memory index, `read_vote`, the last purged id, and truncation and
   purge records.
2. A real entry and membership encoding (the spike writes membership in `Debug` form).
3. Segment files and space reclamation for a shared journal (the spike's file is append-only and never shrinks).
4. A real transport (TCP/RPC with batching across groups). openraft sends one RPC per group per heartbeat, and there is
   no cross-group batching hook.
5. The epoch guard with a durable `promised` per database, and lease integration (C5).
6. A pointer-snapshot install path that reads S3, and per-index membership tracking (C6).
7. fsync error handling: stop the node.

## Problems and surprises

- 0.9: the leader fsyncs per entry and the core waits on each flush (see Latency). This is the main cause of the low
  throughput here.
- A leader's own vote is not renewed by its heartbeats. So in S1 the live leader itself granted node 1's vote at once
  (7 ms), and node 1 won with that vote plus its own. Explanation from reading `handle_vote_req` and `update_vote`;
  **not verified further**.
- The term cannot be pinned to the lease epoch (S2: 31 instead of 30).
- `declare_raft_types!` expands to an unqualified `Cursor`, so `std::io::Cursor` must be imported by the caller.
- Without `storage-v2`, `RaftLogStorage`/`RaftStateMachine` are sealed (the default is the v1 `RaftStorage` adaptor).
- Harness: no bug found. `DelayLine` runs closures on its own thread; the adapter spawns the target-side RPC onto the
  tokio runtime from there.
