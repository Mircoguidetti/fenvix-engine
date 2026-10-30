# Spike: raft-rs (tikv `raft` crate)

Spike date: 2026-10-30. Criteria: `docs/journal3z-libraries.md` (unchanged). All numbers below are **smoke runs**
(this container, 4 vCPU, short runs), not the final measurements.

## Version and build

- `raft = "=0.7.0"` from crates.io (released 2023-03-07, the latest release), `default-features = false`,
  feature `protobuf-codec` (rust-protobuf 2.28.0). `prost-codec` does **not** build here: its build script needs
  `protoc` (`prost_build::protoc_from_env` panics; protoc is not installed).
- Git master (checked with `git clone` on the spike date): HEAD `ad13f3d` (2026-05-13), 29 commits after the `v0.7.0`
  tag (2023: 10, 2024: 11, 2025: 3, 2026: 5), `Cargo.toml` still says `0.7.0`: **unreleased work exists** (e.g.
  `Config::disable_proposal_forwarding` 2024-10, `max_apply_unpersisted_log_limit` 2025-02, `prost-codec` without
  protobuf 2026-03, unstable-buffer memory-leak fix 2026-05). The spike compiles unchanged against `ad13f3d`
  (scratch copy, not committed) and the latency/fence smoke results are the same.

## What the spike does (src/node.rs = adapter, src/main.rs = modes)

- One thread per node drives all `RawNode`s of that node (one per group): `recv_timeout` until the next tick, drain the
  inbox, then process `Ready` for every touched group (no wait for the tick).
- Config: tick 10 ms, `election_tick` 10, `heartbeat_tick` 3 (raft-rs `Config::default()` is 20/2; examples use
  10/3), `check_quorum`, `pre_vote`, `max_size_per_msg` 1 MiB (the default 0 means "one entry per append"),
  `max_inflight_msgs` 256, `skip_bcast_commit` false (default; env `J3Z_RAFTRS_SKIP_BCAST_COMMIT=1` turns it on).
  Env overrides: `J3Z_RAFTRS_TICK_MS`, `J3Z_RAFTRS_ELECTION_TICK`, `J3Z_RAFTRS_HEARTBEAT_TICK`, `J3Z_RAFTRS_ACK`.
- Transport: every `Message` to another node is protobuf-encoded and passed to `DelayLine::send`; decoded in the target
  node's thread. Partitions (fence/trim) drop messages at send and at delivery.
- Persistence (async ready): entries, snapshot and hard state are put into `MemStorage` before
  `advance_append_async`, the frames are queued on the node's `LogFile`; its callback (after write + fdatasync) sends
  the ready's `persisted_messages` and calls `on_persist_ready`. The leader's `messages` (appends) go out before its own
  fsync (allowed, Raft thesis 10.2.1); the leader counts itself in the quorum only after `on_persist_ready`. A hard
  state that changed only its commit index (`must_sync() == false`) is not fsync'd.
- Acknowledgement: a write is acknowledged when `raft_log.committed >= index` and the term at that index is the term it
  was proposed in (`J3Z_RAFTRS_ACK=commit`, default). Reason: 0.7.0 hands out committed entries only up to
  `min(committed, persisted)` (`RaftLog::next_entries_since`), i.e. "apply" also waits for the leader's own fsync even
  when two followers already have it. `J3Z_RAFTRS_ACK=apply` acknowledges at `take_committed_entries` instead
  (smoke: same latency within noise).
- idle: N groups, 3N `RawNode`s, **one shared `LogFile` per node** for all groups (group commit across groups),
  group g campaigns on node g % 3 (balanced leaders).

## Smoke results (not the final numbers)

| run | result |
|---|---|
| latency, `--n 3000 --warmup 300`, conc 8, 0.5–1.0 ms | p50 2.21 ms, p99 4.13 ms, 3,507 writes/s, 8.09 msgs/write, CPU 424 µs/write (244 excl. delay line), HWM 17 MB, 0 errors |
| same, proto (reference, same command) | p50 2.89 ms, p99 4.95 ms, 2,714 writes/s, CPU 166 (113) µs/write |
| latency, `J3Z_RAFTRS_SKIP_BCAST_COMMIT=1` | p50 2.19 ms, p99 4.04 ms, 3,534 writes/s, 4.08 msgs/write, CPU 374 (238) µs/write |
| idle, 1 group, 3 s | 133 msgs/s, 1.9 % of a core (1.6 % excl. net), RSS 4.7 MB |
| idle, 100 groups, 3 s | 13,328 msgs/s, 9.3 % (5.0 %), RSS 5.9 MB |
| idle, 1000 groups, 3 s | 133,014 msgs/s, 45 % (29 %), RSS 16 MB, startup 0.03 s, no leader change during idle |

Idle traffic is exactly heartbeats: 2 followers × (1 heartbeat + 1 response) per 3 ticks of 10 ms = 133.3 msgs/s per
group. With TiKV-like 1 s ticks and `heartbeat_tick` 2 it would be 2 msgs/s per group, still 2,000 msgs/s for 1,000
groups (estimated, not measured) and an election timeout of 10–20 s.

## C5: fencing tied to an external lease (`--mode fence`)

API facts (0.7.0, read in the source):
- No API to set the term from outside or to make a node leader without votes. `RawNode::campaign()` (= `MsgHup`) runs
  a (pre-)vote; with `check_quorum` voters ignore it while they hear from a leader. `transfer_leader(id)` needs the
  current leader's cooperation (it sends `MsgTimeoutNow`). `Raft::campaign(CAMPAIGN_TRANSFER)` is `#[doc(hidden)]`.
- What the spike uses: `Raft::become_follower(seq - 1, INVALID_ID)` (pub) then `RawNode::step(MsgTimeoutNow)` at that
  term on the lease holder: it campaigns at term = lease `seq` with the transfer flag (voters skip the check-quorum
  lease). Elections on timeout are disabled in this mode (`election_tick` 1,000,000), so every leader change is caused
  by a lease acquisition. Both are internal-looking pub APIs; this is not a documented use.
- The vote rules still apply: a voter refuses a candidate whose log is less up-to-date. Raising the term is durable
  (`must_sync`), even on a voter that refuses, so the old term is fenced on that node anyway.

Results (seeds 1–4, identical):
1. lease seq 5 → node 0: leader at term 5, 3 writes acknowledged.
2. node 0 partitioned with an in-flight write; lease seq 9 → node 1: leader at **term 9 = lease seq**, write OK.
3. old writer (node 0, still `Leader` at term 5 in its own view) appends another write locally; after healing it
   steps down to term 9; both its writes fail ("overwritten by a later term"), neither is in the new leader's log.
   `old_writer_write_committed_after_new_lease: false`.
4. **But** a plain `RawNode::propose` on the old writer after it stepped down returns `Ok` and is *forwarded* to the
   new leader and committed (index 7, term 9). 0.7.0 cannot disable forwarding; the application must check
   `raft.state == Leader` (the spike does) — and the old writer's later writes are then fenced only by that check plus
   the term. Master has `disable_proposal_forwarding` (unreleased).
5. **Lease holder behind**: node 0 missed a committed entry; node 1 dies; lease seq 12 → node 0: node 2 refuses the vote
   (log not up-to-date), **node 0 cannot become leader**, writes fail; both are at term 12 (old leader fenced, but no
   writer). The only remedy in raft-rs: the up-to-date node 2 campaigns (an election the lease did not choose), then
   `transfer_leader(node 0)`: node 0 leads at **term 14 ≠ lease seq 12**. D1 §5.2 instead copies the most
   up-to-date log from a promiser to the lease holder; raft-rs has no "leader catches up from a follower" path.

Assessment input (the coordinator grades): fencing by term works when the term is forced to the lease seq through
internal-looking APIs; the lease holder cannot always become leader without a second election → **partial**.

## C6: trim to an externally given index (`--mode trim`)

- `MemStorage::compact(i)` (and any custom `Storage`) can drop entries below any index ≤ `applied`, per group (one
  group per database). No snapshot is created by compaction.
- A follower that lags below the leader's first index needs `Storage::snapshot()`. The `Snapshot` can be
  metadata-only (index, term, conf state; `data` empty). `MemStorage::snapshot()` uses the **commit index** (it would
  let a follower skip entries above the S3 marker that are not in S3), so the spike overrides `snapshot()` in a
  wrapper (`J3zStorage`, ~20 lines) to return the S3 marker `k`.
- Smoke: leader commits 41, node 2 partitioned at 21; trim to k = 36 on nodes 0/1 (`compact(36)`, TRIM frame in the
  log file); heal; node 2 receives a 0-byte-data snapshot at 36, then entries 37–46; its handed-out committed entries
  are 1–20, 37–45 (21–36 are "in S3"). No state-machine data transferred.
- Caveats: the app must report snapshot delivery (`report_snapshot`); with async ready, `advance_apply_to` must not
  pass the persisted index (first attempt panicked: "applied(36) is out of range … persisted(20)").
  Assessment input: **pass** (with a custom `Storage::snapshot`), per group.

## C7: maintenance and maturity (fetched 2026-10-30)

- crates.io `raft`: max version 0.7.0 (2023-03-07); previous 0.6.0 (2021-06-16). 889,929 downloads total, 198,808
  recent. **No release in the last 12 months** (last release 3.6 years ago).
- Repository tikv/raft-rs: active but slow (5 commits in 2026, last 2026-05-13); master is unreleased 0.7.0.
- Production user: TiKV — its `Cargo.toml` (master, fetched) patches `raft`/`raft-proto` to
  `git = "https://github.com/tikv/raft-rs", branch = "master"`, i.e. the production user does not use the crates.io
  release. Other production users: not verified. GitHub stars/issues: not verified (API returned no data).
- Stable line: 0.7.0 is not alpha, but the maintained code is git master.

## C8: adapter size and what production still needs

`wc -l src/*.rs`: `node.rs` 442 (the adapter: storage wrapper, transport, ready loop, ack), `main.rs` 346 (modes,
fence/trim scenarios; fence 121 lines). Still needed for production:
1. A durable `Storage` + log engine: index of entries per group, truncation of a conflicting suffix on disk, hard
   state and conf state, recovery on restart (the spike appends frames and never reads them back), segment files and
   deletion/rewrite after trim, erase. raft-rs gives only the trait (TiKV uses raft-engine, a separate crate).
2. Transport: real connections, batching all groups' messages per node pair, back-pressure, `report_unreachable`.
3. Multi-group driver (the spike's per-node loop is a minimal raftstore): idle-group hibernation (not in raft-rs),
   flow control, many-groups tick cost.
4. Lease glue: term from lease seq via internal APIs, the "lease holder behind" case (second election + transfer),
   the application-level guard against proposal forwarding (or move to unreleased master).
5. Membership: raft-rs offers conf changes (joint consensus) inside the group, not D7's "new member set at an upload
   point" — not explored.

## Problems / surprises

- `prost-codec` needs `protoc` at build time; `protobuf-codec` pulls rust-protobuf 2.28 (old major).
- `Config::default()` `max_size_per_msg = 0` sends one entry per append message.
- Committed entries are handed out only after the local fsync (0.7.0); acknowledging at the commit index needs
  `raft_log.committed` (pub field) and a term check.
- 8 messages per write by default (append + response, plus a commit broadcast append + response, to 2 followers);
  4 with `skip_bcast_commit`.
- No hibernate/quiesce in the library (`grep -ri 'hibernat\|quiesce'` finds nothing in 0.7.0 or master).
- Followers forward proposals by default (see C5 step 4).
