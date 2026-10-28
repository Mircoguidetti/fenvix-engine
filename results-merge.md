# results-merge.md: `compat-indexes` + `engine-cost` on branch `merge-engine`

Status: **not merged into `main`** (the decision is yours). The WAL cleanup costs 0.1% (merged) and 1.3% (engine-cost) at the same offered load of 300 ops/s, inside the 1-3% target (section 4.2); the earlier closed-loop cost of 10-21% on the merged binary remains a measured fact at saturation (section 4.3). Nothing else is red. `main`, `control-ops` and `compat-gaps` were not touched. The load test was not run.

Everything below was measured on this branch. A number that was not measured says "not measured".

## 1. Merge and conflicts

`merge-engine` was created from `origin/main` (`618c3bd`), then `origin/compat-indexes` (including `cce959b`) was merged, then `origin/engine-cost`.
- Merge of `compat-indexes`: no conflicts (`59fa9f5`).
- Merge of `engine-cost`: 3 conflicting files, all resolved by keeping both behaviours (`28bba3f`). Nothing was discarded. `src/lifecycle.rs` and the WAL code (root crate) merged automatically with no conflict.

| File | Conflict | Resolution |
|---|---|---|
| `mongo-server/src/cmd.rs` (`handle`) | compat-indexes runs the command inside the JavaScript scope (`crate::js::scoped`); engine-cost wraps the command in the group-commit scopes (`TOKENS`, `PENDING`) and waits for the last commit after the command released its locks | both kept: inside the `TOKENS`/`PENDING` scopes the call is `crate::js::scoped(db, self.dispatch(...))`, so JS state is scoped per command and the reply still waits for durability |
| `mongo-server/src/dbs.rs` (`Dbs`, `Entry`, `new`, `lock_handle`, `entry`, `lock`, `apply`) | compat-indexes added `ttl`, `changes`, transaction fields (`snap`, `seq`, `modified`, `active_txns`, `locks`), TTL hint fields, `absent`; engine-cost added `inflight`, `last_compaction_request`, the in-flight token in `lock()`, and `stage()` + group-commit wait in `apply()` | union of all fields and both constructors (`inflight_counter(name)` and `last_compaction_request: None` in the `Entry` literal of `lock_handle` and `entry`); `lock()` takes the in-flight token, then resets `snap`/`absent` as before; `apply()` keeps the transaction write bookkeeping (`seq`, `modified`) and then stages the writes and waits for the commit |
| `mongo-tests/server.sh` | compat-indexes passes `$SERVER_EXTRA`; engine-cost passes `${SERVER_BIN:-...}` | both kept in one `nohup` line |

Build of the merged workspace: no errors. `merge-engine` was pushed to origin before the measurements (`28bba3f`).
Binary used for the suites and tests: `target/release/mongo-server` of this branch (sha256 starts `d16a8d34cb225743`). Binary used for engine-cost in the comparisons: built from `origin/engine-cost` (`5f6b5bd`) in a worktree (sha256 starts `d060e6f0b66d8244`).

## 2. Differential suites on the merged binary (all green)

| Suite | Result |
|---|---|
| pymongo, all tests (`run_full.sh`, 4127 tests): core / extra / replset | 3989/3989, 49/49, 89/89 |
| Node, same tests: core / extra / replset | 3989/3989, 49/49, 89/89 |
| `js.*` (674) and `where.*` (30) | inside the core numbers above, both drivers; not counted separately |
| `search_test.py` | 46/46 |
| `search_ops_test.py` (pymongo) / `node/search_ops_test.js` | 143/143, 143/143 |
| engine-cost's own tests (its `tests.json`: 1081 core, 49 extra), pymongo | core 1081/1081, extra 47/49 |
| engine-cost's own tests, Node | core 1081/1081, extra 47/49 |

The 2 failing extra tests in engine-cost's old `tests.json` are `aggregate.extra.group_add_to_set` and `update.extra.current_date`. They fail because the old probe definitions are nondeterministic ($addToSet order is unspecified; $currentDate differs by milliseconds between the two servers). Those probes were replaced in commit `0518068` (they are 49/49 in the current set). Logs: `mongo-tests/out/engine_py.log`, `out/engine_node.log`, `out/run_full_merge.log`.

## 3. Durability with SIGKILL (green, negative controls fail as they should)

Logs: `loadtest/results/durability/dur2_*.log`, JSON `loadtest/results/durability/merged-*.json`.

| Test | Result |
|---|---|
| WAL cleanup, `test_group_commit.py --trials 4 --secs 60` (SIGKILL under write load, default settings with the cleanup passes running) | 287,500 acknowledged ids, 0 lost inserts, 0 lost updates |
| same, negative control (`--control`, acknowledge before durable), 2 trials of 30 s | 89,864 ids, 2,510 lost inserts, 31 lost updates |
| SST/manifest/compaction-state cleanup, `test_open_gc.py --trials 4 --secs 100` | 4 of 4 kills inside a cleanup pass, 262 SSTs deleted, 57,420 acknowledged documents, 0 lost inserts, 0 lost updates, 218,350 reads checked: 0 missing, 0 stale, 0 padding errors, 0 errors |
| same, negative control (`--control`, the pass deletes a referenced SST), 2 trials | 20,218 documents, all 20,218 unreadable after restart |

## 4. WAL cleanup cost

### 4.1 Why the merged binary is 3.5 times faster than engine-cost (a code difference, not a build or configuration difference)

Parity checked first:
- Same build profile (`[profile.release] debug = false`, default optimisation), same Rust toolchain, same pinned versions (`slatedb 0.17.0`, `tokio 1.54.1`, `object_store 0.14.2`, `bson 3.1.0`). `mongo-server/Cargo.toml` differs only by the compat-indexes dependencies (`rquickjs`, `s2`, `md-5`, `chrono`, ...).
- Same command-line flags and defaults for every shared flag (`--help` compared): `--wal-gc-secs 20`, `--wal-gc-mode bulk`, `--sst-gc-secs 60`, `--sst-gc-min-unref-secs 120`, `--durable-reads true`, `--group-commit-window-ms 0`, ... The merged binary has only extra flags (TTL, replica set). The same `loadgen` and the same MinIO served both.
- The root crate (`src/lifecycle.rs`, `src/counting.rs`): `git diff origin/engine-cost HEAD` is **70 insertions and 0 deletions** (byte counters, trace phase labels, `snapshot`, `approx_bytes`, `scan_range`). Not one line of the WAL cleanup, SST cleanup or group-commit code differs.

The difference is in the server's `update` command (`loadgen hot` issues `update_one` by `_id`):
- engine-cost, `mongo-server/src/cmd.rs:719-728`: `self.dbs.lock(db)` (exclusive), then `g.collections()`, `g.indexes(coll)` and `g.scan_collection(coll)` on every update, and it builds the unique-index owner maps from all documents.
- merged: the update path finds its documents through `find_docs` (a point get for an `_id` filter), without scanning the collection.

Evidence (local store, no MinIO, 8 connections, writes only, 20 s, same `loadgen`; `/tmp/claude-0/par.sh`):
- Reads by `_id` are equal: merged 19,848 ops/s, engine-cost 20,942 ops/s.
- Updates: merged 3,782 ops/s (50 documents), 4,177 (8 documents), 4,330 (400 documents); engine-cost 507 (50), 572 (8). Server CPU per operation: merged 0.46-0.51 ms, engine-cost 2.61-2.92 ms. (engine-cost with 400 documents: not measured, the run did not finish in the time allowed.)
- Stack samples with gdb during the engine-cost run (6 samples): 29 frames in slatedb range-scan iterators (`MergeIterator`, `MemTableIterator`, skiplist `RefRange`); the same sampling of the merged binary: 0 such frames, and `Reader::get_key_value_with_options` (a point get) instead.
- Not established: how the 2.6 ms is split between the three calls, and why it hardly depends on the document count (8 against 50 documents: 572 against 510 ops/s). Changing `--l0-sst-bytes` moved engine-cost between 228 (100,000) and 763 (2,000,000) ops/s, which does not isolate a cause either.

### 4.2 WAL cleanup cost at the same offered load (300 ops/s), merged against engine-cost

Method: `loadtest/run_fixed.sh` (`exp_objects.py --rate 300`), one hot tenant on MinIO, 8 connections, writes only, 300 s per run, the cleanup off (`--wal-gc-secs 0`) against default (on), same binary within a pair, 4 pairs per binary, pairs in the order merged off, merged on, engine-cost off, engine-cost on. "Server CPU" is the CPU of the engine process. S3 requests are MinIO's own counters (`/minio/v2/metrics/cluster`, requests by API during the run), not the server's object counters. **Every run is listed; none was discarded.** The 6 invalid runs, the restarts and the interrupted attempts are listed below the table.

| Run | End (UTC, 28 Oct) | ops/s | p50 / p99 ms | Server CPU s | Max WAL objects | S3 DeleteObject | S3 DeleteMultipleObjects | MinIO CPU s | S3 PutObject |
|---|---|---|---|---|---|---|---|---|---|
| merged off 1 | 03:40 | 300.0 | 4.86 / 16.24 | 95.3 | 79,167 | 0 | 2 | 235.5 | 81,676 |
| merged on 1 | 03:46 | 300.0 | 4.66 / 22.48 | 91.5 | 9,292 | 0 | 88 | 264.6 | 80,193 |
| merged off 2 | 04:01 | 300.0 | 4.84 / 13.93 | 97.4 | 80,289 | 0 | 3 | 238.8 | 82,719 |
| merged on 2 | 04:07 | 300.0 | 6.31 / 26.27 | 102.1 | 7,992 | 0 | 84 | 273.4 | 74,062 |
| merged off 3 | 04:23 | 300.0 | 5.05 / 13.12 | 100.2 | 79,854 | 0 | 3 | 228.6 | 82,487 |
| merged on 3 | 04:28 | 300.0 | 6.24 / 23.20 | 100.5 | 8,813 | 0 | 84 | 278.4 | 74,883 |
| merged off 4 | 06:26 | 300.0 | 5.88 / 18.29 | 106.4 | 76,037 | 0 | 2 | 237.3 | 78,571 |
| merged on 4 | 06:31 | 298.8 | 7.62 / 17.18 | 76.6 | 8,560 | 0 | 60 | 180.1 | 49,857 |
| engine-cost off 1 | 03:51 | 295.9 | 8.54 / 47.39 | 274.6 | 75,444 | 0 | 10 | 203.7 | 77,875 |
| engine-cost on 1 | 03:56 | 283.9 | 12.00 / 50.02 | 281.6 | 8,213 | 0 | 80 | 232.7 | 70,383 |
| engine-cost off 2 | 04:12 | 264.5 | 18.13 / 60.61 | 306.9 | 61,183 | 0 | 6 | 210.4 | 63,220 |
| engine-cost on 2 | 04:18 | 272.3 | 16.91 / 56.26 | 300.9 | 5,574 | 0 | 72 | 224.3 | 63,167 |
| engine-cost off 3 | 05:53 | 274.3 | 14.78 / 53.47 | 303.7 | 62,776 | 0 | 2 | 216.8 | 65,035 |
| engine-cost on 3 | 05:58 | 271.5 | 15.09 / 56.70 | 301.0 | 7,541 | 0 | 76 | 225.6 | 66,540 |
| engine-cost off 4 | 06:39 | 293.5 | 9.10 / 44.99 | 288.8 | 75,067 | 0 | 2 | 214.2 | 77,557 |
| engine-cost on 4 | 06:45 | 286.4 | 11.89 / 48.09 | 290.8 | 8,698 | 0 | 79 | 228.2 | 71,277 |

All 16 runs: 0 errors. `merged on 4` is an outlier in the load it put on MinIO (49,857 PutObject against 74,000-83,000 in the other merged runs, MinIO CPU 180 s against 229-278 s); I did not find the cause.

| Binary | Cost per pair (on against off, ops/s) | Mean off / on (ops/s) | Mean cost | p50 off / on (ms) | p99 off / on (ms) | Server CPU off / on (s) | MinIO CPU off / on (s) |
|---|---|---|---|---|---|---|---|
| merged | 0.0%, 0.0%, 0.0%, -0.4% | 300.0 / 299.7 | **-0.10%** | 5.16 / 6.20 | 15.4 / 22.3 | 99.8 / 92.7 | 235.1 / 249.1 |
| engine-cost | -4.1%, +2.9%, -1.0%, -2.4% | 282.1 / 278.5 | **-1.26%** | 12.63 / 13.97 | 51.6 / 52.8 | 293.5 / 293.6 | 211.3 / 227.7 |

Reading:
- **At the same offered load the throughput cost of the WAL cleanup is 0.1% on the merged binary and 1.3% on engine-cost** (engine-cost cannot sustain 300 ops/s even with the cleanup off: 264.5-295.9 ops/s). Both are within the 1-3% target.
- The merged binary pays in latency: p99 about 7 ms higher with the cleanup on (15.4 against 22.3 ms), p50 about 1 ms higher. engine-cost p99 changes by about 1 ms (51.6 against 52.8 ms), at a much higher base.
- MinIO CPU rises by about 14 s (merged) and 16 s (engine-cost) per 300 s run with the cleanup on.
- The cleanup is active and bounded in both binaries: maximum WAL objects 7,992-9,292 on (merged) and 5,574-8,698 on (engine-cost), against 76,037-80,289 and 61,183-75,444 off; **all under the 10,000 target**.
- S3 DELETE requests, counted by MinIO: **0 `DeleteObject` in every run; the deletes go as `DeleteMultipleObjects`: 60-88 requests per run on (merged), 72-80 (engine-cost)**, 2-10 with the cleanup off (the end-of-run tenant removal). The server's own counter (`walGc.deleted`, about 65,000-82,000 objects per run) counts objects: one bulk request carries on the order of a thousand keys.
- The earlier 10,687-11,012 maximum (closed loop, `exp_wal_growth.py`, merged binary at 1,200-1,500 ops/s) is above 10,000. At 300 ops/s the same binary gives 7,992-9,292. In the closed-loop `exp_objects` runs of the first comparison (925-1,101 ops/s) it was 8,061-9,206. I did not establish why the first script reads higher; both are far below the 83,000-108,000 without the cleanup.

Runs not used or repeated (kept in the repository):
- 6 runs were invalid, `fx-e-off3`, `fx-e-on3`, `fx-m-off4`, `fx-m-on4`, `fx-e-off4`, `fx-e-on4` (first attempts, in `results/objects/invalid/`): MinIO had no free disk (`XMinioStorageFull`), so they show 0 operations and 93-103 errors. Cause: the benchmark tenants that machine restarts had interrupted (13 directories, 5.3 GB) were never cleaned up and filled the disk. I removed those directories and `run_fixed.sh` now removes orphan tenants and checks free space before each run, and moves a run with no operations or with errors to `invalid/` and stops. They were rerun and the table has the valid reruns.
- Interrupted by a machine restart and rerun from scratch (`results/objects/fixed-restarts.log`): `fx-m-off1` twice (03:27, 03:34), `fx-m-on4` three times (05:00, 05:05, 05:11), `fx-e-off4` once (06:33), `fx-e-off3` once (04:31), `fx-e-off3` again (04:38) and once after the disk was freed (05:46), `fx-m-off4` three times (06:04, 06:11, 06:18). The restarts come every 5 to 20 minutes on this machine and kill MinIO with the run in progress, so **the runs are not back to back**: the merged runs 1-3 and engine-cost runs 1-2 are from 03:40-04:28, the rest later, and the off/on runs of one pair are 5-11 minutes apart (checked by `run_fixed.sh`, which reruns an off older than 15 minutes). `results/objects/stale/` is empty.
- Earlier closed-loop numbers (not at equal load, kept for the record): merged -21.0% (`cmp2-*`, off 1258.7 / on 994.9 ops/s) and -10.0% (`exp_wal_growth`, 5 minutes, off 1475.4 / on 1327.9 ops/s); engine-cost -2.2% (`cmp2-e-*`, off 358.0 / on 350.1 ops/s).

### 4.3 Where the closed-loop cost comes from

The code of the cleanup (WAL pass, SST pass, group commit) is identical in the two binaries (section 4.1: 70 insertions in unrelated code, 0 deletions). At equal load the cleanup costs 0.1% (merged). So the -10% to -21% measured earlier on the merged binary is not a regression of the cleanup in the merge. What differs is the throughput the closed loop reaches (merged 925-1,475 ops/s, engine-cost 342-364 ops/s, section 4.1): the cleanup does a fixed amount of delete work per run (about 65,000 objects, 27-31 s of delete time, same in both) and MinIO and the server share 4 vCPUs. That the loss appears only when the load saturates the machine is consistent with the data (0.1% at 300 ops/s; MinIO CPU +6 to +37 s with the cleanup on at saturation), but I did **not** prove it by running the merged binary at an intermediate load (for example 1,000 ops/s); that run was not made.

## 5. What remains

- Not done by decision: the load test (3 runs) and the merge into `main`.
- Not measured / not established: the merged binary at an intermediate closed-loop load (for example 1,000 ops/s), which would test the explanation of 4.3; the cause of the low load in `merged on 4`; why `exp_wal_growth.py` reads a higher maximum of WAL objects (10,687-11,012) than `exp_objects.py` (7,992-9,292) for the same binary; how the 2.6 ms of engine-cost's update is split between `collections()`, `indexes()` and `scan_collection()`; engine-cost updates on a 400-document collection.
- The runs of the fixed-load comparison are not back to back (restarts every 5 to 20 minutes, section 4.2); pairs were kept within 15 minutes.
- The engine-cost branch's old `tests.json` still has 2 failing extra probes (nondeterministic, section 2); the current extra suite is 49/49.

## 6. Files

- `loadtest/run_fixed.sh`, `loadtest/results/objects/fx-*.json` (16 runs), `results/objects/invalid/` (6 invalid first attempts), `results/objects/fixed-restarts.log` (restarts and the disk-full incident), `results/objects/stale/` (empty), `results/objects/smoke/` (20 s smoke test of the rate limiter).
- `loadgen hot --rate` and `exp_objects.py --rate` (offered load) and MinIO's request counters (`s3_requests_minio`).
- Earlier closed-loop comparison: `loadtest/run_cmp.sh`, `results/objects/cmp2-*.json` (12 runs) and `cmp-m-off1.json`, `results/wal/mrg-*-5m.json` (6 runs).
- Durability: `loadtest/results/durability/*`. The four tracked result files of engine-cost in `results/gc` and `results/open-gc` that the durability tests overwrote were restored; the merged-binary versions are in `results/durability/merged-*.json`.
