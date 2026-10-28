# results-merge.md: `compat-indexes` + `engine-cost` on branch `merge-engine`

Status: **the merge is NOT merged into `main`.** One check is red: the WAL cleanup costs far more than 1-3% on the merged binary (below). Nothing else is red. `main`, `control-ops` and `compat-gaps` were not touched. The load test was not run (you will run it after the WAL cost is clarified).

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

## 4. WAL cleanup cost (RED)

Method: `loadtest/run_cmp.sh` (`exp_objects.py`), one hot tenant on MinIO, 8 connections, writes only, 300 s per run, the cleanup off (`--wal-gc-secs 0`) against default (on), same binary for each pair, merged binary and engine-cost binary interleaved, 3 runs each, MinIO and loadgen on the same 4 vCPUs. All 12 runs are listed; none was discarded. **The machine restarted several times during this measurement (about every 12 minutes, each time killing MinIO and the run in progress); a killed run was rerun from the start, so the runs are not back to back.** The time of each run (UTC, 27-28 Oct) is given. One extra completed run before that (`cmp-m-off1`, merged, off, 1561.5 ops/s, labelled in `results/objects/`) is not part of the table.

| Run | Time | ops/s | p50 / p99 ms | Max WAL objects | Objects deleted (server counter) | MinIO CPU s | Server CPU s |
|---|---|---|---|---|---|---|---|
| merged off 1 | 27th 20:06 | 1473.6 | 4.8 / 12.7 | 107,972 | 43 | 218.8 | 158.2 |
| merged on 1 | 28th 00:56 | 925.9 | 6.6 / 59.3 | 8,942 | 65,560 | 224.8 | 137.0 |
| engine-cost off 1 | 01:13 | 359.4 | 19.4 / 81.0 | 75,506 | 39 | 188.9 | 323.8 |
| engine-cost on 1 | 01:19 | 346.4 | 20.7 / 54.5 | 8,894 | 65,558 | 234.5 | 335.2 |
| merged off 2 | 01:24 | 1175.4 | 6.1 / 14.8 | 86,539 | 24 | 231.2 | 168.4 |
| merged on 2 | 01:29 | 1101.3 | 6.5 / 17.7 | 9,206 | 81,959 | 259.7 | 168.9 |
| engine-cost off 2 | 01:34 | 363.6 | 20.2 / 51.3 | 77,729 | 39 | 204.9 | 344.0 |
| engine-cost on 2 | 01:40 | 362.0 | 20.6 / 50.9 | 8,106 | 69,656 | 218.9 | 344.1 |
| merged off 3 | 01:45 | 1127.1 | 6.7 / 13.7 | 83,441 | 39 | 227.3 | 177.7 |
| merged on 3 | 01:56 | 957.5 | 7.8 / 18.8 | 8,061 | 65,558 | 264.2 | 161.8 |
| engine-cost off 3 | 02:30 | 350.9 | 20.9 / 57.2 | 72,678 | 23 | 205.1 | 340.1 |
| engine-cost on 3 | 02:39 | 342.0 | 22.0 / 54.3 | 8,265 | 65,558 | 232.1 | 341.3 |

All 12 runs: 0 errors.

Computed means (from the table): merged off 1258.7 (1473.6, 1175.4, 1127.1), merged on 994.9 (925.9, 1101.3, 957.5), **cost -21.0%**; engine-cost off 358.0 (359.4, 363.6, 350.9), on 350.1 (346.4, 362.0, 342.0), **cost -2.2%**.
Pairs that were run close in time (off then on, 5 to 11 minutes apart): merged round 2 -6.3%, merged round 3 -15.0%; engine-cost round 1 -3.6%, round 2 -0.4%, round 3 -2.5%. Merged round 1 is not a usable pair (off was 5 hours before on), which is why the -21.0% mean overstates it; the time-adjacent pairs give -6.3% and -15.0%. Earlier in the same environment, `exp_wal_growth.py` (5 minutes, 3 pairs on the merged binary) gave off 1422.0, 1497.3, 1506.9 (mean 1475.4) and on 1375.2, 1428.5, 1180.1 (mean 1327.9): **-10.0%**, p99 9.1-9.8 ms off against 11.8-24.4 ms on (`results/wal/mrg-*`).

**Result: -6% to -21% on the merged binary (-10.0% in the other script), against -2.2% for engine-cost in the same session. That is outside the 1-3% target.** It is not accepted. `main` is not merged.

What the data does and does not show:
- The cleanup itself works the same in both binaries: 13-14 passes per run, 65,535 to 81,919 objects deleted, 27 to 31 s spent deleting (`walGc.deleteUs`), listing 0.2 to 0.6 s. The bulk pass is active in the merged binary (it is the default and the counters are filled). Maximum WAL objects: 8,061 to 9,206 on merged against 8,106 to 8,894 on engine-cost (83k to 108k on merged and 73k to 78k on engine-cost without the cleanup).
- The merged binary is much faster at the same load: 1127-1474 ops/s off against 351-364 for engine-cost, p50 about 5-7 ms against about 20 ms, and its server CPU per run is about half (158-178 s against 324-344 s) for 3.5 times the operations. I did **not** find out why (not measured). Candidates I did not test: work that `compat-indexes` removed from the write path and `engine-cost` still pays; the load generator is the same for both.
- The extra DELETE work is about the same in both binaries (65,535 to 81,919 objects, 27 to 31 s of delete time) and so is the extra MinIO CPU (merged +6.0, +28.5, +36.9 s; engine-cost +45.6, +14.0, +27.0 s). So the CPU numbers do **not** show that MinIO is more loaded on the merged binary. What differs is that the same cleanup work lands on a run that is 3.5 times faster, and p99 on merged rises more (59.3 / 17.7 / 18.8 ms on, against 12.7-14.8 ms off). I have no proven cause for the larger loss; the explanations I can think of (a share of a fixed amount of delete work that is larger at higher throughput, lock contention between the cleanup and the faster write path) are **not tested**. I have not run the merged binary at engine-cost's throughput (1 connection or throttled) to see whether the cost then falls to 1-3%.
- DELETE requests: the server counter above counts **objects**, not HTTP requests (the bulk delete sends several keys per request). The number of HTTP DELETE requests that reached MinIO was **not measured**; MinIO's request metrics were not reachable.
- The 10,687-11,012 maximum WAL objects seen earlier on the merged binary came from `exp_wal_growth.py` (sampling every 10 s, 5 minutes, 1475 ops/s); this comparison uses `exp_objects.py` and gives 8,061 to 9,206. The two scripts count at different moments of the 20-second pass cycle, and engine-cost's own table shows 8,938 to 10,607 for 1 to 10 minutes. I did not check each run's samples; I cannot say more than that both are in the same range and below 12,000 and far below the 83k-108k without the cleanup.

## 5. What remains

- Not done by decision: the load test (3 runs). You will run it after the WAL cost is clarified.
- Not measured: HTTP DELETE request counts; why the merged binary is 3.5 times faster than engine-cost on this load; the cleanup cost of the merged binary at low throughput (1 connection), which would show whether the cost comes from the higher throughput or from the merge.
- Not done: merge into `main`.

## 6. Files

`loadtest/run_cmp.sh`, `loadtest/results/objects/cmp2-*.json` (12 runs) and `cmp-m-off1.json`, `loadtest/results/wal/mrg-*-5m.json` (6 runs), `loadtest/results/durability/*`. The four tracked result files of engine-cost in `results/gc` and `results/open-gc` that the durability tests overwrote were restored; the merged-binary versions are in `results/durability/merged-*.json`.
