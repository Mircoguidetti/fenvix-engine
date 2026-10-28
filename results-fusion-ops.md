# Fusion of control-ops into merge-engine (branch `fusion-ops`)

Branch `fusion-ops` = `merge-engine` at e4a560c + `origin/control-ops` (1f473f9). `main` and `compat-gaps` untouched; no PR.
Reference for every differential test: `mongo:7.0` (standalone :27017, replica set :27020). Anything not run is marked "not measured".
Fused binary: `target/release/mongo-server` (sha256 starts `d06308b834ba9a39`), built from the merge commit `087f10c` plus nothing else.

## 1. Merge conflicts and how each was resolved

Textual conflicts: two files. `dbs.rs` was expected to conflict and did not (git merged it cleanly, see below).

| file | conflict | what I kept and why |
|---|---|---|
| `src/lifecycle.rs` | control-ops added `WAL_RETAIN` (callback returning the WAL retention window of a database) next to the statics that merge-engine/engine-cost use for the open-writer WAL cleanup (`WAL_GC_MODE` and friends). | Both: all engine-cost statics plus `WAL_RETAIN`. Dropping either would lose point-in-time restore (control-ops) or the open-writer cleanup (engine-cost). |
| `mongo-server/src/main.rs` | `mod ctl;` (control-ops) next to `mod cmd_mapreduce;` (compat-indexes); and the shutdown sequence. | Both modules. At shutdown `ctl::shutdown().await` runs **before** `leases.release_process().await`: ctl releases keep-awake pins and flushes usage counters, which need the leases still held. |
| `mongo-server/src/dbs.rs` | no textual conflict. control-ops adds `crate::ctl::fork::on_drop(&self.name).await?` at the top of `Entry::drop_database`; engine-cost did not touch that function. | Auto-merge kept the hook; checked by reading the merged function. |

Semantic conflict found while reading the merge (no text conflict, would have corrupted restores): in `gc_tenant_wal`, engine-cost's incremental bulk cleanup deletes WAL objects by id range **without an age check**, while control-ops keeps WAL objects younger than the retention window of a database (`WAL_RETAIN`) so that point-in-time restore can replay them. Left as merged, the bulk pass would delete WAL objects inside the retention window.
Fix (`src/lifecycle.rs`, `gc_tenant_wal`): a database with a retention window takes slatedb's own pass with `min_age.max(retain)`; the incremental bulk pass is used only for databases without retention (the mode value is forced to 0 when `retain > 0`). `gc_tenant` already passed `WAL_RETAIN` into `wal_options.min_age`, and `gc_wal_incremental` already skips databases that have a checkpoint. Proof the fix keeps restores right: `ctl-tests/test_03_restore.py` 11/11, `test_03b_restore_gc_crash.py` 11/11 and `test_03c_retention_cost.py` 1/1 pass on the fused binary (section 3).

## 2. Differential suites on the fused binary (4127 generated tests)

Run in slices of 150 tests (`tools/fusion/run_suites.py`, resumable after machine restarts; `--slice START:END` added to `run_py.py` and `run_node.js`). Every slice file is in `results-fusion-ops/` (`py_NNN.json`, `node_NNN.json`).

| driver | core | extra | replset | distinct tests |
|---|---|---|---|---|
| pymongo | 3989/3989 | 49/49 | 89/89 | 4127 |
| Node | 3989/3989 | 49/49 | 89/89 | 4127 |

These include the control-ops regression run (control-ops' own regression was 2925 core + 89 replset + 49 extra; the suite now has 3989 core) and the engine-cost differential tests. No failures on either driver.

## 3. control-ops ctl tests (`ctl-tests/`)

| file | passed | failed/error | seconds |
|---|---|---|---|
| test_01_databases | 10 | 0 | 11.10 |
| test_02_fork | 13 | 0 | 51.10 |
| test_02b_fork_crash | 12 | 0 | 41.52 |
| test_03_restore | 11 | 0 | 114.41 |
| test_03b_restore_gc_crash | 11 | 0 | 148.17 |
| test_03c_retention_cost | 1 | 0 | 89.24 |
| test_04_export | 10 | 0 | 56.88 |
| test_04b_export_vs_real_mongo | 2 | 0 | 4.44 |
| test_05_erase | 19 | 0 | 50.43 |
| test_06_keepawake | 13 | 0 | 147.18 |
| test_07_usage | 10 | 0 | 51.39 |
| test_08_costs | 3 | 0 | 339.77 |
| test_08b_pin_cost | 1 | 0 | 65.05 |
| **total** | **116** | **0** | |

control-ops' final run (`results-ops/ctl_final_run.txt`) reported **111 passed**; that run left out the three cost files, which have their own result files (`costs.json`, `retention_cost.json`, `pin_cost.json`). Here those three files are included: `test_03c_retention_cost` 1 + `test_08_costs` 3 + `test_08b_pin_cost` 1 = 5 tests, and 116 - 5 = 111, the same 111 tests plus the cost tests. All pass. No test was skipped or deleted.

A machine restart interrupted `test_08_costs` once and `test_03b` once; both were rerun from the start of their file. Tooling bug found on the way: the "file already done" check in `run_ctl.sh` looked for a `====` summary line that `pytest -q` does not print, so after a restart finished files were rerun; fixed (the check now matches the `N passed in Xs` line). The rerun files are the ones in the table.

## 4. SIGKILL durability on the fused binary

WAL cleanup: `loadtest/test_group_commit.py` (4 trials, 60 s each; negative control `--ack-before-durable`, acknowledges before the flush; 2 trials, 30 s). SST/manifest/compaction-state cleanup: `loadtest/test_open_gc.py` (4 trials, 100 s; the kill is sent inside a cleanup pass; negative control `--gc-control-delete-live`, the cleanup also deletes a referenced SST; 2 trials, 60 s).

| test | result line |
|---|---|
| sst_1 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 68, 'acked_docs': 12753, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 48716, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_2 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 54, 'acked_docs': 12164, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 47265, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_3 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 53, 'acked_docs': 12177, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 47145, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sst_4 | `TOTAL safe {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 68, 'acked_docs': 12566, 'lost_inserts': 0, 'lost_updates': 0, 'read_violations': {'reads_checked': 49598, 'missing': 0, 'stale': 0, 'padding': 0, 'errors':` |
| sstctl_1 | `TOTAL control {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 16, 'acked_docs': 8750, 'lost_inserts': 8750, 'lost_updates': 0, 'read_violations': {'reads_checked': 34822, 'missing': 0, 'stale': 0, 'padding': 0, 'err` |
| sstctl_2 | `TOTAL control {'trials': 1, 'killed_inside_cleanup_pass': 1, 'ssts_deleted': 20, 'acked_docs': 8938, 'lost_inserts': 8938, 'lost_updates': 0, 'read_violations': {'reads_checked': 33566, 'missing': 0, 'stale': 0, 'padding': 0, 'err` |
| wal_1 | `TOTAL s3 durable {'acked_ids': 56634, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_2 | `TOTAL s3 durable {'acked_ids': 58205, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_3 | `TOTAL s3 durable {'acked_ids': 58267, 'lost_inserts': 0, 'lost_updates': 0}` |
| wal_4 | `TOTAL s3 durable {'acked_ids': 59528, 'lost_inserts': 0, 'lost_updates': 0}` |
| walctl_1 | `TOTAL s3 control {'acked_ids': 34257, 'lost_inserts': 108, 'lost_updates': 16}` |
| walctl_2 | `TOTAL s3 control {'acked_ids': 36907, 'lost_inserts': 996, 'lost_updates': 12}` |

With the protections on: 0 lost inserts and 0 lost updates in all 8 trials; for the SST tests every kill landed inside a cleanup pass and 0 reads were missing or stale. The 4 negative controls lose data (WAL: 108 and 996 inserts, 16 and 12 updates; SST: 8750 and 8938 inserts), so the tests do detect loss.

## 5. WAL cleanup cost at a fixed 300 ops/s (fused binary)

`tools/fusion/run_wal300.sh`: 4 pairs, alternating off (`--wal-gc-secs 0`) and on (default), 300 s each, 300 ops/s offered, 8 connections, write-only (`loadtest/exp_objects.py --rate 300`). S3 request counts come from MinIO's own metrics. Before each run orphan benchmark tenants are removed from MinIO and free space is checked (>= 3000 MB); a run with zero operations or any error is moved to `invalid-fz/` (none was).

| run | ops | ops/s | errors | p50 ms | p99 ms | server CPU s | max WAL objects | S3 PUT | S3 DeleteMultipleObjects | WAL passes (deleted) |
|---|---|---|---|---|---|---|---|---|---|---|
| fz-off1 | 90004 | 300.0 | 0 | 5.16 | 14.27 | 104 | 79535 | 82135 | 2 | 0 (0) |
| fz-on1 | 90006 | 300.0 | 0 | 5.70 | 20.61 | 104 | 8501 | 78882 | 83 | 13 (69633) |
| fz-off2 | 90007 | 300.0 | 0 | 5.77 | 17.09 | 104 | 76728 | 79251 | 2 | 0 (0) |
| fz-on2 | 90005 | 300.0 | 0 | 4.99 | 53.89 | 95 | 8644 | 78617 | 84 | 13 (69633) |
| fz-off3 | 90007 | 300.0 | 0 | 4.40 | 13.42 | 98 | 82267 | 85183 | 11 | 0 (0) |
| fz-on3 | 90005 | 300.0 | 0 | 4.20 | 20.16 | 96 | 8136 | 83472 | 88 | 13 (73728) |
| fz-off4 | 90006 | 300.0 | 0 | 4.33 | 13.24 | 97 | 82674 | 85374 | 2 | 0 (0) |
| fz-on4 | 88697 | 295.7 | 0 | 7.75 | 17.95 | 70 | 8289 | 46424 | 56 | 14 (45060) |

| pair | offered load met by both runs | CPU off → on (s) | CPU on against off | p50 off → on (ms) | p99 off → on (ms) | PUT off → on | max WAL objects off → on |
|---|---|---|---|---|---|---|---|
| 1 | yes | 104 → 104 | +0.3% | 5.16 → 5.70 | 14.27 → 20.61 | 82135 → 78882 | 79535 → 8501 |
| 2 | yes | 104 → 95 | -8.3% | 5.77 → 4.99 | 17.09 → 53.89 | 79251 → 78617 | 76728 → 8644 |
| 3 | yes | 98 → 96 | -1.8% | 4.40 → 4.20 | 13.42 → 20.16 | 85183 → 83472 | 82267 → 8136 |
| 4 | **no** (90006 and 88697 ops) | 97 → 70 | -28.1% | 4.33 → 7.75 | 13.24 → 17.95 | 85374 → 46424 | 82674 → 8289 |

Means over pairs where both runs met the offered load (1,2,3): server CPU -3.3%, p50 -0.15 ms, p99 +16.62 ms (median +6.74), S3 PUT -2.3%, operations completed -0.001%.
Means over all pairs, nothing left out (1,2,3,4): server CPU -9.5%, p50 +0.74 ms, p99 +13.65 ms (median +6.54), S3 PUT -13.1%, operations completed -0.364%.

Spread of server CPU among the off runs that met the offered load: 97 to 104 s (6.9% of the smallest); a CPU difference below about 7% between an off and an on run is inside the run-to-run noise.

Reading, with the definition used in `results-merge.md` (cost = throughput at the same offered load, 1-3% target):
* Pairs 1 to 3: the offered load is met in all six runs (about 90,000 operations, 0 errors), so the throughput cost is 0.00% (+0.002%, -0.002%, -0.002% operations), against 0.1% for the merged binary before control-ops (`results-merge.md` section 4.2). Server CPU differs by +0.3%, -8.3%, -1.8%: inside the run-to-run noise of the off runs (see the spread line above). Within the 1-3% target.
* The cleanup is active and bounded in all four on runs: maximum WAL objects 8.1k-8.6k with the cleanup on against 76.7k-82.7k off (target under 10,000), deleted as `DeleteMultipleObjects` (56-88 requests per run), 0 `DeleteObject`.
* Tail latency: p99 is higher with the cleanup on: +6.34, +36.80, +6.74 ms in pairs 1 to 3 (median +6.74 ms), +4.71 ms in pair 4; `results-merge.md` had +6.9 ms for the merged binary (15.4 against 22.3 ms), so control-ops did not change it. The +36.80 ms (fz-on2, p99 53.89 ms) is an unexplained outlier, not noise.
* **Pair 4 is not comparable.** `fz-on4` did not reach the offered load (88,697 operations, 295.7 ops/s, 0 errors) and shows about half the S3 PUT (46,424 against 78-85k), 70 s server CPU (against 95-104 s), 165 s MinIO CPU (against 230-250 s) and p50 7.75 ms (against 4.2-5.7 ms). The run I redid for `fz-off4` earlier that day (`stale-fz/fz-off4`, cleanup **off**) shows the same shape (87,853 operations, 22,474 PUT, 50 s server CPU, 87 s MinIO CPU, p50 8.55 ms, p99 104 ms). A cleanup-off run behaving like this shows the shape is not caused by the cleanup. The cause is not established: it fits the storage being slower during those runs (fewer, larger group commits and less work for server and MinIO), but I did not measure storage latency. The CPU -28.1% of pair 4 is therefore not a cleanup effect and the pair is listed, not discarded, and left out of the first mean above.


Offered load: 300 ops/s for 300 s = 90,000 operations; runs under 99% of that are marked in the table above.

## 6. Restarts, repeated runs, changes to the tooling

The machine restarted about 25 times during this verification (every 7 to 45 minutes; the longest outage was about 22 minutes). Nothing was discarded: a run killed by a restart has no result file and is simply started again; a finished "off" run whose "on" partner was interrupted was kept in `stale-fz/` when it was redone, and is listed here:

| stale run | ops | server CPU s | p50 ms | p99 ms |
|---|---|---|---|---|
| fz-off1 | 90007 | 107 | 5.63 | 16.23 |
| fz-off2 | 90007 | 100 | 5.04 | 14.46 |
| fz-off4 | 87853 | 50 | 8.55 | 104.00 |

Changes to the measuring method made during the work, all disclosed:
* `env_up.sh` got `MINIO_ONLY=1` (WAL runs need only MinIO; after a restart docker and the reference mongods took up to 5 minutes and cost several runs). Used from pair 4 on (and for the reruns of its partner).
* The age after which a finished "off" run is redone because its "on" partner was interrupted was 15 minutes for pairs 1 to 3 and was raised to 45 minutes before pair 4. If pair 4's two runs were made in different boots of the machine, that is the reason; the file times are in the result files.
* Disk: the session allowance was 89-91% used; I removed MinIO tenants left by my own durability tests (`dur0_*`, `og0_*`, `dt98_*`, about 440 MB) to keep the 3000 MB guard satisfied. Nothing else was deleted.
* The `dl.min.io` connection refused by the proxy in some logs is MinIO's own update check, harmless.

## 7. Open
* Pair 4 (`fz-on4`, and the earlier `stale-fz/fz-off4`): runs that fell short of the offered load with half the PUTs; cause not found, storage latency is only a hypothesis (not measured). A clean fourth pair would need a rerun under a storage state where the off run meets the load; I did not make one.
* The p99 of `fz-on2` (53.89 ms against 13-21 ms in the other on runs): cause not found. These runs keep no per-request trace, so I cannot say whether a cleanup pass, MinIO, or the machine caused it. It does not change throughput or the WAL object bound.
* The `gc_tenant_wal` retention fix is covered by the restore tests passing (11/11, 11/11, 1/1). I did **not** run a before/after experiment showing that restores fail without it (the same tests on a binary without the fix): not measured. The reasoning for the fix is from reading the code (the incremental bulk pass has no age check).
* Latency cost: p99 +6 to +7 ms with the cleanup on, unchanged from `results-merge.md`; no change was made to reduce it.
* The load test (`loadgen`) was not run, as instructed. `main` untouched, no PR.
