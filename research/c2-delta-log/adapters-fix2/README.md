# Agent adapters with derived documents on, final binary (c2-delta-log, 2026-11-10)

Rerun of `../adapters-fix/` on the server with the restart fix (`4995a852`: derived threads stay derived when the
reseal's spec does not know their collection; the registered collections' customer keys are learnt from their stored
options when the compactor starts). This time the derived check takes the plain restart path, with no registration
step: no `createIndexes`, and no write to `checkpoints` or `checkpoint_writes` before the reseal.

Short answer: the fix works. 0 differences from mongod in the four suites and the three end-to-end loops.
LangGraph Python: `fenvixReseal` straight after a warm restart encoded 20,000 documents, with 0 rebuild errors and
0 bytes different before and after. The rows stayed derived across the cold restart: the raw read after it rebuilt
64,000 documents, and the byte comparison was identical.

All numbers are MEASURED on this sandbox (4 vCPU, shared with the judge, which ran in parallel), $0.

## What ran

- Server: `target/release/mongo-server`, md5 `abde8ed5cf557f4d1f50aeab4583f83e`. Its file time is 14:55 UTC, not
  15:40 as the prompt said. HEAD is `2ea4fb90` (15:03), and `2ea4fb90` changes only research files over `4995a852`.
  No source file is newer than the binary, so it is the `4995a852` server code. A copy was pinned in a scratch folder
  before the first run, and every run used that copy. The repository's binary still had the same md5 at the end.
- Harness: `bench/agent-adapters` as tracked on this branch, in a scratch copy, with the same changes as before
  (`scripts/run_suites.sh.diff`): Mastra's replica set on port 27021, Atlas-only Mastra vector files excluded, flags
  and counters saved after each suite.
- Adapters: the same install as the two earlier runs (`/home/user/adapters`, 09:16 to 09:18 UTC today, not
  reinstalled), so the mongod reference results are reused from `../adapters/` and mongod was not rerun.
- Our server's settings, every run:
  `FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1` and
  `--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id`.
  `FENVIX_SEALED_COMPACTOR=small` for the suites, `=1` for the e2e loops; `--sealed-wave` at its default (true).
  The command line and the process environment are in `suites/ours/*.ckeys.txt` and `e2e/ours/derived-*.cmdline.txt`.
  Every `serverStatus().fenvix` read shows the `derived` section (`write: true`).
- Sizes: LangGraph Python and JS at 50 users x 80 turns, Mastra at 50 users x 200 turns. The disk watchdog (stop
  under 4 GB free) never fired; the lowest free space was 5,657 MB (`e2e/disk-free-mb.log`). `SETTLE_SECS=60`.
  Reads go thread by thread (T_pin 1 s); 0 scans had to be repeated after code 134.

## Suites (each on a fresh target)

- LangGraph Python saver: mongod 146 passed, 0 failed, 0 skipped; ours 146 passed, 0 failed, 0 skipped.
  Differences: 0.
- LangGraph Python store: mongod 29 passed, 3 failed, 7 skipped; ours 29 passed, 3 failed, 7 skipped.
  Differences: 0. The 3 failures are the known `test_filters` cases (Atlas vector search: `$listSearchIndexes` on
  mongod, `$vectorSearch` on ours).
- LangGraph JS: mongod 123 passed, 0 failed, 7 skipped; ours 123 passed, 0 failed, 7 skipped. Differences: 0.
- Mastra (without the Atlas-only vector files): mongod 936 passed, 0 failed, 197 skipped; ours 936 passed, 0 failed,
  197 skipped. Differences: 0.
- Every single test has the same status on both targets.
- One note on time: the Mastra notifications bulk test passed on ours in 22.2 s, against mongod's 1.5 s and the
  30 s timeout. It is the same slow test as in `../adapters/` (12.1 s there, 8.9 s in `../adapters-fix/`), so the
  margin under load is thin.
- As before, the suites are too small to encode anything (`derived encoded 0`). The Mastra suite's compactor did
  1 absorb and 1 reseal, with 0 errors.

## End-to-end loops (fake model, 10 users at a time)

- LangGraph Python, 50 x 80: mongod 76.9 s, ours 93.1 s. Differences in the answers of 50 users: 0.
- LangGraph JS, 50 x 80: mongod 58.2 s, ours 88.9 s. Differences: 0.
- Mastra, 50 x 200: mongod 36.3 s, ours 72.6 s. Differences: 0.
- Saver re-reads after the reseal and after the cold restart, for both LangGraph loops: 0 differences from the loop's
  own answers and 0 from mongod's (`e2e/compare_e2e.txt`).

## Derived documents: plain restart path (`scripts/ours_e2e.sh`, `e2e/ours/driver.out`)

Steps: after the loop, run_e2e.sh's own stop, settle and stop; then a warm restart (KEEP=1) on the same bucket.
`derived_check.py` reads the raw BSON, calls `fenvixReseal`, reads the raw BSON again and compares the bytes. As in
every earlier run, if the first `fenvixReseal` finds the database open read-only, it opens it for writing with one
insert into a side collection, `_derive_check`; that is not a checkpoint collection. Then the saver re-reads every
thread. Then a stop, a cold restart, the raw read alone (counters read right after it), and the saver re-read.

LangGraph Python, database `e2e_langgraph`:
- After the warm restart: all derived counters 0, compactor not started.
- `fenvixReseal`: 1 group resealed, `encoded` 20,000.
- `serverStatus().fenvix.derived` after the reseal and the second raw read: `encoded` 20,000, `plain` 4,000,
  `messageRows` 12,600, `fallbacks` 0, `rebuilds` 63,450, `resealRebuilds` 19,828, `rebuildErrors` 0.
  After the saver re-read: `rebuilds` 147,500, `rebuildErrors` 0.
- Raw bytes: `checkpoints` 12,000 documents, 268,179,600 bytes, 0 byte differences before and after the reseal
  (SHA-256 `c5bde021…` both). `checkpoint_writes` 12,000 documents, 5,321,977 bytes, 0 byte differences
  (`bedd8772…`).
- Cold restart: the counters start at 0. The raw read alone gives `rebuilds` 64,000 with `rebuildErrors` 0, so the
  rows are still derived in MinIO. Both collections are byte-identical to before the reseal. After the saver
  re-read: `rebuilds` 84,050, `rebuildErrors` 0.

LangGraph JS, database `e2e_langgraph_js`:
- `fenvixReseal`: 1 group resealed, `encoded` 0, `plain` 24,000. This matches the first run in `../adapters/`.
  `../adapters-fix/` showed `plain` 0, which was the restart bug.
- As before, the JS saver stores its checkpoints as JSON, so there is nothing to encode.
- `checkpoints`: 12,000 documents, 286,096,600 bytes. `checkpoint_writes`: 12,000 documents, 5,251,650 bytes.
- 0 byte differences before and after the reseal, and both collections identical after the cold restart.
- `rebuildErrors` 0 throughout.

## Findings and blockers

1. The restart finding of `../adapters-fix/` is fixed. With the same plain restart path, the reseal now encodes
   (20,000, previously 0), and the derived rows survive the cold restart (64,000 rebuilds on the raw read, previously 0).
2. The binary's time (14:55 UTC) does not match the "about 15:40" in the prompt; details under "What ran".
   A `cargo test --release --workspace` from the main session was running when this run started. It did not change
   the binary during the run: same md5 at the end.
3. The earlier blockers 1 to 3 in `../adapters/README.md` still apply and were worked around the same way
   (`FENVIX_SEALED_COMPACTOR=1` for the loops, 50 x 80 with the watchdog, thread-by-thread reads). The `small` knobs
   were not retested.
4. Nothing blocked this run. The buckets `adapters` and `adapters-27021` were wiped and deleted at the end (77 and
   1,951 objects). MinIO and the judge's ports were not touched, and every server this run started is stopped.

## Files

- `suites/ours/`: JUnit XML, console log, target log and `*.ckeys.txt` of each suite. `suites/<suite>.diff.txt`:
  `compare.py` against the mongod XML in `../adapters/suites/mongod/`.
- `e2e/compare_e2e.txt`: every comparison with mongod (mongod answers in `../adapters/e2e/mongod/`).
- `e2e/ours/`: the loops, the derived checks, the re-reads, and `driver.out` with the counters at every step.
- `scripts/`: the drivers and checks as run (they use absolute scratch paths of this session; adjust before reuse).
