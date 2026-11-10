# Agent adapters with derived documents on, after the fix round (c2-delta-log, 2026-11-10)

Rerun of `../adapters/` (same job, same scripts) on the fix-round server: the one-wave reader on by default, only
LangGraph saver documents of a `thread_id`-keyed collection rewritten (FIX 3), step 7b's erasure merged.

Short answer: 0 differences from mongod in the four suites and the three end-to-end loops, 0 bytes different before
and after the reseal and after a cold restart. One new finding (finding 1 below): after a restart, the compactor does not
know that `checkpoints` / `checkpoint_writes` are keyed by `thread_id` until something writes to them or creates an
index on them, and a reseal in that window turns every derived document of the group back into a plain one. Answers
and bytes stay correct; the storage saving is lost silently. With the collections registered first (one `createIndexes`
of the existing indexes), the derived check gives the same result as the earlier run: 20,000 encoded, 0 rebuild errors.

All numbers are MEASURED on this sandbox (4 vCPU, shared with the judge, which ran in parallel), $0.

## What ran

- Server: `target/release/mongo-server` built 14:11 UTC from the fix-round code, md5 `706eb51e2ce87d9d5fcc5be816d60869`.
  A copy was pinned in a scratch folder before the first run, and every run below used that copy.
- Harness: `bench/agent-adapters` as tracked on this branch, copied to a scratch folder, with the same changes as the
  earlier run (`scripts/run_suites.sh.diff`): Mastra's replica-set target on port 27021, the Atlas-only Mastra vector
  files excluded, and the server's flags and counters saved after each suite (`suites/ours/*.ckeys.txt`).
- Adapters: the same install as the earlier run (`/home/user/adapters`, `install.sh` at 09:16 to 09:18 UTC today, not
  reinstalled; langgraphjs `af1d87cc`, mastra `0d6eda03`). The adapter versions are identical, so the mongod reference
  results (suite XML and e2e answers) are reused from `../adapters/` and mongod was not rerun.
- Our server's settings, every run:
  `FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0 FENVIX_SEALED_TEST_SHORT_E1_WAIT=1` and
  `--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id`;
  `FENVIX_SEALED_COMPACTOR=small` for the suites, `=1` for the e2e loops. `--sealed-wave` was left at its default (true).
  The command line and the `FENVIX_SEALED_*` variables are in `suites/ours/*.ckeys.txt` (suites) and
  `e2e/*/…cmdline.txt` (e2e). Every `serverStatus().fenvix` read shows the `derived` section (`write: true`).
- Sizes: LangGraph Python and JS at 50 users x 80 turns, Mastra at 50 users x 200 turns, with the disk watchdog
  (stop under 4 GB free; it never fired; the lowest free disk I sampled was 6.3 GB). `SETTLE_SECS=60` (sizes are not the
  point of this run). The derived check reads thread by thread (T_pin 1 s; 0 scans repeated after code 134).

## Suites (each on a fresh target)

- LangGraph Python saver: mongod 146 passed, 0 failed, 0 skipped; ours 146 passed, 0 failed, 0 skipped. Differences: 0.
- LangGraph Python store: mongod 29 passed, 3 failed, 7 skipped; ours 29 passed, 3 failed, 7 skipped. Differences: 0.
  The 3 failures are the known `test_filters` cases (Atlas vector search: `$listSearchIndexes` on mongod,
  `$vectorSearch` on ours), the same as in every earlier run.
- LangGraph JS: mongod 123 passed, 0 failed, 7 skipped; ours 123 passed, 0 failed, 7 skipped. Differences: 0.
- Mastra (without the Atlas-only vector files): mongod 936 passed, 0 failed, 197 skipped; ours 936 passed, 0 failed,
  197 skipped. Differences: 0. The notifications bulk test that timed out once in the earlier run passed in 8.9 s.
- The status of every single test is the same on both targets in all four suites (also skipped and failed ones).
- As before, the suites are too small for derived documents: `derived encoded 0` after each suite; the Mastra suite's
  compactor did 1 absorb and 1 reseal, 0 errors.
- Times on ours: saver 14 s, store 9 s, JS 7 s, Mastra 66 s.

## End-to-end loops (fake model, users 10 at a time)

- LangGraph Python 50 x 80: mongod 76.9 s, ours 84.7 s. Differences in the answers of 50 users: 0.
- LangGraph JS 50 x 80: mongod 58.2 s, ours 101.6 s. Differences: 0.
- Mastra 50 x 200: mongod 36.3 s, ours 77.6 s. Differences: 0.
- Second LangGraph Python loop (the registered variant below): ours 93.2 s, differences: 0.
- Every saver re-read (after the reseal and after the cold restart, both LangGraph loops and the variant): 0 differences
  from the loop's own answers and 0 from mongod's (`e2e/compare_e2e.txt`).

## Derived documents

### As in the earlier run (`e2e/ours/derived-langgraph.json`): warm restart, fenvixReseal straight away

- `fenvixReseal`: 1 group resealed, `encoded` 0 (earlier run: 20,000).
- `serverStatus().fenvix.derived` after the reseal: `encoded` 0, `plain` 0, `messageRows` 0, `fallbacks` 0,
  `rebuilds` 63,448, `resealRebuilds` 19,827, `rebuildErrors` 0.
- Raw bytes: `checkpoints` 12,000 documents, 268,179,600 bytes; `checkpoint_writes` 12,000 documents, 5,328,822 bytes.
  Byte differences before/after the reseal: 0 and 0 (same SHA-256); after the cold restart: identical for both.
- After the cold restart and the saver re-read: `rebuilds` 0. No derived document was left: the reseal had written
  every thread back plain (finding 1).
- LangGraph JS (`e2e/ours/derived-langgraph-js.json`): 1 group resealed, `encoded` 0, `plain` 0 (earlier run: plain
  24,000, same cause); `checkpoints` 12,000 documents, 286,096,600 bytes; `checkpoint_writes` 12,000 documents,
  5,251,650 bytes; 0 byte differences before/after, identical after the cold restart. As before, JS payloads are JSON,
  so nothing would be encoded anyway.

### Registered variant (`e2e/ours-reg/`, `scripts/ours_e2e_reg.sh`)

Same loop on a fresh bucket; after the warm restart, `scripts/register.py` re-issues the existing index of each
collection with `createIndexes` ("all indexes already exist", no data change), then the same derived check.
- During the loop (loop process, before the first stop): `encoded` 41,468, `plain` 8,300, `messageRows` 26,076,
  `fallbacks` 0, `rebuilds` 19,878, `resealRebuilds` 21,632, `rebuildErrors` 0; compactor 12 publishes, 9 absorbs,
  3 reseals, 0 errors.
- `fenvixReseal`: 1 group resealed, `encoded` 20,000.
- After the reseal and the second raw read: `encoded` 20,000, `plain` 4,000, `messageRows` 12,575, `fallbacks` 0,
  `rebuilds` 63,479, `resealRebuilds` 19,836, `rebuildErrors` 0. After the saver re-read: `rebuilds` 147,529,
  `rebuildErrors` 0.
- Raw bytes: `checkpoints` 12,000 documents, 268,179,600 bytes, 0 byte differences (SHA-256 `9d91fa5e…` before and
  after); `checkpoint_writes` 12,000 documents, 5,319,831 bytes, 0 byte differences (`ffaa64c8…`). After the cold
  restart: identical for both.
- After the cold restart: the raw read alone gave `rebuilds` 64,000 (derived documents present in MinIO), after the
  saver re-read `rebuilds` 84,050, `rebuildErrors` 0 (the same 84,050 as the earlier run).

## Findings and blockers for the main session

1. NEW. After a restart, a reseal un-derives the agent checkpoint collections until they are registered again.
   `agentck::THREAD_KEYED` is filled only by `custkey::for_write` (a document write or `createIndexes` on the
   collection) and lives in memory. After a restart it is empty, so `AgentSpec::is_checkpoint_doc` returns false for
   every key; the reseal's `Encoder::resolve` still rebuilds each derived document of the old run to plain, and since
   the unit then has no checkpoint document, it is written plain (`encoded 0`, not even counted as `plain`). The comment
   in `agentck.rs` says "an unknown one is left as written", but a derived one is not left as written: it is rewritten
   plain. Measured: cold restart after that reseal, `rebuilds` 0 over 24,000 raw reads and 50 saver re-reads (registered
   variant: 64,000 and 84,050). Correctness is intact (0 byte differences, 0 answer differences); the cost is the lost
   saving, silently. In production it happens whenever a database reopens and its compactor reseals before the saver
   writes a checkpoint, e.g. when only the store (or any other collection of that database) is written first. The
   saver's own setup does not help: it calls `create_index` only when the index is missing, i.e. never after the first
   start. Possible fixes (for the main session to judge): learn the customer key from the stored collection options
   when the compactor starts or before a reseal, or never rewrite an already derived unit plain when its collection is
   unknown (keep it as it is).
2. Earlier blockers 1 to 3 still apply as written in `../adapters/README.md` and were worked around the same way
   (`FENVIX_SEALED_COMPACTOR=1` for the loops, 50 x 80 for LangGraph with the watchdog, thread-by-thread reads). Not
   retested: blocker 1 (the `small` knobs) was not rerun.
3. Nothing blocked this run. Disk: free space went from 9.2 GB to 6.3 GB at the lowest sample; the buckets `adapters` and
   `adapters-27021` were wiped and deleted at the end (403 and 1,963 objects). MinIO and the judge's ports were not
   touched; every server this run started is stopped.

## Files

- `suites/ours/`: JUnit XML, console log, target log and `*.ckeys.txt` (flags, env, counters) of each suite;
  `suites/<suite>.diff.txt`: `compare.py` against the reused mongod XML in `../adapters/suites/mongod/`.
- `e2e/compare_e2e.txt`: every comparison with mongod (answers in `../adapters/e2e/mongod/`).
- `e2e/ours/`: the three loops, the derived checks as in the earlier run, re-reads, driver output.
- `e2e/ours-reg/`: the registered variant (loop, derived check, re-reads, driver output with every counter).
- `scripts/`: the drivers and checks as run (absolute scratch paths of this session; adjust before reuse).
