# Agent adapters with derived documents on (c2-delta-log, 2026-11-10)

Question: with derived documents on (`--sealed-derive`), do the LangGraph and Mastra MongoDB adapters still behave exactly
as on mongod 7.0, and are the documents the compactor stores as a thread message log rebuilt byte for byte?

Short answer: yes for everything that could be run. 0 differences from mongod in the four suites and the three
end-to-end loops; 20,000 checkpoint documents encoded as derived documents, 0 rebuild errors, 0 bytes different before
and after the reseal and after a cold restart. Three things got in the way and are reported below: the `small` compactor
knobs cannot write a run larger than 1 MiB to S3/MinIO (and the database then hangs), the disk could not hold the
full-size loops, and with T_pin = 1 s any single operation longer than 1 s fails with `SealedRunsRetry`.

All numbers are MEASURED on this sandbox (4 vCPU, shared with the judge, which ran in parallel), $0.

## What ran

- Server: `target/release/mongo-server`, md5 `0607739173ea5f39050c366c1309c536`, built 09:22 UTC by the main session from
  `7858d1c2` plus its uncommitted edits of that moment. A copy was pinned for every run below, so a rebuild during the
  runs could not change the binary under test.
- Harness: `bench/agent-adapters` as committed on this branch (`660d46e6`), run from a scratch copy so the committed
  results stay untouched. Two changes to `run_suites.sh` (`scripts/run_suites.sh.diff`):
  - Mastra's replica-set target on port 27021 instead of 27019, because ports 27018 to 27020 belong to the judge.
  - Mastra runs without `src/vector/index.test.ts` and `src/vector/*autoembed.test.ts`. These Atlas-only files connect
    to a hard-coded `localhost:27018`, which is the judge's own server today, so they would have written into it. In the
    earlier runs nothing listened there and their tests failed or were skipped the same way on both targets (the
    10 failures of every earlier Mastra run).
  - For ours, after each suite: `serverStatus().fenvix` (customer keys, derived counters, compactor counters), the server's
    command line and the `FENVIX_SEALED_*` variables go to `suites/ours/<suite>.ckeys.txt`.
- Adapters: `install.sh` on 2026-11-10, i.e. the default branch of each repository today. Mastra's suite is newer
  than the 2026-11-06 one, so its counts differ from the earlier report; only the difference between the two targets
  counts.
- Our server's settings, every "ours" run:
  `FENVIX_SEALED_TPIN_MS=1000 FENVIX_SEALED_E1_WAIT_MS=0` and
  `--sealed-runs 2 --sealed-compactor --sealed-derive --sealed-codec zstd --customer-key checkpoints=thread_id,checkpoint_writes=thread_id`,
  checked in the server's command line (`suites/ours/*.ckeys.txt`, `e2e/ours/derived-*.cmdline.txt`).
  `FENVIX_SEALED_COMPACTOR=small` for the suites; `FENVIX_SEALED_COMPACTOR=1` (default knobs) for the end-to-end loops,
  because the `small` knobs break them (blocker 1).

## Suites (each on a fresh target)

- LangGraph Python `MongoDBSaver` (langgraph-checkpoint-mongodb 0.5.1): mongod 146 passed, 0 failed, 0 skipped; ours
  146 passed, 0 failed, 0 skipped. Differences: 0.
- LangGraph Python `MongoDBStore` (langgraph-store-mongodb 0.4.0): mongod 29 passed, 3 failed, 7 skipped; ours 29
  passed, 3 failed, 7 skipped. Differences: 0. The 3 failures are the same `test_filters` cases on both targets
  (Atlas vector search: `$listSearchIndexes` on mongod, `$vectorSearch` on ours). Known: identical in the 2026-11-06
  results and in this branch's results without derived documents.
- LangGraph JS (`@langchain/langgraph-checkpoint-mongodb` 1.5.1): mongod 123 passed, 0 failed, 7 skipped; ours 123
  passed, 0 failed, 7 skipped. Differences: 0.
- Mastra (`@mastra/mongodb`, repository default branch, without the Atlas-only vector files): mongod 936 passed,
  0 failed, 197 skipped; ours 936 passed, 0 failed, 197 skipped. Differences: 0.
  - The first Mastra run on ours (`suites/ours-first-mastra-run/`) had 1 difference: `NotificationsMongoDB > bulk status
    update accepts more ids than one statement can bind and returns each record once` timed out at 30 s. New name,
    but not a new behaviour: the test passed in every earlier run (ours 5.8 s on 2026-11-06, mongod 0.9 s). Alone it
    passes on ours in 7.5 s with these flags and 6.4 s without them (mongod 1.5 s; `probes/notif-*.xml`), and in the
    final full run in 12.1 s. The machine's load average was 10 to 12 on 4 vCPU at the time (the judge, a build).
    Reading: a slow test under load, not a wrong answer; ours is 4 to 8 times slower than mongod on it.
- The 2026-11-06 differences of Mastra (23 tests, `$toLong` of a date and the unique index between concurrent
  transactions) did not come back: fixed before this branch, as this branch's own results already showed.
- What the suites do not show: the compactor did no absorption and no reseal during any suite (`absorbs 0, reseals 0,
  derived encoded 0`, `suites/ours/*.ckeys.txt`); the suites' databases are too small and short. They prove that the
  flags change nothing on the plain path. The derived path is proven by the end-to-end check below.

## End-to-end loops (fake model, users run 10 at a time)

Sizes: LangGraph Python and JS at 50 users x 80 turns (not 200, blocker 2); Mastra at 50 users x 200 turns.

- LangGraph Python (`MongoDBSaver` + `MongoDBStore`): mongod 77 s, ours 108 s. Differences in the answers of 50 users:
  0.
- LangGraph JS: mongod 58 s, ours 110 s. Differences: 0.
- Mastra: mongod 36 s, ours 87 s. Differences: 0.
- A first run of the same three at the same sizes (whose derived check stopped on blocker 3) also had 0 differences.
- mongod at the full 50 x 200 also ran (langgraph 238 s, langgraph-js 267 s, mastra 29 s); its answers are kept in
  the scratch copy only, since ours could not run at that size.

## Derived documents (after each LangGraph loop, same bucket, warm restart with KEEP=1)

Steps, each with the server's own flags (`scripts/ours_e2e2.sh`, `scripts/derived_check.py`): raw BSON of every
document of `checkpoints` and `checkpoint_writes` (RawBSONDocument, sort by `_id`, batch size 8); then
`admin.command("fenvixReseal", db)` for every database with a `checkpoints` collection; `serverStatus().fenvix.derived`;
raw BSON again; byte comparison; every thread re-read through the saver (`get_state` and `list`, plus the store) and
compared with the loop's own answers and with mongod's; then a stop, a cold restart on the same bucket, and the raw
read and the saver re-read once more.

LangGraph Python, database `e2e_langgraph` (`e2e/ours/derived-langgraph.json`):
- `fenvixReseal`: 1 group resealed, `encoded` 20,000.
- `serverStatus().fenvix.derived` after the reseal and the second raw read: `encoded` 20,000, `plain` 4,000,
  `messageRows` 12,549, `fallbacks` 0, `rebuilds` 127,486, `resealRebuilds` 19,839, `rebuildErrors` 0. After the saver
  re-read: `rebuilds` 147,536, `rebuildErrors` 0. After the cold restart and both re-reads: `rebuilds` 84,050,
  `rebuildErrors` 0 (counters restart with the process).
- Raw bytes: `checkpoints` 12,000 documents, 268,179,600 bytes; `checkpoint_writes` 12,000 documents, 5,321,274
  bytes. Byte differences before and after the reseal: 0 and 0 (same SHA-256). After the cold restart: identical for
  both collections.
- Saver re-read of all 50 threads after the reseal and after the cold restart: 0 differences from the loop's answers
  and 0 from mongod's.

LangGraph JS, database `e2e_langgraph_js` (`e2e/ours/derived-langgraph-js.json`):
- `fenvixReseal`: 1 group resealed, `encoded` 0, `plain` 24,000. The JS saver stores its checkpoints as JSON
  (`type: "json"`, its serializer's `_dumps`), not msgpack, and derived documents only cut msgpack payloads, so every
  document stays plain, by design. Not a fault, but it means derived documents save nothing for LangGraph JS users today.
- Raw bytes: `checkpoints` 12,000 documents, 286,096,600 bytes; `checkpoint_writes` 12,000 documents, 5,251,650
  bytes; 0 byte differences before and after the reseal, identical after the cold restart. Saver re-read: 0
  differences, twice.

## Blockers and findings for the main session

1. `FENVIX_SEALED_COMPACTOR=small` cannot write a run larger than 1 MiB to MinIO (or S3), and the database then hangs.
   `server_knobs()` sets `RunOptions::unchecked(16 << 10, 1 << 20, ..)` for sealed and delta runs: 1 MiB multipart
   parts, under the 5 MiB minimum (`runfmt::MIN_PART`). The first delta run over 1 MiB fails at completion with
   `EntityTooSmall`, every absorb job after it fails the same way (186 failed jobs in 25 minutes, `absorbs 0`), and
   each failure leaves its uploaded parts behind (243 incomplete multipart uploads, 3.2 GB in MinIO, aborted by hand).
   The e2e loop then stopped making progress (its client used 14 s of CPU in 22 minutes), a `count` on the database
   timed out after 50 s, and SIGTERM did not stop the server within about 5 minutes ("shutting down: closing tenants
   cleanly" and nothing more): killed with SIGKILL. A 10 users x 60 turns loop reproduces the failed absorbs with or
   without `--sealed-derive` and with either codec (`probes/repro-*.txt`); with the default knobs there is no error.
   So this is the test knobs plus, more seriously, a database that hangs and a server that cannot stop when its
   compactor keeps failing. The judge runs with `small` on tiny databases and would not see it.
2. Disk: this sandbox gives about 10 GB to root, and the full-size LangGraph loop on ours needs about 6.5 GB of MinIO
   (2026-11-06 results). The first full-size attempt with default knobs filled MinIO (`XMinioStorageFull` on absorb jobs
   and on the process lease, about 10:20 UTC); the run was stopped, the bucket `adapters` wiped. MinIO is shared with
   the judge: if a judge run failed around 10:20 UTC, this is a likely cause. The loops were then run at 50 x 80, with a
   watchdog that stops them under 4 GB free. Until it filled the disk, that full-size run had encoded 200,669 documents
   with 0 rebuild errors (the server's counters at that moment).
3. With `FENVIX_SEALED_TPIN_MS=1000`, any single operation that lasts more than 1 s fails with `SealedRunsRetry`
   (code 134, labelled `TransientTransactionError` and `RetryableWriteError`): a `find` over the whole 12,000-document
   collection with batch size 8 failed on its getMore 20 times out of 20, and even one `distinct("thread_id")` failed.
   The check therefore reads thread by thread (one short cursor per thread, scanned again on code 134; 0 retries were
   needed). The default T_pin is 30 s, so the same applies to any cursor or command longer than 30 s in production. A
   mongod client does not expect a getMore to fail this way, and drivers do not retry a getMore. Worth a decision.
4. Derived documents do not apply to the LangGraph JS saver (JSON payloads), as above.

## Files

- `suites/mongod`, `suites/ours`: JUnit XML and console log of each suite; `suites/<suite>.diff.txt`: `compare.py`
  output; `suites/ours/*.ckeys.txt`: the server's flags and counters after each suite; `suites/ours-first-mastra-run`:
  the run with the timeout.
- `e2e/mongod`, `e2e/ours`: logs, sizes and the answers (`.json.gz`) of each loop; `e2e/compare_e2e.txt`: every
  comparison with mongod; `e2e/ours/derived-*.json`: the derived check; `e2e/ours/driver-*.out`: the driver's output.
- `probes/`: the notifications test alone on both targets, the reproduction of blocker 1 (`repro-*.txt`,
  `hang-small-knobs.txt`) and blocker 2 (`full-size-storage-full.txt`).
- `scripts/`: the check and driver scripts (they use absolute scratch paths of this session; adjust before reuse).
