# results8 — cold index reads, shared reads, update features, TTL default

Branch `compat-indexes` (not merged, no PR). Every number comes from a file in `results8/` (or `results7/` where stated).
Same environment as results7: one 4-vCPU machine, MinIO in Docker as the object store, reference `mongo:7.0.45`.
Latencies are from a noisy shared machine: runs repeated a few hours apart differ by 5–15% on code that did not change
(the unchanged full-scan rows below drift from 457 to 495 ms), and the shared-read throughput runs differ by up to 2x
at 1 and 8 connections. Request counts are exact; latencies are indicative.

## Summary

| | Result |
|---|---|
| 1. Cold indexed lookup | The 929 GETs came from **fetching 982 documents**, not from the index: my benchmark's "10-document" query returned about 1,000 (`k` was drawn from the batch size, not the collection size). Fixed the benchmark; added a cost-based document fetch. 100k docs, 1,000-document lookup: **954 → 19 GETs**. A lookup of ~10 documents costs **22 GETs** (floor 12 for the wake + 1 per document), against 16 for a full scan: **the target (≤ scan, ideally < 20) is not met for ~10 documents**. |
| 2. Shared reads | Restored for `find`, `count`, `distinct`, `aggregate` (also with indexes). Single-tenant reads at 64 connections on MinIO: 12,739 / 12,644 → 26,420 / 22,752 ops/s (two runs). Correctness test passes. |
| 3. Update features | `$`, `$[]`, `$[<id>]` + `arrayFilters`, pipeline updates, `renameCollection` (same database): **110/110** new differential tests on both drivers. |
| Checks | 10/10 backfills killed mid-build finished after restart. TTL no longer wakes sleeping tenants by default. |
| Full suite (pymongo and Node, identical) | core **1718/1718**, replset **89/89**, extra 46/49. |

## 1. Cold reads with an index

### Where the GETs came from

`tenantTrace` now labels every object-store request with the phase of the command (`mongo-tests/trace_cold.py`,
`results8/trace_cold.txt`). Cold read, 100,000 documents of ~220 B (26 MB in one compacted SST), one index on `k`, tenant closed
before the command. Requests per phase (the phase `wake` is lease GET/PUT, LIST of the manifest, HEAD of the clean marker,
manifest and GC-boundary GETs; `metadata` is the SST's bloom-filter block, block-index block, the manifest/GC polls of the reader and
the block holding the index definitions):

| Command | wake | metadata | index scan | doc fetch / collection scan | Total requests | Bytes read | Elapsed |
|---|---|---|---|---|---|---|---|
| `find {_id: 5}` | 8 | 5 | – | 1 | 14 | 0.55 MB | 44 ms |
| full scan (`payload != x`) | 8 | 4 | – | 8 | 20 | 26.7 MB | 943 ms |
| equality, ~12 documents, **after** | 8 | 5 | 1 | 12 | 26 | 0.6 MB | 61 ms |
| equality, ~1,000 documents, **before** (point reads) | 8 | 5 | 1 | **966** | **980** | 4.4 MB | 679 ms |
| equality, ~1,000 documents, **after** (range scan) | 8 | 5 | 1 | **7** | **21** | 26.7 MB | 446 ms |

So: (a) the index lookup itself is one request (the matching entries are contiguous); (b) every matching document is a
separate 4 KB block when documents are fetched by `_id` (the documents of one key value are spread over the collection), so a
lookup costs one GET per document; (c) 13 requests are the price of waking the tenant and loading the SST metadata, whatever the
query (the `_id` lookup pays them too). Bloom filters and block indexes are the two ~250 KB reads in `metadata`; they are read once per wake.

### Fix

`coll.rs` `docs_by_ids`: when the documents to fetch would need more requests as point reads than as one range scan over their id
span (estimated from the tenant's SST sizes in the manifest, no request), the documents are read with a range scan (4 MB per
request). The rule scans only if it saves more than 4x + 8 requests, or if the whole tenant is under 8 MB. Without the second
condition a lookup of ~10 documents sat on the threshold and sometimes read 26 MB to save 3 requests (seen once, 423 ms instead of 61 ms).
`MONGO_NO_FETCH_SCAN=1` turns the rule off (the "before" rows).

### Before / after (cold: tenant closed before every sample; 9 samples; median; GETs = GET + range GET + LIST)

`results8/cold_before_run.json` (before) and `results8/cold_after.json` (after), same tenants. "~10 docs" = `k` equality
(about 10 documents per value), "~1%" = `g` equality (1% of the collection: 96 documents at 10k, 1,004 at 100k). With 0 indexes both are full scans;
with 1 index only `k` is indexed (`g` is scanned); with 3 indexes `k`, `g`, `ts` are indexed.

| Docs | Indexes | Query | GETs before → after | Bytes read before → after | Latency ms before → after |
|---|---|---|---|---|---|
| 10k | 0 | by `_id` | 11 → 11 | 41 KB → 41 KB | 15.9 → 17.1 |
| 10k | 0 | ~10 docs (scan) | 10 → 10 | 2.6 MB → 2.6 MB | 60.9 → 72.0 |
| 10k | 0 | ~1% (scan) | 10 → 10 | 2.6 MB → 2.6 MB | 60.6 → 72.4 |
| 10k | 1 | by `_id` | 12 → 12 | 61 KB → 61 KB | 17.5 → 17.0 |
| 10k | 1 | ~10 docs | 22 → **13** | 101 KB → 2.2 MB | 24.4 → 39.9 |
| 10k | 1 | ~1% (scan) | 12 → 12 | 2.7 MB → 2.7 MB | 65.3 → 70.8 |
| 10k | 3 | by `_id` | 12 → 12 | 95 KB → 95 KB | 17.0 → 19.1 |
| 10k | 3 | ~10 docs | 22 → **13** | 134 KB → 2.3 MB | 22.1 → 41.4 |
| 10k | 3 | ~1% (96 docs) | 104 → **13** | 461 KB → 2.7 MB | 71.4 → 47.6 |
| 100k | 0 | by `_id` | 11 → 11 | 370 KB → 370 KB | 13.5 → 18.8 |
| 100k | 0 | ~10 docs (scan) | 16 → 16 | 26.4 MB → 26.4 MB | 456.6 → 494.5 |
| 100k | 0 | ~1% (scan) | 16 → 16 | 26.4 MB → 26.4 MB | 477.6 → 477.8 |
| 100k | 1 | by `_id` | 12 → 12 | 553 KB → 553 KB | 18.5 → 19.5 |
| 100k | 1 | ~10 docs | 22 → **22** | 593 KB → 593 KB | 22.6 → 27.0 |
| 100k | 1 | ~1% (scan) | 18 → 18 | 26.7 MB → 26.7 MB | 492.7 → 503.9 |
| 100k | 3 | by `_id` | 12 → 12 | 905 KB → 905 KB | 19.4 → 23.8 |
| 100k | 3 | ~10 docs | 22 → **22** | 944 KB → 944 KB | 26.4 → 25.3 |
| 100k | 3 | ~1% (1,004 docs) | **954 → 19** | 4.6 MB → 27.4 MB | 479.0 → 303.6 |

Reading it: the 929-GET case is fixed (954 → 19 GETs, and 479 → 304 ms). On a 10k-document tenant a ~10-document lookup now costs 13 GETs
(was 22) at the price of reading the whole 2.6 MB tenant, so its latency rises from ~24 to ~40 ms. **For ~10 scattered documents on
a 100k tenant the cost stays at 22 GETs** (a full scan is 16): 12 requests are the wake, the rest is one block per document.
Getting below 20 would need fewer than 8 matching documents, a cheaper wake (the manifest and GC-boundary objects are read two to three times
per wake: wake code I did not change), or clustering the documents of one index value (not done).
The number of bytes read goes up when the scan is chosen: GETs are traded for transferred bytes (free within a region on S3, but not
free in latency). "After" and "before" were measured in two runs; unchanged rows (scans) differ by up to 10%, which is the noise level.

## 2. Shared-read path

**What changed.** The read path is generic over the exclusive entry (writes, transactions: reads go through the transaction's snapshot)
and the shared `ReadView` (trait `Rd` in `coll.rs`). `find`, `count`, `distinct` and `aggregate` outside a transaction take the shared
lock and use the planner, indexes and TTL hiding as before; the ownership check runs after each read. Writes and transactions still take
the lock exclusively. `ttl_seen` became an atomic so a shared reader can set it.

**Test.** `loadtest/test_shared_reads.py` (16 readers during writes: no errors, the counter never goes backwards, counts consistent): PASS with and
without `--serialize-reads`. All 1,856 differential tests run on the shared path (reads outside transactions).

**Load test.** The same experiment as `results-load.md` Task 5 (`loadtest/exp_hot.py`, copy `exp_hot8.py`): one tenant of 50 documents, closed loop,
15 s per cell after 3 s warm-up, reads are `find_one` by `_id`, writes `update_one`, 90/10 mix; MinIO. "Serialized" = `--serialize-reads` (what the merged
branch did for these commands), "shared" = default. Two complete runs (`results8/hot_minio_run1.json`, `hot_minio_run2.json`; run 1 was on the build before the
final fetch-rule and absent-database changes, run 2 on the final build). Errors: 0 in every cell.

| Conns | Mix | Serialized ops/s run1 / run2 | p50 / p99 ms (run 2) | Shared ops/s run1 / run2 | p50 / p99 ms (run 2) |
|---|---|---|---|---|---|
| 1 | reads | 6,286 / 3,569 | 0.253 / 0.646 | 3,275 / 3,704 | 0.248 / 0.600 |
| 1 | writes | 346 / 316 | 3.03 / 5.63 | 337 / 344 | 2.81 / 4.97 |
| 1 | 90/10 | 1,672 / 1,575 | 0.339 / 4.02 | 1,170 / 1,492 | 0.334 / 6.10 |
| 8 | reads | 12,455 / 6,245 | 1.09 / 4.41 | 10,204 / 10,269 | 0.587 / 3.31 |
| 8 | writes | 357 / 356 | 21.5 / 34.8 | 381 / 361 | 21.7 / 29.9 |
| 8 | 90/10 | 2,829 / 2,593 | 3.08 / 10.9 | 2,986 / 2,967 | 2.70 / 10.0 |
| 64 | reads | 12,739 / 12,644 | 4.96 / 9.34 | **26,420 / 22,752** | 2.58 / 6.90 |
| 64 | writes | 438 / 465 | 137 / 165 | 470 / 453 | 141 / 164 |
| 64 | 90/10 | 2,774 / 2,807 | 22.0 / 43.0 | 3,023 / 3,081 | 20.0 / 42.6 |

At 64 connections reads are 1.8–2.1x faster (both runs agree). At 1 and 8 connections the runs disagree with each other more than the modes do
(serialized 8-connection reads: 12,455 vs 6,245), so I cannot claim a difference there. Writes and the 90/10 mix are unchanged within the noise, as in
the original report. The absolute numbers are higher than in `results-load.md` (writes 346 vs 122 ops/s at 1 connection) because this MinIO is faster in this
environment; compare the columns with each other, not with that report.
**Not run:** the S1–S4 scenarios (50,000-tenant load, 1,000 active tenants): too long for this session. The local-directory and in-memory stores of that table:
not run. Shared reads with an index lookup under load: not measured.

## 3. Update features

Implemented in `update.rs`, `cmd_data.rs`, `cmd_index.rs`:

* **Positional `$`**: the index of the first array element matched by the query (conditions on `a`, `a.x`, inside `$and`; with conditions on several fields of the same array the first one decides, which is what one test against MongoDB showed).
* **`$[]`** and **`$[<id>]` with `arrayFilters`** (scalar and document elements, `$and`/`$or` inside a filter, several identifiers, nested arrays, combined with `$[]`).
* **Pipeline updates** (`$addFields`/`$set`, `$project`, `$unset`, `$replaceRoot`, `$replaceWith`; update commands, `update_many`, `findAndModify`, upserts, constants `c`), `_id` immutable, a pipeline that removes `_id` keeps it (MongoDB counts that as modified).
* **`renameCollection`** inside one database, one atomic batch (documents, index entries, index definitions, `dropTarget`).

Errors verified against MongoDB: the validation that does not depend on the matched document runs before any statement (so an update that matches nothing still fails), codes 2, 9, 20, 26, 48, 66, 72, 40323, 40414.

**Differential tests** (`mongo-tests/gen_tests_updates.py`, `upd8.*`): 110 tests, pymongo 110/110 and Node 110/110, against `mongo:7.0` (replica set), among them 38
error-case tests (names ending in `_error`). By prefix: positional `$` 20, `$[]` 13, filtered/arrayFilters 30, pipeline 26, rename 21.
They found 13 differences on the first run, all fixed (error codes of invalid identifiers and unknown stages, which condition decides the `$` position, `_id` removed by a pipeline).
Previously failing extra probes `upd.positional_*`, `upd.pipeline_update_extra`, `fam.error_*`, `coll.extra.rename` now pass; 3 extra probes still fail (`aggregate.extra.group_add_to_set`, `update.extra.current_date`, `lookup.extra_let_pipeline_form`).

### Compat scan, 10 apps

The scan did not look for these features before, so the earlier "NodeBB, novu and appsmith at 100%" (results7) was an overstatement for novu and appsmith: they use
positional operators. I added four detectors to `tools/compat-scan/scan.py` (`arrayFilters`, positional update paths, pipeline-style updates, `renameCollection`;
text matching, so false positives are possible) and **re-cloned and re-scanned all 10 repositories** today (`results8/compat-scan-results.json`, `results8/coverage8-*.txt`).
"Before" = the features as of results7 (the four new ones have no tests yet); "after" = now.

| App | Unsupported before | Unsupported after | What disappeared |
|---|---|---|---|
| Rocket.Chat | 6 | 3 | positional x33, pipeline updates x7, arrayFilters x6 |
| parse-server | 13 | 13 | – |
| NodeBB | 0 | 0 | – |
| wekan | 1 | **0** | positional x143 |
| LibreChat | 3 | 1 | positional x35, arrayFilters x8 |
| novu | 2 | **0** | positional x8, arrayFilters x1 |
| appsmith | 1 | **0** | positional x4 |
| countly-server | 38 | 34 | `renameCollection` x6, positional x5, pipeline updates x4 |
| payload | 6 | 6 | – |
| edx-platform | 1 | 1 | – |
| fully covered | 1/10 | **4/10** (NodeBB, wekan, novu, appsmith) | |

Still unsupported after: `$text` and text indexes, `$search`, `$meta`, geo operators and indexes, hashed/wildcard indexes (recorded only), `$geoNear`.
`renameCollection` across databases is not supported (the server answers `NotImplemented`); a rename of more than 1,000,000 keys is refused (one atomic batch).

## Checks

**Backfill after a kill** (`mongo-tests/backfill_kill_test.py`, `results8/backfill_kill.json`). 10 attempts, each on a new database of 20,000 documents with a concurrent writer, `createIndex`,
SIGKILL 50–600 ms later. Counted only when the build was provably unfinished (client error, index not listed before the kill and not listed right after the restart). **10 of 10 kills
landed mid-build and 10 of 10 backfills finished after the restart** (longest: 0.61 s after the first command on the collection); for all 10, `validate` is valid and the number of
documents found through the index equals the collection count (writes during the build included). The earlier crash test (results7) is unchanged: 40 kills, 0 problems.

**TTL by default** (`mongo-tests/ttl_default_test.py`, `results8/ttl_default.json`; benchmark `results8/ttl_bench.json`).
`--ttl-wake-sleeping` (off by default) is the old behaviour: expiry hints written at close, a maintenance pass (one LIST per pass), wake of due tenants at most once per
`--ttl-min-gap-secs`. By default none of it runs.

* 5 tenants with 20 documents expiring after 3 s, closed; 12 s later (all documents expired): **0 requests of any kind, 0 wakes, 0 open tenants**.
* A read wakes the tenant naturally and sees only the non-expired document; nothing is removed (a reader cannot write): 0 documents removed.
* The next write removes them (20 removed, `ttlDocsExpired` 0 → 20). The other four tenants stay asleep.
* So "physically deleted at the next natural wake" holds for the next **writer** session, not for a read-only wake: a tenant that is only read keeps its expired documents (hidden) until its first write.
* Benchmark (60 tenants, 150 s, 283 documents; all 283 hidden in every row): default 0 wakes, 3,000 PUT-tier requests; wake option with gap 0: 300 wakes (5 per tenant), 3,049; gap 30 s: 50; gap 100 s: 20.
  Request totals: PUT-tier 2,978 (default) vs 3,049; GET-tier 4,284 vs 4,304. In this workload 283 documents were also removed in the default mode by the writer-session sweeper because the tenants were still open when they expired.

## Other fixes found on the way

* A first insert into a database that does not exist yet tried to open the tenant once per document (lease GET/PUT, LIST, lease DELETE each time): 2,000 documents took
  17.8 s. The entry now remembers for the duration of a command that the database is absent: 71 ms. (This was in the previous commit.)
* The crash/backfill test scripts could leave a stale server on the port, which made a run use an old binary; `backfill_kill_test.py` now always kills its server.

## Not done / not measured

* **Cold lookup of ~10 documents below 20 GETs**: not achieved (22 on a 100k tenant; floor 12 + 1 per document). Not done: cheaper wake, document clustering, caching metadata across wakes.
* Cold-read numbers: "before" and "after" come from two runs on the same tenants; latency differences below ~10% are noise. Real S3 latency: not measured.
* Load scenarios S1–S4 of `results-load.md`: not rerun. Local/in-memory stores of the single-tenant table: not run. Shared reads on indexed queries under concurrency: not measured.
* A read-only wake does not physically remove expired documents; I did not add a writer upgrade for it because that turns reads into writes.
* `renameCollection` across databases, `$` with `$or`/`$nor` conditions (the query conditions considered are top-level and `$and`), array filters with unusual operators: not tested beyond the 110 tests.
* Three old extra probes still fail (listed above); the previously known differences of results7 (transactions on one database, in-memory change streams, QuickJS `$where`, text/geo) are unchanged.
* The scan is text matching: the new detectors can over- or under-count.
* The suite ran against the reference once more after the last code change (core 1718/1718, replset 89/89, extra 46/49 on both drivers; `results8/results_py.json`, `results8/results_node.json`).
