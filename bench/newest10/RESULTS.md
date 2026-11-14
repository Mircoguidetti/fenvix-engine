# newest10-trips: newest 10 of one customer, cold

Engine: `night-head` 36c6a3e + this branch. Local only, no AWS.

## Reproduction (`repro.py`)

- Collection `messages`, index `{userId: 1, ts: -1}`, `--customer-key messages=userId:ts` (the shape of
  `bench/strong-point/history.py`).
- 3,000 customers x 40 messages (~2 KB each, interleaved), then 16 rounds in which a random half of the customers
  writes 1 more message each. The last 8 rounds stay in SlateDB L0 (`FENVIX_TEST_ABSORB_MIN_L0=1000`, then SIGKILL
  after fsync so no close pass absorbs them): "customers who kept writing after compaction". Store: ~0.9 GB on disk.
- `FENVIX_SEALED_COMPACTOR=small` (the compactor's scaled-down knobs) so a small store has delta runs as at 10 GB.
- Object store: `--store local` + `FENVIX_SIM_S3=20,0,100000` (20 ms per GET/HEAD/LIST, no jitter). MinIO could not
  be downloaded in this sandbox; the delay model matches s3delay (fixed time to first byte per request).
- Every run starts from a pristine copy of the loaded store and an empty box floor; restart, warm-up = newest 10 of
  300 other customers, then newest 10 of 200 customers never read since the restart, one at a time.
- Per read, from `serverStatus().fenvix`: GETs = `s3.get + s3.getRange`; trips = `waiter.trips` (0 -> 1 transitions of
  reads in flight); chain = `waiter.lastChainDepth` (reads waited for one after the other).
- Every answer is checked against the plan (ids and sha256 of id/userId/ts/text in order) and against a full scan
  (`hint: {$natural: 1}`, sorted client-side, `repro.py scan`).

  ```
  FENVIX_SEALED_COMPACTOR=small python3 bench/newest10/repro.py load --dir D --cust 3000 --per1 40 --rounds 16 --per-r 1 --settle 30 --hold-last 8
  FENVIX_SEALED_COMPACTOR=small python3 bench/newest10/repro.py scan --dir D
  FENVIX_SEALED_COMPACTOR=small python3 bench/newest10/repro.py run  --dir D --out X.json --hold --flags "..."
  ```

## Where the trips come from

With the default caches (A) the read is already one trip: the customer's box (`custbox.rs`) sends one GET per run
holding the customer and one per SlateDB SST, all at once. The trips appear when SlateDB's caches are small next to the
data, as at 10 GB under 512 MiB (meta cache 19 MiB, block cache 57 MiB). `FENVIX_SIM_TRACE` of one read (B, before):

- depth 1: the box's wave (sealed run, delta runs, SST data blocks, SST indexes not cached);
- depth 2: SST data blocks whose index came in at depth 1 (index -> block);
- depths 3-5: the **query** reloads the index (~55 KB) of each L0 SST, one SST after the other, although the box had
  just loaded them: foyer evicted them in between (a new entry sits in a small probation queue).

## Fix: the box's pins (`src/meter.rs`, `--customer-box-pin`, on by default)

From the start of a request's box warm-up to the end of its query, every entry SlateDB's shared cache serves, loads
or is given is also kept in a pin table (bytes capped at the box in-flight allowance, at least 8 MiB); a lookup the
cache misses is answered from it. The request drops its pin after the query. Entries are keyed by (SST id, offset) and
SST bytes never change, so a pinned entry is exactly what the store would return: answers cannot change, only the GETs.

## Measured (same build, pin off vs on, 200 reads each, 20 ms per GET)

| caches | pin | GETs mean (max) | trips p50 / max | p50 ms | p99 ms | wrong vs plan / full scan |
|---|---|---|---|---|---|---|
| B: block 1, meta 1, sealed 2 MiB | off | 13.7 (21) | 5 / 7 | 111.4 | 137.4 | 0 / 0 |
| B | on  | 8.5 (13)  | 2 / 4 | 45.6  | 92.7  | 0 / 0 |
| C: block 5, meta 2, sealed 5 MiB | off | 5.95 (10) | 1 / 3 | 24.2 | 71.3 | 0 / 0 |
| C | on  | 5.95 (9)  | 1 / 2 | 24.2  | 48.7  | 0 / 0 |
| A: budget of 512 MiB | off | 5.85 (9) | 1 / 1 | 24.4 | 28.1 | 0 / 0 |
| A | on  | 5.85 (9) | 1 / 1 | 24.5 | 28.6 | 0 / 0 |

All 6 runs return the same answers (sha256 of the 200 answers `7c25783d1a3c...`), equal to the plan and to the full
scan. Rows: `results/*.json`.

What stays: the second trip of B is inside the box itself (an SST whose index is not cached: index first, then the
block). Removing it needs SST indexes resident (the waiter's maps, `--waiter-tools`) or a box that fetches index and a
guessed block together; not done here.

## Follow-up: the 10 GB read is 1 trip, 10 parallel GETs (coordinator, rows-2-B)

At 10 GB the bench's newest-10 cold is already 1 trip (waiter.trips p50 1) with 10 GETs, p50 39.7 ms against 27.4 ms
for one GET (whole_cold h50). So the cost is the number of GETs in the wave (and their bytes), not their order.

### The real HTTP path (`http_path.py`)

Server `--store s3` -> the bench's s3delay (20 ms on the first byte of each response) -> FakeS3 (`ctl-tests/fake_aws.py`,
in memory; MinIO not fetchable here), loaded with the same store. 200 cold newest-10, default budget: 1 trip, 5.85 GETs,
p50 27.7 ms, p99 47.6 ms, 0 wrong. p50 by GETs per read: 1: 24.7, 2: 26.1, 3: 26.4, 4: 27.2, 5: 27.4, 6: 27.6, 7: 28.2,
8: 28.4, 9: 29.3 ms (~0.6 ms per extra GET; simulated store: ~0.1). s3delay saw 11 connections in the whole run: the
client did not serialize a wave. No client-side concurrency limit found; the per-GET cost is at the store.

### Why a customer has so many runs: R3 never counted the box

R3 (reseal a group early once the delta-run GETs its reads pay cost more than a reseal) counted only point reads
(`View::get`, `get_wave`). The box's per-run GETs never counted, so a customer workload served by the box paid one
GET per delta run on every cold read and never moved its group toward the reseal that removes those runs.

Fix (`fx/view.rs`, `--r3-counts-box-reads`, on by default): each box wave of a delta run read from the store (not its
local copy) counts one R3 GET for the group. Only when a group is resealed changes; never what a read returns.

Measured (same build, switch off vs on; `run --pretraffic 1000`: one write to `app_demo.other` starts the compactor,
cold newest-10 of 1,000 other customers, 40 s, SIGKILL, cold restart, then the usual 200 reads; 20 ms per GET):

| R3 counts box | reseals during the traffic | box delta GETs counted | GETs per cold newest-10 mean (max) | trips | p50 / p99 ms | wrong |
|---|---|---|---|---|---|---|
| off | 0 | 0 | 2.755 (6) | 1 | 23.8 / 26.3 | 0 / 0 |
| on  | 1 | 1,245 | 1.0 (1) | 1 | 23.6 / 26.2 | 0 / 0 |

Same answers (sha `7c25783d1a3c...`). Locally a GET in the wave costs ~0.1 ms, so latency barely moves here. In the
bench each extra GET costs ~1.4 ms of the wave ((39.7 - 27.4) / 9), so 10 -> 1-2 GETs would bring newest-10 cold
near one GET's latency. The price is a reseal of the group when the reads have paid for it (R3's own cost model,
470 GETs per GiB of group). Note: a database only read since its open has no compactor (it starts at the first
write), so neither point reads nor box reads count there; the bench's read-only measured phase would not reseal.
