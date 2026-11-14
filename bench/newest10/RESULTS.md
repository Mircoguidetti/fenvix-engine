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
