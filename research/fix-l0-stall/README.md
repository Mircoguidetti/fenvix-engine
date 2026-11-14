# fix-l0-stall (2026-11-14): parallel loaders stalled at ~250k documents

All local, $0. Branch `fix-l0-stall` from night-head-2 61b8fe88.

## Finding to explain

Measured by first-course-short: 2 to 4 parallel loaders stalled the local-store server at about 250k documents. Writes
were not acknowledged within 30 s and the stall was read as "L0 full". It happened at a 512 MiB cap and at a 4 GiB cap.
One sequential loader went through.

## Reproduction (MEASURED)

The driver is `pload.py`. It does the same as `fc_local.py load`: 500-document unordered `insert_many`, chat-message
documents of about 1.7 KB, 40,000 customers, index `{userId:1, ts:-1}`, `--customer-key messages=userId`. The difference
is that it runs N clients in parallel, plus one client doing `insert_one` every 10 ms to time single-write acks. It is
needed because the bench-strong-point harness `fc_local.py` imports is not in this sandbox. `run.sh NAME MEM CLIENTS
TOTAL` starts the server (`--store local`), runs `mon.py` (stored L0 and sorted runs, compaction counters, every 2 s)
and runs the load.

| run | binary | store | cap | clients | docs | result | docs/s | chunk ack p50 / p99 / max ms | single insert p50 / p99 ms |
|---|---|---|---|---|---|---|---|---|---|
| r1 | night-head-2 | local | 512 MiB | 4 | 600k | completed | 3,333 | 505 / 1,596 / 1,934 | 434 / 1,495 |
| r2 | night-head-2 | local | 4 GiB | 4 | 946,570 | completed | 2,743 | 647 / 1,785 / 10,650 | 582 / 1,690 |
| r3 | night-head-2 | local + `FENVIX_SIM_S3=20,0,100000` | 512 MiB | 4 | stalled at ~262k | 30 s WRITE_DEADLINE | 0-300 at the stall | — | — |
| f1 | **fix-l0-stall** | same as r3 | 512 MiB | 4 | 600k | **completed, 0 timeouts** | 1,446 | 1,335 / 2,844 / 3,369 | 1,085 / 2,802 |
| m1 | fix-l0-stall, `FENVIX_INSERT_PREREAD=0` (mutation) | same as r3 | 512 MiB | 4 | stalled at ~260k | 30 s WRITE_DEADLINE (insert of 32.2 s) | 100-300 | — | — |

With the plain local store (no GET latency), r1 and r2 did not stall. Throughput still fell from about 9k/s to about
2.5k/s at about 250k documents. `fc_local.py`'s `up()` sets the simulated S3 (20 ms per GET) unless `sim=False` is
passed. A parallel loader built on `up()` therefore ran with it, and that is the condition that stalls (r3).

m1 ran while the unit tests were running on the same 4 cores. The stall is made of serialized 20 ms GET waits, not CPU,
and r3 (run alone) stalled at the same point.

## Cause (MEASURED, `FX_WAITS`: results/r4-waits-insert.txt)

It is not L0 and not SlateDB's backpressure:

- Every 500-document insert made **1000 point reads one after another under the database's exclusive lock**
  (`read_get=…/1000`). For each document, `Writes::exists` reads the document key (customer, `_id`), and then
  `Writes::put` reads the same key again (the in-box `_id` check, P-big-speed fix 4).
- At first those reads are answered from the memtable, L0 and the caches. Once the compactor has resealed data into
  sealed runs (from about 250k documents), a new `_id` under an existing customer needs that customer's block. The
  filters do not answer "absent" for it (`sealedRuns.counter.filterChecks` 11,836 against `filterSkips` 20 in 10 s).
  So a batch paid 180-736 GETs, 20 ms each, in sequence: 4-14 s of reads per batch.
- The other loaders waited for the lock (`lock_r`/`lock_w` up to 15 s). With 4 clients, an insert passed 30 s.
- `stage` (the SlateDB write, including its backpressure) was about 2 ms per batch, and `wal_flush` about 3 ms. The
  stored L0 stayed between 0 and 8 (5-6 during the stall), and absorption and SlateDB's fold jobs kept running
  (results/r3-compaction-trace.txt). L0 being high at a given moment is what was seen, but it is not what held the
  writes.

## Fix

`coll::Writes::preread_ids`, called once by `insert` before its per-document loop:

- It reads whether every document key of the batch exists with one `Entry::get_live_many`. That goes through
  `layout2::get_many`: physical order, 32 reads in parallel, blocks that keys share fetched once.
- `exists` and `put` consult the answers after `ov`, so the batch's own writes win: a duplicate inside the batch is
  still found.
- It runs only outside transactions, only for a customer-keyed collection whose document key is (customer, `_id`), and
  only with `--id-check-in-box` on. The answers are cleared before the commit. The exclusive lock is held from the read
  to the commit, so no other writer can change those keys in between.
- `FENVIX_INSERT_PREREAD=0` is the negative control (one read per document in turn, as before).
- serverStatus `fenvix.insertPreread.keys` counts the keys it read.

With the fix, a batch makes 500 reads, issued in parallel (results/f1-waits-insert.txt): about 0.4-2 s per batch at
20 ms GETs, against 4-21 s before.

Tests:
- `ctl-tests/test_96_insert_preread.py` covers duplicate keys against stored documents after a restart, the same `_id`
  under another customer, in-batch duplicates (ordered and unordered) and re-inserting a deleted `_id`, each with the
  batch read on and off. It passed locally through a `--store local` stand-in for `lib.py`, since there is no MinIO
  here. CI runs it with MinIO.
- `cargo test --release -p mongo-server`: 133 passed.
- `cargo test --release --lib fx::sim`: 21 passed, 4 ignored.

## Still outside the target (ESTIMATED)

Product target: acknowledged write p99 < 10 ms and at least 10,000 writes/s per log. Even with the fix, this load is at
about 1.4k docs/s with an ack p99 of about 2.8 s on 20 ms GETs. There are two structural reasons:

1. An insert of a new `_id` under a customer that has sealed data costs a block GET when the block is not cached. Our
   filters do not answer "absent" for (customer, `_id`).
2. Writes to one database serialize on its exclusive lock, so a single `insert_one` waits behind every batch in front
   of it.

Both need a design change, not a small fix. One possibility for (1) is an exact-key "absent" answer (per-run `_id`
filters held in RAM). For (2), one possibility is an insert path that checks outside the lock and stages under it.
