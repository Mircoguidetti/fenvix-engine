# Where wake requests go, and how to reduce them — results2

Run against MinIO on localhost (same setup as `results.md`: `bitnamilegacy/minio`, slatedb 0.17.0, object_store 0.14.2, 1,000 fresh tenants per variant, 100 cold wakes each). `results.md` is unchanged. All raw data is in `runs2/variants_n1000.json`.

## Corrections to results.md (found while doing this work)

- **WAL cleanup does not remove the duplicate WAL.** `results.md` said that GC removes the ~205 KiB WAL after `min_age`, giving "$4.60/month". That is wrong. slatedb's WAL GC always keeps the WAL file whose id equals the latest manifest's `replay_after_wal_id`, plus everything newer (`src/garbage_collector/wal_gc.rs:59`, `Bound::Included`). For a freshly loaded tenant that file holds all the data. Measured after an offline GC pass with `min_age = 0` (V2–V4): the `wal/` directory still holds ~209.6 KB per tenant next to ~213.5 KB of SST data. GC freed only about 1 KB per tenant (old manifest and compactions files). The realistic storage cost is therefore the **$9.10/month** figure, not $4.60.
- Everything else in `results.md` (idle closed tenants make 0 requests, storage ≈ 425 KB per tenant, ≈ 43–48 requests per wake) was reproduced here.

## Step 0 — What the slatedb 0.17.0 source confirms

Checked in `~/.cargo/registry/src/*/slatedb-0.17.0/src`. Only items listed here were used.

**(a) Read-only reader.** It exists.
- `DbReader::open(path, object_store, mode, options)` and `DbReader::builder(path, object_store)` with `.with_reader_mode(..)` and `.with_options(..)`; `get`, `close` (`db_reader.rs:910`, builder at `db/builder.rs` `DbReaderBuilder`).
- `DbReaderMode` (`db_reader.rs:54`): `ManagedCheckpoint` (the default; creates and refreshes a checkpoint in the manifest, so it writes), `Checkpoint(Uuid)` (pinned, never polls), `FollowLatest` ("performs no object-store writes", but gives no GC protection).
- `DbReaderOptions` (`config.rs:1130`): `manifest_poll_interval` (default 10 s), `checkpoint_lifetime` (10 min), `skip_wal_replay` (default false; "reads no WAL when it opens or refreshes", so it only sees data already flushed to L0 or below), `max_memtable_bytes`, cache options.

**(b) Background polling and refresh settings** (`Settings`, defaults in `config.rs:1101`).
- `flush_interval` (`Option`, default 100 ms; `None` disables automatic WAL flush, which would make writers lose data unless they flush by hand, so I left it alone).
- `manifest_poll_interval` (`config.rs:722`, default 1 s; used to detect fencing and newly compacted data).
- `compactor_options: Option<CompactorOptions>` (`config.rs:811`); `poll_interval` default 5 s (`config.rs:1231`); `None` disables the in-process compactor.
- `garbage_collector_options: Option<GarbageCollectorOptions>` (`config.rs:827`); each directory has `interval` (default 600 s) and `min_age` (default 300 s) (`garbage_collector.rs:59-60`); `None` disables the in-process GC.
- The source states no upper limit for these intervals, and slatedb's own tests use `3600 s`. "Longest safe" here means **1 hour**, which is the longest value I found used in the crate's tests, not a documented maximum. The cost of a long `manifest_poll_interval` is slower detection of a competing writer or of new compaction output; it does not affect writes.

**(c) GC and close.**
- `Admin::builder(path, store).build().run_gc_once(GarbageCollectorOptions)` runs GC once, offline, without opening the `Db` (`admin.rs:297`). `Admin::list_checkpoints` and `delete_checkpoint` also exist.
- `Db::close()` uses `CloseOptions::default()` whose flush type is `MemTable` (`config.rs:515`), so closing writes the memtable to an L0 SST. Evidence that no WAL replay happens on the next open: the V0 wake trace below contains **no GET of any `wal/*.sst`**, only HEADs and one PUT, and all 100 reads in every variant found their key.
- `Db::flush_with_options(FlushOptions { flush_type: FlushType::MemTable })` exists and was used for the V2–V4 load. It still produced a WAL file (see correction above).

### Variants as implemented

| | Definition |
|---|---|
| V0 | Existing load (`put`×200, `flush()`, `close()`); writer `Db::open` with default `Settings` + get (current behaviour). |
| V1a / V1b / V1c | Same data as V0, read with `DbReader`: V1a `ManagedCheckpoint` (API default); V1b `FollowLatest`; V1c `FollowLatest` + `skip_wal_replay` + 1 h `manifest_poll_interval`. *(Three reader variants instead of one, because the reader modes differ in whether they write.)* |
| V2 | Load with `put`×200, `flush_with_options(MemTable)`, `close()`, then an offline GC pass per tenant (`min_age = 0`; only safe because no writer is open). Writer wake with the in-`Db` GC disabled (`garbage_collector_options = None`). |
| V3 | V2 + `manifest_poll_interval` and compactor `poll_interval` set to 1 h. |
| V4 *(added)* | V3 + `compactor_options = None`. Added because the trace shows the compactor accounts for a large share of wake requests. Compaction would have to run elsewhere (`Admin::run_compactor`, not tested here); in the meantime L0 SSTs accumulate up to `l0_max_ssts = 8` per tenant, after which writes are blocked. |

Reading the trace below: the purpose of each call is **inferred from object names and order**, not from reading slatedb's open path. Time-based "overlapped" flags come from the counting layer; bodies are fully read inside the timed window.

## Step 1 — Request trace of one cold wake (V0: writer open + get)

One tenant created by the existing load phase, woken once with tracing on. `start ms` is relative to the start of that phase (the get phase is shifted to follow the open phase). Bodies are fully read inside the timed window. *overlapped* = another call was in flight at some point during this call.

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.28 | 1.22 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.53 | 0.99 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 2.54 | 0.69 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.27 | 1.21 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 3.31 | 0.63 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 3.35 | 1.16 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 3.38 | 1.14 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 3.43 | 1.10 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 3.45 | 1.09 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 3.48 | 1.08 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 3.50 | 1.07 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 4.61 | 2.49 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 7.12 | 0.88 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 8.02 | 2.72 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 10.77 | 0.87 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 11.67 | 1.05 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 12.81 | 0.81 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 13.65 | 0.99 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 14.67 | 2.61 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 17.30 | 1.17 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 18.49 | 1.14 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 19.65 | 1.03 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 0 | 20.70 | 0.86 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 21.59 | 3.71 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 25.32 | 0.97 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 26.32 | 2.65 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 28.98 | 1.06 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 30.21 | 1.00 | yes |
| 29 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 31.01 | 0.30 | yes |
| 30 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 31.02 | 0.30 | yes |
| 31 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 31.12 | 0.19 | yes |
| 32 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 31.19 | 0.12 | yes |
| 33 | get | SST | GET_RANGE | `compacted/01M5SPQHQGH2E0RJD09X4SE6NM.sst` | 2444 | 31.41 | 1.66 | yes |
| 34 | get | other | GET | `gc/manifest.boundary` | 0 | 31.84 | 0.99 | yes |
| 35 | get | other | GET | `gc/compactions.boundary` | 0 | 32.45 | 0.49 | yes |
| 36 | get | other | GET | `gc/compactions.boundary` | 0 | 32.58 | 0.59 | yes |
| 37 | get | other | GET | `gc/manifest.boundary` | 0 | 32.71 | 0.63 | yes |
| 38 | get | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 32.96 | 0.60 | yes |
| 39 | get | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 33.18 | 0.86 | yes |
| 40 | get | SST | GET_RANGE | `compacted/01M5SPQHQGH2E0RJD09X4SE6NM.sst` | 3152 | 33.26 | 1.01 | yes |
| 41 | get | manifest | LIST | `manifest` | 0 | 33.35 | 0.98 | yes |
| 42 | get | other | GET | `gc/compactions.boundary` | 0 | 33.57 | 0.76 | yes |
| 43 | get | other | GET | `gc/manifest.boundary` | 0 | 34.06 | 0.27 | yes |

**Grouped by purpose (this wake):**

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| SST | GET_RANGE ×2 (2.7 ms) | 2 |
| WAL | HEAD ×9 (9.5 ms), PUT(create) ×1 (2.7 ms) | 10 |
| manifest | GET ×6 (4.0 ms), LIST ×2 (2.2 ms), PUT(create) ×2 (5.1 ms) | 10 |
| other | GET ×14 (11.4 ms) | 14 |
| other (compactions/) | GET ×4 (2.1 ms), LIST ×1 (1.1 ms), PUT(create) ×2 (6.4 ms) | 7 |

**This wake:** 43 requests; **19 strictly sequential** (never overlapped with another call), **24 parallel** (overlapped with at least one other call); 22 sequential steps after merging overlapping calls.

**Mean over 10 traced wakes of different tenants:** 43.1 requests, 19.5 strictly sequential, 23.6 parallel, 22.3 sequential steps.

**The matching close (same tenant)** — 9 requests:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | GET | `manifest/00000000000000000007.manifest` | 442 | 0.17 | 1.08 | yes |
| 2 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 1.24 | 2.70 | yes |
| 3 | close | manifest | GET | `manifest/00000000000000000008.manifest` | 442 | 1.28 | 5.79 | yes |
| 4 | close | other | GET | `gc/manifest.boundary` | 0 | 3.95 | 0.71 | yes |
| 5 | close | manifest | GET | `manifest/00000000000000000009.manifest` | 0 | 7.09 | 0.76 | no |
| 6 | close | other | GET | `gc/manifest.boundary` | 0 | 7.87 | 0.66 | no |
| 7 | close | manifest | GET | `manifest/00000000000000000009.manifest` | 0 | 8.55 | 0.76 | no |
| 8 | close | other | GET | `gc/manifest.boundary` | 0 | 9.34 | 0.69 | no |
| 9 | close | WAL | LIST | `wal` | 0 | 10.04 | 1.69 | no |

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | LIST ×1 (1.7 ms) | 1 |
| manifest | GET ×4 (8.4 ms), PUT(create) ×1 (2.7 ms) | 5 |
| other | GET ×3 (2.1 ms) | 3 |


Close, mean over 10: 9.5 requests, 4.8 strictly sequential, 4.7 parallel, 5.9 sequential steps.

### Where an open-but-idle Db spends its requests (60 s window, V0)

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| manifest | GET ×71 (135.3 ms) | 71 |
| other | GET ×96 (186.6 ms) | 96 |
| other (compactions/) | GET ×25 (44.2 ms) | 25 |

<details><summary>Traces of one wake and its close for every other variant</summary>

#### V1a — `DbReader` ManagedCheckpoint (API default) + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.22 | 1.44 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.68 | 1.18 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 3.02 | 1.17 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.23 | 0.94 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.20 | 0.97 | no |
| 6 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 506 | 6.21 | 2.71 | no |
| 7 | open | other | GET | `gc/manifest.boundary` | 0 | 8.94 | 0.92 | no |
| 8 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 506 | 9.87 | 1.23 | no |
| 9 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 11.15 | 0.88 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 11.24 | 0.73 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 11.29 | 1.33 | yes |
| 12 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 11.34 | 1.30 | yes |
| 13 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 11.49 | 1.16 | yes |
| 14 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 11.54 | 1.12 | yes |
| 15 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 11.56 | 1.10 | yes |
| 16 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 11.59 | 1.09 | yes |
| 17 | get | SST | GET_RANGE | `compacted/01M5SPTG7EN3QZ5AWHAG0JRGWJ.sst` | 2444 | 12.81 | 1.38 | yes |
| 18 | get | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 12.84 | 0.94 | yes |
| 19 | get | other | GET | `gc/manifest.boundary` | 0 | 13.81 | 0.98 | yes |
| 20 | get | SST | GET_RANGE | `compacted/01M5SPTG7EN3QZ5AWHAG0JRGWJ.sst` | 3151 | 14.35 | 1.36 | yes |
| 21 | get | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 14.81 | 0.83 | yes |
| 22 | get | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 14.87 | 1.32 | yes |
| 23 | get | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 14.92 | 1.27 | yes |
| 24 | get | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 14.96 | 1.24 | yes |
| 25 | get | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 14.98 | 1.21 | yes |
| 26 | get | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 15.03 | 1.16 | yes |
| 27 | get | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 15.06 | 1.13 | yes |
| 28 | get | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 15.10 | 1.09 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 0.53 | 0.88 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 1.43 | 0.86 | no |
| 3 | close | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 2.34 | 4.38 | no |
| 4 | close | other | GET | `gc/manifest.boundary` | 0 | 6.74 | 0.89 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | HEAD ×48 (80.6 ms) | 48 |
| manifest | GET ×6 (10.1 ms) | 6 |
| other | GET ×6 (6.6 ms) | 6 |

#### V1b — `DbReader` FollowLatest (no object-store writes) + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.29 | 1.51 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.82 | 1.13 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 2.99 | 0.97 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.00 | 0.97 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.00 | 0.81 | no |
| 6 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 5.83 | 1.45 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 5.90 | 1.47 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 5.95 | 1.37 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 5.98 | 1.35 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 6.01 | 1.33 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 6.13 | 1.22 | yes |
| 12 | open | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 6.16 | 1.19 | yes |
| 13 | open | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 6.23 | 1.14 | yes |
| 14 | get | SST | GET_RANGE | `compacted/01M5SPX77QRSC0TRWZ2XTD2RRY.sst` | 2444 | 7.45 | 1.42 | yes |
| 15 | get | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 8.59 | 1.01 | yes |
| 16 | get | SST | GET_RANGE | `compacted/01M5SPX77QRSC0TRWZ2XTD2RRY.sst` | 3152 | 8.99 | 2.22 | yes |
| 17 | get | other | GET | `gc/manifest.boundary` | 0 | 9.63 | 0.70 | yes |
| 18 | get | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 10.35 | 1.09 | yes |
| 19 | get | WAL | HEAD | `wal/00000000000000000004.sst` | 0 | 10.44 | 0.99 | yes |
| 20 | get | WAL | HEAD | `wal/00000000000000000006.sst` | 0 | 10.48 | 0.96 | yes |
| 21 | get | WAL | HEAD | `wal/00000000000000000010.sst` | 0 | 10.50 | 0.90 | yes |
| 22 | get | WAL | HEAD | `wal/00000000000000000018.sst` | 0 | 10.52 | 0.92 | yes |
| 23 | get | WAL | HEAD | `wal/00000000000000000034.sst` | 0 | 10.58 | 0.87 | yes |
| 24 | get | WAL | HEAD | `wal/00000000000000000066.sst` | 0 | 10.61 | 0.84 | yes |
| 25 | get | WAL | HEAD | `wal/00000000000000000130.sst` | 0 | 10.63 | 0.82 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| WAL | HEAD ×48 (75.1 ms) | 48 |
| manifest | GET ×6 (12.6 ms) | 6 |
| other | GET ×6 (6.9 ms) | 6 |

#### V1c — `DbReader` FollowLatest + skip_wal_replay + 1 h poll + get

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.33 | 2.12 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 2.47 | 1.31 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 0 | 3.82 | 0.82 | no |
| 4 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 4.67 | 0.74 | no |
| 5 | open | other | GET | `gc/manifest.boundary` | 0 | 5.44 | 0.96 | no |
| 6 | get | SST | GET_RANGE | `compacted/01M5SPZNC400NTMC41JACF3B1P.sst` | 2444 | 6.48 | 1.31 | yes |
| 7 | get | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 7.54 | 0.87 | yes |
| 8 | get | SST | GET_RANGE | `compacted/01M5SPZNC400NTMC41JACF3B1P.sst` | 3151 | 7.96 | 1.29 | yes |
| 9 | get | other | GET | `gc/manifest.boundary` | 0 | 8.43 | 0.91 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|

#### V2 — memtable-flush load + offline GC (min_age 0); writer open with in-Db GC off

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.30 | 1.69 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 2.01 | 1.78 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 3.83 | 1.09 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 4.97 | 1.82 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 5.08 | 1.57 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 5.13 | 1.69 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 5.18 | 1.76 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 5.21 | 3.12 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 5.24 | 1.59 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 5.28 | 1.62 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 5.34 | 1.58 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 8.52 | 3.62 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 12.16 | 1.04 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 13.22 | 2.99 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 16.24 | 1.03 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 17.30 | 1.00 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 18.37 | 1.25 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 19.66 | 0.97 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 20.68 | 3.33 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 24.03 | 0.87 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 24.92 | 1.19 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 26.13 | 1.12 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 1 | 27.26 | 1.19 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 28.47 | 5.27 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 33.76 | 1.29 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 35.08 | 2.65 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 37.75 | 0.99 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 38.95 | 1.02 | yes |
| 29 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 38.96 | 0.86 | yes |
| 30 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 39.87 | 1.19 | yes |
| 31 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 39.92 | 1.77 | yes |
| 32 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 39.93 | 1.44 | yes |
| 33 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 40.00 | 1.66 | yes |
| 34 | open | other | GET | `gc/compactions.boundary` | 0 | 41.08 | 0.85 | yes |
| 35 | open | other | GET | `gc/manifest.boundary` | 0 | 41.40 | 1.58 | yes |
| 36 | open | other | GET | `gc/compactions.boundary` | 0 | 41.71 | 1.44 | yes |
| 37 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 41.79 | 1.19 | yes |
| 38 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 41.95 | 1.06 | yes |
| 39 | open | other | GET | `gc/compactions.boundary` | 0 | 43.03 | 0.20 | yes |
| 40 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 43.16 | 0.07 | yes |
| 41 | get | SST | GET_RANGE | `compacted/01M5SQ2F9EG398586EDBM75Y6K.sst` | 2444 | 43.34 | 1.25 | yes |
| 42 | get | other | GET | `gc/manifest.boundary` | 0 | 44.35 | 0.87 | yes |
| 43 | get | SST | GET_RANGE | `compacted/01M5SQ2F9EG398586EDBM75Y6K.sst` | 3152 | 44.72 | 1.12 | yes |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 0.27 | 2.77 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.06 | 0.88 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| manifest | GET ×71 (110.9 ms) | 71 |
| other | GET ×96 (120.4 ms) | 96 |
| other (compactions/) | GET ×25 (46.5 ms) | 25 |

#### V3 — V2 + 1 h manifest/compactor poll intervals

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.27 | 1.26 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000005.manifest` | 442 | 1.55 | 1.13 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 2.73 | 0.89 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 3.66 | 1.32 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.77 | 1.22 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 3.82 | 1.50 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 3.87 | 1.14 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 3.92 | 1.02 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 3.97 | 1.05 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 4.00 | 0.96 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 4.03 | 1.57 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 442 | 5.64 | 3.25 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 8.90 | 1.21 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 10.14 | 3.05 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 13.22 | 0.92 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 14.16 | 1.18 | no |
| 17 | open | manifest | GET | `manifest/00000000000000000007.manifest` | 0 | 15.42 | 0.90 | no |
| 18 | open | other | GET | `gc/manifest.boundary` | 0 | 16.36 | 0.99 | no |
| 19 | open | manifest | PUT(create) | `manifest/00000000000000000007.manifest` | 442 | 17.39 | 3.23 | no |
| 20 | open | other | GET | `gc/manifest.boundary` | 0 | 20.64 | 1.00 | no |
| 21 | open | other (compactions/) | LIST | `compactions` | 0 | 21.67 | 1.21 | no |
| 22 | open | other (compactions/) | GET | `compactions/00000000000000000003.compactions` | 42 | 22.90 | 1.01 | no |
| 23 | open | other | GET | `gc/compactions.boundary` | 1 | 23.93 | 1.14 | no |
| 24 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000004.compactions` | 42 | 25.10 | 2.70 | no |
| 25 | open | other | GET | `gc/compactions.boundary` | 0 | 27.82 | 0.91 | no |
| 26 | open | other (compactions/) | PUT(create) | `compactions/00000000000000000005.compactions` | 42 | 28.75 | 3.19 | no |
| 27 | open | other | GET | `gc/compactions.boundary` | 0 | 31.95 | 1.16 | no |
| 28 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 33.27 | 1.09 | yes |
| 29 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 33.28 | 1.25 | yes |
| 30 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 34.27 | 0.95 | yes |
| 31 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 34.37 | 2.49 | yes |
| 32 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 34.44 | 1.35 | yes |
| 33 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 34.44 | 1.11 | yes |
| 34 | open | other | GET | `gc/manifest.boundary` | 0 | 35.26 | 0.73 | yes |
| 35 | open | other | GET | `gc/compactions.boundary` | 0 | 35.57 | 1.29 | yes |
| 36 | open | other | GET | `gc/compactions.boundary` | 0 | 35.80 | 0.76 | yes |
| 37 | open | other (compactions/) | GET | `compactions/00000000000000000006.compactions` | 0 | 36.58 | 0.77 | yes |
| 38 | open | manifest | GET | `manifest/00000000000000000008.manifest` | 0 | 36.88 | 0.71 | yes |
| 39 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 37.00 | 1.50 | yes |
| 40 | open | other | GET | `gc/compactions.boundary` | 0 | 37.36 | 1.17 | yes |
| 41 | open | other | GET | `gc/manifest.boundary` | 0 | 37.62 | 1.19 | yes |
| 42 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 2444 | 38.92 | 1.28 | no |
| 43 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 3150 | 40.34 | 1.26 | no |
| 44 | get | SST | GET_RANGE | `compacted/01M5SQ591132ARN8Z43FS77ZRF.sst` | 3151 | 41.80 | 1.24 | no |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000008.manifest` | 442 | 0.28 | 2.84 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.14 | 1.02 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|
| other | GET ×11 (14.7 ms) | 11 |
| other (compactions/) | GET ×11 (22.8 ms) | 11 |

#### V4 — V3 + in-Db compactor off (compaction would have to run offline)

Wake:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | open | manifest | LIST | `manifest` | 0 | 0.24 | 1.09 | no |
| 2 | open | manifest | GET | `manifest/00000000000000000004.manifest` | 434 | 1.35 | 0.90 | no |
| 3 | open | other | GET | `gc/manifest.boundary` | 1 | 2.27 | 0.91 | no |
| 4 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 3.21 | 2.08 | yes |
| 5 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 3.27 | 1.98 | yes |
| 6 | open | WAL | HEAD | `wal/00000000000000000005.sst` | 0 | 3.31 | 1.78 | yes |
| 7 | open | WAL | HEAD | `wal/00000000000000000009.sst` | 0 | 3.36 | 1.90 | yes |
| 8 | open | WAL | HEAD | `wal/00000000000000000017.sst` | 0 | 3.38 | 0.68 | yes |
| 9 | open | WAL | HEAD | `wal/00000000000000000033.sst` | 0 | 3.40 | 1.70 | yes |
| 10 | open | WAL | HEAD | `wal/00000000000000000065.sst` | 0 | 3.42 | 1.90 | yes |
| 11 | open | WAL | HEAD | `wal/00000000000000000129.sst` | 0 | 3.44 | 1.68 | yes |
| 12 | open | manifest | PUT(create) | `manifest/00000000000000000005.manifest` | 434 | 5.36 | 3.66 | no |
| 13 | open | other | GET | `gc/manifest.boundary` | 0 | 9.03 | 0.98 | no |
| 14 | open | WAL | PUT(create) | `wal/00000000000000000003.sst` | 0 | 10.04 | 2.93 | no |
| 15 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 13.01 | 0.98 | no |
| 16 | open | other | GET | `gc/manifest.boundary` | 0 | 14.02 | 0.86 | no |
| 17 | open | WAL | HEAD | `wal/00000000000000000002.sst` | 209620 | 15.02 | 0.78 | yes |
| 18 | open | WAL | HEAD | `wal/00000000000000000003.sst` | 0 | 15.03 | 1.00 | yes |
| 19 | open | manifest | GET | `manifest/00000000000000000006.manifest` | 0 | 15.09 | 0.84 | yes |
| 20 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 131072 | 15.81 | 2.41 | yes |
| 21 | open | other | GET | `gc/manifest.boundary` | 0 | 15.95 | 2.23 | yes |
| 22 | open | WAL | GET_RANGE | `wal/00000000000000000002.sst` | 209338 | 18.33 | 0.97 | no |
| 23 | get | SST | GET_RANGE | `compacted/01M5SQ82QWKRAG8REYE7V8522D.sst` | 2444 | 19.37 | 1.12 | no |
| 24 | get | SST | GET_RANGE | `compacted/01M5SQ82QWKRAG8REYE7V8522D.sst` | 3150 | 20.68 | 1.24 | no |

Close:

| # | phase | purpose | op | object (relative to tenants/{id}/) | bytes | start ms | dur ms | overlapped |
|---|---|---|---|---|---|---|---|---|
| 1 | close | manifest | PUT(create) | `manifest/00000000000000000006.manifest` | 434 | 0.65 | 2.51 | no |
| 2 | close | other | GET | `gc/manifest.boundary` | 0 | 3.17 | 1.23 | no |

Idle-open window by purpose:

| purpose | operations (count, summed duration) | total calls |
|---|---|---|

</details>

## Step 2 — Variant comparison (1000 fresh tenants per variant, 100 cold wakes each, MinIO on localhost)

| | Variant | Sequential steps / wake | Strictly seq. / parallel / total req. per wake | Wake: PUT/LIST-tier · GET-tier | Close: PUT/LIST-tier · GET-tier | Wake p50 / p99 (ms) | Idle-open req. / 60s (PUT/LIST · GET) | Storage B/tenant (raw → after cleanup) |
|---|---|---|---|---|---|---|---|---|
| **V0** | writer `Db::open` + get, default settings (current behaviour) | **22.3** | 19.5 / 23.6 / 43.1 | 7.97 · 35.67 | 2.21 · 7.39 | 35.9 / 46.2 | 192 (0 · 192) | 424616 → no cleanup run |
| **V1a** | `DbReader` ManagedCheckpoint (API default) + get | **10.1** | 8.0 / 15.4 / 23.4 | 2.00 · 23.54 | 1.00 · 5.86 | 16.9 / 26.6 | 60 (0 · 60) | 424614 → no cleanup run |
| **V1b** | `DbReader` FollowLatest (no object-store writes) + get | **7.4** | 5.1 / 16.1 / 21.2 | 1.00 · 21.12 | 0.00 · 3.18 | 10.5 / 17.4 | 60 (0 · 60) | 424620 → no cleanup run |
| **V1c** | `DbReader` FollowLatest + skip_wal_replay + 1 h poll + get | **6.1** | 5.1 / 4.0 / 9.1 | 1.00 · 8.28 | 0.00 · 0.01 | 9.2 / 14.4 | 0 (0 · 0) | 424611 → no cleanup run |
| **V2** | memtable-flush load + offline GC (min_age 0); writer open with in-Db GC off | **22.9** | 20.3 / 22.7 / 43.0 | 7.00 · 36.39 | 1.00 · 1.00 | 40.6 / 69.1 | 192 (0 · 192) | 424590 → **423610** |
| **V3** | V2 + 1 h manifest/compactor poll intervals | **23.0** | 20.5 / 22.6 / 43.1 | 7.00 · 36.44 | 1.00 · 1.01 | 37.0 / 60.7 | 22 (0 · 22) | 424590 → **423668** |
| **V4** | V3 + in-Db compactor off (compaction would have to run offline) | **12.9** | 10.9 / 13.5 / 24.4 | 3.00 · 21.51 | 1.00 · 1.00 | 22.7 / 34.3 | 0 (0 · 0) | 424342 → **423639** |

PUT/LIST-tier = PUT + COPY + LIST (multipart included); GET-tier = GET + range GET + HEAD. Per-wake figures are exact means over 100 untraced wakes of distinct tenants; sequential-step figures are means over 10 separate traced wakes (tracing buffers response bodies, so latency is taken only from the untraced wakes).

Offline maintenance (not part of any wake):

| Variant | Load time | Offline GC requests (all 1000 tenants) | per tenant PUT/LIST · GET · DELETE | Final objects/tenant |
|---|---|---|---|---|
| V0 | 19.7 s | 0 | 0.00 · 0.00 · 0.00 | 11.95 |
| V1a | 22.5 s | 0 | 0.00 · 0.00 · 0.00 | 11.49 |
| V1b | 20.6 s | 0 | 0.00 · 0.00 · 0.00 | 11.31 |
| V1c | 22.3 s | 0 | 0.00 · 0.00 · 0.00 | 11.25 |
| V2 | 17.3 s | 43001 | 10.00 · 27.00 · 6.00 | 7.77 |
| V3 | 18.2 s | 43000 | 10.00 · 27.00 · 6.00 | 7.77 |
| V4 | 13.2 s | 36002 | 9.00 · 24.00 · 3.00 | 5.60 |

## Step 3 — Projections (NOT measurements)

Projected wake latency = sequential steps × per-request latency. Assumptions: **5 ms** per request (S3 Express One Zone) and **30 ms** (S3 Standard). This ignores transfer time, request-size effects and tail latency, so it is a rough lower bound; parallel requests are assumed to cost one step.

Break-even idle time = (cost of wake + cost of close) ÷ (cost per second of an open-but-idle Db), at S3 Standard prices (PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000). If a tenant will stay idle longer than this, closing it and waking it later is cheaper than leaving it open.

| Variant | Sequential steps | Projected wake @ 5 ms | Projected wake @ 30 ms | Wake + close cost | Idle-open cost | Break-even idle time |
|---|---|---|---|---|---|---|
| V0 | 22.3 | 112 ms | 669 ms | $0.00006812 | $3.32/month | 53.2 s |
| V1a | 10.1 | 50 ms | 303 ms | $0.00002676 | $1.04/month | 1.1 min |
| V1b | 7.4 | 37 ms | 222 ms | $0.00001472 | $1.04/month | 36.8 s |
| V1c | 6.1 | 30 ms | 183 ms | $0.00000832 | $0.00000000/month | never (no idle requests) |
| V2 | 22.9 | 114 ms | 687 ms | $0.00005496 | $3.32/month | 42.9 s |
| V3 | 23.0 | 115 ms | 690 ms | $0.00005498 | $0.38016000/month | 6.2 min |
| V4 | 12.9 | 64 ms | 387 ms | $0.00002900 | $0.00000000/month | never (no idle requests) |

## What the trace and variants show

1. **A V0 wake is 43 requests, 19–20 of them strictly sequential and 22 sequential steps** (the rest overlap). By purpose (single wake, V0): WAL 10 (9 parallel HEADs on `wal/` — probes at ids 5, 6, 8, 12, 20, 36, 68, 132, apparently looking for the end of the WAL — plus 1 empty-WAL PUT that fences the writer), manifest 10 (LIST, GETs, 2 conditional creates), compactions state 7 (LIST, GET, 2 conditional creates), SST 2, and **14 GETs of the 1-byte `gc/manifest.boundary` and `gc/compactions.boundary` files**, which are read after almost every manifest or compactions read.
2. **The in-`Db` GC is not what makes wakes expensive.** V2 (GC off in the `Db`, tenants cleaned offline) has the same 43 requests and ~23 steps as V0; it only makes the *close* cheaper (1 PUT + 1 GET versus 2.2 PUT/LIST + 7.4 GET in V0).
3. **The in-`Db` compactor is the largest single saving for a writer.** V4 turns off the compactor and goes from 23.0 to 12.9 steps and from 7 to 3 PUT/LIST-tier requests per wake. It also brings idle-open requests to 0. Longer polling alone (V3) cut idle-open requests from 192 to 22 per minute but did not change the wake; the remaining 22 are compactor requests (11 compactions-state GETs + 11 `gc/*.boundary` GETs).
4. **A reader is much cheaper than a writer.** V1b (`FollowLatest`) needs 7.4 steps and 21 requests with no object-store writes except one LIST; V1c (+ `skip_wal_replay`) needs **6.1 steps, 9 requests, 1 PUT/LIST-tier and 8 GET-tier**, with no close cost and no idle requests. `skip_wal_replay` was safe here only because every tenant had been closed cleanly (memtable flushed to L0); a reader would not see data still only in the WAL.
5. **Open readers poll expensively unless the WAL is skipped.** V1a/V1b make 60 requests per minute while open (every 10 s: 8 WAL HEADs + manifest/boundary GETs). With `skip_wal_replay` and a 1 h interval (V1c), an open reader makes 0.
6. **Storage cannot be reduced by GC** (see the correction at the top). The `Storage B/tenant` column shows 424.6 KB before and 423.6–423.7 KB after cleanup. Object count per tenant did fall (11.95 → 7.77 for V2/V3, → 5.60 for V4), but object count does not affect the storage bill. Offline GC itself costs ~10 PUT/LIST-tier + ~27 GET-tier requests per tenant (V2/V3), about $0.00006 once.

## Caveats

- **Latency is MinIO on localhost**, a lower bound for real S3. Wake p50/p99 come from 100 untraced wakes; p99 over 100 samples is noisy (V0's p99 was 44–122 ms across my runs).
- **Sequential steps** merge any calls whose time intervals overlap into one step, using times measured on MinIO. On a slower backend, calls that barely overlap here might serialise differently. The step counts are means of 10 traced wakes per variant, not of the 100.
- **The 5 ms / 30 ms projections are assumptions, not measurements**, and multiply only the request count; they ignore transfer time and tail latency.
- **Break-even** treats an open idle `Db` as costing exactly its measured idle requests per second, and ignores compute, memory, and S3 request-rate limits. A value of "never" means no idle requests were measured in 60 s, not that holding a tenant open is free.
- V4's cost saving depends on moving compaction elsewhere; that cost is not measured.
- Run-to-run variation: counts were stable (±0.3 requests), latencies less so.

## Conclusion

The 43 requests of a V0 wake are mostly writer start-up work: WAL-tail probes, fencing writes, the in-process compactor's own state reads and writes, and many reads of GC boundary files; none of them is the data read, which is two ranged GETs. For a tenant that is only read, the best variant is **V1c** (`DbReader` in `FollowLatest` mode with `skip_wal_replay` and a 1 h poll interval): 6.1 sequential steps instead of 22.3 (projected 30 ms at 5 ms per request and 183 ms at 30 ms, against 112 ms and 669 ms for V0), 9 requests costing $0.0000083 per wake instead of $0.0000681 for wake plus close, no close cost, and 0 idle requests, so the break-even time never applies. For a tenant that must also accept writes, the best variant is **V4** (writer with the GC and compactor off and 1 h polling): 12.9 steps (64 ms / 387 ms projected), $0.0000290 per wake plus close, and 0 idle requests, provided compaction and GC are run offline. Moving GC offline alone (V2) and lengthening polling alone (V3) barely change the wake, and no GC setting reduces stored bytes because slatedb always keeps the latest WAL file.

## Reproduce

```bash
# Docker + MinIO + bucket as in results.md, then:
cargo build --release
./target/release/tenant-bench --experiment wake-variants --tenants 1000 --out-dir runs2
#   --only V0,V4        run a subset of variants
#   --idle-secs 60      length of the idle-open window (default 60)
#   needs --tenants >= 111 (100 untraced + 10 traced + 1 idle tenant, all distinct)
# The default benchmark is unchanged:  cargo run --release -- --tenants 1000
```

Takes about 15 minutes at 1,000 tenants (seven variants, each with a load phase and a 60 s idle window).
