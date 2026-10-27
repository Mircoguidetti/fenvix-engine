## Step 1 — Lazy-writer lifecycle: sequential steps and requests per phase (100 traced samples each, distinct tenants)

Wake = clean-marker HEAD in parallel with a speculative `DbReader` (FollowLatest, `skip_wal_replay`, 1 h poll) open. Upgrade = marker DELETE in parallel with the V4 writer open (reader closed first). Close = `Db::close()` flush, then marker PUT. Request counts are exact; steps merge overlapping calls (see results2.md).

| Phase | Samples | Sequential steps (mean) | Requests: PUT/LIST-tier · GET-tier · DELETE | Latency p50 / p99 (ms, MinIO) | Request cost |
|---|---|---|---|---|---|
| (1) Read-only wake (wake + 1 get) | 100 | 6.2 | 1.00 · 9.31 · 0.00 | 8.8 / 19.7 | $0.00000872 |
| (1b) Read-only close | 100 | 0.0 | 0.00 · 0.00 · 0.00 | 0.1 / 2.3 | $0.00000000 |
| Write session: reader wake only | 100 | 4.8 | 1.00 · 5.00 · 0.00 | 5.3 / 9.4 | $0.00000700 |
| (2) Upgrade reader → writer | 100 | 11.1 | 3.00 · 14.89 · 1.00 | 15.0 / 26.2 | $0.00002096 |
| Write session: 1–10 puts + 1 get (in memory) | 100 | 0.4 | 0.06 · 0.35 · 0.00 | 0.5 / 1.2 | $0.00000044 |
| (3) Clean writer close (flush + marker) | 100 | 8.0 | 5.00 · 4.75 · 0.00 | 14.8 / 25.0 | $0.00002690 |
| Unclean-path wake (marker missing; wake + 1 get)* | 20 | 12.2 | 2.00 · 27.30 · 0.00 | 16.9 / 28.9 | $0.00002092 |

\* Marker removed by hand on 20 tenants whose data was actually flushed, to price the slow path. Real unclean tenants are in Step 4.

## Step 2 — Mixed workload: 2000 wakes on 1000 tenants (1563 read-only, 437 write sessions), run sequentially with the compactor and GC loops active

| Wake type | Wakes | Latency p50 / p99 (ms, MinIO) | Sequential steps (mean, 4 traced) | Requests per wake: PUT/LIST-tier · GET-tier · DELETE | Total requests: PUT/LIST-tier · GET-tier |
|---|---|---|---|---|---|
| Read-only (1–5 gets, then idle/closed) | 1563 | 12.8 / 27.6 | 8.6 | 1.00 · 11.87 · 0.00 | 1563 · 18560 |
| Write session (1–10 puts, 1 get, clean close) | 437 | 38.0 / 78.9 | 24.3 | 9.03 · 25.55 · 1.00 | 3945 · 11164 |
| **All wakes (lifecycle only, excl. maintenance)** | 2000 | | | | **5508 · 29724** |

Workload wall time 38.4 s. Per-phase breakdown of the same wakes:

| Phase | Samples | Sequential steps (mean) | Requests: PUT/LIST-tier · GET-tier · DELETE | Latency p50 / p99 (ms, MinIO) | Request cost |
|---|---|---|---|---|---|
| read: close | 1563 | 0.0 | 0.00 · 0.00 · 0.00 | 0.1 / 1.0 | $0.00000000 |
| read: wake+get | 1563 | 8.6 | 1.00 · 11.87 · 0.00 | 12.6 / 26.7 | $0.00000975 |
| write: close | 437 | 8.0 | 5.00 · 4.51 · 0.00 | 15.1 / 42.5 | $0.00002680 |
| write: upgrade | 437 | 11.1 | 3.00 · 15.74 · 1.00 | 15.9 / 34.9 | $0.00002130 |
| write: wake | 437 | 4.9 | 1.00 · 5.00 · 0.00 | 5.5 / 10.5 | $0.00000700 |
| write: work | 437 | 0.3 | 0.03 · 0.29 · 0.00 | 0.5 / 2.5 | $0.00000025 |

Unclean wakes taken in steps 1–3: 20 (expected 20 from the deliberate marker removal).

## Step 3 — Offline compaction and GC while the lifecycle runs

Compaction: a bounded standalone compactor pass (`CompactorBuilder::run`, 50 ms poll, embedded worker) per tenant, queued when a writer closes with ≥ 4 L0 SSTs, 4 passes at a time. GC: `Admin::run_gc_once` 20s after a tenant's last write, files older than 15s. Writers (V4 settings) have no in-process compactor or GC. A writer that wakes with ≥ 6 L0 SSTs waits for compaction first (the L0 limit is 8).

| Scenario | Tenants × sessions | Max L0 SSTs after any close | Max L0 at wake | Slowest upgrade / close (ms) | Ops ≥ 2 s or timed out | Guard waits |
|---|---|---|---|---|---|---|
| Round-robin sessions | 20 × 50 | 4 | 3 | 83 / 75 | 0 | 0 |
| Stress: one tenant, back-to-back | 1 × 50 | 6 | 6 | 173 / 32 | 0 | 8 |

Per-tenant maximum L0 count after a session, 20-tenant run: 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4.

**Control (detector check): no compactor, no guard, one tenant, 5 puts per session, 5 s timeout per phase:**

- session 1: ok in 37 ms, L0 1 -> 2
- session 2: ok in 34 ms, L0 2 -> 3
- session 3: ok in 47 ms, L0 3 -> 4
- session 4: ok in 41 ms, L0 4 -> 5
- session 5: ok in 38 ms, L0 5 -> 6
- session 6: ok in 53 ms, L0 6 -> 7
- session 7: ok in 46 ms, L0 7 -> 8
- session 8: STALL in phase close (> 5s) (L0 at start 8, now 8)

| Maintenance (during steps 1–4) | Passes | Requests per pass: PUT/LIST-tier · GET-tier · DELETE | Wall time per pass p50 / p99 (s) | Mean L0 before → after, checkpoints deleted per pass |
|---|---|---|---|---|
| Compaction (standalone compactor, incl. checkpoint cleanup) | 268 | 18.18 · 72.54 · 0.00 | 0.21 / 0.34 | 4.0 → 0.1, 1.0 |
| GC (`run_gc_once`, min_age 15s) | 474 | 8.95 · 24.88 · 28.91 | | |

Maintenance errors: 0. Final sweep after quiesce: 0 tenants still needed compaction; final GC (min_age 0) over 999 tenants used 8101 PUT/LIST-tier + 24085 GET-tier + 3717 DELETE requests in total. Maximum L0 count across all tenants after the final sweep: 3; maximum checkpoints left in any manifest: 0.

| Storage per tenant | Bytes | Objects |
|---|---|---|
| After load | 424619 | |
| After steps 1–4, before final compaction/GC | 344073 | |
| **After final compaction + GC (min_age 0)** | **321545** | 10.51 |

Final bytes per tenant by usage: never written 390379 (535 tenants); written at least once 241723 (464 tenants); the 20 tenants with 50 sessions each 532753; the stress tenant (50 back-to-back sessions) 491867.

By directory, before final pass: _clean/ 5 · compacted/ 223577 · compactions/ 101 · gc/ 1 · manifest/ 1145 · wal/ 119243. After: _clean/ 5 · compacted/ 223577 · compactions/ 63 · gc/ 2 · manifest/ 634 · wal/ 97264.

## Step 4 — Crash safety: 20 writers killed with SIGKILL

Each child opens a writer through the lifecycle, writes 10 documents, awaits durability (`WriteHandle::await_durable`) for the first K (3–10) and prints `ACK i` for each; the parent kills it with SIGKILL as soon as all writes were issued. "WAL rows not in L0" is an independent oracle that does not use the marker: rows in non-empty WAL files whose sequence number is above the manifest's `last_l0_seq`.

| Tenant | Acked | WAL rows not in L0 (seq > `last_l0_seq`) | Marker present after kill | Unclean path taken | Acked writes lost | Un-acked writes recovered | Acked visible with unsafe `skip_wal_replay` | Clean after recovery session | Re-wake clean and complete |
|---|---|---|---|---|---|---|---|---|---|
| 442 | 6 | 6 | no | yes | **0** | 0 | 0 of 6 | yes | yes |
| 735 | 10 | 10 | no | yes | **0** | 0 | 0 of 10 | yes | yes |
| 963 | 6 | 6 | no | yes | **0** | 0 | 0 of 6 | yes | yes |
| 804 | 10 | 10 | no | yes | **0** | 0 | 0 of 10 | yes | yes |
| 719 | 9 | 9 | no | yes | **0** | 0 | 0 of 9 | yes | yes |
| 499 | 6 | 6 | no | yes | **0** | 0 | – | yes | yes |
| 923 | 10 | 10 | no | yes | **0** | 0 | – | yes | yes |
| 759 | 3 | 3 | no | yes | **0** | 0 | – | yes | yes |
| 832 | 8 | 8 | no | yes | **0** | 0 | – | yes | yes |
| 468 | 6 | 6 | no | yes | **0** | 0 | – | yes | yes |
| 852 | 10 | 10 | no | yes | **0** | 0 | – | yes | yes |
| 531 | 6 | 6 | no | yes | **0** | 0 | – | yes | yes |
| 52 | 10 | 10 | no | yes | **0** | 0 | – | yes | yes |
| 94 | 9 | 9 | no | yes | **0** | 0 | – | yes | yes |
| 868 | 4 | 4 | no | yes | **0** | 0 | – | yes | yes |
| 518 | 3 | 3 | no | yes | **0** | 0 | – | yes | yes |
| 454 | 5 | 5 | no | yes | **0** | 0 | – | yes | yes |
| 355 | 10 | 10 | no | yes | **0** | 0 | – | yes | yes |
| 630 | 5 | 5 | no | yes | **0** | 0 | – | yes | yes |
| 674 | 9 | 9 | no | yes | **0** | 0 | – | yes | yes |

**Totals:** 145 acknowledged writes, **0 lost**; unclean path taken for 20 of 20 tenants; 20 of 20 tenants had WAL rows not yet in L0 (so skipping WAL replay would have lost data).

**End-to-end check after final compaction and GC:** every write acknowledged in steps 1–4 (incl. post-recovery writes) was read back through the normal lifecycle: 8702 verified, **0 missing or wrong**, across 464 tenants.

## Step 5 — Cost (S3 Standard list prices: PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000) and projected latency

| Operation | Basis | Request cost |
|---|---|---|
| Read-only wake (wake + 1–5 gets + close) | mean of 1563 wakes | **$0.00000975** |
| Write session (wake + upgrade + puts/get + close) | mean of 437 sessions | **$0.00005536** |
| of which the upgrade | 100 samples, step 1 | $0.00002096 |
| One compaction pass | mean of 268 passes | **$0.00011993** |
| One GC pass per tenant | mean of 474 passes | $0.00005469 |
| Compaction + GC amortised over all 1607 write sessions of steps 1–4 | 268 compaction + 474 GC passes | $0.00003613 per write session |
| **All-in write session** (session + amortised maintenance) | | **$0.00009149** |

**Projected** latency (NOT measured): sequential steps × 5 ms (S3 Express One Zone assumption) or × 30 ms (S3 Standard assumption). Ignores transfer time and tail latency.

| Phase / wake type | Sequential steps (traced mean) | Projected @ 5 ms | Projected @ 30 ms |
|---|---|---|---|
| Read-only wake + 1 get (step 1) | 6.2 | 31 ms | 186 ms |
| Upgrade reader → writer (step 1) | 11.1 | 56 ms | 334 ms |
| Clean writer close (step 1) | 8.0 | 40 ms | 241 ms |
| Unclean-path wake + 1 get (step 1) | 12.2 | 61 ms | 366 ms |
| Whole read-only session incl. close (step 2) | 8.6 | 43 ms | 258 ms |
| Whole write session, wake→close (step 2) | 24.3 | 121 ms | 728 ms |
