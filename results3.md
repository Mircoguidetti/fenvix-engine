# Wake lifecycle prototype: correctness, stalls, cost — results3

Run against MinIO on localhost (same setup as before: `bitnamilegacy/minio`, slatedb 0.17.0, object_store 0.14.2, 1,000 fresh tenants). `results.md` and `results2.md` are unchanged. Raw report: `runs3/lifecycle_n1000.md`.

## Step 0 — What the slatedb 0.17.0 source confirms

Checked in `~/.cargo/registry/src/*/slatedb-0.17.0/src`. Only items listed here were used.

**(a) A compactor outside the writer: possible.**
- `CompactorBuilder::new(path, object_store).with_options(CompactorOptions).build()` returns a `Compactor`; its public methods are `run()` (starts and blocks until stopped) and `stop()` (`db/builder.rs:1228-1276`, `compactor.rs:376,457`; `start` and `join` are `pub(crate)`). `Admin::run_compactor_with_options(cancellation_token, options)` wraps the same thing (`admin.rs:375`).
- There is **no "compact once and return" call**: a standalone compactor runs until stopped. The prototype therefore starts one per tenant, waits until the tenant's L0 count drops, and stops it. The wait is a test-harness poll of the manifest through an uncounted store handle; a production job would watch the compactions state instead.
- A writer opened with `Settings::compactor_options = None` coexists with an external compactor; verified by the runs below (writes, compactions and reads interleave with no errors).
- Defaults that matter: the size-tiered scheduler compacts once L0 holds `min_compaction_sources = 4` SSTs (`config.rs:1500`); `Settings::l0_max_ssts = 8`, and the docs state that memtables "will not be flushed ... until compaction" when L0 would exceed it (`config.rs`, `Settings::l0_max_ssts`). The control run below shows what that looks like in practice.
- `Admin::submit_compaction(spec)` also exists (`admin.rs:211`); not used.

**(b) A garbage collector outside the writer: possible.** `Admin::run_gc_once(GarbageCollectorOptions)` (`admin.rs:297`), `Admin::run_gc_with_options(token, options)` (`admin.rs:327`) and `GarbageCollectorBuilder` (exported from the crate root). The options give each directory (manifest, WAL, compacted SSTs, compactions) its own `min_age`. `min_age` is what protects objects a concurrent reader or writer might still use: the prototype uses 15 s while tenants are live and 0 only after everything is closed.

**(c) Knowing whether a Db has WAL data not yet in L0: no single flag, but it can be derived exactly.**
- The pieces: `Admin::read_manifest` gives `last_l0_seq()` and `replay_after_wal_id()` (`manifest/mod.rs:895,905`); `WalReader::list(..)` and `WalFile::iterator()` return the WAL rows with their sequence numbers (`wal_reader.rs`, `RowEntry::seq`). WAL rows with `seq > last_l0_seq` are exactly the data not in L0.
- **Counting WAL files newer than `replay_after_wal_id` is wrong.** A probe on a cleanly closed tenant showed `replay_after_wal_id = 11` with a non-empty `wal/12` whose rows are already in L0 (`last_l0_seq = 209`, 0 rows above it): a clean close leaves one redundant non-empty WAL behind. My first version of the oracle used file counting and reported "unclean" for every recovered tenant; the row-based version reports 0.
- The row-based check costs a manifest read, a WAL listing, and one GET per non-empty WAL file after `replay_after_wal_id`, so I did **not** use it on the wake path. The lifecycle uses the **marker** `tenants/{id}/_clean` as specified (one HEAD, run in parallel with the reader open). The row-based check is used only as an independent oracle in the crash test.

## Design as implemented (`src/lifecycle.rs`)

- **Wake** (read-only): `DbReader` in `FollowLatest` mode, `skip_wal_replay = true`, 1 h poll, opened **in parallel** with a HEAD of the marker. Nothing is served before both finish. If the marker is missing the speculative reader is closed and a reader with WAL replay is opened instead (unclean path).
- **Upgrade** (first write): the reader is closed; the marker DELETE runs in parallel with the writer open (V4 settings: in-`Db` GC and compactor off, 1 h manifest poll). The first `put` is only accepted after both finished, so a crash after any acknowledged write leaves the marker missing.
- **Clean close**: `Db::close()` (flushes the memtable to L0), then marker PUT. A tenant that closed with ≥ 4 L0 SSTs is queued for compaction.
- **L0 guard**: a writer that wakes with ≥ 6 L0 SSTs (limit 8) waits for a compaction pass before writing. The control run shows why.
- **Maintenance** (`Maintenance`): a queue-driven pool of 4 compaction passes (one standalone compactor per tenant, 50 ms poll) and a GC loop (`run_gc_once` 20 s after a tenant's last write, `min_age` 15 s). Each pass gets its own request counter. After a pass the compactor's checkpoints are deleted (see finding 5).
- Read-only wakes do not repair an unclean tenant; it stays on the slow path until the next write session cleans it.

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

## Findings

1. **No acknowledged write was lost.** 20 writers were killed with SIGKILL after 3–10 durably acknowledged writes each: 145 acknowledged writes, 0 lost. Every killed tenant took the unclean path (marker missing), and the independent row-level oracle confirmed each had WAL rows not yet in L0. On the first 5 tenants, a reader with `skip_wal_replay` (the fast path, used wrongly) saw **0 of** the acknowledged writes (0 of 6, 10, 6, 10, 9), so the marker is what makes the fast path safe. After the final compaction and GC, all 8,702 writes acknowledged in steps 1–4 were read back through the normal lifecycle: 0 missing.
2. **No write stalled on the L0 limit.** With the compactor and GC loops running, L0 never exceeded 4 after any close in the 20 × 50 round-robin run, and 6 in the one-tenant stress run (50 back-to-back sessions); the slowest upgrade was 173 ms and the slowest close 75 ms. The control run (no compactor, no guard) proves the detector works: with the same code, the 8th session's close blocks for more than 5 s once L0 reaches 8. The stress run is the only one where the guard fired (8 waits); without it that run could have hit the limit, so the L0 guard is part of the design, not optional.
3. **Compactor checkpoints leak storage unless deleted.** My first full run left one 15-minute checkpoint per compaction pass in each manifest (12 on one tenant). Checkpoints pin old manifests, SSTs and WAL files, so even GC with `min_age = 0` could not free them and the 50-session tenants ended at ~1.35 MB. The lifecycle now deletes the checkpoints at the end of each pass (`Admin::list_checkpoints` + `delete_checkpoint`, counted in the per-pass requests), after which no manifest keeps any checkpoint and those tenants are ~533 KB. All numbers above are from the corrected run; the first run's report is not included.
4. **A write session is much more expensive than a read-only wake.** Upgrade (11.1 sequential steps, ~19 requests) and clean close (8.0 steps, ~10 requests, 5 of them PUT-tier) together cost about 5× a read-only wake in request cost ($0.000048 vs $0.0000098). A compaction pass costs more than a write session ($0.00012 vs $0.000055, including checkpoint cleanup), but runs about once per 6 write sessions, so the amortised maintenance cost is $0.000036 per write session.
5. **Writing shrinks a tenant at first.** A freshly loaded tenant carries a ~205 KB duplicate WAL (see results2.md). The first write session after load advances `replay_after_wal_id`, GC then removes the old WAL, and the average falls from 390 KB (tenants never written) to 242 KB (written at least once). Tenants with 50 write sessions each hold ~533 KB.
6. **The unclean path costs about twice a clean wake** (12.2 steps and ~29 requests vs 6.2 steps and ~10), and only the next write session repairs it.

## Caveats

- **Latency is MinIO on localhost** and measured sequentially (one wake at a time, with the maintenance loops running alongside); it is a lower bound for real S3. The 5 ms / 30 ms figures are **projections** from sequential-step counts, not measurements, and ignore transfer time and tail latency.
- **One process owns a tenant at a time.** The marker protocol assumes a single writer per tenant; two concurrent writers on the same tenant would fence each other (slatedb's own writer fencing still protects the data, but wakes could fail or retry). The experiment never opens a tenant twice concurrently.
- **GC `min_age` is a correctness parameter.** Read-only wakes use `FollowLatest`, which gives no GC protection; readers here live for milliseconds, far below the 15 s `min_age`. A reader held open longer than `min_age` after a compaction could hit a deleted SST. The `min_age = 0` GC pass only ran with every tenant closed.
- **Compaction request counts exclude the harness's manifest polling** (it uses an uncounted store handle to know when to stop the compactor); the compactor's own polling and start-up cost are included, as is the checkpoint cleanup.
- Step 3's L0 results depend on the timing of 4 compaction workers on one machine against ~40 ms sessions; the stress run is the evidence for that margin, and the guard is the safety net.
- **Not tested:** concurrent wakes of many tenants (latency under load), tenants larger than ~0.5 MB, long idle periods, GC of the redundant WAL left by a clean close of tenants that never write again, and S3-specific behaviour (conditional puts, request-rate limits).

## Conclusion

The lifecycle is correct and does not stall in these tests. Across 20 SIGKILLed writers and 8,702 acknowledged writes read back after compaction and GC, **no acknowledged write was lost**, and the unclean path was taken for every killed tenant. With the standalone compactor and GC running beside it, no write blocked on the L0 limit (maximum L0 of 4, or 6 under back-to-back stress), while the same code without compaction blocks at L0 = 8. The cost at S3 Standard list prices is **$0.0000098 per read-only wake**, **$0.000055 per write session** (wake, upgrade, close), $0.00012 per compaction pass and **$0.000091 per write session including amortised compaction and GC**; storage stays at about 0.25–0.53 MB per written tenant. Projected sequential latency (assumptions, not measurements) is **43 ms at 5 ms per request and 258 ms at 30 ms for a read-only wake**, and **121 ms and 728 ms for a whole write session** (24.3 steps, of which the upgrade is 11.1 and the close 8.0). On the local MinIO the measured p50/p99 were 12.8/27.6 ms for read-only wakes and 38.0/78.9 ms for write sessions.

## Reproduce

```bash
# Docker + MinIO + bucket as in results.md, then:
cargo build --release
./target/release/tenant-bench --experiment lifecycle --tenants 1000 --out-dir runs3
#   needs --tenants >= 400; takes about 4 minutes at 1,000 tenants
#   spawns itself (--experiment crash-child) for the SIGKILL test
#   --experiment probe-wal --tenant N   prints a tenant's WAL position, marker and unflushed-row count
```
