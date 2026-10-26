# Wakes, concurrency and activity peaks on one server process — results-load

Branch `load-test` (from `slatedb-tenant-bench`, not merged). Everything ran locally: `mongo-server` processes, the load generator and MinIO (Docker) on one machine; no cloud account or paid API.

**Read this first.** The machine is small (4 vCPU) and MinIO, the server and the load generator share it. In every scenario MinIO used more CPU than the server (about 1.6 cores against about 1.1) and its PUT latency (median about 60 ms, tail up to 1 s) is what the clients see in their tails. The latency percentiles below are therefore **not** what real S3 would give; the numbers that transfer to another environment are the request counts, CPU and memory per active tenant, and the correctness results. Two incidents affected the schedule: the host machine rebooted during the first S3 attempt (processes lost, MinIO data intact; S3 was rerun), and S4 ran in the first minutes after the reboot while the disk was still cold (60% I/O wait), so its latencies are much worse than the other scenarios'; its correctness result is unaffected (a slower store makes the ownership race harder to win, not easier).

## Step 0 — Machine

4 vCPU (Intel(R) Xeon(R) Processor @ 2.10GHz), 16,091 MB RAM (14,731 MB free at start), 252 GB disk (26 GB free at start), kernel 6.18.44-fc-v153, open-file limit 20,000. Scenario sizes are CLI parameters (`loadtest/run_load.py --help`). Everything the task asked for fitted on this machine at the full size (50,000 tenants, 5,000-tenant burst, 1,000 active tenants) except two reductions, both stated here: **S3 ran 5 minutes instead of 10**, because of the reboot, and the hot tenant could not reach 500 ops/s (see S3).

## Step 1 — Tenant ownership with a lease object

**Which conditional writes exist.** In `object_store` 0.14.2 (`src/aws/mod.rs`, `S3ConditionalPut`) the S3 implementation supports `PutMode::Create` (sent as `If-None-Match: *`, mapped to `AlreadyExists`) and `PutMode::Update` (sent as `If-Match: <etag>`, mapped to `Precondition`; an ETag is required). `S3ConditionalPut::ETagMatch` is the default and works on MinIO. There is no conditional delete, so only those two modes are used, and a lease is released by a conditional update that expires it.

**Design** (`mongo-server/src/lease.rs`, hooked into `dbs.rs`). `tenants/{id}/_lease` holds `{holder, expiresMs, seq}`.
- Opening a tenant, as reader or writer, first takes the lease: create-if-absent, or replace-if-ETag-matches when the lease is ours, released or expired (expiry plus a 1.5 s clock-skew allowance). Otherwise the request gets error code **134** (`TenantLeaseHeld`) with the `RetryableWriteError` label; 134 is on the drivers' retryable-read list and, unlike the not-primary codes, does not make them clear their connection pools. A refusal is remembered for 500 ms so refused requests do not each cost a GET.
- Reads need the lease too (not only writes): another process's reader would otherwise serve stale data after the owner's writes.
- Open tenants' leases are renewed by a background task, each one a third of a TTL after it was last written (staggered, not in a burst). A process refuses to serve a tenant whose lease it has not renewed recently enough (local deadline, checked on every command), and abandons it (no clean marker) if the renewal finds the lease taken over.
- Clean close writes the `_clean` marker and then releases. After a crash the marker is missing, so whoever takes over (another process after expiry, or the same holder id restarting at once) wakes on the **unclean WAL-replay path**. A database that does not exist leaves no lease object.
- Lease holders are logged (`LEASE acquired|release_begin|writer_open|writer_close|lost|invalid|takeover_wake`) for the S4 checker.

**Functional test** (`loadtest/test_lease.py`, two server processes, 13/13 checks pass): non-owner read and write refused with 134 and the label; the owner keeps reading; after the owner's clean close the other process opens the tenant and sees all writes; reading a non-existent database creates no objects; after SIGKILL of the owner the other process is refused until the lease expires (5.7 s with a 4 s TTL), then takes over on the unclean path and sees all 25 acknowledged writes; a restarted process with the same holder id re-takes its tenants at once (0.5 s) and replays the WAL; 40 concurrent first writes through both processes: exactly one process wins and the lease log shows a single holder. The 1,081 differential tests of results6 passed against the first version of the lease code (pymongo 1,081/1,081); they were not rerun after the later fixes listed below.

**Cost of leases** (estimate from the counters): a wake adds 1 GET and 1 PUT, a close 1 PUT, and every open tenant costs 1 PUT per TTL/3. With 1,500 open tenants and a 60 s TTL that is 75 PUT/s, about $1.35 per hour at S3 prices, 14% of the PUT-tier requests of S1.

## Step 2 — 50,000 tenants

Loaded in parallel (64 at a time, closing each tenant after loading it so the server stays small) with `loadgen load`: **50,000 tenants in 1211 s (41.3 tenants/s)**, 50 documents of about 1 KB each, no failures. Storage per tenant, measured inside the MinIO container over the whole `tenants/` prefix: **179 KB of disk blocks, 101 KB of object bytes, 8.8 objects** (two thirds of the bytes are the WAL object and the compacted SST that hold the same 50 documents until garbage collection; about 50 KB of data). Total 8.8 GB on disk.

## Step 3 — Scenarios

Server settings: idle close after 30 s, lease TTL 60 s (S1 to S3) and 10 s (S4, so takeovers happen within the test), compaction and GC on. S1 and S3 draw from tenants 0 to 19,999, S2 from 30,000 to 34,999, S4 from 20,000 to 29,999. Each active tenant is one sequential task sending one operation per second: 40% find by `_id`, 40% find with a filter (scans the 50 documents), 10% insert, 10% update. A tenant stays active for 60 s and is then replaced by a new one; the first lifetimes are staggered so about 17 tenants wake per second. The load generator uses the official Rust driver (`mongodb` 3.9.1, pinned), 256 connections per server, retries disabled in the driver and done by the generator (every failed attempt is counted by code). Latency is client-perceived, including retries. "Wake" is the latency of a tenant's first request. CPU is percent of one core (400 = the whole machine); requests and cost come from the servers' own counters of every object-store request (lifecycle, leases, compaction, GC, listings) at S3 Standard prices (PUT, COPY, POST, LIST $0.005 per 1,000; GET $0.0004 per 1,000).

### Summary

| Scenario | Ops ok / failed | Ops/s | Latency p50 / p99 / p999 (ms), all ops | Wake (first request) p50 / p99 / p999 (ms) | Errors by code (every failed attempt) | CPU avg / peak (% of one core) | RSS avg / peak (MB) | Max open tenants (readers + writers) | Object-store requests/s (PUT-tier / GET-tier, avg) | Cost at S3 prices ($/h) |
|---|---|---|---|---|---|---|---|---|---|---|
| S1 | 597,417 / 0 | 981 | 12.29 / 1,440.8 / 4,147.2 | 392.96 / 8,503.3 / 8,871.9 | none | 107 / 122 | 383 / 458 | 1,701 | 545 / 1,027 | 11.30 |
| S1b (5 min, rerun beside S3) | 297,404 / 0 | 976 | 22.78 / 1,790.0 / 4,190.2 | 420.35 / 4,567.0 / 4,669.4 | none | 106 / 116 | 392 / 427 | 1,529 | 552 / 1,169 | 11.62 |
| S2 | 216,582 / 0 | 1,204 | 2,535.42 / 7,491.6 / 9,224.2 | 4,575.23 / 9,101.3 / 9,379.8 | timeout ×25 | 85 / 156 | 542 / 972 | 5,000 | 399 / 387 | 7.73 |
| S3 | 295,211 / 0 | 1,001 | 22.61 / 2,127.9 / 5,083.1 | 491.01 / 5,943.3 / 6,725.6 | none | 107 / 122 | 406 / 483 | 1,964 | 522 / 1,108 | 10.99 |
| S4 | 51,358 / 0 | 164 | 2,066.43 / 50,757.6 / 87,621.6 | 10,600.45 / 77,004.8 / 101,318.7 | cmd:134 ×50,337, timeout ×4,622, pool_cleared ×2,676, io ×2,331 | 35 / 103 | 153 / 312 | 1,365 | 243 / 548 | 5.16 |

S1: 1,000 active tenants for 10 min. S1b: the same for 5 min, rerun beside S3 so S3 has a like-for-like baseline. S2: 5,000 cold tenants, first request spread over 60 s, then 1 op/s for 120 s. S3: S1 plus one hot tenant, 5 min. S4: two server processes, 10 SIGKILLs. S4's CPU and memory are summed over both processes and include the downtime of the killed one.

### S1 by operation type (10 minutes, 1,000 active tenants)

| S1 operation | count | p50 (ms) | p99 (ms) | p999 (ms) | max (ms) |
|---|---|---|---|---|---|
| find_by_id (all, incl. wakes) | 240,749 | 7.45 | 1,025.5 | 4,098.0 | 8,962 |
| find_filter (all, incl. wakes) | 239,531 | 7.79 | 976.9 | 4,059.1 | 8,905 |
| insert (all, incl. wakes) | 58,737 | 95.23 | 2,277.4 | 4,333.6 | 9,085 |
| update (all, incl. wakes) | 58,400 | 98.30 | 2,203.6 | 4,382.7 | 9,134 |
| find_by_id (already open) | 234,972 | 7.01 | 548.9 | 2,055.2 | 7,303 |
| find_filter (already open) | 233,692 | 7.43 | 535.0 | 2,049.0 | 7,303 |
| insert (already open) | 58,737 | 95.23 | 2,277.4 | 4,333.6 | 9,085 |
| update (already open) | 58,400 | 98.30 | 2,203.6 | 4,382.7 | 9,134 |
| first request = find_by_id (wake) | 5,777 | 414.98 | 8,503.3 | 8,871.9 | 8,962 |
| first request = find_filter (wake) | 5,839 | 370.69 | 8,495.1 | 8,871.9 | 8,905 |

Reads of an open tenant are fast (median 7 ms); writes wait for a durable WAL PUT on a loaded MinIO (median 95 ms); the p99 and p999 of everything follow MinIO's PUT/GET tail (server-side, 17,540 commands took over 500 ms in the first S1 run; the object-store call latency histograms show PUT median about 60 ms and GET about 10 ms under this load).

### S2 by operation type (5,000 cold tenants within 60 s)

| S2 operation | count | p50 (ms) | p99 (ms) | p999 (ms) | max (ms) |
|---|---|---|---|---|---|
| find_by_id (all, incl. wakes) | 87,120 | 2,498.56 | 6,942.7 | 8,855.6 | 9,413 |
| find_filter (all, incl. wakes) | 87,305 | 2,480.13 | 6,922.2 | 8,839.2 | 9,454 |
| insert (all, incl. wakes) | 21,083 | 2,740.22 | 8,339.5 | 9,764.9 | 17,121 |
| update (all, incl. wakes) | 21,074 | 2,729.98 | 8,286.2 | 9,830.4 | 17,285 |
| find_by_id (already open) | 84,610 | 2,488.32 | 6,647.8 | 6,922.2 | 7,336 |
| find_filter (already open) | 84,815 | 2,471.93 | 6,651.9 | 6,885.4 | 7,139 |
| insert (already open) | 21,083 | 2,740.22 | 8,339.5 | 9,764.9 | 17,121 |
| update (already open) | 21,074 | 2,729.98 | 8,286.2 | 9,830.4 | 17,285 |
| first request = find_by_id (wake) | 2,510 | 4,603.90 | 9,084.9 | 9,347.1 | 9,413 |
| first request = find_filter (wake) | 2,490 | 4,550.65 | 9,109.5 | 9,388.0 | 9,454 |

The burst is 83 first requests per second, each needing a lease GET and PUT, the manifest, the clean-marker HEAD and several block reads. MinIO saturated (peak 5,000 open tenants, 972 MB resident); first requests took a median 4.6 s. 25 attempts timed out at the generator's 10 s limit and succeeded on retry; no operation failed.

### S3 — noisy neighbour

| Latency of the ordinary S1 tenants (requests to already-open tenants), ms | p50 | p99 | p999 | ops |
|---|---|---|---|---|
| S1, 10 min (alone) | 11.8 | 1186 | 3009 | 585,801 |
| S1b, 5 min (alone, run right after S3) | 21.6 | 1319 | 3047 | 290,768 |
| S3, 5 min (same tenants plus the hot tenant) | 21.5 | 1422 | 3650 | 288,604 |
| &nbsp;&nbsp;S1b find_by_id | 15.0 | 1201 | 2609 | 116,409 |
| &nbsp;&nbsp;S1b insert | 88.2 | 2646 | 4252 | 28,812 |
| &nbsp;&nbsp;S3 find_by_id | 14.8 | 1356 | 3469 | 115,560 |
| &nbsp;&nbsp;S3 insert | 90.1 | 2468 | 4891 | 28,942 |

The hot tenant was asked for 500 ops/s (16 workers) and achieved **34 ops/s** (p50 407 ms, p99 1766 ms): commands to one tenant run one at a time and each write waits for a durable WAL flush, so one tenant is capped by its own latency, not by the server. It therefore consumed little (CPU 107% against 107% in S1), and the ordinary tenants were barely affected: median unchanged, p99 and p999 higher by about 8% and 20% against S1b (table above; run-to-run noise on this machine is about 10%, so only the p999 difference is plausibly real). The reason is that the hot tenant could not generate more load than one tenant's serialized commands allow; a tenant with many concurrent connections and cheap commands would stress the shared MinIO more, which was not measured.

### S4 — two processes, random routing, 10 SIGKILLs

Two server processes (holders `S0` and `S1`, ports 28001 and 28002, one bucket), 1,000 active tenants for 5 minutes, every request sent to a randomly chosen process, 10 SIGKILLs of a random process at about 27 s intervals, each restarted after 0.5 to 2 s with the same holder id. A request refused with 134 (the other process owns the tenant) is retried on a random process, so about half of all attempts are refusals by design: **50,337 refusals**, 2,331 connection errors, 2,676 pool-cleared and 4,622 timeouts around the kills; **0 operations failed after retries** (59,966 retries). A production router would send a tenant to one process; random routing is the stress case.

**Correctness.**
- **No acknowledged write lost.** After the run, every tenant that received an acknowledged write was read back (3,694 tenants): **4,534 acknowledged inserts, 0 missing; 2,675 updated documents, 0 older than the last acknowledged value; 0 unreadable tenants.** No read of a loaded document missed (0 read misses).
- **No tenant open as a writer in both processes at once.** From the lease logs of both processes and the kill times (`loadtest/check_leases.py`): 4,340 writer intervals, **0 overlaps between different holders**, at most 1 concurrent writer per tenant.
- **Lease records did overlap in the log 466 times**, and this should be read carefully rather than as a pass. In each case the earlier holder was stalled for longer than the 10 s TTL (the cold disk), so its lease expired and the other process took it over legitimately; the earlier holder noticed only later (a failed renewal, 'lost', 333 cases; a kill, 110; the next command's validity check, 17; its own late release, 6) and the log line is written at that moment. Median apparent overlap 1.3 s, maximum 10.5 s. What prevented harm was the local validity deadline (a process stops serving a tenant before its lease can expire) and slatedb's own writer fencing (a second writer opened by the new owner fences the first); it held in this run, but a process that stalls inside a command for longer than the TTL is a window the lease alone does not close, and fencing is what covers it. A longer TTL narrows it at the price of slower takeover.
- **Crash path.** 1,726 expired leases were taken over by the other process, 4,880 tenants were woken after a takeover or crash, **4,087 of them on the unclean (WAL replay) path**; the others had been closed cleanly (marker present) before the lease changed hands.

## Step 4 — Resource estimate per active tenant (an estimate)

Sizes below are for **the machine used: 4 vCPU, 16 GB RAM, with MinIO and the load generator on the same host**. Derived from S1 (1,000 active tenants at 1 op/s, about 1,500 open because idle tenants stay open 30 s, 93% of them writers) and S2.

| Per active tenant (1 op/s, 80% reads) | Estimate | Basis |
|---|---|---|
| Memory | about 0.26 MB per open tenant, about 0.4 MB per active tenant (1.5 open per active), plus about 10 MB base | S1: 383 MB average resident for 1,490 open; regression 0.30 MB per open tenant; S2: 972 MB peak for 5,000 open (0.19 MB, mostly readers) |
| CPU | about 0.11% of one core (1.07 cores per 1,000 active tenants at about 1,000 ops/s, so about 1.1 ms of server CPU per operation, including wakes, closes, lease renewals and GC) | S1 |
| Object-store requests | about 0.55 PUT-tier and 1.03 GET-tier per second, about **$0.011 per active tenant per hour** ($11.3 per 1,000 active tenants per hour); leases are 0.075 PUT/s of that at a 60 s TTL | S1 counters |
| Disk | 179 KB per tenant at rest (50 KB of data) | Step 2 |

**How many active tenants one such server holds (estimate).**
- CPU limit: with 70% of 4 cores usable, about **2,600 active tenants at 1 op/s each** (about 2.6 times what was run).
- Memory limit: with 12 GB of 16 GB usable, about 30,000 active tenants, so CPU binds first at 1 op/s per tenant. For tenants that are open but nearly idle (0.05 ops/s) CPU stops binding and memory does: about 12 GB / 0.26 MB, roughly **45,000 open tenants**, an estimate that includes no allowance for caches growing with data size.
- What was measured, not estimated: on this machine 1,000 active tenants (about 1,500 open, 550 PUT/s, 1,000 GET/s) already saturated MinIO (about 1.7 cores, I/O wait up to 20%), so the store, not the server, was the limit here; against real S3 the same request rates would cost money, not capacity. 5,000 tenants waking within 60 s did not fit: first-request median 4.6 s.
- Extrapolation beyond 1,000 active tenants and the 2,600 figure are not measured.

## Defects and findings, fixed or noted

1. **Idle close was sequential** (fixed). One reaper closed tenants one at a time; at about 17 closes per second it fell behind and open tenants grew to 3,900 (750 MB) instead of about 1,500. Closes now run 64 at a time, as does the shutdown close.
2. **Garbage collection was sequential** (fixed, `GC_CONCURRENCY` 16 in `lifecycle.rs`). The backlog grew without bound and a graceful shutdown with 2,000 tenants took minutes.
3. **Lease release raced with renewal** (fixed). A renewal in flight could change the lease's ETag so the release failed (tenant locked for a TTL), or a stale renewal could drop a freshly re-acquired lease and make the server abandon a tenant it had just opened. Release now re-reads and retries while the lease is still ours; a failed renewal checks who holds the lease before declaring it lost.
4. **Renewals arrived in bursts** (fixed): each lease is renewed on its own schedule now. It did not change latency here, because the store was the bottleneck.
5. **Reads of a non-existent database would have left a lease object** (fixed): the lease is discarded.
6. **Hot tenant capped at about 34 ops/s** (noted, not changed): commands to one tenant are serialized and each write waits for a durable flush. Whether that is acceptable depends on the workload; batching concurrent writes to one tenant into one WAL flush would raise it.
7. **Lease expiry under stalls** (noted): see S4. Fencing covers the window; the TTL is a parameter (`--lease-ttl-secs`).
8. **Harness defects**: `cargo build --release` in a workspace root builds only the root package, so some runs used a stale server binary (found by missing counters; those runs were discarded and repeated); activation ids restarted at 0 on every run and collided with documents from earlier runs (false duplicate-key failures; ids are now time-based); the first S3 attempt was lost to the host reboot (results of finished scenarios are now copied to `loadtest/results/` and committed after each).

## Reproduce

```
cargo build --release --workspace                         # note --workspace: builds mongo-server and loadgen too
python3 loadtest/run_load.py machine
python3 loadtest/run_load.py load                         # 50,000 tenants
python3 loadtest/run_load.py s1 --lease-ttl 60            # --s1-secs, --s1-active ... are parameters
python3 loadtest/run_load.py s2 --lease-ttl 60
python3 loadtest/run_load.py s3 --lease-ttl 60 --s3-secs 300
python3 loadtest/run_load.py s4                           # needs two free ports, 28001 and 28002
python3 loadtest/analyze.py loadtest/out/<scenario>       # CPU, memory, requests, cost
python3 loadtest/check_leases.py loadtest/out/s4          # ownership check from the lease logs
mongo-tests/.venv/bin/python loadtest/test_lease.py       # Step 1 functional checks
python3 loadtest/make_report.py                           # this file's tables, from loadtest/results/
```

Raw results (summaries, analyses, samples every 5 s, kill events) are in `loadtest/results/`; server logs stay in `loadtest/out/`.
