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

<!-- round2:start -->
# Second round: request cost, leases, failover, wakes, hot tenant

Same rules as the first round: every number below is computed from a results file in `loadtest/results/` (`t1` request cost, `t2` ownership, `t3` S4 redo, `t4` wakes, `t5` hot tenant); what could not be measured is listed at the end. Same machine (4 vCPU, 16 GB, MinIO and the load generator on the same host); latencies are a lower bound for MinIO-class stores and are dominated by MinIO under load. The host rebooted again between the two rounds, so MinIO's disk was warmed before the S4 redo and the wake experiments (the objects of the tenants used were read once).

**Prices used.** S3 Standard, US East (N. Virginia), from AWS's price list file `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json` (publication date 2026-10-15T17:35:40Z, fetched 2026-10-26): PUT, COPY, POST and LIST $0.005 per 1,000 requests; GET and all other requests $0.004 per 10,000. These are the figures the AWS pricing page https://aws.amazon.com/s3/pricing/ shows for S3 Standard; I read the machine-readable price list, not the web page (a JavaScript page). DELETE is counted as free (S3 does not charge for DELETE requests; that is not in the price file I read, so it is the one price assumption). HEAD is counted with GET.

## Task 1 — Request cost per tenant

Method: a fresh server process per cell (MinIO, lease TTL 60 s, tenants never idle-closed), 200 tenants opened, then the server's own request counters over a 60 s window (`tenantStats`, requests attributed by object path: WAL, manifest, compacted SST, lease, clean marker; compaction and GC totals reported separately). "Idle open reader/writer" = opened by a short burst, then no operations. Rates per tenant = counter delta / window / 200. The loads for 1 op/s and 0.1 op/s are 80% reads, 10% inserts, 10% updates per tenant. In an open tenant nothing else touches the store, so the window shows the cost of *staying open*; the cost of waking and closing a tenant is a per-session cost, given separately below.

| Flush setting | Scenario | PUT/s per tenant: WAL | manifest | compacted SST | lease | clean marker | other | GET-tier/s per tenant | LIST/s | PUTs per tenant-hour | GETs per tenant-hour | $ per tenant-hour |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline: explicit flush per write (100 ms periodic) | idle open readers | 0.0000 | 0.0000 | 0.0000 | 0.0481 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 173.1 | 0.0 | $0.00087 |
| baseline: explicit flush per write (100 ms periodic) | idle open writers | 0.0000 | 0.0000 | 0.0000 | 0.0483 | 0.0000 | 0.0167 | 0.3500 | 0.1167 | 654.0 | 1,260.0 | $0.00377 |
| baseline: explicit flush per write (100 ms periodic) | 1.0 op/s per tenant (80% reads) | 0.2005 | 0.0000 | 0.0000 | 0.0545 | 0.0000 | 0.0126 | 0.1483 | 0.0425 | 1,116.4 | 533.8 | $0.00580 |
| baseline: explicit flush per write (100 ms periodic) | 0.1 op/s per tenant (80% reads) | 0.0266 | 0.0059 | 0.0000 | 0.0545 | 0.0000 | 0.0136 | 0.4715 | 0.1014 | 727.5 | 1,697.2 | $0.00432 |
| durable ack, flush every 10 ms | idle open readers | 0.0000 | 0.0000 | 0.0000 | 0.0487 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 175.2 | 0.0 | $0.00088 |
| durable ack, flush every 10 ms | idle open writers | 0.0000 | 0.0000 | 0.0000 | 0.0487 | 0.0000 | 0.0167 | 0.3833 | 0.1170 | 656.4 | 1,380.0 | $0.00383 |
| durable ack, flush every 10 ms | 1.0 op/s per tenant (80% reads) | 0.2025 | 0.0000 | 0.0000 | 0.0545 | 0.0000 | 0.0069 | 0.1568 | 0.0477 | 1,121.9 | 564.5 | $0.00584 |
| durable ack, flush every 10 ms | 0.1 op/s per tenant (80% reads) | 0.0306 | 0.0071 | 0.0000 | 0.0545 | 0.0000 | 0.0249 | 0.5121 | 0.1032 | 793.3 | 1,843.5 | $0.00470 |
| durable ack, flush every 100 ms | idle open readers | 0.0000 | 0.0000 | 0.0000 | 0.0481 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 173.1 | 0.0 | $0.00087 |
| durable ack, flush every 100 ms | idle open writers | 0.0000 | 0.0000 | 0.0000 | 0.0482 | 0.0000 | 0.0227 | 0.3833 | 0.1167 | 675.6 | 1,380.0 | $0.00393 |
| durable ack, flush every 100 ms | 1.0 op/s per tenant (80% reads) | 0.1976 | 0.0000 | 0.0000 | 0.0545 | 0.0000 | 0.0066 | 0.1505 | 0.0458 | 1,096.7 | 542.0 | $0.00570 |
| durable ack, flush every 100 ms | 0.1 op/s per tenant (80% reads) | 0.0265 | 0.0059 | 0.0000 | 0.0545 | 0.0000 | 0.0134 | 0.5196 | 0.0995 | 719.3 | 1,870.7 | $0.00434 |
| durable ack, flush every 1000 ms | idle open readers | 0.0000 | 0.0000 | 0.0000 | 0.0480 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 172.8 | 0.0 | $0.00086 |
| durable ack, flush every 1000 ms | idle open writers | 0.0000 | 0.0000 | 0.0000 | 0.0480 | 0.0000 | 0.0274 | 0.3833 | 0.1167 | 691.5 | 1,380.0 | $0.00401 |
| durable ack, flush every 1000 ms | 1.0 op/s per tenant (80% reads) | 0.2020 | 0.0000 | 0.0000 | 0.0545 | 0.0000 | 0.0060 | 0.1025 | 0.0312 | 1,057.4 | 368.8 | $0.00543 |
| durable ack, flush every 1000 ms | 0.1 op/s per tenant (80% reads) | 0.0282 | 0.0072 | 0.0000 | 0.0545 | 0.0000 | 0.0135 | 0.5481 | 0.0988 | 727.9 | 1,973.1 | $0.00443 |

Standalone compaction and GC passes inside the windows: present in c0_idle_writers, c0_rate1, c0_rate01, c1_idle_writers, c1_rate1, c1_rate01, c2_idle_writers, c2_rate1, c2_rate01, c3_idle_writers, c3_rate1, c3_rate01 (see `cost.json`, field `maintenance`).

Acknowledgement latency of writes on already-open tenants in the same cells:

| Flush setting | Scenario | update p50 / p99 (ms) | insert p50 / p99 (ms) | find_by_id p50 (ms) |
|---|---|---|---|---|
| baseline: explicit flush per write (100 ms periodic) | 1.0 op/s per tenant (80% reads) | 5.1 / 38.2 | 4.6 / 33.9 | 0.7 |
| baseline: explicit flush per write (100 ms periodic) | 0.1 op/s per tenant (80% reads) | 11.5 / 53.1 | 18.6 / 97.3 | 0.7 |
| durable ack, flush every 10 ms | 1.0 op/s per tenant (80% reads) | 11.8 / 37.4 | 11.1 / 33.8 | 0.8 |
| durable ack, flush every 10 ms | 0.1 op/s per tenant (80% reads) | 21.0 / 125.4 | 17.1 / 87.9 | 0.7 |
| durable ack, flush every 100 ms | 1.0 op/s per tenant (80% reads) | 59.6 / 128.4 | 56.8 / 127.2 | 0.8 |
| durable ack, flush every 100 ms | 0.1 op/s per tenant (80% reads) | 76.0 / 156.8 | 76.2 / 141.7 | 0.8 |
| durable ack, flush every 1000 ms | 1.0 op/s per tenant (80% reads) | 73.2 / 1030.1 | 71.2 / 1028.1 | 0.6 |
| durable ack, flush every 1000 ms | 0.1 op/s per tenant (80% reads) | 86.3 / 1066.0 | 98.7 / 1090.6 | 0.7 |

Write-heavy single tenant (8 connections writing back to back, 20 s): the flush interval matters only when several writes arrive within one interval.

| Flush setting | writes/s | write p50 / p99 (ms) | WAL PUTs/s | writes per WAL object |
|---|---|---|---|---|
| baseline: explicit flush per write (100 ms periodic) | 202 | 38.9 / 66.6 | 201.7 | 1.00 |
| durable ack, flush every 10 ms | 87 | 89.5 / 173.1 | 86.6 | 1.00 |
| durable ack, flush every 100 ms | 10 | 810.5 / 835.1 | 9.9 | 1.00 |
| durable ack, flush every 1000 ms | 1 | 8011.8 / 8052.7 | 1.1 | 0.97 |

**Durability window.** One tenant, writes back to back, SIGKILL at a random moment 2.5 to 4 s in, restart with the same holder id, read back (5 trials per setting). "Durable ack" settings acknowledge a write only after its WAL object is in the store: an acknowledged write waits for durability *before* the acknowledgement (up to one flush interval), and nothing acknowledged can be lost. "Ack before durable" settings acknowledge at once and the write becomes durable at the next flush; the oldest lost acknowledged write bounds the window.

| Setting | acked | lost | oldest lost write: ack to kill (ms), worst trial | ack-to-durable bound by design |
|---|---|---|---|---|
| baseline durable ack (explicit flush) | 4,133 | 0 | 0 | 0 (durable before ack); ack waits up to the interval |
| durable ack, flush 1000 ms | 18 | 0 | 0 | 0 (durable before ack); ack waits up to the interval |
| ack before durable, flush 100 ms | 74,200 | 1,459 | 79 | up to one flush interval plus one WAL PUT |
| ack before durable, flush 1000 ms | 75,433 | 17,431 | 951 | up to one flush interval plus one WAL PUT |

**Wake and close cost per session** is in Task 4 (requests per wake, from the trace); a close costs the memtable SST, the manifest update, the clean marker and the lease release, counted in the first-round S1 as the difference between its request rate and the steady-state rates above.

## Task 2 — Lease cost and safety

**Lease cost today (measured).** An idle open tenant costs 0.0481 lease PUTs per second = 173.1 per hour (TTL 60 s, one renewal every 20 s), $0.00087 per tenant-hour, and that is the *only* store request an idle open reader makes (all other categories in the idle rows of Task 1 are above). At 1,500 open tenants that is 72 PUT/s.

**Per-process lease (estimate, not implemented).** One lease object per process, renewed every TTL/3 (1 PUT per 20 s per process, independent of the number of tenants), plus a per-database owner record written once when a tenant is opened and once when it is released, replaces the per-tenant renewals: the renewal cost per open tenant falls from 173.1 PUTs per hour to 0.002 per hour at 1,500 open tenants per process (3 PUTs/min per process / 1,500, an arithmetic estimate). The acquire/release writes per session stay (2 PUTs), and an acquire gains one GET (the owner record), plus a second GET of the holder's process lease when the owner record names another process. At S1's rates the saving is the whole lease-renewal share of the PUT-tier requests (first round: 75 of 545 PUT/s at 1,500 open tenants, about 14%, $1.35 per hour per 1,000 active tenants).

**Why it was not implemented.** Cheap to build, but not cheap to make safe. With one process lease, a takeover is a conditional update of the *owner record*, while the evidence that the old owner is dead lives in a *different* object (its process lease). Object stores give conditional writes per object, not across two, so a takeover that read an expired process lease can race with that process renewing it (check-then-act), and both would think they own the tenant. The per-tenant lease avoids it because renewal and takeover contend on the same object's ETag. Closing it needs a fencing epoch in the owner record that the writer carries into slatedb's manifest (slatedb already fences writers by manifest epoch, which is what saved S4's write path), and a read path that re-validates the owner record, which costs a GET per read or a bounded staleness window. I did not build or test that, so the saving above is an estimate and the safety of the design is unproven.

**Ownership check on reads (implemented and tested).** Reads already went through `ensure_open`, which refuses a tenant whose lease this process has not renewed in time. This round adds the check *after* the read (`ReadView`): a read that began while the process still owned the tenant but finishes after ownership was lost returns a retryable error instead of data (`Leases::check_owner`).

**SIGSTOP / SIGCONT test** (`loadtest/test_stop.py`): process A owns a tenant (value 1); 40 reads are sent to A while it is frozen; A is stopped for longer than the lease TTL (4 s); B takes the tenant over and writes value 2 (acknowledged); A is continued; 40 more reads are sent to A immediately. Every read must return 2 or a retryable error. 5 rounds per row.

| A's settings | Reads queued at the frozen process | Reads right after SIGCONT | Returned new value | Refused (134) | Other errors | **Stale reads (old value)** |
|---|---|---|---|---|---|---|
| ownership checks on, serialized reads | 200 | 200 | 0 | 400 | 0 | **0** |
| ownership checks on, shared reads | 200 | 200 | 0 | 400 | 0 | **0** |
| ownership checks OFF (negative control), serialized reads | 200 | 200 | 0 | 0 | 0 | **400** |

The control row shows the test can see a stale read; the rows with the checks on show none. What this does not cover: a read already executing inside the process when it is frozen is not reproducible from outside (the test freezes the process while 40 reads are queued in its socket, not mid-command), so the post-read check is verified by construction and by the control, not by catching a read in flight; and a process frozen *inside* a write for longer than the TTL is covered only by slatedb's writer fencing.

## Task 3 — S4 redone on a warmed disk, with a read-your-writes check

Same scenario as S4 of the first round: two server processes on one bucket, 1,000 active tenants for 5 minutes, every request to a random process, 10 SIGKILLs of a random process each followed by an automatic restart (same holder id), TTL 10 s, retries by the generator. New: the disk was warmed first (objects of the 10,000 tenants read once; I/O wait was normal), the generator keeps a log of every operation, and after every acknowledged insert or update it reads the document back **once, through the other process**: the read must see the write or fail, never return an older value or a missing document.

| Metric | First S4 (cold disk, after reboot) | S4 redo (warmed disk) |
|---|---|---|
| Operations ok / failed after retries | 51,358 / 0 | 111,287 / 0 |
| Ops/s | 164 | 364 |
| Latency p50 / p99 / p999 (ms) | 2,066.43 / 50,757.6 / 87,621.6 | 991.23 / 15,163.4 / 23,773.2 |
| Wake p50 / p99 / p999 (ms) | 10,600.45 / 77,004.8 / 101,318.7 | 4,358.14 / 19,464.2 / 25,952.3 |
| Errors by code (failed attempts) | cmd:134 ×50,337, timeout ×4,622, pool_cleared ×2,676, io ×2,331 | cmd:134 ×107,219, io ×2,141, pool_cleared ×1,299, timeout ×223 |
| CPU avg / peak (% of one core, both processes) | 35 / 103 | 71 / 120 |
| RSS avg / peak (MB, summed) | 153 / 312 | 283 / 371 |
| Max open tenants (both processes) | 1365 | 1565 |
| Object-store requests/s PUT-tier / GET-tier (avg, both) | 243 / 548 | 405 / 1409 |
| Cost at S3 prices ($/h) | 5.16 | 9.32 |

**Correctness.**
- Acknowledged writes lost: 10,606 acknowledged inserts, **0 missing**; 4,643 updated documents, **0 older than the last acknowledged value**; 0 unreadable tenants out of 5,368 read back; 0 reads of loaded documents found nothing.
- **Read-your-writes through the other process: 21,174 checks; 10 saw the write; 20,914 were refused with the retryable 134 (the other process did not own the tenant); 250 failed otherwise ({'io': 80, 'pool_cleared': 170}); STALE READS: 0.**
- Writer intervals 8,525, overlaps between different holders: **0**; lease-record overlaps in the log: 52 ({'lost': 50, 'kill': 1, 'invalid': 1}; median apparent overlap 0.7 s, max 3.7 s, as in the first round the earlier holder had stalled past the TTL and noticed late); takeovers of expired leases 1,370; wakes on the unclean WAL-replay path 10,173.

**Failover time** (`analyze_failover.py`): for each of the 10 kills, the tenants whose lease the killed process held at that moment (6,848 tenant-kill pairs; 2165 had no further operation before their activation ended and are excluded), time from the kill to the first successful operation on the same tenant (through either process): **p50 7.7 s, p90 13.2 s, p99 21.5 s, max 44.3 s** (n = 4,683). Each tenant sends one operation per second, so this includes up to a second of waiting for the tenant's next operation; the lease TTL (10 s) plus the 1.5 s skew allowance is the floor for a tenant that the *other* process must take over, while a tenant whose killed process restarts quickly with the same holder id is served again as soon as it is back (0.5 to 2 s). Duration of the first successful operation itself (including its retries), measured from the later of its start and the kill: p50 7.6 s, p99 21.2 s.

## Task 4 — Wake breakdown and wake concurrency

**Cold wake of one tenant, by step.** `tenantTrace` closes the tenant, then runs one `find` by `_id` with an object-store trace on (every request, in order, with start and duration), on a quiet server; 40 tenants per variant, the first 5 discarded (connection and disk warm-up), medians over the rest. "Clean" = the tenant was closed cleanly (marker present); "unclean" = marker missing, which sends the wake through WAL replay. Start/end are milliseconds from the start of the command; request durations of a step overlap each other and other steps (the steps run partly in parallel), so they are not additive.

*clean (marker present)*: total 14.4 ms p50 / 17.8 ms p99 / 19.4 ms max over 35 wakes; **12 object-store requests per wake (median).**

| Step | Requests (p50) | Request types (p50 counts) | Sum of request durations (ms) | Starts at (ms) | Ends at (ms) |
|---|---|---|---|---|---|
| lease acquire (GET + conditional PUT) | 2 | GET ×1, PUT(update) ×1 | 5.1 | 0.0 | 5.2 |
| manifest (list + read) | 7 | LIST ×1, GET ×6 | 7.1 | 5.5 | 14.3 |
| clean-marker HEAD | 1 | HEAD ×1 | 0.8 | 5.6 | 6.3 |
| first read (SST index, filter, blocks) | 2 | GET_RANGE ×2 | 2.6 | 11.5 | 14.3 |

*unclean (marker missing, WAL replay)*: total 19.8 ms p50 / 27.8 ms p99 / 30.3 ms max over 35 wakes; **29 object-store requests per wake (median).**

| Step | Requests (p50) | Request types (p50 counts) | Sum of request durations (ms) | Starts at (ms) | Ends at (ms) |
|---|---|---|---|---|---|
| lease acquire (GET + conditional PUT) | 2 | GET ×1, PUT(update) ×1 | 4.7 | 0.0 | 4.8 |
| manifest (list + read) | 12 | LIST ×2, GET ×10 | 11.0 | 5.2 | 19.6 |
| clean-marker HEAD | 1 | HEAD ×1 | 0.8 | 5.3 | 6.0 |
| WAL replay (reads) | 12 | HEAD ×12 | 10.5 | 15.0 | 19.3 |
| first read (SST index, filter, blocks) | 2 | GET_RANGE ×2 | 2.7 | 16.9 | 19.7 |

**Concurrent wakes before p99 degrades.** Waves of K first requests sent at the same instant to K distinct cold tenants (objects pre-read so the disk is warm; a fresh server per wave; 2 loadgen threads), first-request latency:

| Simultaneous cold wakes K | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | Errors |
|---|---|---|---|---|---|
| 4 | 26 | 28 | 28 | 28 | none |
| 8 | 42 | 48 | 48 | 48 | none |
| 16 | 129 | 135 | 136 | 136 | none |
| 32 | 156 | 187 | 187 | 187 | none |
| 64 | 315 | 347 | 351 | 351 | none |
| 128 | 537 | 655 | 666 | 668 | none |
| 256 | 994 | 1270 | 1312 | 1314 | none |
| 512 | 1890 | 2329 | 2415 | 2421 | none |

**Concurrency limit with a queue.** `--max-concurrent-wakes N` makes cold wakes (lease acquire plus open) wait for a permit instead of all starting at once; a waiting request is not subject to any timeout of the server (the client's own 10 s limit applies). The limit was derived from the knee by the rule in `exp_wake.py` (largest K whose p99 stays within 2x of the smallest wave's and under 1 s). Test: the S2 burst again (5,000 cold tenants, first requests spread over 60 s, then 60 s at 1 op/s each), warmed disk, without and with the limit:

| | Wake p50 / p99 / p999 (ms) | All ops p50 / p99 (ms) | Timeouts (10 s) | Failed ops | Ops | Max queued wakes |
|---|---|---|---|---|---|---|
| unlimited | 27.21 / 1,551.4 / 1,737.7 | 2 / 889 | 0 | 0 | 300,000 | 0 |
| limit 128 | 25.18 / 250.4 / 479.0 | 2 / 48 | 0 | 0 | 300,000 | 1 |

**Overload: the same 5,000 tenants in 10 s instead of 60 s** (about 500 first requests per second, more than the server completes), same warmed disk, 30 s hold at 1 op/s, client timeout 10 s. Limits tried: 32 (throughput plateau of the knee table) and 128 (the p99 rule's choice). The unlimited configuration was run twice to show the run-to-run spread.

| | Wake p50 / p99 / max (ms) | Timeouts (10 s) of 5,000 first requests | Max queued wakes | Mean wait in queue (ms) |
|---|---|---|---|---|
| unlimited | 7545 / 14467 / 14803 | 1018 | 0 | 0 |
| limit 32 | 5001 / 13509 / 14230 | 291 | 568 | 2036 |
| limit 128 | 5710 / 11739 / 11870 | 203 | 461 | 1433 |
| unlimited (repeat, to see run-to-run spread) | 6713 / 12640 / 12911 | 795 | 0 | 0 |

Reading: the two unlimited runs timed out 795 and 1018 first requests, so the run-to-run spread is about 223; limits 32 and 128 timed out fewer, but this is one run per limit and the difference between 32 and 128 is inside that spread. A limit with a queue lowers the timeouts and the wake p50; it does not remove them: when arrivals exceed the server's wake rate for longer than the client's timeout, requests waiting in the queue time out at the client. The queue only turns an unbounded pile of simultaneous wakes into an ordered one; capacity (or load shedding with a retryable error) is still needed above the sustained wake rate.

## Task 5 — The hot tenant

**Why commands on one tenant were serialized.** Every command took an exclusive lock on the tenant's entry (`Dbs::lock`, one `Mutex` per database). It was introduced for the write commands, which read the collection, decide (unique-index checks, `_id` duplicates, update matching) and write a batch: two interleaved writers could both pass a duplicate check, so writes must be serialized per tenant. Reads were serialized only because they went through the same lock. With a durable write costing one WAL PUT, the first round's hot tenant (16 workers, MinIO busy with 1,000 other tenants) was capped at about 35 ops/s by write latency, not by the server's CPU.

Single-tenant throughput, closed loop (each connection sends its next operation when the previous one returns), 15 s per cell after 3 s warm-up, one tenant of 50 documents; reads are `find_one` by `_id`, writes are `update_one` `$set` on one of 50 documents. "Serialized" is the server before this round (`--serialize-reads`), "shared" is after the change below. The local-directory and in-memory stores run without leases (`--no-leases`; the local store has no conditional update).

| Store | Reads | Connections | Reads only (ops/s, p50 / p99 ms) | Writes only | 90/10 mix |
|---|---|---|---|---|---|
| MinIO | serialized | 1 | 4,018 (0.2 / 0.6) | 122 (7.8 / 14.0) | 752 (0.4 / 11.9) |
| MinIO | serialized | 8 | 13,493 (0.5 / 1.8) | 125 (61.4 / 95.6) | 958 (8.6 / 33.6) |
| MinIO | serialized | 64 | 17,442 (3.5 / 8.1) | 136 (450.0 / 588.8) | 991 (62.6 / 135.2) |
| MinIO | shared | 1 | 4,040 (0.2 / 0.6) | 120 (8.0 / 14.2) | 680 (0.4 / 15.7) |
| MinIO | shared | 8 | 21,124 (0.3 / 1.4) | 126 (61.2 / 114.8) | 1,015 (8.3 / 32.7) |
| MinIO | shared | 64 | 27,370 (2.2 / 5.8) | 130 (473.6 / 594.4) | 1,051 (59.5 / 124.1) |
| local directory | serialized | 1 | 6,750 (0.1 / 0.4) | 427 (2.3 / 5.0) | 2,007 (0.3 / 2.7) |
| local directory | serialized | 8 | 14,696 (0.5 / 1.8) | 463 (17.4 / 27.3) | 3,468 (2.0 / 9.0) |
| local directory | serialized | 64 | 19,774 (3.0 / 7.4) | 447 (142.1 / 260.4) | 2,586 (23.2 / 56.9) |
| local directory | shared | 1 | 7,046 (0.1 / 0.4) | 467 (2.1 / 4.8) | 1,990 (0.3 / 2.9) |
| local directory | shared | 8 | 22,109 (0.3 / 1.3) | 493 (16.5 / 27.4) | 3,557 (1.9 / 9.2) |
| local directory | shared | 64 | 31,048 (1.9 / 5.3) | 450 (138.9 / 239.0) | 2,216 (27.7 / 62.9) |
| in memory | serialized | 1 | 6,983 (0.1 / 0.4) | 536 (1.8 / 4.0) | 2,022 (0.3 / 2.8) |
| in memory | serialized | 8 | 14,569 (0.5 / 1.7) | 516 (15.3 / 28.7) | 3,658 (1.7 / 9.2) |
| in memory | serialized | 64 | 17,024 (3.6 / 8.1) | 447 (151.3 / 216.2) | 2,010 (30.7 / 67.2) |
| in memory | shared | 1 | 6,833 (0.1 / 0.5) | 514 (1.9 / 4.4) | 1,851 (0.3 / 3.0) |
| in memory | shared | 8 | 21,360 (0.3 / 1.4) | 519 (15.3 / 27.3) | 3,774 (1.6 / 9.6) |
| in memory | shared | 64 | 28,825 (2.0 / 5.6) | 437 (153.1 / 228.1) | 1,980 (31.0 / 70.8) |

**What limits it.** Writes: 122 ops/s on MinIO with one connection, 427 on a local directory and 536 in memory; adding connections does not raise it (8 and 64 connections: 125 and 136 on MinIO, 447 in memory), latency just grows with the queue. So the write limit is a per-tenant serialization of a step that costs about 1.9 ms of engine and server work even with no network (scan of the collection, update, WAL object creation and flush in slatedb) and 8.2 ms against MinIO: the engine plus server floor is about 4x lower than MinIO's, and MinIO adds the rest. It is not MinIO alone and not the lock alone; removing either would not remove the cap. Reads: 17,442 ops/s serialized on MinIO with 64 connections against 17,024 in memory: reads do not touch the store once the tenant is open, so the cap is the single lock plus the load generator and server sharing 4 cores.

**The safe fix for concurrent reads (separate commit, with tests).** The entry lock became a read-write lock: `find`, `count`, `distinct` and `aggregate` share it, writes and tenant state changes (open, upgrade, close, abandon) are exclusive. Ownership is re-checked after each read. Reads at 64 connections go from 17,442 to 27,370 ops/s on MinIO (+57%) and at 8 connections from 13,493 to 21,124; writes and the 90/10 mix are unchanged within noise (991 to 1,051 ops/s), as expected: every write still takes the lock exclusively, and a write waits for the readers in flight. Tests: `loadtest/test_shared_reads.py` (16 readers running find, count, aggregate and distinct against a tenant that is being written: no errors, the counter never goes backwards, a count is never below the counter read just before it, final state exact, reads of a non-existent database empty) passes with shared and serialized reads; the full differential suite (pymongo, 1,081 in-scope tests) passes with shared reads on (the log is in `loadtest/results/t5/py_suite_shared_reads.log`); the Node suite was not rerun.

**Not fixed, and why.** Making *writes* to one tenant concurrent would need per-collection or per-key locking and a re-check of duplicate-key and unique-index decisions inside the write batch; I did not attempt it. Batching concurrent writes into one WAL flush (group commit) would raise the write cap on MinIO without changing the locking, and is what the flush-interval rows of Task 1 begin to show.

## Defects found in this round

1. **Graceful shutdown hangs with non-S3 stores (not diagnosed).** After the long single-tenant runs (about 3 minutes of mixed load) against the local-directory and in-memory stores, `SIGTERM` left the process alive and idle (0% CPU, every runtime thread parked, stuck while closing tenants: the log has no `tenants closed` line) in 4 of the 5 such runs (local: serialized and shared reads; memory: serialized, twice) and not in the fifth (memory, shared reads). It did not happen in 6 shorter runs (6 to 50 s) against the same stores. With MinIO the same long matrices shut down within the 120 s the harness allowed (the MinIO server logs of that run predate the extra log lines, so for MinIO I only know it exited). I did not find the cause, so I do not know whether it is in this server (the close path, the lock) or in slatedb with those stores. The harness kills such a server after 25 s. It is a graceful-shutdown hang, which is serious enough to list even though it was seen only with the two stores I added for diagnosis.
2. **Local-directory store cannot run with leases.** `object_store`'s `LocalFileSystem` does not implement `PutMode::Update`, so lease renewal fails there; added `--no-leases` (single process only) for those measurements.
3. **Read ownership gap closed** (see Task 2): the check was before the read only.
4. **Harness slips**: three chained experiments (SIGSTOP test, wake trace, knee, limit, durability, cost) failed instantly the first time because their output directories did not exist (a shell redirect fails before the script can create them); rerun after creating them.

## What I could not measure

- A read frozen *inside* a command (see Task 2): only reads queued before and sent after the freeze were tested.
- The per-process lease: no implementation, so no measured PUT saving and no safety test; only the arithmetic estimate from the measured per-tenant renewal rate.
- Request costs against real S3 (MinIO counts the same requests but real S3 has different latencies and failure behaviour; GETs served from a cache in front of S3 would not be counted the same way).
- The cost of a wake and of a close as steady-state per-tenant-hour rates: they are per-session costs (Task 4 gives the request count of a wake; the close was not traced).
- Whether the shutdown hang (defect 1) affects S3 deployments: it was not observed there, but those runs were not instrumented to show where a hang would be.
- Node.js driver results for this round's server changes (only the pymongo suite was rerun).
<!-- round2:end -->

<!-- round3:start -->
## Round 3 — branch `engine-cost`: per-process lease, group commit, shutdown hang, driver suites

Branch `engine-cost` from `load-test` (32facb8), not merged. Every number below is computed from a file in `loadtest/results/` (`gc/`, `lease-cost/`, `t2/stop_cont-*.json`, `s4-engine-{off,on}/`, `shutdown/`, `engine-py-*`, `engine-node-*`); what could not be measured is listed at the end. One commit per change so they rebase cleanly: group commit (`cmd.rs`, `dbs.rs`, `lifecycle.rs`: one function pair and a `handle` wrapper, no change to the write commands), the shutdown fix, and the per-process lease (`lease.rs`, flag off by default). A document and its index entries are still written in **one** `slatedb::WriteBatch` (`Tenant::stage` takes the same `Vec<Op>` that `apply` took), so the atomic write that `compat-indexes` relies on is unchanged.

### Step 0 — environment

`cargo build --release --workspace` works (first build 3 min 29 s). MinIO could not be downloaded (`dl.min.io` and GitHub release downloads are refused by the proxy, HTTP 403), so it was **built from source** with `go install github.com/minio/minio@latest` through the reachable Go module proxy (version string `DEVELOPMENT.GOGET`, go1.24.7) and run natively with its data on the local disk. Docker was not running; `dockerd` was started by hand and `mongo:7.0` (MongoDB 7.0.45) pulled for the differential suites. Same machine as the earlier rounds (4 vCPU, 16 GB); MinIO, the server and the load generator share it, so latencies are a lower bound for MinIO-class stores, and MinIO here is on a faster local disk than in the earlier rounds (the "before" write numbers are therefore not those of round 2).

### Task 1 — per-process lease (`--process-lease`, default off)

**Design.** `processes/{holder}/_lease` is one lease per process, renewed every TTL/3 with a conditional update. `tenants/{id}/_lease` becomes an *owner record* `{holder, inc, seq}` written once when the tenant is opened and not renewed. A record is valid while its owner's process lease is live **and carries the generation (`inc`) the record names**; otherwise another process takes the tenant over with a conditional update of the record. A process whose own lease lapsed before it could renew (frozen or starved for longer than the TTL) bumps its generation and abandons every tenant it held before the lease can look valid again; every command also checks that the tenant's generation is the current one and the process lease is valid. Clean close releases the owner record as before; clean shutdown expires the process lease. All processes sharing a bucket must use the same mode (a per-tenant-mode process refuses a tenant with an owner record, which is the safe direction). This answers the objection of round 2 (a takeover reads one object, the old owner's liveness lives in another): the check-then-act race is closed by the owner's local deadline (lapse detection) being earlier than the takeover deadline (expiry plus the 1.5 s skew allowance), the same clock assumption as the per-tenant lease; I did not model-check it.

**PUTs per tenant-hour, idle** (`exp_idle_lease.py idle`: tenants opened by one process, then left alone for 120 s, TTL 30 s, writers with the 30 s manifest poll of Task 3):

| Open idle tenants | Flag | Lease PUTs per tenant-hour | All PUTs per tenant-hour | GETs per tenant-hour |
|---|---|---|---|---|
| 100 | off | 330.6 | 360.6 | 895.8 |
| 400 | off | 354.0 | 383.9 | 900.0 |
| 100 | on | 3.6 | 33.6 | 900.0 |
| 400 | on | 0.9 | 30.9 | 900.0 |
| 50 (300 s window) | on | 7.0 | 19.0 | 504.0 |

With the flag off the cost is one PUT per tenant per TTL/3 (360 per hour at TTL 30 s, measured 331 to 354 in a 120 s window); with it on it is one PUT per process per TTL/3 whatever the number of tenants (12 PUTs in 120 s: 360 per hour per process), so 3.6 and 0.9 per tenant-hour at 100 and 400 tenants. The remaining 30 PUTs per tenant-hour in the 120 s windows are one PUT per tenant, once, in category "other" (the 300 s window shows 50 PUTs for 50 tenants, not 125), so it is a start-up cost, not a rate; I did not identify what writes it (it is also in the unmodified binary). GETs: the unmodified binary measured 660 per tenant-hour (`idle-base.json`), the current one 900 in both modes: the 30 s writer manifest poll costs 240 extra GETs per tenant-hour (about $0.0001 per tenant-hour at S3 prices against $0.0017 saved by 330 fewer PUTs).

**Failover time, kill to first success** (`exp_idle_lease.py failover`, SIGKILL of the owner, second process retries every 50 ms, TTL 10 s, 8 trials per flag, 20 documents verified after takeover):

| Flag | min / median / max (s) |
|---|---|
| off | 11.03 / 11.41 / 11.45 |
| on | 11.07 / 11.37 / 11.40 |

Both are TTL plus the 1.5 s skew allowance (11.5 s); the process lease does not change failover.

**Safety.**
- `test_lease.py` (two processes, 13 checks incl. refusal with 134, clean-close handover, SIGKILL takeover on the unclean path, same-holder restart, 40-thread race): 13/13 with the flag off and on, twice each.
- **Frozen process** (`test_stop.py`, SIGSTOP of the owner for longer than the TTL, 5 rounds, TTL 4 s; the other process takes the tenant over at about 5.6 s and writes a newer value; then 40 reads queued during the freeze and 40 sent after SIGCONT, and now also 20 writes queued and 20 sent after SIGCONT per round): flag off and on, both with serialized and shared reads: 400 of 400 reads refused with 134, **0 stale reads, 0 writes acknowledged by the frozen process, 200 of 200 writes refused, final value never clobbered**. **Negative control** (`--unsafe-skip-ownership-checks` on the frozen process): flag off 11 stale reads of 400 and 72 of 200 writes acknowledged; flag on 89 stale reads of 400 and 91 of 200 writes acknowledged, so the test detects staleness in both modes (the other reads of the control failed with I/O errors from the fenced writer; the split between stale and failed varies from run to run).
- **SIGKILL scenario** (`run_load.py s4`, reduced: two processes, 200 active tenants from a pool of 2,000 preloaded tenants, 150 s, 8 SIGKILLs of a random process each followed by a restart with the same holder id, TTL 10 s, random routing, retries and a read-your-writes check through the other process; the earlier S4 had 1,000 tenants, 300 s and 10 kills): 

| | Flag off | Flag on |
|---|---|---|
| Writer intervals / overlaps between different holders | 1,308 / **0** | 1,338 / **0** |
| Lease-record overlaps in the log | 0 | 0 |
| Max concurrent writer intervals on one tenant | 1 | 1 |
| Takeovers of expired leases / unclean WAL-replay wakes | 116 / 1,333 | 136 / 1,370 |
| Operations ok / failed after retries | 29,105 / 0 | 28,997 / 0 |
| Read-your-writes checks / stale | 5,664 / **0** (5,662 refused with 134, 2 I/O errors) | 5,676 / **0** (5,675 refused, 1 I/O error) |
| Acknowledged inserts missing | 0 of 2,785 | 0 of 2,834 |
| Acknowledged updates older than the last acknowledged value | 1 of 629 (see below) | 1 of 635 (see below) |

The single "stale" update of each run is on tenant `t000011`, whose preload had failed half-way (2 documents instead of 50): the update matched no document, in both modes and the same tenant. (A first attempt of this scenario was discarded: I had not preloaded the tenants, so every update matched nothing and the checker reported hundreds; after preloading, the remaining 7 failed preloads were reloaded except this one.)
- **Read-your-writes across two processes**: the S4 checks above (0 stale of 11,340), plus `test_lease.py` (after a clean close the other process sees 10 of 10 writes; after SIGKILL 25 of 25).

Not shown: a model of the clock assumption, a frozen process whose *process lease* is renewed by the same holder id started twice (that case abandons everything and recreates the lease, untested), and mixed-mode clusters.

### Task 2 — group commit

**What it does.** A write command takes the tenant lock exclusively as before, decides, and puts its batch into the database (`Tenant::stage`, one atomic `WriteBatch`); it no longer waits for the flush there. `Server::handle` waits for durability after the command released its locks and **before the reply is sent**, so writes that arrive while a flush is in flight share the next WAL object, and a write is acknowledged only after the object holding it is durable. `--no-group-commit` restores the old behaviour (flush under the lock). One consequence, not present before: after the lock is released and before the flush finishes, other connections' reads and writes can see the staged batch (a read of not yet durable data); if the process dies in that window the value disappears without ever having been acknowledged, and if the flush fails the batch stays visible but unacknowledged. Acknowledged data is not affected.

**Single tenant, writes only** (`exp_groupcommit.py`, closed loop, `update_one` on 50 documents, 15 s per cell, no warm-up; before = the unmodified binary, after = this branch; cells: ops/s (p50 / p99 ms) and WAL PUTs per write / all PUTs per write):

| Store | Connections | Before | After | Speed-up |
|---|---|---|---|---|
| MinIO | 1 | 285 (3.4 / 6.2) 1.00 / 1.00 | 265 (3.7 / 6.7) 1.00 / 1.00 | 0.93 |
| MinIO | 8 | 274 (29.3 / 42.1) 1.00 / 1.00 | 484 (16.7 / 27.7) 0.93 / 0.93 | 1.77 |
| MinIO | 64 | 331 (204.3 / 261.2) 0.78 / 0.78 | 644 (109.6 / 183.8) 0.67 / 0.67 | 1.94 |
| local directory | 1 | 541 (1.8 / 3.6) 2.00 / 2.00 | 510 (1.9 / 3.8) 2.00 / 2.00 | 0.94 |
| local directory | 8 | 534 (14.8 / 25.9) 2.00 / 2.00 | 785 (10.1 / 20.4) 1.93 / 1.93 | 1.47 |
| local directory | 64 | 647 (100.5 / 169.0) 1.56 / 1.56 | 762 (84.9 / 177.8) 1.54 / 1.54 | 1.18 |
| in memory (ceiling, 10 s) | 1 / 8 / 64 | 677 / 776 / 821 | 670 / 797 / 891 | 0.99 / 1.03 / 1.09 |

The local-directory store counts 2 PUT-type requests per WAL object (measured, cause not determined), so its PUTs per write are twice the MinIO figures at one connection. The write step stays serialized per tenant (it scans the collection and decides under the lock); the in-memory store, which has no flush cost at all, tops out at 797 to 891 ops/s with 8 to 64 connections, which is the ceiling group commit can reach here: on MinIO it gets to 644 of it (72%). PUTs per write fall from 1.00 to 0.67 at 64 connections, not further, because only writes that queue behind the lock share an object.

**Durability** (`test_group_commit.py`: 32 threads inserting and then updating their own documents on one tenant, SIGKILL at a random moment between 2.5 and 4 s, restart, read back everything that was acknowledged): MinIO 6 trials, 7,033 acknowledged documents (about 14,000 acknowledged writes), **0 lost inserts, 0 lost updates**; local directory 4 trials, 5,886 acknowledged documents, **0 lost, 0 lost**; WAL PUTs per acknowledged write before the kill 0.57 to 0.61 on MinIO (so the objects were shared). **Negative control** (`--ack-before-durable`, 1.5 s flush interval): 4 trials, 5,738 acknowledged documents, **681 lost inserts and 66 lost updates**, so the check detects loss. (My first run of this test was invalid: an earlier crashed run had left a server on the port, so every trial talked to a stale server; the script now refuses to start if the port is taken.)

### Task 3 — the graceful-shutdown hang

**Cause.** A writer that the server opens has the in-`Db` compactor off and polled the manifest every hour (`LONG_POLL`); compaction was requested only *after* a clean close. slatedb freezes the memtable by WAL count, not by size: on the local store a hot tenant wrote an L0 SST (51 KB each) about every 10 s, reached `l0_max_ssts` = 8 after about 80 s and the memtable flush then stalled, retrying only once per `manifest_poll_interval`, that is hourly. From then on the WAL piled up (38,545 WAL objects, 153 MB after 150 s) and `Db::close` called by SIGTERM never returned. Evidence: with a debug build, `close_all` logged "got entry lock" and "db.close() begin" and never "done"; the store held exactly 8 compacted SSTs; thread dumps (gdb) showed every runtime thread parked, i.e. an await that never completes, not a lock cycle. It reproduced in 3 of 3 runs of 150 to 180 s load (64 connections, 90/10 mix) on the memory store (unmodified binary and group-commit binary) and the local store, and in none of the 6 shorter runs of the earlier round, which matches the time to fill L0. It is **not** specific to the non-S3 stores: I did not rerun it against MinIO; the earlier rounds' MinIO runs wrote far fewer operations per second (about 130 against 450), which may be why they stayed below the limit, but the mechanism applies to any store: any tenant written long enough would hit it, and so would an idle close of such a tenant.

**Fix** (own commit). (1) The server asks the maintenance queue for a compaction of an open writer once its L0 reaches the compaction threshold (4), at most every 10 s. (2) Writers poll the manifest every `--writer-poll-secs` (default 30 instead of 3,600) so they see the result and retry a stalled flush (cost: 240 GETs per open writer-hour, see Task 1). (3) Safety net: `Tenant::close` gives the final flush 20 s, then closes without it and writes no clean marker (the WAL is durable, the next wake replays it) and reports the error.

**Result.** After a 150 s hot run (64 connections, 90/10 mix) and SIGTERM: **local directory 10 of 10 and in-memory 10 of 10 exited within 2 to 3 s**, every log has "tenants closed" and "maintenance stopped" and none the timeout message (`shutdown/fixed-10runs-*.txt`, logs in `shutdown/fixed-*-logs/`). Safety net tested separately: with `--no-maintenance --writer-poll-secs 3600` (the stall comes back) the server exits after 21 s with "final flush did not finish in 20s; closing without it (unclean)", twice; without the net the same configuration is the original hang. Whether the data written before such an unclean close is fully recovered on the next wake was not tested.

### Task 4 — differential driver suites against this branch's server

Real MongoDB 7.0.45 on :27017, `mongo-server` (this branch, default flags) on :27018 and :27019 with MinIO, `tests.json` regenerated and identical to the committed one.

| Suite | In scope ("core") | Out-of-scope ("extra") |
|---|---|---|
| pymongo (`run_py.py`) | **1081/1081** | 24/49 |
| Node.js `mongodb` 7.7.0 (`run_node.js`) | **1081/1081** | 25/49 |

The extra tests are the out-of-scope features and failed in the earlier rounds too (24/49 in `results6.md`); the one difference between the two drivers' runs is `aggregate.extra.group_add_to_set` (fails with pymongo, passes with Node.js; `$addToSet` order is unspecified, I did not look further).

### Defects found in this round

1. **Shutdown hang: cause found and fixed** (Task 3). Related and *not fixed*: a hot writer keeps every WAL object until the tenant has been idle for 30 s (GC only runs then): 56,517 WAL objects after a 150 s run in the fixed build, so a crash of a long-lived hot tenant means a very long WAL replay on the next wake.
2. **Group commit makes staged, not yet durable data readable** (Task 2): a change of the read guarantee, described there; acknowledged writes are unaffected.
3. **Harness defects** (not server defects): `test_lease.py` listed the bucket with `docker exec` (now `MINIO_DATA_DIR` when MinIO runs outside docker; the first runs failed that check); its "single holder in the log" check once failed for a tenant that process A acquired, discarded (the tenant did not exist) and process B then acquired: A never wrote (one failure in 5 flag-off runs, unreproduced afterwards); a stale server from a crashed earlier run answered on the test port and invalidated a whole first durability run; the first S4 attempt ran on tenants that had never been loaded; and my S4 runs overwrote the tracked outputs of the earlier round in `loadtest/out/s4` and `lease-test` (restored from 32facb8 in a separate commit; this round's copies are in `results/s4-engine-*`).
4. The unmodified binary's idle open writers already cost about 660 GETs per tenant-hour (manifest and one more GET per roughly 11 s per tenant); not investigated.

### What I could not measure

- Anything against real S3: MinIO runs on the same machine with local-disk latency; the PUT counts transfer, the latencies do not.
- Per-process lease: the clock-drift assumption under which a takeover is safe (only SIGSTOP of up to a few TTLs and SIGKILL were tested, no clock skew, no network partition between a process and the store); mixed-mode clusters; a duplicated holder id; the full-size S4 (1,000 tenants, 300 s, 10 kills) and the S4 on a cold disk.
- Group commit: more than one tenant at a time (this was a single-tenant measurement), the interaction with `compat-indexes` (that branch's code was not available here), and any real-S3 gain: the PUT saving at 64 connections is 0.33 PUT per write on MinIO, bounded by the serialized write step.
- What writes the one PUT per tenant ("other" category) after a tenant opens, and why the unmodified binary issues about 11 manifest-related GETs per tenant per 120 s while idle.
- Recovery of the data after the shutdown safety net's unclean close.
- Whether `--writer-poll-secs 30` is the right value for S3 prices at very large numbers of open writers (the cost is 240 GETs per open writer-hour; no sweep was run).
<!-- round3:end -->
