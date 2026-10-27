#!/usr/bin/env python3
"""Assembles results-load.md from loadtest/results/*."""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "loadtest", "results")
sys.path.insert(0, os.path.join(ROOT, "loadtest"))
import report_tables as rt  # noqa: E402

rt.OUT = R


def j(*p):
    return json.load(open(os.path.join(R, *p)))


m = j("machine.json")
load = j("load", "result.json")
lease = j("s4", "lease_check.json")
s4 = j("s4", "summary.json")
s3 = j("s3", "summary.json")
have_s1b = os.path.exists(os.path.join(R, "s1b", "summary.json"))
s1b = j("s1b", "summary.json") if have_s1b else None
tests = json.load(open(os.path.join(ROOT, "loadtest", "out", "lease-test", "results.json"))) if os.path.exists(os.path.join(ROOT, "loadtest", "out", "lease-test", "results.json")) else j("results.json")
names = ["s1"] + (["s1b"] if have_s1b else []) + ["s2", "s3", "s4"]
summary = rt.summary_table(names).replace("| S1B |", "| S1b (5 min, rerun beside S3) |")
pt = load["per_tenant"]
v = s4["verification"]
hot = s3["hot"]


def row(name, g):
    h = g["steady_ops"]
    return f"{name}: p50 {h['p50_ms']:.1f} / p99 {h['p99_ms']:.0f} / p999 {h['p999_ms']:.0f} ms ({h['count']:,} ops)"


cmp_rows = ["| Latency of the ordinary S1 tenants (requests to already-open tenants), ms | p50 | p99 | p999 | ops |", "|---|---|---|---|---|"]
for lab, s in (("S1, 10 min (alone)", j("s1", "summary.json")), ("S1b, 5 min (alone, run right after S3)", s1b), ("S3, 5 min (same tenants plus the hot tenant)", s3)):
    if s:
        h = s["normal"]["steady_ops"]
        cmp_rows.append(f"| {lab} | {h['p50_ms']:.1f} | {h['p99_ms']:.0f} | {h['p999_ms']:.0f} | {h['count']:,} |")
for lab, s in (("S1b", s1b), ("S3", s3)):
    if s:
        for k in ("find_by_id", "insert"):
            h = s["normal"]["steady_by_type"][k]
            cmp_rows.append(f"| &nbsp;&nbsp;{lab} {k} | {h['p50_ms']:.1f} | {h['p99_ms']:.0f} | {h['p999_ms']:.0f} | {h['count']:,} |")
cmp_table = "\n".join(cmp_rows)

doc = f"""# Wakes, concurrency and activity peaks on one server process — results-load

Branch `load-test` (from `slatedb-tenant-bench`, not merged). Everything ran locally: `mongo-server` processes, the load generator and MinIO (Docker) on one machine; no cloud account or paid API.

**Read this first.** The machine is small (4 vCPU) and MinIO, the server and the load generator share it. In every scenario MinIO used more CPU than the server (about 1.6 cores against about 1.1) and its PUT latency (median about 60 ms, tail up to 1 s) is what the clients see in their tails. The latency percentiles below are therefore **not** what real S3 would give; the numbers that transfer to another environment are the request counts, CPU and memory per active tenant, and the correctness results. Two incidents affected the schedule: the host machine rebooted during the first S3 attempt (processes lost, MinIO data intact; S3 was rerun), and S4 ran in the first minutes after the reboot while the disk was still cold (60% I/O wait), so its latencies are much worse than the other scenarios'; its correctness result is unaffected (a slower store makes the ownership race harder to win, not easier).

## Step 0 — Machine

{m['cpus']} vCPU ({m['cpu_model']}), {m['ram_mb']:,} MB RAM ({m['ram_free_mb']:,} MB free at start), {m['disk_total_gb']:.0f} GB disk ({m['disk_free_gb']:.0f} GB free at start), kernel {m['kernel']}, open-file limit {m['open_file_limit']:,}. Scenario sizes are CLI parameters (`loadtest/run_load.py --help`). Everything the task asked for fitted on this machine at the full size (50,000 tenants, 5,000-tenant burst, 1,000 active tenants) except two reductions, both stated here: **S3 ran 5 minutes instead of 10**, because of the reboot, and the hot tenant could not reach 500 ops/s (see S3).

## Step 1 — Tenant ownership with a lease object

**Which conditional writes exist.** In `object_store` 0.14.2 (`src/aws/mod.rs`, `S3ConditionalPut`) the S3 implementation supports `PutMode::Create` (sent as `If-None-Match: *`, mapped to `AlreadyExists`) and `PutMode::Update` (sent as `If-Match: <etag>`, mapped to `Precondition`; an ETag is required). `S3ConditionalPut::ETagMatch` is the default and works on MinIO. There is no conditional delete, so only those two modes are used, and a lease is released by a conditional update that expires it.

**Design** (`mongo-server/src/lease.rs`, hooked into `dbs.rs`). `tenants/{{id}}/_lease` holds `{{holder, expiresMs, seq}}`.
- Opening a tenant, as reader or writer, first takes the lease: create-if-absent, or replace-if-ETag-matches when the lease is ours, released or expired (expiry plus a 1.5 s clock-skew allowance). Otherwise the request gets error code **134** (`TenantLeaseHeld`) with the `RetryableWriteError` label; 134 is on the drivers' retryable-read list and, unlike the not-primary codes, does not make them clear their connection pools. A refusal is remembered for 500 ms so refused requests do not each cost a GET.
- Reads need the lease too (not only writes): another process's reader would otherwise serve stale data after the owner's writes.
- Open tenants' leases are renewed by a background task, each one a third of a TTL after it was last written (staggered, not in a burst). A process refuses to serve a tenant whose lease it has not renewed recently enough (local deadline, checked on every command), and abandons it (no clean marker) if the renewal finds the lease taken over.
- Clean close writes the `_clean` marker and then releases. After a crash the marker is missing, so whoever takes over (another process after expiry, or the same holder id restarting at once) wakes on the **unclean WAL-replay path**. A database that does not exist leaves no lease object.
- Lease holders are logged (`LEASE acquired|release_begin|writer_open|writer_close|lost|invalid|takeover_wake`) for the S4 checker.

**Functional test** (`loadtest/test_lease.py`, two server processes, {sum(1 for t in tests if t['ok'])}/{len(tests)} checks pass): non-owner read and write refused with 134 and the label; the owner keeps reading; after the owner's clean close the other process opens the tenant and sees all writes; reading a non-existent database creates no objects; after SIGKILL of the owner the other process is refused until the lease expires (5.7 s with a 4 s TTL), then takes over on the unclean path and sees all 25 acknowledged writes; a restarted process with the same holder id re-takes its tenants at once (0.5 s) and replays the WAL; 40 concurrent first writes through both processes: exactly one process wins and the lease log shows a single holder. The 1,081 differential tests of results6 passed against the first version of the lease code (pymongo 1,081/1,081); they were not rerun after the later fixes listed below.

**Cost of leases** (estimate from the counters): a wake adds 1 GET and 1 PUT, a close 1 PUT, and every open tenant costs 1 PUT per TTL/3. With 1,500 open tenants and a 60 s TTL that is 75 PUT/s, about $1.35 per hour at S3 prices, 14% of the PUT-tier requests of S1.

## Step 2 — 50,000 tenants

Loaded in parallel (64 at a time, closing each tenant after loading it so the server stays small) with `loadgen load`: **{load['load']['loaded']:,} tenants in {load['load']['seconds']:.0f} s ({load['load']['tenants_per_sec']:.1f} tenants/s)**, 50 documents of about 1 KB each, no failures. Storage per tenant, measured inside the MinIO container over the whole `tenants/` prefix: **{pt['disk_kb']:.0f} KB of disk blocks, {pt['file_bytes'] / 1024:.0f} KB of object bytes, {pt['objects']:.1f} objects** (two thirds of the bytes are the WAL object and the compacted SST that hold the same 50 documents until garbage collection; about 50 KB of data). Total {load['usage_after']['disk_kb'] / 1048576:.1f} GB on disk.

## Step 3 — Scenarios

Server settings: idle close after 30 s, lease TTL 60 s (S1 to S3) and 10 s (S4, so takeovers happen within the test), compaction and GC on. S1 and S3 draw from tenants 0 to 19,999, S2 from 30,000 to 34,999, S4 from 20,000 to 29,999. Each active tenant is one sequential task sending one operation per second: 40% find by `_id`, 40% find with a filter (scans the 50 documents), 10% insert, 10% update. A tenant stays active for 60 s and is then replaced by a new one; the first lifetimes are staggered so about 17 tenants wake per second. The load generator uses the official Rust driver (`mongodb` 3.9.1, pinned), 256 connections per server, retries disabled in the driver and done by the generator (every failed attempt is counted by code). Latency is client-perceived, including retries. "Wake" is the latency of a tenant's first request. CPU is percent of one core (400 = the whole machine); requests and cost come from the servers' own counters of every object-store request (lifecycle, leases, compaction, GC, listings) at S3 Standard prices (PUT, COPY, POST, LIST $0.005 per 1,000; GET $0.0004 per 1,000).

### Summary

{summary}

S1: 1,000 active tenants for 10 min. S1b: the same for 5 min, rerun beside S3 so S3 has a like-for-like baseline. S2: 5,000 cold tenants, first request spread over 60 s, then 1 op/s for 120 s. S3: S1 plus one hot tenant, 5 min. S4: two server processes, 10 SIGKILLs. S4's CPU and memory are summed over both processes and include the downtime of the killed one.

### S1 by operation type (10 minutes, 1,000 active tenants)

{rt.by_type('s1')}

Reads of an open tenant are fast (median {j('s1', 'summary.json')['normal']['steady_by_type']['find_by_id']['p50_ms']:.0f} ms); writes wait for a durable WAL PUT on a loaded MinIO (median {j('s1', 'summary.json')['normal']['steady_by_type']['insert']['p50_ms']:.0f} ms); the p99 and p999 of everything follow MinIO's PUT/GET tail (server-side, 17,540 commands took over 500 ms in the first S1 run; the object-store call latency histograms show PUT median about 60 ms and GET about 10 ms under this load).

### S2 by operation type (5,000 cold tenants within 60 s)

{rt.by_type('s2')}

The burst is 83 first requests per second, each needing a lease GET and PUT, the manifest, the clean-marker HEAD and several block reads. MinIO saturated (peak 5,000 open tenants, 972 MB resident); first requests took a median 4.6 s. 25 attempts timed out at the generator's 10 s limit and succeeded on retry; no operation failed.

### S3 — noisy neighbour

{cmp_table}

The hot tenant was asked for 500 ops/s (16 workers) and achieved **{hot['ops'] / s3['seconds']:.0f} ops/s** (p50 {hot['all_ops']['p50_ms']:.0f} ms, p99 {hot['all_ops']['p99_ms']:.0f} ms): commands to one tenant run one at a time and each write waits for a durable WAL flush, so one tenant is capped by its own latency, not by the server. It therefore consumed little (CPU {rt.load('s3')[1]['total']['cpu_pct_of_one_core']['avg']:.0f}% against {rt.load('s1')[1]['total']['cpu_pct_of_one_core']['avg']:.0f}% in S1), and the ordinary tenants were barely affected: median unchanged, p99 and p999 higher by about 8% and 20% against S1b (table above; run-to-run noise on this machine is about 10%, so only the p999 difference is plausibly real). The reason is that the hot tenant could not generate more load than one tenant's serialized commands allow; a tenant with many concurrent connections and cheap commands would stress the shared MinIO more, which was not measured.

### S4 — two processes, random routing, 10 SIGKILLs

Two server processes (holders `S0` and `S1`, ports 28001 and 28002, one bucket), 1,000 active tenants for 5 minutes, every request sent to a randomly chosen process, 10 SIGKILLs of a random process at about 27 s intervals, each restarted after 0.5 to 2 s with the same holder id. A request refused with 134 (the other process owns the tenant) is retried on a random process, so about half of all attempts are refusals by design: **{s4['errors_by_code'].get('cmd:134', 0):,} refusals**, {s4['errors_by_code'].get('io', 0):,} connection errors, {s4['errors_by_code'].get('pool_cleared', 0):,} pool-cleared and {s4['errors_by_code'].get('timeout', 0):,} timeouts around the kills; **0 operations failed after retries** ({s4['retries']:,} retries). A production router would send a tenant to one process; random routing is the stress case.

**Correctness.**
- **No acknowledged write lost.** After the run, every tenant that received an acknowledged write was read back ({v['tenants_checked']:,} tenants): **{v['acknowledged_inserts']:,} acknowledged inserts, {v['missing_inserts']} missing; {v['acknowledged_update_docs']:,} updated documents, {v['stale_updates']} older than the last acknowledged value; {v['unreadable_tenants']} unreadable tenants.** No read of a loaded document missed (0 read misses).
- **No tenant open as a writer in both processes at once.** From the lease logs of both processes and the kill times (`loadtest/check_leases.py`): {lease['writer_intervals']:,} writer intervals, **{lease['writer_overlaps_between_different_holders']} overlaps between different holders**, at most {lease['max_concurrent_writer_intervals_per_tenant']} concurrent writer per tenant.
- **Lease records did overlap in the log {lease['lease_overlaps_between_different_holders']} times**, and this should be read carefully rather than as a pass. In each case the earlier holder was stalled for longer than the 10 s TTL (the cold disk), so its lease expired and the other process took it over legitimately; the earlier holder noticed only later (a failed renewal, 'lost', {lease['lease_overlaps_by_how_the_earlier_holder_ended'].get('lost', 0)} cases; a kill, {lease['lease_overlaps_by_how_the_earlier_holder_ended'].get('kill', 0)}; the next command's validity check, {lease['lease_overlaps_by_how_the_earlier_holder_ended'].get('invalid', 0)}; its own late release, {lease['lease_overlaps_by_how_the_earlier_holder_ended'].get('release_begin', 0)}) and the log line is written at that moment. Median apparent overlap {lease['lease_overlap_median_ms'] / 1000:.1f} s, maximum {lease['lease_overlap_max_ms'] / 1000:.1f} s. What prevented harm was the local validity deadline (a process stops serving a tenant before its lease can expire) and slatedb's own writer fencing (a second writer opened by the new owner fences the first); it held in this run, but a process that stalls inside a command for longer than the TTL is a window the lease alone does not close, and fencing is what covers it. A longer TTL narrows it at the price of slower takeover.
- **Crash path.** {lease['takeovers_of_expired_leases']:,} expired leases were taken over by the other process, {lease['wakes_after_takeover_or_crash']:,} tenants were woken after a takeover or crash, **{lease['unclean_wal_replay_wakes']:,} of them on the unclean (WAL replay) path**; the others had been closed cleanly (marker present) before the lease changed hands.

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
6. **Hot tenant capped at about {hot['ops'] / s3['seconds']:.0f} ops/s** (noted, not changed): commands to one tenant are serialized and each write waits for a durable flush. Whether that is acceptable depends on the workload; batching concurrent writes to one tenant into one WAL flush would raise it.
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
"""
open(os.path.join(ROOT, "results-load.md"), "w").write(doc)
print(len(doc))
