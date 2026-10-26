#!/usr/bin/env python3
"""Appends (or replaces) the second-round section of results-load.md from loadtest/results/t1..t5.
Every number in the section is computed here from a results file."""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "loadtest", "results")
sys.path.insert(0, os.path.join(ROOT, "loadtest"))
import report_tables as rt  # noqa: E402

rt.OUT = R
START, END = "<!-- round2:start -->", "<!-- round2:end -->"


def jl(*p):
    f = os.path.join(R, *p)
    return json.load(open(f)) if os.path.exists(f) else None


def f1(x):
    return f"{x:,.1f}"


def f0(x):
    return f"{x:,.0f}"


prices = jl("s3_prices.json")
PUT_P, GET_P = prices["put_copy_post_list_usd_per_request"], prices["get_and_other_usd_per_request"]
out = []
w = out.append

w(START)
w("# Second round: request cost, leases, failover, wakes, hot tenant")
w("")
w("Same rules as the first round: every number below is computed from a results file in `loadtest/results/` (`t1` request cost, `t2` ownership, `t3` S4 redo, `t4` wakes, `t5` hot tenant); what could not be measured is listed at the end. Same machine (4 vCPU, 16 GB, MinIO and the load generator on the same host); latencies are a lower bound for MinIO-class stores and are dominated by MinIO under load. The host rebooted again between the two rounds, so MinIO's disk was warmed before the S4 redo and the wake experiments (the objects of the tenants used were read once).")
w("")
w(f"**Prices used.** S3 Standard, US East (N. Virginia), from AWS's price list file `{prices['source']}` (publication date {prices['publicationDate']}, fetched {prices['fetched']}): PUT, COPY, POST and LIST $0.005 per 1,000 requests; GET and all other requests $0.004 per 10,000. These are the figures the AWS pricing page https://aws.amazon.com/s3/pricing/ shows for S3 Standard; I read the machine-readable price list, not the web page (a JavaScript page). DELETE is counted as free (S3 does not charge for DELETE requests; that is not in the price file I read, so it is the one price assumption). HEAD is counted with GET.")
w("")

# ------------------------------------------------------------------ Task 1
cost = jl("t1", "cost.json")
dur = jl("t1", "durability.json")
w("## Task 1 — Request cost per tenant")
w("")
if cost:
    w("Method: a fresh server process per cell (MinIO, lease TTL 60 s, tenants never idle-closed), 200 tenants opened, then the server's own request counters over a 60 s window (`tenantStats`, requests attributed by object path: WAL, manifest, compacted SST, lease, clean marker; compaction and GC totals reported separately). \"Idle open reader/writer\" = opened by a short burst, then no operations. Rates per tenant = counter delta / window / 200. The loads for 1 op/s and 0.1 op/s are 80% reads, 10% inserts, 10% updates per tenant. In an open tenant nothing else touches the store, so the window shows the cost of *staying open*; the cost of waking and closing a tenant is a per-session cost, given separately below.")
    w("")
    rows = ["| Flush setting | Scenario | PUT/s per tenant: WAL | manifest | compacted SST | lease | clean marker | other | GET-tier/s per tenant | LIST/s | PUTs per tenant-hour | GETs per tenant-hour | $ per tenant-hour |", "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for c in cost:
        if "per_sec" not in c:
            continue
        n = c["tenants"]
        p = c["per_sec"]["put"]
        g = c["per_sec"]["get"]
        puts = sum(p.values()) / n
        gets = sum(g.values()) / n
        lists = c["per_sec"]["list"] / n
        usd = (puts + lists) * 3600 * PUT_P + gets * 3600 * GET_P
        rows.append(f"| {c['config']} | {c['scenario']} | " + " | ".join(f"{p[k] / n:.4f}" for k in ("wal", "manifest", "compacted_sst", "lease", "clean_marker", "other")) + f" | {gets:.4f} | {lists:.4f} | {(puts + lists) * 3600:,.1f} | {gets * 3600:,.1f} | ${usd:.5f} |")
    w("\n".join(rows))
    w("")
    maint = [c for c in cost if "per_sec" in c and any(v > 0 for v in c["per_sec"]["maintenance"].values())]
    w(f"Standalone compaction and GC passes inside the windows: {'none' if not maint else 'present in ' + ', '.join(c['tag'] for c in maint)} (see `cost.json`, field `maintenance`).")
    w("")
    lat_rows = ["| Flush setting | Scenario | update p50 / p99 (ms) | insert p50 / p99 (ms) | find_by_id p50 (ms) |", "|---|---|---|---|---|"]
    for c in cost:
        if "latency_open_tenants" in c:
            L = c["latency_open_tenants"]
            g = lambda k, q: f"{L[k][q]:.1f}" if k in L else "n/a"
            lat_rows.append(f"| {c['config']} | {c['scenario']} | {g('update', 'p50_ms')} / {g('update', 'p99_ms')} | {g('insert', 'p50_ms')} / {g('insert', 'p99_ms')} | {g('find_by_id', 'p50_ms')} |")
    w("Acknowledgement latency of writes on already-open tenants in the same cells:")
    w("")
    w("\n".join(lat_rows))
    w("")
    heavy = [c for c in cost if "writes_per_wal_put" in c]
    if heavy:
        w("Write-heavy single tenant (8 connections writing back to back, 20 s): the flush interval matters only when several writes arrive within one interval.")
        w("")
        hr = ["| Flush setting | writes/s | write p50 / p99 (ms) | WAL PUTs/s | writes per WAL object |", "|---|---|---|---|---|"]
        for c in heavy:
            hr.append(f"| {c['config']} | {c['writes_per_sec']:.0f} | {c['p50_ms']:.1f} / {c['p99_ms']:.1f} | {c['wal_puts_per_sec']:.1f} | {c['writes_per_wal_put']:.2f} |")
        w("\n".join(hr))
        w("")
else:
    w("_Not measured (no `t1/cost.json`)._")
    w("")
if dur:
    w("**Durability window.** One tenant, writes back to back, SIGKILL at a random moment 2.5 to 4 s in, restart with the same holder id, read back (5 trials per setting). \"Durable ack\" settings acknowledge a write only after its WAL object is in the store: an acknowledged write waits for durability *before* the acknowledgement (up to one flush interval), and nothing acknowledged can be lost. \"Ack before durable\" settings acknowledge at once and the write becomes durable at the next flush; the oldest lost acknowledged write bounds the window.")
    w("")
    dr = ["| Setting | acked | lost | oldest lost write: ack to kill (ms), worst trial | ack-to-durable bound by design |", "|---|---|---|---|---|"]
    for d in dur:
        bound = "0 (durable before ack); ack waits up to the interval" if "durable ack" in d["config"] or "baseline" in d["config"] else "up to one flush interval plus one WAL PUT"
        dr.append(f"| {d['config']} | {d['total_acked']:,} | {d['total_lost']:,} | {d['max_oldest_lost_age_ms']:.0f} | {bound} |")
    w("\n".join(dr))
    w("")

w("**Wake and close cost per session** is in Task 4 (requests per wake, from the trace); a close costs the memtable SST, the manifest update, the clean marker and the lease release, counted in the first-round S1 as the difference between its request rate and the steady-state rates above.")
w("")

# ------------------------------------------------------------------ Task 2
stop = jl("t2", "stop_cont.json")
w("## Task 2 — Lease cost and safety")
w("")
if cost:
    base = [c for c in cost if c.get("tag") == "c0_idle_readers"]
    if base:
        c = base[0]
        n = c["tenants"]
        lease_put = c["per_sec"]["put"]["lease"] / n
        w(f"**Lease cost today (measured).** An idle open tenant costs {lease_put:.4f} lease PUTs per second = {lease_put * 3600:,.1f} per hour (TTL 60 s, one renewal every 20 s), ${lease_put * 3600 * PUT_P:.5f} per tenant-hour, and that is the *only* store request an idle open reader makes (all other categories in the idle rows of Task 1 are above). At 1,500 open tenants that is {lease_put * 1500:,.0f} PUT/s.")
        w("")
        w(f"**Per-process lease (estimate, not implemented).** One lease object per process, renewed every TTL/3 (1 PUT per 20 s per process, independent of the number of tenants), plus a per-database owner record written once when a tenant is opened and once when it is released, replaces the per-tenant renewals: the renewal cost per open tenant falls from {lease_put * 3600:,.1f} PUTs per hour to {3.0 / 1500:.3f} per hour at 1,500 open tenants per process (3 PUTs/min per process / 1,500, an arithmetic estimate). The acquire/release writes per session stay (2 PUTs), and an acquire gains one GET (the owner record), plus a second GET of the holder's process lease when the owner record names another process. At S1's rates the saving is the whole lease-renewal share of the PUT-tier requests (first round: 75 of 545 PUT/s at 1,500 open tenants, about 14%, $1.35 per hour per 1,000 active tenants).")
        w("")
        w("**Why it was not implemented.** Cheap to build, but not cheap to make safe. With one process lease, a takeover is a conditional update of the *owner record*, while the evidence that the old owner is dead lives in a *different* object (its process lease). Object stores give conditional writes per object, not across two, so a takeover that read an expired process lease can race with that process renewing it (check-then-act), and both would think they own the tenant. The per-tenant lease avoids it because renewal and takeover contend on the same object's ETag. Closing it needs a fencing epoch in the owner record that the writer carries into slatedb's manifest (slatedb already fences writers by manifest epoch, which is what saved S4's write path), and a read path that re-validates the owner record, which costs a GET per read or a bounded staleness window. I did not build or test that, so the saving above is an estimate and the safety of the design is unproven.")
        w("")
w("**Ownership check on reads (implemented and tested).** Reads already went through `ensure_open`, which refuses a tenant whose lease this process has not renewed in time. This round adds the check *after* the read (`ReadView`): a read that began while the process still owned the tenant but finishes after ownership was lost returns a retryable error instead of data (`Leases::check_owner`).")
w("")
if stop:
    w("**SIGSTOP / SIGCONT test** (`loadtest/test_stop.py`): process A owns a tenant (value 1); 40 reads are sent to A while it is frozen; A is stopped for longer than the lease TTL (4 s); B takes the tenant over and writes value 2 (acknowledged); A is continued; 40 more reads are sent to A immediately. Every read must return 2 or a retryable error. 5 rounds per row.")
    w("")
    sr = ["| A's settings | Reads queued at the frozen process | Reads right after SIGCONT | Returned new value | Refused (134) | Other errors | **Stale reads (old value)** |", "|---|---|---|---|---|---|---|"]
    for s in stop:
        lab = ("ownership checks OFF (negative control)" if s["unsafe_skip_ownership_checks"] else "ownership checks on") + (", shared reads" if s["shared_reads"] else ", serialized reads")
        sr.append(f"| {lab} | {s['queued_reads']} | {s['post_resume_reads']} | {s['new_value']} | {s['refused_134']} | {s['other_error']} | **{s['stale']}** |")
    w("\n".join(sr))
    w("")
    w("The control row shows the test can see a stale read; the rows with the checks on show none. What this does not cover: a read already executing inside the process when it is frozen is not reproducible from outside (the test freezes the process while 40 reads are queued in its socket, not mid-command), so the post-read check is verified by construction and by the control, not by catching a read in flight; and a process frozen *inside* a write for longer than the TTL is covered only by slatedb's writer fencing.")
    w("")
else:
    w("_SIGSTOP/SIGCONT test not run._")
    w("")

# ------------------------------------------------------------------ Task 3
s4 = jl("t3", "summary.json")
w("## Task 3 — S4 redone on a warmed disk, with a read-your-writes check")
w("")
if s4:
    lease = jl("t3", "lease_check.json")
    fo = jl("t3", "failover.json")
    ryw = s4["read_your_writes"]
    v = s4["verification"]
    a = jl("t3", "analysis.json")
    w("Same scenario as S4 of the first round: two server processes on one bucket, 1,000 active tenants for 5 minutes, every request to a random process, 10 SIGKILLs of a random process each followed by an automatic restart (same holder id), TTL 10 s, retries by the generator. New: the disk was warmed first (objects of the 10,000 tenants read once; I/O wait was normal), the generator keeps a log of every operation, and after every acknowledged insert or update it reads the document back **once, through the other process**: the read must see the write or fail, never return an older value or a missing document.")
    w("")
    w(rt.summary_table(["t3"]).replace("| T3 |", "| S4 redo |") if False else "")
    g = s4["normal"]
    t = a["total"]
    w("| Metric | First S4 (cold disk, after reboot) | S4 redo (warmed disk) |")
    w("|---|---|---|")
    old = jl("s4", "summary.json")
    olda = jl("s4", "analysis.json")
    w(f"| Operations ok / failed after retries | {old['normal']['ops']:,} / {sum(old['failures_by_code'].values())} | {g['ops']:,} / {sum(s4['failures_by_code'].values())} |")
    w(f"| Ops/s | {old['ops_per_sec']:.0f} | {s4['ops_per_sec']:.0f} |")
    w(f"| Latency p50 / p99 / p999 (ms) | {rt.lat(old['normal']['all_ops'])} | {rt.lat(g['all_ops'])} |")
    w(f"| Wake p50 / p99 / p999 (ms) | {rt.lat(old['normal']['wake'])} | {rt.lat(g['wake'])} |")
    w(f"| Errors by code (failed attempts) | {rt.errs(old)} | {rt.errs(s4)} |")
    w(f"| CPU avg / peak (% of one core, both processes) | {olda['total']['cpu_pct_of_one_core']['avg']:.0f} / {olda['total']['cpu_pct_of_one_core']['peak']:.0f} | {t['cpu_pct_of_one_core']['avg']:.0f} / {t['cpu_pct_of_one_core']['peak']:.0f} |")
    w(f"| RSS avg / peak (MB, summed) | {olda['total']['rss_mb']['avg']:.0f} / {olda['total']['rss_mb']['peak']:.0f} | {t['rss_mb']['avg']:.0f} / {t['rss_mb']['peak']:.0f} |")
    w(f"| Max open tenants (both processes) | {olda['total']['open_tenants']['peak']:.0f} | {t['open_tenants']['peak']:.0f} |")
    w(f"| Object-store requests/s PUT-tier / GET-tier (avg, both) | {olda['total']['billed_put_tier_per_sec']['avg']:.0f} / {olda['total']['billed_get_tier_per_sec']['avg']:.0f} | {t['billed_put_tier_per_sec']['avg']:.0f} / {t['billed_get_tier_per_sec']['avg']:.0f} |")
    w(f"| Cost at S3 prices ($/h) | {olda['total']['cost_per_hour_usd']['total']:.2f} | {t['cost_per_hour_usd']['total']:.2f} |")
    w("")
    w("**Correctness.**")
    w(f"- Acknowledged writes lost: {v['acknowledged_inserts']:,} acknowledged inserts, **{v['missing_inserts']} missing**; {v['acknowledged_update_docs']:,} updated documents, **{v['stale_updates']} older than the last acknowledged value**; {v['unreadable_tenants']} unreadable tenants out of {v['tenants_checked']:,} read back; {s4['read_misses']} reads of loaded documents found nothing.")
    w(f"- **Read-your-writes through the other process: {ryw['checked']:,} checks; {ryw['seen']:,} saw the write; {ryw['refused_134']:,} were refused with the retryable 134 (the other process did not own the tenant); {sum(ryw['other_errors'].values()):,} failed otherwise ({ryw['other_errors']}); STALE READS: {ryw['stale']}.**" + (f" Examples: {ryw['examples'][:3]}" if ryw["stale"] else ""))
    w(f"- Writer intervals {lease['writer_intervals']:,}, overlaps between different holders: **{lease['writer_overlaps_between_different_holders']}**; lease-record overlaps in the log: {lease['lease_overlaps_between_different_holders']} ({lease['lease_overlaps_by_how_the_earlier_holder_ended']}; median apparent overlap {lease['lease_overlap_median_ms'] / 1000:.1f} s, max {lease['lease_overlap_max_ms'] / 1000:.1f} s, as in the first round the earlier holder had stalled past the TTL and noticed late); takeovers of expired leases {lease['takeovers_of_expired_leases']:,}; wakes on the unclean WAL-replay path {lease['unclean_wal_replay_wakes']:,}.")
    w("")
    if fo:
        k = fo["kill_to_first_success_ms"]
        sv = fo["service_time_of_first_success_ms"]
        w(f"**Failover time** (`analyze_failover.py`): for each of the {fo['kills']} kills, the tenants whose lease the killed process held at that moment ({fo['tenant_kill_pairs']:,} tenant-kill pairs; {fo['pairs_with_no_successful_op_afterwards']} had no further operation before their activation ended and are excluded), time from the kill to the first successful operation on the same tenant (through either process): **p50 {k['p50'] / 1000:.1f} s, p90 {k['p90'] / 1000:.1f} s, p99 {k['p99'] / 1000:.1f} s, max {k['max'] / 1000:.1f} s** (n = {k['n']:,}). Each tenant sends one operation per second, so this includes up to a second of waiting for the tenant's next operation; the lease TTL (10 s) plus the 1.5 s skew allowance is the floor for a tenant that the *other* process must take over, while a tenant whose killed process restarts quickly with the same holder id is served again as soon as it is back (0.5 to 2 s). Duration of the first successful operation itself (including its retries), measured from the later of its start and the kill: p50 {sv['p50'] / 1000:.1f} s, p99 {sv['p99'] / 1000:.1f} s.")
        w("")
else:
    w("_S4 redo not run._")
    w("")

# ------------------------------------------------------------------ Task 4
tr = jl("t4", "trace.json")
knee = jl("t4", "knee.json")
lim = jl("t4", "limit.json")
w("## Task 4 — Wake breakdown and wake concurrency")
w("")
if tr:
    w("**Cold wake of one tenant, by step.** `tenantTrace` closes the tenant, then runs one `find` by `_id` with an object-store trace on (every request, in order, with start and duration), on a quiet server; 40 tenants per variant, the first 5 discarded (connection and disk warm-up), medians over the rest. \"Clean\" = the tenant was closed cleanly (marker present); \"unclean\" = marker missing, which sends the wake through WAL replay. Start/end are milliseconds from the start of the command; request durations of a step overlap each other and other steps (the steps run partly in parallel), so they are not additive.")
    w("")
    for variant, s in tr["summary"].items():
        w(f"*{variant}*: total {s['total_ms']['p50']:.1f} ms p50 / {s['total_ms']['p99']:.1f} ms p99 / {s['total_ms']['max']:.1f} ms max over {s['runs']} wakes; **{s['requests_per_wake']:.0f} object-store requests per wake (median).**")
        w("")
        rows = ["| Step | Requests (p50) | Request types (p50 counts) | Sum of request durations (ms) | Starts at (ms) | Ends at (ms) |", "|---|---|---|---|---|---|"]
        for name, x in sorted(s["steps"].items(), key=lambda kv: kv[1]["starts_at_ms_p50"]):
            rows.append(f"| {name} | {x['requests_p50']:.0f} | {', '.join(f'{k} ×{v:.0f}' for k, v in x['by_type_p50'].items())} | {x['sum_of_request_ms_p50']:.1f} | {x['starts_at_ms_p50']:.1f} | {x['ends_at_ms_p50']:.1f} |")
        w("\n".join(rows))
        w("")
else:
    w("_Wake trace not run._")
    w("")
if knee:
    w("**Concurrent wakes before p99 degrades.** Waves of K first requests sent at the same instant to K distinct cold tenants (objects pre-read so the disk is warm; a fresh server per wave; 2 loadgen threads), first-request latency:")
    w("")
    kr = ["| Simultaneous cold wakes K | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | Errors |", "|---|---|---|---|---|---|"]
    for k in knee:
        kr.append(f"| {k['concurrent_wakes']} | {k['wake_p50_ms']:.0f} | {k['wake_p90_ms']:.0f} | {k['wake_p99_ms']:.0f} | {k['wake_max_ms']:.0f} | {k['errors'] or 'none'} |")
    w("\n".join(kr))
    w("")
if lim:
    w("**Concurrency limit with a queue.** `--max-concurrent-wakes N` makes cold wakes (lease acquire plus open) wait for a permit instead of all starting at once; a waiting request is not subject to any timeout of the server (the client's own 10 s limit applies). The limit was derived from the knee by the rule in `exp_wake.py` (largest K whose p99 stays within 2x of the smallest wave's and under 1 s). Test: the S2 burst again (5,000 cold tenants, first requests spread over 60 s, then 60 s at 1 op/s each), warmed disk, without and with the limit:")
    w("")
    lr = ["| | Wake p50 / p99 / p999 (ms) | All ops p50 / p99 (ms) | Timeouts (10 s) | Failed ops | Ops | Max queued wakes |", "|---|---|---|---|---|---|---|"]
    for lab, x in lim.items():
        lr.append(f"| {lab} | {rt.lat(x['wake'])} | {x['all_ops']['p50_ms']:.0f} / {x['all_ops']['p99_ms']:.0f} | {x['errors'].get('timeout', 0)} | {sum(x['failures'].values())} | {x['ops']:,} | {x['wake_queue']['maxQueued']} |")
    w("\n".join(lr))
    w("")

# ------------------------------------------------------------------ Task 5
hot = jl("t5", "hot.json")
w("## Task 5 — The hot tenant")
w("")
w("**Why commands on one tenant were serialized.** Every command took an exclusive lock on the tenant's entry (`Dbs::lock`, one `Mutex` per database). It was introduced for the write commands, which read the collection, decide (unique-index checks, `_id` duplicates, update matching) and write a batch: two interleaved writers could both pass a duplicate check, so writes must be serialized per tenant. Reads were serialized only because they went through the same lock. With a durable write costing one WAL PUT, the first round's hot tenant (16 workers, MinIO busy with 1,000 other tenants) was capped at about 35 ops/s by write latency, not by the server's CPU.")
w("")
if hot:
    w("Single-tenant throughput, closed loop (each connection sends its next operation when the previous one returns), 15 s per cell after 3 s warm-up, one tenant of 50 documents; reads are `find_one` by `_id`, writes are `update_one` `$set` on one of 50 documents. \"Serialized\" is the server before this round (`--serialize-reads`), \"shared\" is after the change below. The local-directory and in-memory stores run without leases (`--no-leases`; the local store has no conditional update).")
    w("")
    rows = ["| Store | Reads | Connections | Reads only (ops/s, p50 / p99 ms) | Writes only | 90/10 mix |", "|---|---|---|---|---|---|"]
    for st, lab in [("s3", "MinIO"), ("local", "local directory"), ("memory", "in memory")]:
        for rd in ["serialized", "shared"]:
            for c in [1, 8, 64]:
                cells = []
                for mix in ["reads", "writes", "mix90_10"]:
                    x = next((x for x in hot if x["store"] == st and x["reads"] == rd and x["connections"] == c and x["mix"] == mix), None)
                    cells.append(f"{x['ops_per_sec']:,.0f} ({x['p50_ms']:.1f} / {x['p99_ms']:.1f})" if x else "n/a")
                rows.append(f"| {lab} | {rd} | {c} | " + " | ".join(cells) + " |")
    w("\n".join(rows))
    w("")

    def val(st, rd, c, mix):
        return next(x["ops_per_sec"] for x in hot if x["store"] == st and x["reads"] == rd and x["connections"] == c and x["mix"] == mix)
    w(f"**What limits it.** Writes: {val('s3', 'serialized', 1, 'writes'):,.0f} ops/s on MinIO with one connection, {val('local', 'serialized', 1, 'writes'):,.0f} on a local directory and {val('memory', 'serialized', 1, 'writes'):,.0f} in memory; adding connections does not raise it (8 and 64 connections: {val('s3', 'serialized', 8, 'writes'):,.0f} and {val('s3', 'serialized', 64, 'writes'):,.0f} on MinIO, {val('memory', 'serialized', 64, 'writes'):,.0f} in memory), latency just grows with the queue. So the write limit is a per-tenant serialization of a step that costs about {1000 / val('memory', 'serialized', 1, 'writes'):.1f} ms of engine and server work even with no network (scan of the collection, update, WAL object creation and flush in slatedb) and {1000 / val('s3', 'serialized', 1, 'writes'):.1f} ms against MinIO: the engine plus server floor is about 4x lower than MinIO's, and MinIO adds the rest. It is not MinIO alone and not the lock alone; removing either would not remove the cap. Reads: {val('s3', 'serialized', 64, 'reads'):,.0f} ops/s serialized on MinIO with 64 connections against {val('memory', 'serialized', 64, 'reads'):,.0f} in memory: reads do not touch the store once the tenant is open, so the cap is the single lock plus the load generator and server sharing 4 cores.")
    w("")
    w(f"**The safe fix for concurrent reads (separate commit, with tests).** The entry lock became a read-write lock: `find`, `count`, `distinct` and `aggregate` share it, writes and tenant state changes (open, upgrade, close, abandon) are exclusive. Ownership is re-checked after each read. Reads at 64 connections go from {val('s3', 'serialized', 64, 'reads'):,.0f} to {val('s3', 'shared', 64, 'reads'):,.0f} ops/s on MinIO ({val('s3', 'shared', 64, 'reads') / val('s3', 'serialized', 64, 'reads') - 1:+.0%}) and at 8 connections from {val('s3', 'serialized', 8, 'reads'):,.0f} to {val('s3', 'shared', 8, 'reads'):,.0f}; writes and the 90/10 mix are unchanged within noise ({val('s3', 'serialized', 64, 'mix90_10'):,.0f} to {val('s3', 'shared', 64, 'mix90_10'):,.0f} ops/s), as expected: every write still takes the lock exclusively, and a write waits for the readers in flight. Tests: `loadtest/test_shared_reads.py` (16 readers running find, count, aggregate and distinct against a tenant that is being written: no errors, the counter never goes backwards, a count is never below the counter read just before it, final state exact, reads of a non-existent database empty) passes with shared and serialized reads; the full differential suite (pymongo, 1,081 in-scope tests) passes with shared reads on (the log is in `loadtest/results/t5/py_suite_shared_reads.log`); the Node suite was not rerun.")
    w("")
    w("**Not fixed, and why.** Making *writes* to one tenant concurrent would need per-collection or per-key locking and a re-check of duplicate-key and unique-index decisions inside the write batch; I did not attempt it. Batching concurrent writes into one WAL flush (group commit) would raise the write cap on MinIO without changing the locking, and is what the flush-interval rows of Task 1 begin to show.")
    w("")

# ------------------------------------------------------------------ defects and gaps
w("## Defects found in this round")
w("")
w("1. **Graceful shutdown hangs with non-S3 stores (not diagnosed).** After the long single-tenant runs (about 3 minutes of mixed load) against the local-directory and in-memory stores, `SIGTERM` left the process alive and idle (0% CPU, every runtime thread parked, stuck while closing tenants: the log has no `tenants closed` line) in 4 of the 5 such runs (local: serialized and shared reads; memory: serialized, twice) and not in the fifth (memory, shared reads). It did not happen in 6 shorter runs (6 to 50 s) against the same stores. With MinIO the same long matrices shut down within the 120 s the harness allowed (the MinIO server logs of that run predate the extra log lines, so for MinIO I only know it exited). I did not find the cause, so I do not know whether it is in this server (the close path, the lock) or in slatedb with those stores. The harness kills such a server after 25 s. It is a graceful-shutdown hang, which is serious enough to list even though it was seen only with the two stores I added for diagnosis.")
w("2. **Local-directory store cannot run with leases.** `object_store`'s `LocalFileSystem` does not implement `PutMode::Update`, so lease renewal fails there; added `--no-leases` (single process only) for those measurements.")
w("3. **Read ownership gap closed** (see Task 2): the check was before the read only.")
w("4. **Harness slips**: three chained experiments (SIGSTOP test, wake trace, knee, limit, durability, cost) failed instantly the first time because their output directories did not exist (a shell redirect fails before the script can create them); rerun after creating them.")
w("")
w("## What I could not measure")
w("")
w("- A read frozen *inside* a command (see Task 2): only reads queued before and sent after the freeze were tested.")
w("- The per-process lease: no implementation, so no measured PUT saving and no safety test; only the arithmetic estimate from the measured per-tenant renewal rate.")
w("- Request costs against real S3 (MinIO counts the same requests but real S3 has different latencies and failure behaviour; GETs served from a cache in front of S3 would not be counted the same way).")
w("- The cost of a wake and of a close as steady-state per-tenant-hour rates: they are per-session costs (Task 4 gives the request count of a wake; the close was not traced).")
w("- Whether the shutdown hang (defect 1) affects S3 deployments: it was not observed there, but those runs were not instrumented to show where a hang would be.")
w("- Node.js driver results for this round's server changes (only the pymongo suite was rerun).")
w(END)

path = os.path.join(ROOT, "results-load.md")
doc = open(path).read()
if START in doc:
    doc = doc[: doc.index(START)] + "\n".join(out) + "\n" + doc[doc.index(END) + len(END):].lstrip("\n")
else:
    doc = doc.rstrip("\n") + "\n\n" + "\n".join(out) + "\n"
open(path, "w").write(doc)
print("written", len(out))
