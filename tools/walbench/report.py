#!/usr/bin/env python3
"""results-walbench.md from results-walbench/ (raw JSON, prices.json, cost_live.csv, teardown.json).

Pass criteria are fixed in docs/walbench.md BEFORE the run and applied here mechanically:
  loss     zero acknowledged writes lost: every crash cycle, the quorum failure scenario, the EBS reattach cycles
  latency  per configuration (backend x variant): p99 ack < 15 ms in every closed-loop cell (200 B / 1 KB x conc 1/8/64)
  cost     per configuration: variable cost per million acknowledged writes < 1.5 USD in every closed-loop cell,
           from the requests / WCU / bytes counted in that cell at the fetched prices (+ 24 h of storage of what it wrote).
           EBS and the quorum have (almost) no per-write charge: their fixed cost per hour is reported separately with
           the write rate at which it equals 1.5 USD per million; the criterion is applied to the variable cost.
A configuration passes when all three hold; a backend passes when at least one of its configurations passes.
usage: report.py [results dir] [output .md]"""
import csv
import glob
import json
import os
import statistics
import sys

D = sys.argv[1] if len(sys.argv) > 1 else "results-walbench"
OUTMD = sys.argv[2] if len(sys.argv) > 2 else "results-walbench.md"
RAW = f"{D}/raw/results"
P99_MAX, COST_MAX = 15.0, 1.5
NAMES = {"s3": "S3 Standard", "express": "S3 Express One Zone", "ddb": "DynamoDB on-demand", "file": "EBS gp3 + fdatasync", "quorum": "3-AZ quorum (2 of 3)"}
ORDER = ["s3", "express", "ddb", "file", "quorum"]


def load(p, default=None):
    try:
        return json.load(open(p))
    except Exception:
        return default


prices = load(f"{D}/prices.json", {})
F = None
if prices:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from infra import flat_prices
    try:
        F = flat_prices(prices)
    except Exception:
        F = None
res = {os.path.basename(p)[:-5]: load(p) for p in glob.glob(f"{RAW}/*.json")}


def bkey(b):
    return str(b).lower().replace("ddb", "ddb")


def variable_usd(cn, size_bytes_written=0):
    """USD for the counted requests of one cell (+ 24 h of storage of the bytes it wrote)."""
    g = lambda k: cn.get(k, 0) or 0  # noqa: E731
    usd = ((g("s3_put") + g("s3_list")) * F["s3_put"] + g("s3_get") * F["s3_get"]
           + (g("xz_put") + g("xz_list")) * F["xz_put"] + g("xz_get") * F["xz_get"]
           + g("xz_put_bytes") / 2**30 * F["xz_upload_gb"] + g("xz_get_bytes") / 2**30 * F["xz_retrieval_gb"]
           + g("ddb_wcu") * F["ddb_wru"] + g("ddb_rcu") * F["ddb_rru"])
    qs, qr = cn.get("q_sent", [0, 0, 0]), cn.get("q_recv", [0, 0, 0])
    usd += (qs[1] + qs[2] + qr[1] + qr[2]) / 2**30 * F["inter_az_gb"] * 2
    day = 1 / 30
    usd += g("s3_put_bytes") / 2**30 * F["s3_gb_month"] * day + g("xz_put_bytes") / 2**30 * F["xz_gb_month"] * day + g("ddb_put_bytes") / 2**30 * F["ddb_gb_month"] * day
    return usd


def fmt(x, d=2):
    return "–" if x is None else (f"{x:.{d}f}" if isinstance(x, (int, float)) else str(x))


cells = [r for r in res.values() if r and r.get("kind") == "cell" and not r["name"].startswith("smoke")]
by_cfg = {}
for r in cells:
    b = bkey(r["backend"])
    r["_b"] = b
    if F and r.get("writes_acked"):
        r["_usd_m"] = variable_usd(r["counters"]) / r["writes_acked"] * 1e6
    by_cfg.setdefault((b, r["variant"]), []).append(r)

crash = {bkey(r["backend"]): r for r in res.values() if r and r.get("kind") == "crashtest"}
qt = res.get("quorumtest")
reattach = load(f"{D}/raw/reattach.json")
fleet = [r for r in res.values() if r and r.get("kind") == "fleet"]
state = load(f"{D}/state.json", {})
td = load(f"{D}/teardown.json", {})
cost_final = load(f"{D}/cost_final.json", {})
L = []
w = L.append

w("# results-walbench: where to put the durable write log (branch `walbench`)\n")
w(f"Run `{state.get('run', '?')}`, eu-west-1. Client and EBS volume in AZ id `{state.get('express_az_id', '?')}` (the AZ of the Express directory bucket); "
  f"quorum nodes in AZ ids {', '.join(sorted({z.get('az_id', '?') for k, z in state.get('zones', {}).items() if k.startswith('node')}))}. "
  f"Instances {', '.join(sorted(set(['c6i.large'])))}, AMI `{state.get('ami_name', '?')}`. Raw JSON: `results-walbench/raw/`. "
  "Method, variants and the pass criteria (fixed before the run): `docs/walbench.md`.\n")
if state.get("automatic_stop"):
    w(f"**The run was stopped automatically at the spend threshold**: {state['automatic_stop']}. Steps not run are listed as not measured.\n")
if state.get("error"):
    w(f"**The orchestrator stopped on an error** ({state['error']}); what was measured before it is below.\n")

# ---------------------------------------------------------------- pass table
w("## Against the pass criteria\n")
w(f"Criteria: zero acknowledged writes lost; p99 ack < {P99_MAX:.0f} ms in every closed-loop cell; variable cost < {COST_MAX} USD per million writes in every cell.\n")
w("| backend | variant | loss | worst p99 (ms) | p99 | worst variable USD / M writes | cost | configuration |")
w("|---|---|---|---|---|---|---|---|")
backend_pass = {}
for b in ORDER:
    cr = crash.get(b)
    lost = None
    if cr:
        lost = cr.get("lost_total", 0)
        if cr.get("harness_failures"):
            lost = lost if lost else 0
    if b == "quorum" and qt:
        lost = (lost or 0) + (qt.get("lost") or 0)
    if b == "file" and reattach and reattach.get("cycles"):
        lost = (lost or 0) + sum(c.get("lost", 0) for c in reattach["cycles"])
    loss_ok = None if lost is None else lost == 0
    variants = sorted({v for (bb, v) in by_cfg if bb == b})
    if not variants:
        w(f"| {NAMES[b]} | – | {'not measured' if lost is None else lost} | not measured | – | not measured | – | **not measured** |")
        continue
    for v in variants:
        rs = by_cfg[(b, v)]
        complete = len(rs) == 6 and all(r.get("latency", {}).get("n") for r in rs)
        p99 = max((r["latency"].get("p99") or 1e9) for r in rs)
        usd = max((r.get("_usd_m") or 0) for r in rs) if F else None
        lat_ok = complete and p99 < P99_MAX
        cost_ok = usd is not None and usd < COST_MAX
        ok = bool(loss_ok and lat_ok and cost_ok)
        backend_pass[b] = backend_pass.get(b, False) or ok
        w(f"| {NAMES[b]} | {v} | {'0 lost' if loss_ok else ('not measured' if lost is None else f'**{lost} LOST**')} | {fmt(p99)} | {'pass' if lat_ok else ('incomplete' if not complete else 'FAIL')} "
          f"| {fmt(usd, 3)} | {'pass' if cost_ok else 'FAIL'} | {'**PASS**' if ok else 'fail'} |")
for r in sorted(fleet, key=lambda r: (ORDER.index(bkey(r["backend"])), r["layout"], r["variant"])):
    b = bkey(r["backend"])
    cr = crash.get(b)
    lost = cr.get("lost_total") if cr else None
    loss_ok = None if lost is None else lost == 0
    n = r.get("writes_measured") or 0
    p99 = r["latency"].get("p99")
    usd = variable_usd(r["counters"]) / n * 1e6 if F and n else None
    lat_ok = p99 is not None and p99 < P99_MAX and not r.get("write_errors")
    cost_ok = usd is not None and usd < COST_MAX
    ok = bool(loss_ok and lat_ok and cost_ok)
    backend_pass[b] = backend_pass.get(b, False) or ok
    w(f"| {NAMES[b]} | fleet {r['layout']} {r['variant']} ({r['dbs']} dbs × {r['rate_per_db']}/s) | {'0 lost' if loss_ok else ('not measured' if lost is None else f'**{lost} LOST**')} "
      f"| {fmt(p99)} | {'pass' if lat_ok else 'FAIL'} | {fmt(usd, 3)} | {'pass' if cost_ok else 'FAIL'} | {'**PASS**' if ok else 'fail'} |")
w("")
w("Closed-loop configurations are judged on all 6 cells (200 B / 1 KB × conc 1, 8, 64); fleet configurations on their one cell (1 KB). "
  "Loss is judged per backend (all its crash cycles, plus the quorum scenario / EBS reattach cycles).\n")
w("Backends with at least one configuration meeting all three criteria: " + (", ".join(NAMES[b] for b in ORDER if backend_pass.get(b)) or "none") + ".")
w("No other ranking is made.\n")

# ---------------------------------------------------------------- latency tables
w("## Ack latency (closed loop, ms)\n")
w("Each cell: 100 warm-up writes, then up to 1500 measured writes (or 90 s). `conc` writers on one log, each waits for its ack. Acks are released in log order.\n")
w("| backend | variant | size | conc | p50 | p95 | p99 | max | writes/s | avg batch | requests / write | variable USD / M |")
w("|---|---|---|---|---|---|---|---|---|---|---|---|")
for b in ORDER:
    for (bb, v), rs in sorted(by_cfg.items()):
        if bb != b:
            continue
        for r in sorted(rs, key=lambda r: (r["size"], r["conc"])):
            l = r["latency"]
            cn = r["counters"]
            nreq = sum(cn.get(k, 0) for k in ("s3_put", "xz_put", "ddb_put", "file_syncs"))
            w(f"| {NAMES[b]} | {v} | {r['size']} B | {r['conc']} | {fmt(l.get('p50'))} | {fmt(l.get('p95'))} | {fmt(l.get('p99'))} | {fmt(l.get('max'))} "
              f"| {fmt(r.get('writes_per_s'), 0)} | {fmt(r.get('avg_batch'), 1)} | {fmt(nreq / r['writes_acked'] if r.get('writes_acked') else None, 3)} | {fmt(r.get('_usd_m'), 3)} |"
              + (f" errors: {r['write_errors']}" if r.get("write_errors") else ""))
w("")

# ---------------------------------------------------------------- fleet
w("## Many databases (open loop)\n")
if fleet:
    f0 = fleet[0]
    w(f"{f0['dbs']} databases, each {f0['rate_per_db']} write/s (Poisson), {f0['size']} B, latency from the scheduled time. "
      "`perdb` = one log per database; `shared` = one batch for all databases.\n")
    w("| backend | layout | variant | writes | p50 | p99 | max | commits / write | variable USD / M |")
    w("|---|---|---|---|---|---|---|---|---|")
    for r in sorted(fleet, key=lambda r: (ORDER.index(bkey(r["backend"])), r["layout"], r["variant"])):
        cn = r["counters"]
        n = r.get("writes_measured") or 0
        nreq = sum(cn.get(k, 0) for k in ("s3_put", "xz_put", "ddb_put"))
        usd = variable_usd(cn) / n * 1e6 if F and n else None
        w(f"| {NAMES[bkey(r['backend'])]} | {r['layout']} | {r['variant']} | {n} | {fmt(r['latency'].get('p50'))} | {fmt(r['latency'].get('p99'))} | {fmt(r['latency'].get('max'))} "
          f"| {fmt(nreq / n if n else None, 3)} | {fmt(usd, 3)} |")
else:
    w("Not measured.")
w("")

# ---------------------------------------------------------------- crash
w("## Crash test (kill -9 of the writer, restart, verify)\n")
w("Per cycle: random variant, concurrency (1/8/64), size (200 B / 1 KB), kill after a random 150–1500 ms of writing; a new process reads the log back. "
  "An id counts as acknowledged only after the harness read its ack line (strictly after the ack). Quorum: the read uses a random pair of the 3 nodes.\n")
w("| backend | cycles | acknowledged writes | lost | corrupt batches | bad payloads | harness failures | recovery p50 / max (ms) |")
w("|---|---|---|---|---|---|---|---|")
for b in ORDER:
    cr = crash.get(b)
    if not cr:
        w(f"| {NAMES[b]} | not measured | | | | | | |")
        continue
    cy = [c for c in cr["cycles"] if "acked" in c]
    rec = [c.get("recover_ms") for c in cy if isinstance(c.get("recover_ms"), (int, float))]
    corrupt = sum(len([x for x in (c.get("bad_batches") or []) if not str(x).startswith("end of log")]) for c in cy)
    badp = sum(c.get("bad_payload") or 0 for c in cy)
    w(f"| {NAMES[b]} | {cr['cycles_run']} / {cr['cycles_planned']} | {cr['acked_total']} | {cr['lost_total']} | {corrupt} | {badp} | {cr['harness_failures']} "
      f"| {fmt(statistics.median(rec) if rec else None, 0)} / {fmt(max(rec) if rec else None, 0)} |")
w("")
w("A torn last frame of a local log after kill -9 is expected (never acknowledged) and is not counted as corruption. "
  "kill -9 kills the process, not the machine: for EBS the page cache survives it, so the machine-loss case is the reattach test below.\n")

# ---------------------------------------------------------------- quorum
w("## Quorum: a node killed, a stale node returning\n")
if qt:
    w(f"8 writers, 1 KB, group commit. Acknowledged {qt['acked']}, recovered from nodes 1+2 only: {qt['recovered_from_nodes_1_2']}, **lost {qt['lost']}**.\n")
    for e in qt["events"]:
        extra = {k: v for k, v in e.items() if k not in ("t_s", "event")}
        w(f"- {e['t_s']:.1f} s: {e['event']}" + (f" — `{json.dumps(extra)}`" if extra else ""))
    w("\n| phase | acks | p50 | p99 | max |\n|---|---|---|---|---|")
    for k, v in qt["latency_phases"].items():
        w(f"| {k} | {v.get('n')} | {fmt(v.get('p50'))} | {fmt(v.get('p99'))} | {fmt(v.get('max'))} |")
else:
    w("Not measured.")
w("")

# ---------------------------------------------------------------- reattach
w("## EBS: force-detach while writing, attach to the other instance, resume\n")
if reattach and reattach.get("cycles"):
    w("| cycle | from→to | detach call → available (s) | attach call → attached (s) | attach call → device visible (s) | mount (ms) | log recovery (ms) | first ack (ms) | detach call → first ack (s) | acked before | lost |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    for c in reattach["cycles"]:
        if "error" in c:
            w(f"| {c['cycle']} | | error: {c['error']} | | | | | | | | |")
            continue
        w(f"| {c['cycle']} | {c['from']}→{c['to']} | {fmt(c['detach_api_to_available_s'], 1)} | {fmt(c['attach_api_to_attached_s'], 1)} | {fmt(c['attach_call_to_device_visible_s'], 1)} "
          f"| {fmt(c['mount_ms'], 0)} | {fmt(c['recover_ms'], 0)} | {fmt(c['first_ack_after_recovery_ms'], 0)} | {fmt(c['total_detach_call_to_first_ack_s'], 1)} | {c['acked_before_total']} | {c['lost']} |")
    w(f"\n{reattach.get('note', '')}. Both instances were already running (warm standby); a cold start would add the instance launch.\n")
else:
    w(f"Not measured{(': ' + reattach['error']) if reattach and reattach.get('error') else ''}.\n")

# ---------------------------------------------------------------- fixed costs
if F:
    w("## Fixed costs (EBS, quorum) at the fetched prices\n")
    gp3 = 20 * F["gp3_gb_month"] / 730
    q = 3 * (F["ec2_hour"] + F["ipv4_hour"] + 10 * F["gp3_gb_month"] / 730)
    w(f"- EBS log volume (20 GB gp3, baseline IOPS/throughput): {gp3:.4f} USD/h. Equal to {COST_MAX} USD per million writes at {gp3 / COST_MAX * 1e6 / 3600:.2f} writes/s; below that rate it costs more per write.")
    w(f"- Quorum log nodes as measured (3 × c6i.large + public IPv4 + 10 GB root): {q:.4f} USD/h, = {COST_MAX} USD/M at {q / COST_MAX * 1e6 / 3600:.0f} writes/s. "
      "If the three logs live on machines that exist anyway (the database replicas), only the inter-AZ transfer is extra.")
    w("")

# ---------------------------------------------------------------- prices
w("## Prices used (eu-west-1, fetched at run time)\n")
if prices:
    w(f"Fetched {prices.get('fetched_utc')}. Public offer files (publication dates below) and the AWS Price List Query API; the human-readable pages are listed for reference.\n")
    w("| item | USD | unit | source |\n|---|---|---|---|")
    for grp in ("s3", "dynamodb", "vpc", "transfer", "ec2"):
        g = prices.get(grp, {})
        src = g.get("_source", {})
        for k, v in g.items():
            if k.startswith("_"):
                continue
            s = f"{src.get('url', v.get('source', ''))} (published {src.get('publicationDate', v.get('publicationDate', '?'))})"
            w(f"| {grp}.{k}: {v.get('description', '')} | {v.get('usd')} | {v.get('unit', '')} | {s} |")
    w("\nPages: " + "; ".join(f"{k}: {u}" for k, u in prices.get("pages", {}).items()))
else:
    w("Not fetched.")
w("")

# ---------------------------------------------------------------- spend & teardown
w("## Spend and clean-up\n")
rows = list(csv.DictReader(open(f"{D}/cost_live.csv"))) if os.path.exists(f"{D}/cost_live.csv") else []
if rows:
    w(f"Live spend log `results-walbench/cost_live.csv` ({len(rows)} rows, every 20 s). Last estimate: **{float(rows[-1]['total_usd']):.2f} USD** "
      f"(cap {rows[-1]['cap_usd']}, automatic stop at {rows[-1]['stop_usd']}). This is computed from counted requests and instance/volume time at list prices, with margins; "
      "AWS billing data lags by about a day and was not read.\n")
v = td.get("verify", {})
if v:
    w(f"Teardown verify ({v.get('checked_utc')}): **{'nothing left' if v.get('clean') else 'RESOURCES LEFT: ' + json.dumps(v.get('left')) + json.dumps(v.get('tag_api_extra'))}** "
      "(instances, volumes, security groups, network interfaces, S3 buckets, directory buckets, DynamoDB tables, plus the Resource Groups Tagging API for tag project=fenvix-walbench).\n")

# ---------------------------------------------------------------- not measured
w("## What was not measured\n")
w("- Machine crash of the writer for S3 / Express / DynamoDB / quorum (the writer is killed with kill -9; for these backends the ack comes from a remote service, so a process kill and a machine loss look the same to the log). For EBS the machine-loss case is the force-detach test.")
w("- Loss of an AZ, of the Express directory bucket's AZ, or service-side incidents; long runs (each cell is ~1500 writes; p99 from a short window).")
w("- Writes larger than 1 KB, read latency, compaction or truncation of the log, cost of retaining the log beyond 24 h.")
w("- The quorum protocol is benchmark grade (no epochs/fencing, catch-up from the writer's buffer); a production protocol adds work not measured here.")
w("- Group-commit windows are timer based (tokio, 1 ms resolution): a 1 ms window waits 1–2 ms.")
w("- S3 Express `CreateSession` calls are made by the SDK and are not counted (one per bucket per ~5 minutes).")
skipped = load(f"{D}/raw/markers/client-done.json", {}).get("skipped")
if skipped:
    w(f"- Backends skipped after a failed smoke test: {', '.join(skipped)}.")
missing = [n for n in ("crash-s3", "crash-express", "crash-ddb", "crash-file", "crash-quorum", "quorumtest") if n not in res]
if missing:
    w(f"- Steps without a result: {', '.join(missing)}.")
open(OUTMD, "w").write("\n".join(L) + "\n")
print(f"wrote {OUTMD}")
