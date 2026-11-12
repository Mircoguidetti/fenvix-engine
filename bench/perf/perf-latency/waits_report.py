"""Summarise the server's WAITS lines: for every command over the threshold, its biggest category; overlaps with stalls
and background jobs. Usage: waits_report.py cook.log [threshold_ms]"""
import re, sys, collections, gzip
p = sys.argv[1]; thr = float(sys.argv[2]) if len(sys.argv) > 2 else 50
op = gzip.open if p.endswith(".gz") else open
reqs, stalls, jobs, polls = [], [], [], []
for line in op(p, "rt", errors="replace"):
    if line.startswith("WAITS t="):
        d = dict(kv.split("=", 1) for kv in line.split()[1:])
        if d["cmd"] in ("hello", "isMaster", "ismaster", "serverStatus", "tenantStats", "getMore") and d["cmd"] != "getMore":
            continue
        reqs.append(d)
    elif line.startswith("WAITS_STALL"):
        d = dict(kv.split("=", 1) for kv in line.split()[1:]); stalls.append((float(d["t"]), float(d["ms"])))
    elif line.startswith("WAITS_JOB"):
        d = dict(kv.split("=", 1) for kv in line.split()[1:]); jobs.append(d)
    elif line.startswith("WAITS_POLL"):
        d = dict(kv.split("=", 1) for kv in line.split()[1:]); polls.append(d)
cats = ["admit","lock_w","lock_r","refresh","stage","stage_snap","log_ack","visible","fallback","wal_flush","io_wait","comp_start","read_get","read_scan"]
def val(d, k):
    v = d.get(k); return float(v.split("/")[0]) if v else 0.0
stall_iv = [(t, t + ms) for t, ms in stalls]
def stall_overlap(t0, t1):
    return sum(max(0, min(t1, b) - max(t0, a)) for a, b in stall_iv)
slow = [r for r in reqs if float(r["ms"]) >= thr]
print(f"commands over {thr} ms: {len(slow)} (of {len(reqs)} logged)")
by_cmd = collections.Counter((r["cmd"], r["coll"]) for r in slow); print("by command:", by_cmd.most_common(10))
dom = collections.Counter(); tot = collections.Counter()
for r in slow:
    t0 = float(r["t"]); ms = float(r["ms"])
    parts = {k: val(r, k) for k in cats}
    parts["rest"] = float(r.get("rest", 0))
    parts["(stall overlap)"] = stall_overlap(t0, t0 + ms)
    for k, v in parts.items(): tot[k] += v
    k = max((k for k in parts if k != "(stall overlap)"), key=lambda k: parts[k])
    if k == "rest" and parts["(stall overlap)"] > 0.5 * ms: k = "rest, runtime stalled"
    dom[k] += 1
print("largest share per slow command:", dom.most_common())
print("total ms in slow commands by category:", {k: round(v) for k, v in tot.most_common()})
print(f"stalls >=10 ms: {len(stalls)}, total {sum(m for _, m in stalls):.0f} ms, max {max([m for _, m in stalls] or [0]):.0f}")
print("long polls by job:", collections.Counter(p["job"] for p in polls).most_common(), "max", max([float(p["ms"]) for p in polls] or [0]))
jc = collections.defaultdict(list)
for j in jobs: jc[j["job"]].append(float(j["ms"]))
print("jobs:", {k: (len(v), round(sum(v)/len(v)), round(max(v))) for k, v in jc.items()})
