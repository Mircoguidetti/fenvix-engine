#!/usr/bin/env python3
"""Turns loadtest/out/<scenario>/{summary.json,samples.jsonl} into numbers for results-load.md.

    analyze.py <scenario-dir> [--skip-secs 10]     writes analysis.json next to the inputs and prints it

Per server process: CPU (percent of one core; 400 = all four cores of this machine), resident memory, open
tenants, object-store requests per second by type. Requests are priced at S3 Standard list prices
(PUT/COPY/POST/LIST $0.005 per 1,000, GET and the rest $0.0004 per 1,000), from the server's own request
counters (every request it makes: lifecycle, leases, compaction, GC, listings).
"""
import json
import os
import statistics
import sys

PUT_PRICE, GET_PRICE = 0.005 / 1000, 0.0004 / 1000


def load_samples(d):
    return [json.loads(l) for l in open(os.path.join(d, "samples.jsonl")) if l.strip()]


def rates(series):
    """Per-interval rate of a cumulative counter given [(t, value, pid)]; a drop means a restart (counter restarted at 0)."""
    out = []
    for (t0, v0, p0), (t1, v1, p1) in zip(series, series[1:]):
        dt = t1 - t0
        if dt <= 0:
            continue
        d = v1 - v0 if (p0 == p1 and v1 >= v0) else v1
        out.append((t1, d / dt))
    return out


def stat(vals):
    if not vals:
        return {"avg": 0, "peak": 0, "p50": 0}
    return {"avg": sum(vals) / len(vals), "peak": max(vals), "p50": statistics.median(vals)}


def analyze(d, skip=10.0):
    samples = [s for s in load_samples(d) if s["t"] >= skip]
    n_srv = len(samples[0]["servers"]) if samples else 0
    res = {"servers": [], "samples": len(samples)}
    for i in range(n_srv):
        ser = [(s["t"], s["servers"][i]) for s in samples]
        up = [(t, x) for t, x in ser if x.get("up")]
        cpu = rates([(t, x["cpu_s"], x["pid"]) for t, x in up])
        entry = {
            "uri": up[0][1]["uri"] if up else "",
            "up_fraction": len(up) / max(len(ser), 1),
            "cpu_pct_of_one_core": stat([r * 100 for _, r in cpu]),
            "rss_mb": stat([x["rss_mb"] for _, x in up]),
            "open_readers": stat([x["open_readers"] for _, x in up]),
            "open_writers": stat([x["open_writers"] for _, x in up]),
            "open_tenants": stat([x["open_readers"] + x["open_writers"] for _, x in up]),
            "entries": stat([x["entries"] for _, x in up]),
            "leases_held": stat([x["leases_held"] for _, x in up]),
            "requests_per_sec": {},
        }
        for k in ("put", "copy", "list", "get", "head", "delete", "billed_put_tier", "billed_get_tier"):
            entry["requests_per_sec"][k] = stat([r for _, r in rates([(t, x["req"][k], x["pid"]) for t, x in up])])
        last = up[-1][1] if up else {}
        entry["lease_stats_last"] = last.get("lease_stats", {})
        res["servers"].append(entry)
    # Totals over all server processes, interval by interval (the processes are sampled together).
    tot_put, tot_get, tot_open, tot_cpu, tot_rss = [], [], [], [], []
    for a, b in zip(samples, samples[1:]):
        dt = b["t"] - a["t"]
        if dt <= 0:
            continue
        p = g = cpu = 0.0
        for x, y in zip(a["servers"], b["servers"]):
            if x.get("up") and y.get("up"):
                same = x["pid"] == y["pid"]
                f = (lambda k: (y["req"][k] - x["req"][k]) if same and y["req"][k] >= x["req"][k] else y["req"][k])
                p += f("billed_put_tier")
                g += f("billed_get_tier")
                cpu += ((y["cpu_s"] - x["cpu_s"]) if same and y["cpu_s"] >= x["cpu_s"] else y["cpu_s"]) / dt * 100
        tot_put.append(p / dt)
        tot_get.append(g / dt)
        tot_cpu.append(cpu)
        tot_open.append(sum(s["open_readers"] + s["open_writers"] for s in b["servers"] if s.get("up")))
        tot_rss.append(sum(s["rss_mb"] for s in b["servers"] if s.get("up")))
    res["total"] = {
        "cpu_pct_of_one_core": stat(tot_cpu), "rss_mb": stat(tot_rss), "open_tenants": stat(tot_open),
        "billed_put_tier_per_sec": stat(tot_put), "billed_get_tier_per_sec": stat(tot_get),
    }
    avg_put, avg_get = res["total"]["billed_put_tier_per_sec"]["avg"], res["total"]["billed_get_tier_per_sec"]["avg"]
    res["total"]["cost_per_hour_usd"] = {
        "put_tier": avg_put * 3600 * PUT_PRICE, "get_tier": avg_get * 3600 * GET_PRICE,
        "total": avg_put * 3600 * PUT_PRICE + avg_get * 3600 * GET_PRICE,
    }
    # Other processes
    for name in ("loadgen", "minio"):
        ser = []
        for s in samples:
            x = s.get("procs", {}).get(name) if name != "loadgen" else s.get("loadgen")
            if x:
                ser.append((s["t"], x["cpu_s"], 0))
        mem = [s["procs"][name]["rss_mb"] if name != "loadgen" else s["loadgen"]["rss_mb"] for s in samples if (s.get("procs", {}).get(name) if name != "loadgen" else s.get("loadgen"))]
        res[name] = {"cpu_pct_of_one_core": stat([r * 100 for _, r in rates(ser)]), "rss_mb": stat(mem)}
    json.dump(res, open(os.path.join(d, "analysis.json"), "w"), indent=1)
    return res


if __name__ == "__main__":
    skip = 10.0
    if "--skip-secs" in sys.argv:
        skip = float(sys.argv[sys.argv.index("--skip-secs") + 1])
    print(json.dumps(analyze(sys.argv[1], skip), indent=1))
