#!/usr/bin/env python3
"""Compaction 2 step 5: summarise the cold reads of fx_cold runs.

Usage: python3 -I analyze.py results/*.jsonl > results/summary.txt

For every run (cfg) and every group of reads it prints the mean, p50 and p99 of S3 GETs per cold read, the share of reads
in one trip (one wave of S3 GETs), NVMe block reads, bytes fetched, wall time (with the injected S3 latency), and where the
GETs went. GETs = sealed + delta runs on S3 + SlateDB L0 + SlateDB sorted runs (NVMe reads are not GETs).
"""
import collections
import json
import sys

SRC = ["sealed", "delta_s3", "l0", "slate_runs"]
GET_PRICE = 0.0004 / 1000  # $ per GET, S3 Standard eu-west-1 (DECLARED, the price the plan and D1 used)


def pct(v, p):
    v = sorted(v)
    if not v:
        return 0
    i = min(len(v) - 1, max(0, int(round(p / 100 * (len(v) - 1)))))
    return v[i]


def gets(x):
    return sum(x[s] for s in SRC)


def line(name, v):
    n = len(v)
    if n == 0:
        return f"  {name}: none"
    g = [gets(x) for x in v]
    tr = [x["trips"] for x in v]
    one = sum(1 for t in tr if t <= 1) / n
    by = {s: sum(x[s] for x in v) / n for s in SRC}
    touch = {s: sum(1 for x in v if x[s] > 0) / n for s in SRC}
    nv = sum(x["nvme"] for x in v) / n
    b = sum(x["bytes"] for x in v) / n
    ms = [x["ms"] for x in v]
    wrong = sum(1 for x in v if not x["ok"])
    return (
        f"  {name}: n {n}, GETs mean {sum(g)/n:.2f} p50 {pct(g,50)} p99 {pct(g,99)} max {max(g)}; trips mean {sum(tr)/n:.2f} p50 {pct(tr,50)} p99 {pct(tr,99)}, one trip {one*100:.1f}%; "
        f"by source sealed {by['sealed']:.2f} delta_s3 {by['delta_s3']:.2f} L0 {by['l0']:.2f} slate_runs {by['slate_runs']:.2f} (reads touching: {touch['sealed']*100:.0f}/{touch['delta_s3']*100:.0f}/{touch['l0']*100:.0f}/{touch['slate_runs']*100:.0f}%); "
        f"NVMe reads {nv:.2f}; KB fetched mean {b/1024:.0f} p99 {pct([x['bytes'] for x in v],99)/1024:.0f}; ms p50 {pct(ms,50):.1f} p99 {pct(ms,99):.1f}; wrong {wrong}"
    )


def age_bucket(a):
    if a <= 1 / 1440:
        return "a: last write <= 1 min"
    if a <= 1 / 24:
        return "b: <= 1 hour"
    if a <= 1:
        return "c: <= 1 day"
    if a <= 7:
        return "d: <= 7 days"
    return "e: > 7 days (sealed or old)"


def main():
    rows, summ = [], []
    for f in sys.argv[1:]:
        for l in open(f):
            x = json.loads(l)
            (summ if x.get("summary") else rows).append(x)
    by_cfg = collections.OrderedDict()
    for x in rows:
        by_cfg.setdefault(x["cfg"], []).append(x)
    for cfg, v in by_cfg.items():
        print(f"== {cfg}")
        s = [y for y in summ if y["cfg"] == cfg]
        if s:
            s = s[0]
            print(f"  run: reads {s['reads']}, wrong {s['wrong']}, reseals in window {s['reseals']} (R3 {s['r3_reseals']}, theta {s['theta_reseals']}), stored amplification {s['amp']:.2f}, reseal raw bytes per absorbed byte {s['reseal_raw']/max(1,s['absorbed_raw']):.2f}, L0 max {s['max_l0']}, wall {s['wall_s']} s")
        doc = [x for x in v if not x["pat"].startswith("agent")]
        print(line("ALL document reads (patterns + zipf top)", doc))
        pats = [x for x in doc if x["pat"] != "zipftop"]
        for kind in ["latest20", "range", "byid"]:
            print(line(f"all patterns, {kind}", [x for x in pats if x["kind"] == kind]))
        print(" by pattern (all three kinds):")
        for p in ["idle", "once", "weekly", "daily", "session", "bursty", "hourly", "zipftop"]:
            print(line(p, [x for x in doc if x["pat"] == p]))
        print(" by pattern, latest20 only:")
        for p in ["idle", "once", "weekly", "daily", "session", "bursty", "hourly", "zipftop"]:
            print(line(p, [x for x in doc if x["pat"] == p and x["kind"] == "latest20"]))
        print(" by age of the last write (pattern customers):")
        ages = collections.defaultdict(list)
        for x in pats:
            ages[age_bucket(x["age"])].append(x)
        for k in sorted(ages):
            print(line(k, ages[k]))
        print(" agent threads (get_tuple):")
        for k in sorted({x["kind"] for x in v if x["pat"].startswith("agent")}):
            for p in ["agent_young", "agent_old"]:
                print(line(f"{p} {k}", [x for x in v if x["pat"] == p and x["kind"] == k]))
        n_delta = [x["n_delta"] for x in v]
        print(f"  state at the measurement points: delta runs mean {sum(n_delta)/len(n_delta):.1f} max {max(n_delta)}, on NVMe mean {sum(x['n_local'] for x in v)/len(v):.1f}; SlateDB L0 mean {sum(x['l0_ssts'] for x in v)/len(v):.1f}, sorted runs mean {sum(x.get('slate_srs',0) for x in v)/len(v):.1f}")
        print()


if __name__ == "__main__":
    main()
