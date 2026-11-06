#!/usr/bin/env python3
"""Summarizes the 5-second windows of one loadgen output (write.jsonl or read.jsonl of run.sh).

Usage: summarize.py FILE [--every SECONDS] [--from SECONDS] [--to SECONDS]
Prints one line per period (default 60 s) and, for the part between --from and --to, the totals:
operations, operations per second, server CPU seconds and cores, server CPU microseconds per operation,
generator cores, and the documents written before the period (the size of the data the period works on).
"""
import argparse, json


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def agg(ws):
    ops = sum(w["ops"] for w in ws)
    secs = sum(w["secs"] for w in ws)
    cpu = sum(w["server_cpu_s"] for w in ws)
    gen = sum(w["gen_cores"] * w["secs"] for w in ws)
    return {"ops": ops, "secs": secs, "ops_per_sec": ops / secs if secs else 0, "server_cores": cpu / secs if secs else 0,
            "us_per_op": cpu * 1e6 / ops if ops else 0, "gen_cores": gen / secs if secs else 0,
            "p50_ms_median": sorted(w.get("p50_ms", 0) for w in ws)[len(ws) // 2] if ws else 0,
            "p99_ms_max": max((w.get("p99_ms", 0) for w in ws), default=0)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--every", type=float, default=60)
    ap.add_argument("--from", dest="t0", type=float, default=0)
    ap.add_argument("--to", dest="t1", type=float, default=1e12)
    a = ap.parse_args()
    ws = load(a.file)
    period, cur, start, before = [], [], 0.0, 0
    for w in ws:
        cur.append(w)
        if w["t"] - start >= a.every:
            period.append((start, w["t"], before, cur))
            before += sum(x["ops"] for x in cur)
            start, cur = w["t"], []
    if cur:
        period.append((start, ws[-1]["t"], before, cur))
    print("from_s to_s docs_before ops_per_sec server_cores us_per_op gen_cores p50_ms p99_ms")
    for s, e, b, c in period:
        g = agg(c)
        print(f"{s:6.0f} {e:6.0f} {b:11d} {g['ops_per_sec']:9.0f} {g['server_cores']:5.2f} {g['us_per_op']:7.1f} "
              f"{g['gen_cores']:5.2f} {g['p50_ms_median']:7.1f} {g['p99_ms_max']:7.1f}")
    sel = [w for w in ws if a.t0 < w["t"] <= a.t1]
    g = agg(sel)
    print(f"TOTAL ({a.t0:g}-{min(a.t1, ws[-1]['t']):.0f} s): ops {g['ops']} ops/s {g['ops_per_sec']:.0f} "
          f"server cores {g['server_cores']:.2f} us/op {g['us_per_op']:.1f} gen cores {g['gen_cores']:.2f} "
          f"errors {ws[-1]['errors']}")


if __name__ == "__main__":
    main()
