#!/usr/bin/env python3
"""A/B of two engine binaries on the read workloads (bench/perf, see docs/perf-letture.md).

Runs A, B, A, B ... (alternated, so slow drifts of the shared machine hit both the same way), every run on a fresh
engine started on a copy of the snapshot (bench.py), and prints the median ops/s and ops per CPU second of each binary
and the gain of B over A.

Usage: ab.py BIN_A BIN_B [--cells read:1:8,read:100:16] [--reps 3] [--warmup 30] [--duration 30] [--label TEXT]
"""
import argparse, os, statistics, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("a")
    ap.add_argument("b")
    ap.add_argument("--cells", default="read:1:8,read:100:16")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--label", default="ab")
    a = ap.parse_args()
    os.makedirs(bench.DATA, exist_ok=True)
    summary = []
    try:
        for cell in a.cells.split(","):
            w, layout, clients = cell.split(":")
            res = {"A": [], "B": []}
            for rep in range(1, a.reps + 1):
                for side, binary in (("A", a.a), ("B", a.b)):
                    r = bench.one("fenvix", int(layout), w, int(clients), a.warmup, a.duration,
                                  f"{a.label}-{side}", binary, rep)
                    res[side].append(r)
            line = [f"{w} layout={layout} clients={clients}:"]
            med = {}
            for side in ("A", "B"):
                ops = statistics.median(r["ops_per_s"] for r in res[side])
                cpu = statistics.median(r.get("ops_per_cpu_s", 0) for r in res[side])
                med[side] = (ops, cpu)
                line.append(f"{side} ops/s={ops:.0f} ops/cpu_s={cpu:.0f}")
            line.append(f"gain ops/s {100 * (med['B'][0] / med['A'][0] - 1):+.1f}% "
                        f"ops/cpu_s {100 * (med['B'][1] / med['A'][1] - 1):+.1f}%")
            summary.append(" ".join(line))
            bench.log("AB " + summary[-1])
    finally:
        bench.stop_all()
    print("\n".join(summary))


if __name__ == "__main__":
    main()
