"""P-B1-strong-point: the comparison from history.py's summaries.

  compare.py RESULTS_DIR   (RESULTS_DIR/{fenvix,mongo,postgres}/summary-<size>-<round>.json and result.json)

For every size, history length and read: p50 / p99 per system, our S3 GETs and bytes per request, and the ratio
ours / best of mongod and Postgres (below 0.9 FASTER, 0.9-1.1 EQUAL, above 1.1 SLOWER, bench-big's rule). Our line is
read in round B (20 ms added to every MinIO response, the stand-in for S3) unless --round says otherwise; round A (no
delay) is printed beside it. Writes RESULTS_DIR/compare.json and prints the text."""
from __future__ import annotations

import argparse
import json
import os

OPS = ["whole_cold|h50", "whole_cold|h500", "whole_cold|h5000", "newest10_cold|newest",
       "whole_warm|h50", "whole_warm|h500", "whole_warm|h5000", "newest10_warm|newest"]


def load(d, size, rnd):
    p = os.path.join(d, f"summary-{size}-{rnd}.json")
    return json.load(open(p)) if os.path.exists(p) else None


def verdict(r):
    return "FASTER" if r < 0.9 else ("EQUAL" if r <= 1.1 else "SLOWER")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--round", default="B")
    a = ap.parse_args()
    out, lines = {}, []
    for size in (1, 2, 3):
        f = {r: load(os.path.join(a.dir, "fenvix"), size, r) for r in ("A", "B", "B2off")}
        m = load(os.path.join(a.dir, "mongo"), size, "A")
        p = load(os.path.join(a.dir, "postgres"), size, "A")
        if not any([f["A"], m, p]):
            continue
        lines.append(f"== size {size}")
        for op in OPS:
            row = {}
            for name, s in (("ours_" + a.round, f[a.round]), ("ours_A", f["A"]), ("ours_B2off", f["B2off"]), ("mongod", m), ("postgres", p)):
                if s and op in s["ops"]:
                    d = s["ops"][op]
                    row[name] = {k: d.get(k) for k in ("n", "p50_ms", "p99_ms", "client_cpu_share", "valid", "s3_gets_p50", "s3_gets_mean",
                                                        "s3_get_bytes_p50", "s3_get_bytes_mean", "disk_read_bytes_p50", "server_cpu_ms_p50",
                                                        "background_gets_total")}
            o = row.get("ours_" + a.round)
            others = [row[k] for k in ("mongod", "postgres") if k in row]
            if o and others:
                for q in ("p50_ms", "p99_ms"):
                    best = min(x[q] for x in others)
                    row[f"ratio_{q}"] = round(o[q] / best, 3)
                    row[f"verdict_{q}"] = verdict(o[q] / best)
            out[f"{size}|{op}"] = row
            txt = f"{op:22s}"
            for k in ("ours_" + a.round, "ours_A", "ours_B2off", "mongod", "postgres"):
                if k in row:
                    v = row[k]
                    txt += f" | {k} {v['p50_ms']}/{v['p99_ms']}" + (f" GETs {v['s3_gets_p50']} {v['s3_get_bytes_p50']}B" if v.get("s3_gets_p50") is not None and k.startswith("ours") else "")
                    if not v.get("valid", True):
                        txt += " NOT-VALID(client)"
            if "ratio_p50_ms" in row:
                txt += f" | ours/best p50 {row['ratio_p50_ms']} {row['verdict_p50_ms']}, p99 {row['ratio_p99_ms']} {row['verdict_p99_ms']}"
            lines.append(txt)
    json.dump(out, open(os.path.join(a.dir, "compare.json"), "w"), indent=1)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
