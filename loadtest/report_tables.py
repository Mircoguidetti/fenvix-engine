#!/usr/bin/env python3
"""Markdown tables for results-load.md from loadtest/out/<scenario>/{summary,analysis}.json."""
import json
import os
import sys

OUT = os.environ.get("LOAD_OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))


def load(name):
    d = os.path.join(OUT, name)
    s = json.load(open(os.path.join(d, "summary.json")))
    a = json.load(open(os.path.join(d, "analysis.json")))
    return s, a


def f(x, n=1):
    return f"{x:,.{n}f}"


def lat(h):
    if not h or not h.get("count"):
        return "n/a"
    return f"{f(h['p50_ms'], 2)} / {f(h['p99_ms'], 1)} / {f(h['p999_ms'], 1)}"


def errs(s):
    e = s["errors_by_code"]
    if not e:
        return "none"
    return ", ".join(f"{k} ×{v:,}" for k, v in sorted(e.items(), key=lambda kv: -kv[1]))


def summary_table(names):
    rows = ["| Scenario | Ops ok / failed | Ops/s | Latency p50 / p99 / p999 (ms), all ops | Wake (first request) p50 / p99 / p999 (ms) | Errors by code (every failed attempt) | CPU avg / peak (% of one core) | RSS avg / peak (MB) | Max open tenants (readers + writers) | Object-store requests/s (PUT-tier / GET-tier, avg) | Cost at S3 prices ($/h) |", "|---|---|---|---|---|---|---|---|---|---|---|"]
    for n in names:
        s, a = load(n)
        g = s["normal"]
        failed = sum(s["failures_by_code"].values())
        t = a["total"]
        rows.append(
            f"| {n.upper()} | {g['ops']:,} / {failed:,} | {f(s['ops_per_sec'], 0)} | {lat(g['all_ops'])} | {lat(g['wake'])} | {errs(s)} | "
            f"{f(t['cpu_pct_of_one_core']['avg'], 0)} / {f(t['cpu_pct_of_one_core']['peak'], 0)} | {f(t['rss_mb']['avg'], 0)} / {f(t['rss_mb']['peak'], 0)} | "
            f"{f(t['open_tenants']['peak'], 0)} | {f(t['billed_put_tier_per_sec']['avg'], 0)} / {f(t['billed_get_tier_per_sec']['avg'], 0)} | {f(t['cost_per_hour_usd']['total'], 2)} |")
    return "\n".join(rows)


def by_type(name, group="normal"):
    s, _ = load(name)
    g = s[group]
    rows = [f"| {name.upper()} operation | count | p50 (ms) | p99 (ms) | p999 (ms) | max (ms) |", "|---|---|---|---|---|---|"]
    for k, h in g["by_type"].items():
        rows.append(f"| {k} (all, incl. wakes) | {h['count']:,} | {f(h['p50_ms'], 2)} | {f(h['p99_ms'], 1)} | {f(h['p999_ms'], 1)} | {f(h['max_ms'], 0)} |")
    for k, h in g["steady_by_type"].items():
        rows.append(f"| {k} (already open) | {h['count']:,} | {f(h['p50_ms'], 2)} | {f(h['p99_ms'], 1)} | {f(h['p999_ms'], 1)} | {f(h['max_ms'], 0)} |")
    for k, h in g["wake_by_type"].items():
        rows.append(f"| first request = {k} (wake) | {h['count']:,} | {f(h['p50_ms'], 2)} | {f(h['p99_ms'], 1)} | {f(h['p999_ms'], 1)} | {f(h['max_ms'], 0)} |")
    return "\n".join(rows)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "summary":
        print(summary_table(sys.argv[2:]))
    elif cmd == "types":
        for n in sys.argv[2:]:
            print(by_type(n))
            print()
