#!/usr/bin/env python3
"""Markdown tables for latency (out/latency.json) and object-store requests (out/requests.json)."""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
lat = json.load(open(os.path.join(HERE, "out", "latency.json")))
req = json.load(open(os.path.join(HERE, "out", "requests.json")))
ours = lat["mongo-server (slatedb on MinIO)"]
real = lat["MongoDB 7.0 (reference)"]

rows = ["| Operation (1,000 docs of ~1 KB) | Tenant state | n | p50 (ms) | p99 (ms) | MongoDB 7.0 p50 / p99 (ms), warm |", "|---|---|---|---|---|---|"]
for op, st in ours.items():
    for state in ("warm", "cold"):
        if state in st:
            s = st[state]
            ref = ""
            if state == "warm" and op in real:
                r = real[op]["warm"]
                ref = f"{r['p50_ms']:.3f} / {r['p99_ms']:.3f}"
            rows.append(f"| {op} | {state} | {s['n']} | {s['p50_ms']:.3f} | {s['p99_ms']:.3f} | {ref} |")
print("\n".join(rows))
print()
rows = ["| Operation | Tenant state | PUT/LIST-tier requests | GET-tier requests | DELETE | Request cost (S3 Standard list prices) |", "|---|---|---|---|---|---|"]
for op, st in req.items():
    for state in ("warm", "cold"):
        r = st[state]
        rows.append(f"| {op} | {state} | {r['put_tier']:.2f} | {r['get_tier']:.2f} | {r['delete']:.2f} | ${r['usd']:.8f} |")
print("\n".join(rows))
