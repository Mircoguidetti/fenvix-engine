#!/usr/bin/env python3
"""Compares the replayed draws (walbench-cycle1 output, TSV on stdin) with results-walbench/raw/results/crash-quorum.json.
Prints: how many cycles match on every recorded field, the read pair of the failed cycle, and how many cycles read node 0."""
import json, sys
raw = json.load(open(sys.argv[1]))["cycles"]
draws = [l.rstrip("\n").split("\t") for l in sys.stdin if l.strip()]
match = mism = 0
for c, d in zip(raw, draws):
    cyc, variant, conc, size, kill_ms, kill_acks, pair = d
    ok = (c["variant"] == variant and c["conc"] == int(conc) and c["size"] == int(size) and c["kill_ms"] == int(kill_ms)
          and (c.get("read_nodes") is None or c["read_nodes"] == pair) and (c.get("kill_acks") is None or c["kill_acks"] == int(kill_acks)))
    match += ok; mism += not ok
    if "harness_error" in c:
        print(f"failed cycle {c['cycle']}: replayed read pair = {pair}; error = {c['harness_error'].strip()!r}")
print(f"cycles compared: {match + mism}, all recorded fields equal: {match}, different: {mism}")
reads0 = [c for c, d in zip(raw, draws) if "0" in d[6].split(",")]
print(f"cycles whose recovery read node 0: {len(reads0)} (failed: {sum('harness_error' in c for c in reads0)})")
first_other = next(c["cycle"] for c, d in zip(raw, draws) if "0" in d[6].split(",") and "harness_error" not in c)
print(f"first successful cycle that read node 0: {first_other}")
