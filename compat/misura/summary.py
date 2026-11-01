#!/usr/bin/env python3
"""Prints the counts of results/*.jsonl per area: pass, fail (by kind), skip (by reason family)."""
import collections, glob, json, sys
for p in sorted(glob.glob(sys.argv[1] if len(sys.argv) > 1 else "results/*.jsonl")):
    rs = [json.loads(l) for l in open(p)]
    c = collections.Counter(r["status"] for r in rs)
    run = c["PASS"] + c["FAIL"]
    pct = 100 * c["PASS"] / run if run else 0
    print(f"{p}: total {len(rs)} pass {c['PASS']} fail {c['FAIL']} skip {c['SKIP']} -> {pct:.1f}% of the tests run")
    print("  fail:", dict(collections.Counter(r.get("kind") for r in rs if r["status"] == "FAIL")))
    print("  skip:", dict(collections.Counter(r.get("reason", "").split(" ")[0] for r in rs if r["status"] == "SKIP")))
