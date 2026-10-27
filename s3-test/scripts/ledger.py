#!/usr/bin/env python3
"""Cost ledger (s3-test/ledger.json on branch s3-test-results).
    ledger.py total <ledger.json>                      -> prints the sum of gross_usd (0 if the file does not exist)
    ledger.py add <ledger.json> <run_id> <tests> <gross_usd> <put_tier> <get_tier> <instance_secs> <note> [unknown]
A run whose counters could not be read is booked at the cap plus its instance cost ("unknown": true): never at zero."""
import json
import os
import sys
import time


def load(p):
    return json.load(open(p)) if os.path.exists(p) else {"runs": [], "total_usd": 0.0}


if sys.argv[1] == "total":
    print(f"{sum(r['gross_usd'] for r in load(sys.argv[2])['runs']):.6f}")
elif sys.argv[1] == "add":
    p, run, tests, gross, put, get, secs, note = sys.argv[2:10]
    d = load(p)
    d["runs"].append({"run_id": run, "ts": int(time.time()), "tests": tests, "gross_usd": float(gross), "put_tier": int(float(put)), "get_tier": int(float(get)),
                      "instance_secs": int(float(secs)), "note": note, "unknown": len(sys.argv) > 10})
    d["total_usd"] = round(sum(r["gross_usd"] for r in d["runs"]), 6)
    os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
    json.dump(d, open(p, "w"), indent=1)
    print(f"ledger total {d['total_usd']}")
