#!/usr/bin/env python3
"""pymongo reads every document once through a cursor whose batches are cut at 16 MiB (find and aggregate).
Usage: drivers_read.py --ours mongodb://... [--fill] ; the Node check (drivers_read.js) reads the same collection."""
import argparse, sys
from pymongo import MongoClient
ap = argparse.ArgumentParser(); ap.add_argument("--ours", required=True); ap.add_argument("--fill", action="store_true")
a = ap.parse_args()
c = MongoClient(a.ours).app_batch_drivers.c
if a.fill:
    c.drop()
    for s in range(0, 60_000, 10_000):
        c.insert_many([{"_id": i, "p": "y" * 1000} for i in range(s, s + 10_000)])
    for i in range(12):  # big documents in the middle of the _id order too
        c.insert_one({"_id": 30_000.5 + i, "p": "z" * 5_000_000})
expected = 60_000 + 12
bad = 0
for name, cur in [("find", c.find({})), ("find batch_size 50000", c.find({}).batch_size(50_000)), ("aggregate", c.aggregate([{"$match": {}}]))]:
    ids = [d["_id"] for d in cur]
    ok = len(ids) == len(set(ids)) == expected
    bad += not ok
    print(f"{'PASS' if ok else 'FAIL'}  pymongo {name}: {len(ids)} read, {len(set(ids))} distinct, expected {expected}")
print("RESULT", "PASS" if not bad else "FAIL"); sys.exit(1 if bad else 0)
