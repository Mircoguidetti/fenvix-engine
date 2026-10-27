#!/usr/bin/env python3
"""Representative subset of mongo-tests/tests.json: up to N tests from every group (evenly spread), core category only,
plus the idle-close tests. Usage: make_subset.py <tests.json> <out.json> [per_group=5]"""
import collections
import json
import sys

tests = json.load(open(sys.argv[1]))
per = int(sys.argv[3]) if len(sys.argv) > 3 else 5
groups = collections.OrderedDict()
for t in tests:
    if t["category"] == "core":
        groups.setdefault(t["group"], []).append(t)
pick = []
for g, ts in groups.items():
    step = max(1, len(ts) // per)
    pick += ts[::step][:per]
names = {t["name"] for t in pick}
pick += [t for t in tests if t.get("idle") and t["name"] not in names]
json.dump(pick, open(sys.argv[2], "w"))
print(f"{len(pick)} tests from {len(groups)} groups")
