#!/usr/bin/env python3
"""Pass/total tables for results7.md, computed from out/results_py.json and out/results_node.json (a full run of
run_full.sh) and the test list of the load-test base (the original suite). Prints markdown."""
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")


def load(name):
    return {r["name"]: r for r in json.load(open(os.path.join(OUT, name)))}


py, nd = load("results_py.json"), load("results_node.json")
tests = json.load(open(os.path.join(HERE, "tests.json")))
try:
    old = {t["name"]: t for t in json.loads(subprocess.check_output(["git", "show", "4aca51d:mongo-tests/tests.json"], cwd=HERE))}
except Exception:
    old = {}

GROUPS = [
    ("Old suite, core (in scope of results5/6)", lambda t: t["name"] in old and t["category"] == "core"),
    ("Old suite, extra probes (unsupported features of results5/6)", lambda t: t["name"] in old and t["category"] == "extra"),
    ("Step 2 — secondary indexes (`idx.`)", lambda t: t["name"].startswith("idx.") and t["name"] not in old),
    ("Step 3 — TTL indexes (`ttl.`)", lambda t: t["name"].startswith("ttl.")),
    ("Transactions (`txn.`, replica-set reference)", lambda t: t["name"].startswith("txn.")),
    ("Change streams (`cs.`, replica-set reference)", lambda t: t["name"].startswith("cs.")),
    ("`$where` (`where.`)", lambda t: t["name"].startswith("where.")),
    ("Step 4 — expression operators (`op.`)", lambda t: t["name"].startswith("op.")),
    ("Step 4 — stages and accumulators (`stage7.`)", lambda t: t["name"].startswith("stage7.")),
]


def tally(sel):
    rows = []
    for drv, res in (("pymongo", py), ("node", nd)):
        n = [t for t in tests if sel(t) and t["name"] in res]
        p = [t for t in n if res[t["name"]]["status"] == "pass"]
        rows.append((drv, len(p), len(n)))
    return rows


print("| Group | pymongo pass/total | Node pass/total |\n|---|---|---|")
tot = {"pymongo": [0, 0], "node": [0, 0]}
for label, sel in GROUPS:
    r = tally(sel)
    for drv, p, n in r:
        tot[drv][0] += p
        tot[drv][1] += n
    print(f"| {label} | {r[0][1]}/{r[0][2]} | {r[1][1]}/{r[1][2]} |")
print(f"| **All tests** | {tot['pymongo'][0]}/{tot['pymongo'][1]} | {tot['node'][0]}/{tot['node'][1]} |")
fails = [(n, d) for d, res in (("pymongo", py), ("node", nd)) for n, r in res.items() if r["status"] != "pass" and r["category"] != "extra"]
print()
print("Failing in-scope tests:", ", ".join(f"{n} ({d})" for n, d in fails) or "none")
