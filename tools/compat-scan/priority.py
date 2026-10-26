#!/usr/bin/env python3
"""Step 1 priority table: every feature that is not (fully) supported, grouped by kind, with the number
of repositories (of the scanned 10) and application-code call sites that need it.

    priority.py [--profile baseline|after] [--out FILE]

Status comes from coverage.py (`after` = results5 + results6 features that have passing differential
tests in mongo-tests/out). Index options that are only *recorded* (sparse, partialFilterExpression, TTL,
compound, text, geo, hashed) are listed as "recorded only", because the server accepts them but does
not honour them.
"""
import argparse, collections, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import coverage, report  # noqa: E402

RECORDED_ONLY = {"index: sparse", "index: partialFilterExpression", "index: TTL (expireAfterSeconds)", "index: compound",
                 "index: text", "index: hashed / wildcard", "index: 2dsphere / 2d (geo)"}

def group_of(key, kind):
    if key == "withTransaction / startTransaction": return "transactions"
    if key.startswith("watch"): return "change streams"
    if key.startswith("index:"): return "index options recorded but not honoured"
    if key in ("$where", "$function", "$accumulator"): return "server-side JavaScript"
    if kind == "driver/ODM method": return "driver methods"
    return {"query operator": "query operators", "update operator": "update operators", "aggregation stage": "aggregation stages",
            "aggregation expression": "aggregation expressions", "aggregation accumulator": "aggregation accumulators"}.get(kind, kind)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--profile", default="after"); ap.add_argument("--out")
    a = ap.parse_args()
    data = json.load(open(os.path.join(HERE, "results.json")))
    scanned = [r for r in data["repos"] if r["status"] == "scanned"]
    feats = collections.defaultdict(lambda: {"repos": [], "sites": 0})
    for r in scanned:
        for sec in ("operators", "methods", "indexes"):
            for key, c in r[sec].items():
                if not c["app"]: continue
                st = coverage.status(key, a.profile)
                if st != report.NOT and key not in RECORDED_ONLY: continue
                f = feats[key]; f["repos"].append(r["name"]); f["sites"] += c["app"]
    groups = collections.defaultdict(list)
    for key, f in feats.items():
        label, kind, st, _ = report.display(key)
        groups[group_of(label, kind)].append((label, len(f["repos"]), f["sites"], f["repos"]))
    lines = []
    allrows = []
    for g, rows in groups.items():
        rows.sort(key=lambda x: (-x[1], -x[2], x[0]))
        allrows.append((g, sum(r[2] for r in rows), max(r[1] for r in rows), rows))
    allrows.sort(key=lambda x: (-x[2], -x[1]))
    for g, sites, maxapps, rows in allrows:
        lines.append(f"\n**{g}** — {len(rows)} features, {sites} call sites, up to {maxapps}/10 apps\n")
        lines.append("| Feature | Apps | Call sites | Apps (names) |\n|---|---|---|---|")
        for label, napps, s, names in rows:
            lines.append(f"| `{label}` | {napps}/10 | {s} | {', '.join(sorted(names))} |")
    out = "\n".join(lines)
    if a.out: open(a.out, "w").write(out + "\n")
    else: print(out)
main()
