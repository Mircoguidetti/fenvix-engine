#!/usr/bin/env python3
"""Coverage of each scanned repository by the server: for every repo, which features used in its
application code the server does not support, and how many repos have none.

    coverage.py [--profile baseline|after] [--markdown]

`baseline` is the status in results5.md / compat-scan.md (bulkWrite counted as supported).
`after` adds what results6.md implements; it must list only features that have passing
differential tests. Index kinds are never "not supported" (they are recorded), so they are excluded.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import report  # noqa: E402  (status of every feature as of results5.md)

NOT = report.NOT

# Features implemented by results6, each with the differential tests that prove it. A feature counts as
# supported in the "after" profile only if it has tests and every in-scope (core) test whose name matches
# passes on both drivers (mongo-tests/out/results_py.json and results_node.json).
FEATURE_TESTS = {}
FEATURE_TESTS["$regex|query"] = FEATURE_TESTS["$options|query"] = r"^regex\."
FEATURE_TESTS["$size|query"] = r"^query\.size_"
FEATURE_TESTS["$elemMatch|query"] = r"^query\.elemmatch_"
FEATURE_TESTS["$all|query"] = r"^query\.all_"
FEATURE_TESTS["$type|query"] = r"^query\.type_"
FEATURE_TESTS["$mod|query"] = r"^query\.mod_"
FEATURE_TESTS["$expr|query"] = r"^expr\."
FEATURE_TESTS["$slice|query"] = r"^(find\.projection_slice|upd\.push_each_slice|expr_op\.slice)"
for n in "push pull pullAll addToSet pop setOnInsert rename min max mul".split():
    FEATURE_TESTS[f"${n}|update"] = rf"^upd\.{n.lower()}_"
FEATURE_TESTS["$currentDate|update"] = r"^upd\.currentdate_"
FEATURE_TESTS["$each|update"] = r"^upd\.(push_each|addtoset_each)"
FEATURE_TESTS["$position|update"] = r"^upd\.push_position"
FEATURE_TESTS["$unwind|stage"] = r"^stage\.unwind_"
FEATURE_TESTS["$lookup|stage"] = r"^lookup\."
FEATURE_TESTS["$addFields|stage"] = FEATURE_TESTS["$set|stage"] = r"^stage\.(addfields|set)_"
FEATURE_TESTS["$addToSet|accumulator"] = r"^group\.addtoset_"
for op, rx in {"cond": "cond_", "ifNull": "ifnull", "add": "add", "subtract": "subtract", "multiply": "multiply", "divide": "divide",
               "concat": "concat", "toLower": "tolower_toupper", "toUpper": "tolower_toupper", "isArray": "isarray",
               "arrayElemAt": "arrayelemat", "concatArrays": "concatarrays", "filter": "filter", "map": "map",
               "literal": "literal", "setUnion": "setunion", "dateToString": "datetostring_"}.items():
    FEATURE_TESTS[f"${op}|expression"] = rf"^expr_op\.{rx}"
for m in ("findOneAndUpdate", "findOneAndReplace", "findOneAndDelete"):
    FEATURE_TESTS[m] = r"^fam\."


def _load_results():
    import re
    out_dir = os.path.join(HERE, "..", "..", "mongo-tests", "out")
    try:
        py = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_py.json")))}
        nd = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_node.json")))}
    except OSError:
        return None
    supported, evidence = set(), {}
    for key, rx in FEATURE_TESTS.items():
        names = [n for n, r in py.items() if r["category"] == "core" and re.search(rx, n)]
        ok = [n for n in names if py[n]["status"] == "pass" and nd.get(n, {}).get("status") == "pass"]
        evidence[key] = (len(ok), len(names))
        if names and len(ok) == len(names):
            supported.add(key)
    return supported, evidence


_res = _load_results()
AFTER_SUPPORTED = _res[0] if _res else set()
EVIDENCE = _res[1] if _res else {}


def status(key, profile):
    st = report.display(key)[2]
    if profile == "after" and key in AFTER_SUPPORTED:
        return report.SUPPORTED
    return st


def compute(data, profile):
    out = []
    for r in data["repos"]:
        if r["status"] != "scanned":
            continue
        un = []
        for section in ("operators", "methods", "indexes"):
            for key, c in r[section].items():
                if c["app"] and status(key, profile) == NOT:
                    un.append((report.display(key)[0], c["app"]))
        un.sort(key=lambda x: (-x[1], x[0]))
        out.append({"repo": r["name"], "unsupported": un})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="baseline", choices=["baseline", "after"])
    ap.add_argument("--markdown", action="store_true")
    ap.add_argument("--in", dest="inp", default=os.path.join(HERE, "results.json"))
    a = ap.parse_args()
    data = json.load(open(a.inp))
    res = compute(data, a.profile)
    full = [x["repo"] for x in res if not x["unsupported"]]
    if a.markdown:
        print(f"| Repository | Unsupported features used in application code | Count |\n|---|---|---|")
        for x in res:
            names = ", ".join(f"`{n}` ×{c}" for n, c in x["unsupported"]) or "none"
            print(f"| {x['repo']} | {names} | {len(x['unsupported'])} |")
        print(f"\nRepositories fully covered (zero unsupported features): **{len(full)}/{len(res)}**" + (f" ({', '.join(full)})" if full else ""))
    else:
        for x in res:
            print(f"{x['repo']}: {len(x['unsupported'])} unsupported -> {', '.join(f'{n} x{c}' for n, c in x['unsupported'][:12])}{' ...' if len(x['unsupported']) > 12 else ''}")
        print(f"fully covered: {len(full)}/{len(res)} {full}")


if __name__ == "__main__":
    main()
