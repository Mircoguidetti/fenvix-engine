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


FEATURE_TESTS_R6 = dict(FEATURE_TESTS)

# ---- results7: evidence from tests.json + the full run ------------------------------------------------------------
FEATURE_TESTS["withTransaction / startTransaction"] = r"^txn\."
FEATURE_TESTS["watch (change streams)"] = r"^cs\."
FEATURE_TESTS["$where|query"] = r"^where\."
# index options that results5 only recorded; they are honoured now and have passing differential tests
INDEX_TESTS = {
    "index: sparse": r"^idx\.(plan_sparse|unique_sparse|res_sparse|backfill_sparse|sparse_compound)",
    "index: partialFilterExpression": r"^idx\.(plan_partial|res_partial|unique_partial|backfill_sparse_and_partial|create_partial)",
    "index: TTL (expireAfterSeconds)": r"^ttl\.",
    "index: compound": r"^idx\.(plan_compound|res_compound|unique_compound|backfill_3000|compound)",
    "index: unique": r"^idx\.unique",
    "index: any definition": r"^idx\.",
}
STAGE_NAMES = set("match group sort limit skip project count addFields set unset unwind lookup facet collStats indexStats replaceRoot replaceWith sortByCount bucket sample unionWith out merge changeStream".split())
ACC_NAMES = set("sum avg min max first last push addToSet count mergeObjects stdDevPop stdDevSamp firstN lastN maxN minN top bottom topN bottomN accumulator".split())


def _walk_pipeline(pipeline, found):
    for st in pipeline:
        if not isinstance(st, dict) or len(st) != 1:
            continue
        (name, arg), = st.items()
        found.add((name, "stage"))
        if name == "$match":
            _walk_query(arg, found)
        elif name == "$group" and isinstance(arg, dict):
            for k, v in arg.items():
                if k == "_id":
                    _walk_expr(v, found)
                elif isinstance(v, dict):
                    for op, a in v.items():
                        found.add((op, "accumulator"))
                        _walk_expr(a, found)
        elif name == "$facet" and isinstance(arg, dict):
            for p in arg.values():
                _walk_pipeline(p, found)
        elif name == "$lookup" and isinstance(arg, dict):
            _walk_pipeline(arg.get("pipeline", []), found)
            _walk_expr(arg.get("let", {}), found)
        elif name == "$unionWith" and isinstance(arg, dict):
            _walk_pipeline(arg.get("pipeline", []), found)
        elif name == "$merge" and isinstance(arg, dict) and isinstance(arg.get("whenMatched"), list):
            _walk_pipeline(arg["whenMatched"], found)
        elif name in ("$bucket",) and isinstance(arg, dict):
            _walk_expr(arg.get("groupBy"), found)
            for k, v in (arg.get("output") or {}).items():
                if isinstance(v, dict):
                    for op, a in v.items():
                        found.add((op, "accumulator"))
        else:
            _walk_expr(arg, found)


def _walk_expr(x, found):
    if isinstance(x, dict):
        for k, v in x.items():
            if k.startswith("$"):
                found.add((k, "expression"))
            _walk_expr(v, found)
    elif isinstance(x, list):
        for v in x:
            _walk_expr(v, found)


def _walk_query(x, found):
    if isinstance(x, dict):
        for k, v in x.items():
            if k == "$expr":
                _walk_expr(v, found)
            elif k.startswith("$"):
                found.add((k, "query"))
                _walk_query(v, found)
            else:
                _walk_query(v, found)
    elif isinstance(x, list):
        for v in x:
            _walk_query(v, found)


def _walk_update(x, found):
    if isinstance(x, dict):
        for k, v in x.items():
            if k.startswith("$") and k not in ("$each", "$position", "$slice", "$sort"):
                found.add((k, "update"))
            elif k in ("$each", "$position"):
                found.add((k, "update"))
            _walk_query(v, found) if k in ("$pull",) else None


def _operators_of_test(t):
    found = set()
    for st in t["steps"]:
        op = st.get("op")
        if op in ("aggregate", "watch"):
            _walk_pipeline(st.get("pipeline", []), found)
        if op == "watch":
            for a in st.get("actions", []) + st.get("resume_actions", []):
                if a.get("op") == "aggregate":
                    _walk_pipeline(a.get("pipeline", []), found)
        for key in ("filter", "query"):
            if key in st:
                _walk_query(st[key], found)
        if "update" in st and isinstance(st["update"], dict):
            _walk_update(st["update"], found)
        if op == "command" and isinstance(st.get("cmd"), dict):
            for k in ("updates", "deletes"):
                for u in st["cmd"].get(k, []):
                    _walk_query(u.get("q"), found)
                    if isinstance(u.get("u"), dict):
                        _walk_update(u["u"], found)
    return found


def _auto_supported():
    """(operator, role) pairs used by tests, and whether every test using them passed on both drivers."""
    import re
    out_dir = os.path.join(HERE, "..", "..", "mongo-tests", "out")
    try:
        tests = json.load(open(os.path.join(HERE, "..", "..", "mongo-tests", "tests.json")))
        py = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_py.json")))}
        nd = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_node.json")))}
    except OSError:
        return set(), {}
    users = {}
    for t in tests:
        if t["category"] == "extra" or not re.match(r"^(op|stage7|where|cs|txn|idx|ttl|expr_op|expr|stage|group|lookup|query|upd|regex|fam|find|agg)", t["name"]):
            continue
        ok = py.get(t["name"], {}).get("status") == "pass" and nd.get(t["name"], {}).get("status") == "pass"
        for key in _operators_of_test(t):
            users.setdefault(key, []).append(ok)
    supported = {f"{op}|{role}" for (op, role), oks in users.items() if oks and all(oks)}
    evidence = {f"{op}|{role}": (sum(oks), len(oks)) for (op, role), oks in users.items()}
    return supported, evidence


def _load_results(feature_tests=None):
    import re
    out_dir = os.path.join(HERE, "..", "..", "mongo-tests", "out")
    try:
        py = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_py.json")))}
        nd = {r["name"]: r for r in json.load(open(os.path.join(out_dir, "results_node.json")))}
    except OSError:
        return None
    supported, evidence = set(), {}
    for key, rx in (feature_tests or FEATURE_TESTS).items():
        names = [n for n, r in py.items() if r["category"] in ("core", "replset") and re.search(rx, n)]
        ok = [n for n in names if py[n]["status"] == "pass" and nd.get(n, {}).get("status") == "pass"]
        evidence[key] = (len(ok), len(names))
        if names and len(ok) == len(names):
            supported.add(key)
    return supported, evidence


_r6 = _load_results(FEATURE_TESTS_R6)
R6_SUPPORTED = _r6[0] if _r6 else set()
_res = _load_results()
AFTER_SUPPORTED = _res[0] if _res else set()
EVIDENCE = _res[1] if _res else {}
_auto, _auto_ev = _auto_supported()
AFTER_SUPPORTED |= _auto
EVIDENCE.update(_auto_ev)
INDEX_SUPPORTED = set()
try:
    import re as _re
    _py = {r["name"]: r for r in json.load(open(os.path.join(HERE, "..", "..", "mongo-tests", "out", "results_py.json")))}
    _nd = {r["name"]: r for r in json.load(open(os.path.join(HERE, "..", "..", "mongo-tests", "out", "results_node.json")))}
    for _k, _rx in INDEX_TESTS.items():
        _names = [n for n in _py if _re.search(_rx, n) and _py[n]["category"] == "core"]
        _ok = [n for n in _names if _py[n]["status"] == "pass" and _nd.get(n, {}).get("status") == "pass"]
        EVIDENCE[_k] = (len(_ok), len(_names))
        if _names and len(_ok) == len(_names):
            INDEX_SUPPORTED.add(_k)
except OSError:
    pass


def status(key, profile):
    st = report.display(key)[2]
    if profile == "r6" and key in R6_SUPPORTED:
        return report.SUPPORTED
    if profile == "after" and (key in AFTER_SUPPORTED or key in INDEX_SUPPORTED or key in R6_SUPPORTED):
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
    ap.add_argument("--profile", default="baseline", choices=["baseline", "r6", "after"])
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
