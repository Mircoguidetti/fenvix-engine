#!/usr/bin/env python3
"""Compatibility table and pass rates for results6.md, from out/results_py.json and out/results_node.json.

Each row selects tests by name. In-scope (core) tests decide "supported, passing"; rows made only of
beyond-scope probes (category "extra") are reported as "not supported" / "partly supported".
"""
import json
import os
import re
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
py = {r["name"]: r for r in json.load(open(os.path.join(HERE, "out", "results_py.json")))}
nd = {r["name"]: r for r in json.load(open(os.path.join(HERE, "out", "results_node.json")))}

ROWS = [
    ("Query operators", "`$regex` / `$options` (i m s x u), BSON regex as `$regex` operand, in `$in`/`$nin`/`$not`/plain equality, vs array elements; lookahead, lookbehind, backreferences, POSIX classes, `\\p{L}`; invalid pattern/flag/options errors", r"^regex\."),
    ("Query operators", "`$size`", r"^query\.size_"),
    ("Query operators", "`$all` (values, regexes, `$elemMatch` elements)", r"^query\.all_"),
    ("Query operators", "`$elemMatch` (document form, operator form, nested, `$and/$or/$nor`)", r"^query\.elemmatch_"),
    ("Query operators", "`$type` (numeric codes, string aliases incl. `number`, lists)", r"^query\.type_"),
    ("Query operators", "`$mod`", r"^query\.mod_"),
    ("Query operators", "`$expr` (find, count, delete, update, aggregate `$match`, inside `$or`/`$and`)", r"^expr\."),
    ("Query operators", "projection `$slice` (`n`, `-n`, `[skip, limit]`, nested)", r"^find\.projection_slice"),
    ("Commands", "findAndModify: `findOneAndUpdate` / `findOneAndReplace` / `findOneAndDelete`, raw command shape (`lastErrorObject`, `value`), sort, fields, upsert, new, errors", r"^fam\."),
    ("Update operators", "`$push` (`$each`, `$slice`, `$sort`, `$position`)", r"^upd\.push_"),
    ("Update operators", "`$pull` (value, condition, sub-document query)", r"^upd\.pull_"),
    ("Update operators", "`$pullAll`", r"^upd\.pullall_"),
    ("Update operators", "`$addToSet` (`$each`, BSON equality)", r"^upd\.addtoset_"),
    ("Update operators", "`$pop`", r"^upd\.pop_"),
    ("Update operators", "`$setOnInsert`", r"^upd\.setoninsert_"),
    ("Update operators", "`$rename`", r"^upd\.rename_"),
    ("Update operators", "`$min`, `$max`", r"^upd\.(min|max)_"),
    ("Update operators", "`$mul`", r"^upd\.mul_"),
    ("Update operators", "`$currentDate` (date, timestamp)", r"^upd\.currentdate_"),
    ("Update operators", "conflicting paths, unknown operators, non-array targets, overflow", r"^upd\.(conflict_|error_|noop_|update_many_|operators_)"),
    ("Aggregation stages", "`$project` with expressions, nested paths, `$$REMOVE`, `$$ROOT`", r"^stage\.project_"),
    ("Aggregation stages", "`$addFields` / `$set`", r"^stage\.(addfields|set)_"),
    ("Aggregation stages", "`$unwind` (string/object form, `includeArrayIndex`, `preserveNullAndEmptyArrays`)", r"^stage\.unwind_"),
    ("Aggregation stages", "`$lookup` (`localField`/`foreignField`, same database)", r"^lookup\.(?!extra)"),
    ("Aggregation stages", "`$group` with expression keys and accumulators, `$addToSet` accumulator", r"^group\."),
    ("Aggregation expressions", "`$cond` (array and object form), `$ifNull`, `$literal`", r"^expr_op\.(cond_|ifnull|literal|array_and_object)"),
    ("Aggregation expressions", "`$eq $ne $gt $gte $lt $lte`, `$and $or $not`, `$in`", r"^expr_op\.(comparison|and_or|not_wrong|and_short|in_)"),
    ("Aggregation expressions", "`$add $subtract $multiply $divide $mod`", r"^expr_op\.(add|subtract|multiply|divide|mod|math)"),
    ("Aggregation expressions", "`$concat $toLower $toUpper`", r"^expr_op\.(concat|tolower)"),
    ("Aggregation expressions", "`$size $isArray $arrayElemAt $concatArrays $setUnion $slice`", r"^expr_op\.(size|isarray|arrayelemat|concatarrays|setunion|slice)"),
    ("Aggregation expressions", "`$filter`, `$map`, `$$variables`, field paths", r"^expr_op\.(filter|map|variable|field_path)"),
    ("Aggregation expressions", "`$dateToString` (formats, UTC and fixed offsets)", r"^expr_op\.datetostring_(?!week|from|other|unknown)"),
    # outside the supported set
    ("Not supported", "`$dateToString` with named time zones (no tz database), `%U %V %G`, ObjectId/Timestamp input via `$toDate`", r"^expr_op\.datetostring_(week|from|other|unknown)"),
    ("Not supported", "other expression operators (`$switch`, `$reduce`, `$let`, `$substr`, `$toString`, `$objectToArray`, `$round`, `$abs`, `$regexMatch`, `$max`, `$year`)", r"^expr_op\.unsupported"),
    ("Not supported", "`$lookup` with `pipeline` / `let`", r"^lookup\.extra"),
    ("Not supported", "pipeline-style updates, `arrayFilters`, positional `$` / `$[]` / `$[id]`, `$bit` (also via findAndModify)", r"^(upd\.(positional|pipeline|bit)|fam\.error_(array|pipeline)|update\.extra\.(positional|pipeline|array))"),
    ("Not supported", "`$replaceRoot`, `$facet`", r"^aggregate\.extra\.(replace|facet)"),
    ("Not supported", "dropIndexes, renameCollection, collStats, dbStats", r"^(idx\.extra|coll\.extra)"),
]


def rate(res, names, cat):
    sel = [res[n] for n in names if n in res and res[n]["category"] == cat]
    return sum(r["status"] == "pass" for r in sel), len(sel)


def main():
    names = list(py)
    out = []
    t = ["| Area | Feature | pymongo | Node.js | Status |", "|---|---|---|---|---|"]
    used = set()
    for area, feat, rx in ROWS:
        ns = [n for n in names if re.search(rx, n)]
        used.update(ns)
        (cp, ct), (cn, _) = rate(py, ns, "core"), rate(nd, ns, "core")
        (ep, et), (en, _) = rate(py, ns, "extra"), rate(nd, ns, "extra")
        parts_py = []
        parts_nd = []
        if ct:
            parts_py.append(f"{cp}/{ct}")
            parts_nd.append(f"{cn}/{ct}")
        if et:
            parts_py.append(f"{ep}/{et} probes")
            parts_nd.append(f"{en}/{et} probes")
        if not ns:
            status = "no tests"
        elif ct and cp == ct and cn == ct and not et:
            status = "supported, passing"
        elif ct and (cp < ct or cn < ct):
            status = "supported with differences"
        elif not ct and ep == 0 and en == 0:
            status = "not supported"
        elif not ct:
            status = "partly supported"
        else:
            status = "supported, passing"
        t.append(f"| {area} | {feat} | {' + '.join(parts_py)} | {' + '.join(parts_nd)} | {status} |")
    out.append("\n".join(t))
    out.append("")
    # pass rates
    old_names = set(t["name"] for t in json.loads(subprocess.run(["git", "show", "24cd3cb:mongo-tests/tests.json"], cwd=HERE, capture_output=True, text=True).stdout))
    rt = ["| Driver | Suite | In-scope tests (core) | Beyond-scope probes (extra) |", "|---|---|---|---|"]
    for label, res in (("pymongo 4.19.0", py), ("Node.js mongodb 7.7.0", nd)):
        for suite, sel in (("results5 suite (existing)", lambda n: n in old_names), ("new in results6", lambda n: n not in old_names), ("all", lambda n: True)):
            c = [r for n, r in res.items() if sel(n) and r["category"] == "core"]
            e = [r for n, r in res.items() if sel(n) and r["category"] == "extra"]
            cp, ep = sum(r["status"] == "pass" for r in c), sum(r["status"] == "pass" for r in e)
            rt.append(f"| {label} | {suite} | {cp}/{len(c)} ({100 * cp / max(len(c), 1):.1f}%) | {ep}/{len(e)} |")
    out.append("\n".join(rt))
    fails = [n for n in names if py[n]["category"] == "core" and (py[n]["status"] != "pass" or nd.get(n, {}).get("status") != "pass")]
    out += ["", "In-scope tests failing on either driver: " + (", ".join(fails) if fails else "none")]
    # per group
    groups = {}
    for n, r in py.items():
        g = groups.setdefault(r["group"], [0, 0, 0, 0])
        if r["category"] != "core":
            continue
        g[0] += 1
        g[1] += r["status"] == "pass"
        g[2] += nd.get(n, {}).get("status") == "pass"
    gl = ["| Group | Tests | pymongo | Node.js |", "|---|---|---|---|"]
    for g, v in groups.items():
        if v[0]:
            gl.append(f"| {g} | {v[0]} | {v[1]}/{v[0]} | {v[2]}/{v[0]} |")
    out += ["", "\n".join(gl)]
    unattributed = [n for n in names if n not in used and py[n]["category"] == "core" and n not in old_names]
    out += ["", f"New in-scope tests not attributed to a table row: {len(unattributed)} ({', '.join(unattributed[:10])}{'...' if len(unattributed) > 10 else ''})"]
    print("\n".join(out))


if __name__ == "__main__":
    main()
