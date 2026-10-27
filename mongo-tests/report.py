#!/usr/bin/env python3
"""Builds the compatibility table and pass rates from out/results_py.json and out/results_node.json."""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
py = {r["name"]: r for r in json.load(open(os.path.join(HERE, "out", "results_py.json")))}
nd = {r["name"]: r for r in json.load(open(os.path.join(HERE, "out", "results_node.json")))}

# (area, feature, regex over test names, kind) -- kind "req" = in the requested scope, "extra" = outside it
ROWS = [
    ("Handshake", "hello / isMaster over OP_QUERY and OP_MSG (also with serverApi)", r"^admin\.(hello|ismaster|server_api_client)$"),
    ("Handshake", "legacy OP_QUERY isMaster -> OP_REPLY (raw socket)", r"^admin\.legacy_op_query_handshake$"),
    ("Handshake", "unacknowledged writes (OP_MSG moreToCome, w:0)", r"^admin\.unacknowledged_write_w0$"),
    ("Handshake", "client requests compression (server does not advertise it)", r"^admin\.client_compression_requested$"),
    ("Handshake", "ping", r"^admin\.ping$"),
    ("Handshake", "buildInfo", r"^admin\.buildinfo$"),
    ("Handshake", "getParameter (featureCompatibilityVersion)", r"^admin\.getparameter_fcv$"),
    ("Handshake", "listDatabases (incl. nameOnly)", r"^admin\.list_databases"),
    ("Handshake", "listCollections (filter, nameOnly)", r"^(coll\.create_and_list|cursor\.list_collections_batches|persist\.many_collections)$"),
    ("Handshake", "endSessions / logical sessions", r"^admin\.session_insert$"),
    ("Handshake", "unknown command -> CommandNotFound (code 59), connection stays open", r"^admin\.unknown_command$"),
    ("Commands", "insert: single, many, ordered/unordered, generated _id, 1,000 docs", r"^insert\.(one_|many_|id_|1000|types|big|field)"),
    ("Commands", "insert: duplicate key (11000, MongoDB-shaped message, keyPattern/keyValue)", r"^insert\.(dup_|id_numeric)"),
    ("Commands", "unique indexes: enforced on insert, update, upsert, index build", r"^(insert\.unique_|update\.unique_|update\.many_unordered)"),
    ("Commands", "find: filter, projection, sort, skip, limit", r"^(find\.(projection|sort|skip_limit|limit_without|filter_sort|find_one)|find\.filter\.eq_id)"),
    ("Commands", "find: batchSize, getMore paging, killCursors", r"^cursor\.(batch_size|paging|default_first|kill|getmore|aggregate_batches)"),
    ("Commands", "update: updateOne / updateMany / replaceOne, nModified, immutable _id", r"^update\.(one\.(?!.*extra)|many_|on_missing|one_picks)"),
    ("Commands", "update: upsert", r"^update\.upsert_"),
    ("Commands", "delete: deleteOne / deleteMany", r"^delete\."),
    ("Commands", "count (legacy command), countDocuments, estimatedDocumentCount", r"^count\."),
    ("Commands", "aggregate (cursor, batching, errors)", r"^aggregate\.(?!extra)"),
    ("Commands", "drop, create, dropDatabase", r"^(coll\.(create_duplicate|implicit|drop_(existing|then|removes|missing))|persist\.drop_)"),
    ("Commands", "createIndexes, listIndexes (definitions recorded; queries scan)", r"^idx\.(?!extra)"),
    ("Commands", "collection names with . / % space unicode; prefix isolation", r"^coll\.(special_names|prefix_collections|invalid_name)"),
    ("Filters", "equality, $eq, $ne", r"^find\.filter\.(eq_|op_eq|op_ne)"),
    ("Filters", "$gt, $gte, $lt, $lte (incl. type bracketing, arrays, null)", r"^find\.filter\.op_(gt|gte|lt|lte|range)"),
    ("Filters", "$in, $nin", r"^find\.filter\.op_(in|nin)"),
    ("Filters", "$exists", r"^find\.filter\.op_exists"),
    ("Filters", "$and, $or, $nor", r"^find\.filter\.op_(and|or|nor)"),
    ("Filters", "$not", r"^find\.filter\.op_not"),
    ("Filters", "dotted paths into documents and arrays of documents", r"^find\.filter\.(eq_dotted|eq_array_index|op_gt_dotted|multi_field)"),
    ("Filters", "equality against array elements and whole arrays", r"^find\.filter\.eq_(array|empty)"),
    ("Filters", "BSON comparison order for mixed types (sort and compare)", r"^find\.filter\.(cross_type|mixed_types|id_types)"),
    ("Filters", "invalid operators rejected with MongoDB's error code", r"^find\.filter\.(invalid|and_not)"),
    ("Aggregation", "$match, $sort, $limit, $skip, $project (include/exclude/$field), $count stage", r"^aggregate\.(match_|sort_|project_|count_stage|empty_|empty)"),
    ("Aggregation", "$group with $sum, $avg, $min, $max, $count", r"^aggregate\.(group_(?!first|push|add)|count_documents_shape|sum_|1000|missing)"),
    ("Update operators", "$set, $unset, $inc (int32/int64/double, overflow, nested, array index)", r"^update\.one\.(set|unset|inc|replace|modify|unknown|no_match|filter_op)"),
    ("Persistence", "idle close and wake (reader on read, upgrade on first write)", r"^persist\.(idle_|read_only|forced|1000|indexes|cursor|id_types|many)"),
    ("Persistence", "server restart (graceful and SIGKILL): acknowledged writes survive", r"^persist\.restart_"),
    ("Concurrency", "parallel inserts / increments / databases / reads during writes", r"^concurrency\."),
    # --- outside the requested scope: working ---
    ("Beyond scope (working)", "distinct", r"^distinct\."),
    ("Beyond scope (working)", "$group $first, $last, $push", r"^aggregate\.extra\.(group_first_last|group_push)$"),
    # --- not supported ---
    ("Query operators", "$regex, $elemMatch, $size, $all, $type, $mod, $expr", r"^find\.extra\."),
    ("Aggregation", "$addToSet, $unwind, $lookup, $addFields, arithmetic expressions, $replaceRoot, $facet", r"^aggregate\.extra\.(?!group_first_last|group_push)"),
    ("Update operators", "$push, $pull, $addToSet, $rename, $min, $max, $mul, $currentDate, $pop, positional $, pipeline updates, arrayFilters", r"^update\.extra\.(?!find_one_and_update)"),
    ("Commands", "findAndModify (find_one_and_update)", r"^update\.extra\.find_one_and_update$"),
    ("Commands", "dropIndexes, renameCollection, collStats, dbStats", r"^(idx\.extra|coll\.extra)"),
]


def rate(res, names):
    sel = [res[n] for n in names if n in res]
    return sum(r["status"] == "pass" for r in sel), len(sel)


def main():
    names = list(py)
    table = ["| Area | Feature | pymongo | Node.js | Status |", "|---|---|---|---|---|"]
    counted = set()
    for area, feat, rx in ROWS:
        ns = [n for n in names if re.search(rx, n)]
        counted.update(ns)
        (pp, pt), (np_, nt) = rate(py, ns), rate(nd, ns)
        extra = all(py[n]["category"] == "extra" for n in ns) if ns else False
        if pt == 0:
            status = "no tests"
        elif pp == pt and np_ == nt:
            status = "supported, passing"
        elif extra and pp == 0 and np_ == 0:
            status = "not supported"
        elif extra and (pp < pt or np_ < nt):
            status = "partly supported" if (pp or np_) else "not supported"
        else:
            status = "supported with differences"
        table.append(f"| {area} | {feat} | {pp}/{pt} | {np_}/{nt} | {status} |")
    missing = [n for n in names if n not in counted]
    out = ["\n".join(table), ""]
    out.append(f"Tests not attributed to a table row: {len(missing)} ({', '.join(missing[:12])}{'...' if len(missing) > 12 else ''}).")
    # pass rates
    rates = ["| Driver | In-scope tests (core) | Beyond-scope probes (extra) | All |", "|---|---|---|---|"]
    for label, res in (("pymongo", py), ("Node.js mongodb", nd)):
        c = [r for r in res.values() if r["category"] == "core"]
        e = [r for r in res.values() if r["category"] == "extra"]
        cp, ep = sum(r["status"] == "pass" for r in c), sum(r["status"] == "pass" for r in e)
        rates.append(f"| {label} | **{cp}/{len(c)} ({100*cp/len(c):.1f}%)** | {ep}/{len(e)} | {cp+ep}/{len(c)+len(e)} |")
    out += ["", "\n".join(rates)]
    # per group
    groups = {}
    for n, r in py.items():
        g = groups.setdefault(r["group"], {"core": [0, 0, 0, 0], "extra": [0, 0, 0, 0]})
        k = g[r["category"]]
        k[1] += 1
        k[0] += r["status"] == "pass"
        k[3] += nd[n]["status"] == "pass" if n in nd else 0
        k[2] += 1 if n in nd else 0
    gl = ["| Group | Tests | pymongo | Node.js |", "|---|---|---|---|"]
    for g, v in groups.items():
        for cat in ("core", "extra"):
            c = v[cat]
            if c[1]:
                gl.append(f"| {g}{' (beyond scope)' if cat == 'extra' else ''} | {c[1]} | {c[0]}/{c[1]} | {c[3]}/{c[2]} |")
    out += ["", "\n".join(gl)]
    # failing tests, for the record
    fails = [(n, "pymongo" if py[n]["status"] != "pass" else "", "node" if nd.get(n, {}).get("status", "pass") != "pass" else "") for n in names if py[n]["category"] == "core" and (py[n]["status"] != "pass" or nd.get(n, {}).get("status", "pass") != "pass")]
    out += ["", "Failing in-scope tests: " + (", ".join(f"{n} ({'/'.join(x for x in (a, b) if x)})" for n, a, b in fails) if fails else "none")]
    print("\n".join(out))


if __name__ == "__main__":
    main()
