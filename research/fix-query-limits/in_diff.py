#!/usr/bin/env python3
"""`$in` on `_id` (and on other indexed fields): the same answers as mongod 7.0, byte for byte, in the same order.

Usage: in_diff.py --real mongodb://... --ours mongodb://...
Each case runs on both servers against the same data; documents are compared as raw BSON (bytes and order), errors by
code. Also prints our plan for each case (it must not be COLLSCAN where mongod uses an index)."""
import argparse, datetime, sys
import bson
from bson.raw_bson import RawBSONDocument
from bson import ObjectId, Int64, Decimal128
from pymongo import MongoClient
from pymongo.errors import OperationFailure

ap = argparse.ArgumentParser()
ap.add_argument("--real", required=True)
ap.add_argument("--ours", required=True)
a = ap.parse_args()
OPTS = dict(document_class=RawBSONDocument)
real, ours = MongoClient(a.real, **OPTS), MongoClient(a.ours, **OPTS)
DB = "app_in_check"
oid = [ObjectId("65a0000000000000000000%02x" % i) for i in range(10)]
when = datetime.datetime(2024, 1, 2, 3, 4, 5)
DOCS = [{"_id": i, "k": i % 7, "s": f"n{i}", "t": i * 3} for i in range(200)]
DOCS += [{"_id": f"str{i}", "k": i, "s": f"S{i}"} for i in range(20)]
DOCS += [{"_id": o, "k": n, "s": "oid"} for n, o in enumerate(oid)]
DOCS += [{"_id": {"a": 1, "b": i}, "k": i} for i in range(5)]
DOCS += [{"_id": 1000.5, "k": 1}, {"_id": Int64(5000), "k": 2}, {"_id": when, "k": 3}, {"_id": Decimal128("7.25"), "k": 4},
         {"_id": None, "k": 9}, {"_id": True, "k": 5}, {"_id": bson.MinKey(), "k": 6}, {"_id": bson.MaxKey(), "k": 7}]

for c in (real, ours):
    c.drop_database(DB)
    d = c[DB]
    d.c.insert_many(DOCS)
    d.c.create_index("k")
    d.ci.insert_many([{"_id": f"A{i}", "v": i} for i in range(5)] + [{"_id": f"a{i}", "v": i} for i in range(5)])
    d.command("create", "cc", collation={"locale": "en", "strength": 2})
    d.cc.insert_many([{"_id": f"Q{i}", "v": i} for i in range(5)])

CASES = [
    ("ints", "c", {"_id": {"$in": [5, 3, 199, 77]}}, {}),
    ("duplicates", "c", {"_id": {"$in": [5, 5, 3, 5, 3]}}, {}),
    ("int long double equal", "c", {"_id": {"$in": [7, Int64(7), 7.0, Decimal128("7")]}}, {}),
    ("mixed types", "c", {"_id": {"$in": ["str3", 9, oid[2], {"a": 1, "b": 2}, 1000.5, Int64(5000), when, Decimal128("7.25"), True]}}, {}),
    ("min max key", "c", {"_id": {"$in": [bson.MaxKey(), bson.MinKey(), 4]}}, {}),
    ("missing values", "c", {"_id": {"$in": [10**9, "nope", 4]}}, {}),
    ("empty list", "c", {"_id": {"$in": []}}, {}),
    ("one value", "c", {"_id": {"$in": [42]}}, {}),
    ("null in list", "c", {"_id": {"$in": [None, 3]}}, {}),
    ("regex in list", "c", {"_id": {"$in": [bson.Regex("^str1"), 3]}}, {}),
    ("array in list", "c", {"_id": {"$in": [[1, 2], 3]}}, {}),
    ("doc key order", "c", {"_id": {"$in": [{"b": 2, "a": 1}, {"a": 1, "b": 3}]}}, {}),
    ("with other predicate", "c", {"_id": {"$in": list(range(0, 200, 3))}, "k": 2}, {}),
    ("with range on _id", "c", {"_id": {"$in": list(range(50)), "$gte": 40}}, {}),
    ("in $and", "c", {"$and": [{"_id": {"$in": [9, 1, 2]}}, {"t": {"$gt": 2}}]}, {}),
    ("in $or", "c", {"$or": [{"_id": {"$in": [9, 1]}}, {"k": 6}]}, {}),
    ("sort desc", "c", {"_id": {"$in": [5, 3, 199, 77]}}, {"sort": [("_id", -1)]}),
    ("sort other field", "c", {"_id": {"$in": list(range(30))}}, {"sort": [("k", 1), ("_id", -1)]}),
    ("limit", "c", {"_id": {"$in": [5, 3, 199, 77, 1]}}, {"limit": 2}),
    ("skip limit", "c", {"_id": {"$in": list(range(0, 100, 7))}}, {"skip": 3, "limit": 4}),
    ("sort limit", "c", {"_id": {"$in": list(range(100))}}, {"sort": [("t", -1)], "limit": 5}),
    ("projection", "c", {"_id": {"$in": [5, 3]}}, {"projection": {"s": 1}}),
    ("batch size", "c", {"_id": {"$in": list(range(150))}}, {"batch_size": 7}),
    ("big list", "c", {"_id": {"$in": list(range(-500, 1500))}}, {}),
    ("$nin", "c", {"_id": {"$nin": list(range(190))}}, {}),
    ("$in on indexed k", "c", {"k": {"$in": [6, 2]}}, {}),
    ("$in on indexed k with sort", "c", {"k": {"$in": [6, 2]}}, {"sort": [("k", -1), ("_id", 1)]}),
    ("strings, simple collation", "ci", {"_id": {"$in": ["a1", "A3", "zz"]}}, {}),
    ("strings, query collation", "ci", {"_id": {"$in": ["a1", "A3"]}}, {"collation": {"locale": "en", "strength": 2}}),
    ("strings, collection collation", "cc", {"_id": {"$in": ["q1", "Q3"]}}, {}),
    ("numbers, collection collation", "c", {"_id": {"$in": [3, 1]}}, {"collation": {"locale": "en", "strength": 2}}),
]

def run(cl, coll, flt, kw):
    try:
        kw = dict(kw)
        return [r.raw for r in cl[DB][coll].find(flt, **kw)]
    except OperationFailure as e:
        return ("error", e.code)

def plan(coll, flt, kw):
    cmd = {"find": coll, "filter": flt}
    if "sort" in kw: cmd["sort"] = dict(kw["sort"])
    if "limit" in kw: cmd["limit"] = kw["limit"]
    if "collation" in kw: cmd["collation"] = kw["collation"]
    try:
        e = ours[DB].command("explain", cmd, verbosity="executionStats")
        e = bson.decode(e.raw) if hasattr(e, "raw") else e
        ex = e["executionStats"]
        return f"{ex.get('totalDocsExamined')} docs examined, plan {e['queryPlanner']['winningPlan'].get('stage')}/{e['queryPlanner']['winningPlan'].get('inputStage', {}).get('stage')}"
    except Exception as x:
        return f"explain failed: {x}"

bad = 0
for name, coll, flt, kw in CASES:
    r, o = run(real, coll, flt, kw), run(ours, coll, flt, kw)
    same = r == o
    bad += not same
    n = len(r) if isinstance(r, list) else r
    print(f"{'PASS' if same else 'FAIL'}  {name}: {n} documents; ours {plan(coll, flt, kw)}")
    if not same:
        print("   mongod:", [bson.decode(x) for x in r][:8] if isinstance(r, list) else r)
        print("   ours:  ", [bson.decode(x) for x in o][:8] if isinstance(o, list) else o)

# writes by `_id: {$in}`: update_many and delete_many, then the whole collection compared
for c in (real, ours):
    d = c[DB]
    d.c.update_many({"_id": {"$in": [1, 2, 3, "str4", oid[1], 10**9]}}, {"$set": {"u": 1}})
    d.c.delete_many({"_id": {"$in": [5, 6, "str5", 10**9]}})
    d.c.update_one({"_id": {"$in": [8, 7]}}, {"$inc": {"one": 1}})
    d.c.delete_one({"_id": {"$in": [11, 10]}})
r, o = run(real, "c", {}, {"sort": [("_id", 1)]}), run(ours, "c", {}, {"sort": [("_id", 1)]})
print(f"{'PASS' if r == o else 'FAIL'}  writes by _id $in: {len(r)} documents after update_many, delete_many, update_one, delete_one")
bad += r != o
for name, flt in [("count", {"_id": {"$in": [1, 2, 3, 4, 10**9]}}), ("count empty", {"_id": {"$in": []}})]:
    x, y = real[DB].c.count_documents(flt), ours[DB].c.count_documents(flt)
    print(f"{'PASS' if x == y else 'FAIL'}  {name}: {x} / {y}")
    bad += x != y
for c in (real, ours):
    c.drop_database(DB)
print("RESULT", "PASS" if bad == 0 else f"FAIL ({bad})")
sys.exit(1 if bad else 0)
