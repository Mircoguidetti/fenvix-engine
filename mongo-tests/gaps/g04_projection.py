"""Item 4: find projection — positional arr.$, $elemMatch, expression values, $slice variants, inclusion/exclusion rules."""
from gaps_py import case
from pymongo.errors import OperationFailure

DOCS = [
    {"_id": 1, "a": [{"x": 1, "y": 2}, {"x": 2, "y": 3}, {"x": 2, "y": 4}], "b": 5, "c": {"d": 1, "e": [1, 2, 3], "f": {"g": 1, "h": 2}}, "s": "str", "n": None, "arr": [1, 2, 3, 4, 5], "mixed": [1, {"k": 1}, [2, 3]], "empty": []},
    {"_id": 2, "a": [{"x": 3, "y": 1}], "b": 6, "c": {"d": 2}, "s": "other", "arr": [9, 8], "mixed": "scalar"},
    {"_id": 3, "a": {"x": 7}, "b": None, "arr": 5},
    {"_id": 4, "a": 5, "c": [{"d": 1}, {"d": 2, "e": 3}]},
]


def find(db, spec, flt=None, sort=None, limit=None, ids=None):
    db.t.drop()
    db.t.insert_many(DOCS)
    cur = db.t.find(flt if flt is not None else ({"_id": {"$in": ids}} if ids else {}), spec)
    cur = cur.sort(sort or [("_id", 1)])
    if limit:
        cur = cur.limit(limit)
    try:
        return list(cur)
    except OperationFailure as e:
        return {"err": (e.code, (e.details or {}).get("codeName"))}


def group(name, specs, **kw):
    @case(name)
    def _g(client, db):
        out = {}
        for i, s in enumerate(specs):
            spec, flt = (s if isinstance(s, tuple) else (s, None))
            out[f"{i}:{spec}" + (f" | {flt}" if flt is not None else "")] = find(db, spec, flt, **kw)
        return out
    return _g


group("g04.proj.inclusion_exclusion", [
    {"b": 1}, {"b": 0}, {"b": 1, "s": 1}, {"b": 0, "s": 0}, {"_id": 0}, {"_id": 0, "b": 1}, {"_id": 1, "b": 1}, {"_id": 1, "b": 0}, {"_id": 0, "b": 0}, {"b": 1, "s": 0}, {"b": True}, {"b": False},
    {"b": 2}, {"b": -1}, {"b": 1.5}, {"b": 0.0}, {"c.d": 1}, {"c.d": 0}, {"c.f.g": 1}, {"c.f.g": 0}, {"c": {"d": 1}}, {"c": {"d": 0}}, {"c": {"f": {"g": 1}}}, {"c.d": 1, "c": 1}, {"c": 1, "c.d": 1},
    {"c.d": 1, "c.e": 1}, {"a.x": 1}, {"a.x": 0}, {"a.x": 1, "a.y": 1}, {"mixed.k": 1}, {"mixed.k": 0}, {"arr.x": 1}, {"nonexistent": 1}, {"nonexistent": 0}, {"a.b.c": 1}, {"_id": 0, "nonexistent": 1},
    {"b": 1, "_id": 0, "s": 1}, {"": 1}, {"a..b": 1}, {"a.": 1}, {".a": 1}, {"$b": 1}, {"a.$b": 1}, {"b": None}, {"b": "x"},
])

group("g04.proj.expressions", [
    {"t": "$b"}, {"t": "$s", "b": 1}, {"t": {"$add": ["$b", 1]}}, {"t": {"$add": ["$b", 1]}, "b": 1}, {"t": {"$size": "$arr"}}, {"t": {"$literal": 1}}, {"t": {"$literal": "$b"}}, {"t": "abc"}, {"t": True, "b": 1}, {"t": [1, 2]}, {"t": {"x": 1}},
    {"t": {"$concat": ["$s", "-", "x"]}}, {"t": {"$toUpper": "$s"}}, {"t": {"$cond": [{"$gt": ["$b", 5]}, "big", "small"]}}, {"t": {"$ifNull": ["$n", "dflt"]}}, {"t": "$c.d"}, {"t": "$a.x"}, {"t": "$arr"},
    {"x.y": "$b"}, {"x.y": "$b", "x.z": "$s"}, {"x": {"y": "$b"}}, {"c.d": "$b"}, {"c.z": {"$add": [1, 1]}}, {"c.d": "$b", "c.e": 1}, {"_id": "$b"}, {"_id": 0, "t": "$b"}, {"_id": "$s", "b": 1},
    {"t": "$$ROOT"}, {"t": "$$CURRENT.b"}, {"t": "$$REMOVE"}, {"t": {"$cond": [True, "$$REMOVE", 1]}}, {"b": "$$REMOVE"},
    {"t": {"$map": {"input": "$arr", "as": "v", "in": {"$multiply": ["$$v", 2]}}}}, {"t": {"$filter": {"input": "$arr", "as": "v", "cond": {"$gt": ["$$v", 2]}}}}, {"t": {"$arrayElemAt": ["$arr", 1]}},
    {"t": {"$bogus": 1}}, {"t": {"$add": 1, "$sub": 2}}, {"t": "$"}, {"t": "$$"}, {"t": "$$nope"}, {"t": {"$add": ["$b", "$s"]}},
    {"b": 0, "t": "$s"}, {"b": 1, "s": 0, "t": 1}, {"t": 1, "u": 0}, {"t": {"$meta": "textScore"}},
    {"t": {"$toString": "$b"}}, {"t": {"$type": "$b"}}, {"t": {"$isArray": "$arr"}}, {"t": {"$in": [2, "$arr"]}}, {"t": {"$concatArrays": ["$arr", [0]]}}, {"t": {"$let": {"vars": {"q": "$b"}, "in": {"$add": ["$$q", 10]}}}},
])

group("g04.proj.slice", [
    {"arr": {"$slice": 2}}, {"arr": {"$slice": -2}}, {"arr": {"$slice": 0}}, {"arr": {"$slice": 10}}, {"arr": {"$slice": -10}}, {"arr": {"$slice": [1, 2]}}, {"arr": {"$slice": [-2, 1]}}, {"arr": {"$slice": [-10, 2]}}, {"arr": {"$slice": [10, 2]}},
    {"arr": {"$slice": [1, 0]}}, {"arr": {"$slice": [1, -1]}}, {"arr": {"$slice": [0, 2]}}, {"arr": {"$slice": [1]}}, {"arr": {"$slice": [1, 2, 3]}}, {"arr": {"$slice": "x"}}, {"arr": {"$slice": 1.0}}, {"arr": {"$slice": 1.5}}, {"arr": {"$slice": None}},
    {"b": {"$slice": 1}}, {"s": {"$slice": 1}}, {"nonexistent": {"$slice": 1}}, {"c.e": {"$slice": 2}}, {"c.e": {"$slice": [1, 1]}}, {"a": {"$slice": 1}}, {"a": {"$slice": -1}}, {"mixed": {"$slice": 2}}, {"c": {"e": {"$slice": 1}}},
    {"arr": {"$slice": 2}, "b": 1}, {"arr": {"$slice": 2}, "b": 0}, {"arr": {"$slice": 2}, "s": 1, "_id": 0}, {"arr": {"$slice": 2}, "arr2": {"$slice": 1}}, {"arr": {"$slice": 2}, "c.e": {"$slice": 1}},
    {"arr": {"$slice": 2, "$foo": 1}}, {"arr": {"$slice": {"a": 1}}}, {"a.x": {"$slice": 1}}, {"empty": {"$slice": 1}}, {"empty": {"$slice": -1}},
    {"arr": {"$slice": 2147483647}}, {"arr": {"$slice": -2147483648}}, {"arr": {"$slice": [2, 2147483647]}}, {"arr": {"$slice": 9223372036854775807}},
])

group("g04.proj.elemMatch", [
    {"a": {"$elemMatch": {"x": 2}}}, {"a": {"$elemMatch": {"x": 2, "y": {"$gt": 3}}}}, {"a": {"$elemMatch": {"x": 9}}}, {"a": {"$elemMatch": {"x": {"$gt": 0}}}}, {"arr": {"$elemMatch": {"$gt": 3}}}, {"arr": {"$elemMatch": {"$gt": 100}}},
    {"b": {"$elemMatch": {"x": 1}}}, {"c.e": {"$elemMatch": {"$gt": 1}}}, {"c": {"e": {"$elemMatch": {"$gt": 1}}}}, {"a": {"$elemMatch": {"x": 2}}, "b": 1}, {"a": {"$elemMatch": {"x": 2}}, "b": 0}, {"a": {"$elemMatch": {"x": 2}}, "_id": 0},
    {"a": {"$elemMatch": {"x": 2}}, "arr": {"$elemMatch": {"$gt": 4}}}, {"a": {"$elemMatch": {"x": 2}}, "arr": {"$slice": 2}}, {"a": {"$elemMatch": {"x": 2}, "$slice": 1}}, {"a": {"$elemMatch": 5}}, {"a": {"$elemMatch": "x"}}, {"a": {"$elemMatch": []}},
    {"a": {"$elemMatch": {}}}, {"a": {"$elemMatch": {"$or": [{"x": 1}, {"y": 4}]}}}, {"a": {"$elemMatch": {"$where": "1"}}}, {"mixed": {"$elemMatch": {"k": 1}}}, {"mixed": {"$elemMatch": {"$type": "array"}}}, {"nonexistent": {"$elemMatch": {"x": 1}}},
    {"a": {"$elemMatch": {"x": 2}}, "t": "$b"}, {"a": {"$elemMatch": {"x": {"$in": [1, 3]}}}}, {"a": {"$elemMatch": {"$and": [{"x": 2}, {"y": 4}]}}}, {"a": {"$elemMatch": {"x": 2}}, "a2": {"$elemMatch": {"x": 1}}},
    ({"a": {"$elemMatch": {"y": 4}}}, {"a.x": 2}), ({"a": {"$elemMatch": {"x": 1}}}, {"b": 5}),
])

group("g04.proj.positional", [
    ({"a.$": 1}, {"a.x": 2}), ({"a.$": 1}, {"a": {"$elemMatch": {"x": 2, "y": 4}}}), ({"a.$": 1}, {"a.y": {"$gt": 3}}), ({"arr.$": 1}, {"arr": {"$gt": 2}}), ({"arr.$": 1}, {"arr": 4}), ({"arr.$": 1}, {"arr": {"$in": [8, 9]}}),
    ({"arr.$": 1}, {"arr": {"$gt": 100}}), ({"arr.$": 1}, {}), ({"arr.$": 1}, {"b": 5}), ({"arr.$": 1}, {"_id": {"$lt": 3}}), ({"arr.$": 1, "b": 1}, {"arr": 4}), ({"arr.$": 1, "b": 0}, {"arr": 4}), ({"arr.$": 1, "_id": 0}, {"arr": 4}),
    ({"arr.$": 0}, {"arr": 4}), ({"arr.$": 2}, {"arr": 4}), ({"arr.$": 1, "a.$": 1}, {"arr": 4, "a.x": 2}), ({"arr.$": 1, "a": {"$elemMatch": {"x": 2}}}, {"arr": 4}), ({"arr.$": 1, "arr2": {"$slice": 1}}, {"arr": 4}),
    ({"c.e.$": 1}, {"c.e": 2}), ({"c.$": 1}, {"c.d": 2}), ({"c.d.$": 1}, {"c.d": 1}), ({"$": 1}, {"arr": 4}), ({"a.$.x": 1}, {"a.x": 2}), ({"a.$.$": 1}, {"a.x": 2}), ({"arr.$": 1}, {"$or": [{"arr": 4}, {"b": 6}]}),
    ({"arr.$": 1}, {"$and": [{"arr": {"$gt": 1}}, {"arr": {"$lt": 4}}]}), ({"arr.$": 1}, {"arr": {"$all": [2, 3]}}), ({"arr.$": 1}, {"arr": {"$ne": 1}}), ({"arr.$": 1}, {"arr": {"$exists": True}}), ({"arr.$": 1}, {"arr": {"$size": 5}}),
    ({"a.$": 1}, {"a": {"$elemMatch": {"x": {"$gt": 1}}}}), ({"a.$": 1}, {"a.x": {"$gt": 1}, "a.y": 4}), ({"mixed.$": 1}, {"mixed": 1}), ({"mixed.$": 1}, {"mixed.k": 1}), ({"arr.$": 1}, {"arr": {"$not": {"$gt": 3}}}),
    ({"arr.$": 1, "t": "$b"}, {"arr": 4}), ({"arr.$": {"$slice": 1}}, {"arr": 4}), ({"s.$": 1}, {"s": "str"}), ({"b.$": 1}, {"b": 5}), ({"nonexistent.$": 1}, {"nonexistent": 1}),
])

group("g04.proj.positional_with_sort_limit", [({"arr.$": 1}, {"arr": {"$gt": 1}})], sort=[("_id", -1)], limit=1)


@case("g04.proj.positional_find_one_and_update_and_cursor_apis")
def _api(client, db):
    db.t.insert_many(DOCS)
    from pymongo import ReturnDocument
    out = {}
    out["find_one"] = db.t.find_one({"arr": 4}, {"arr.$": 1})
    out["find_one_and_update"] = db.t.find_one_and_update({"_id": 1, "arr": 4}, {"$set": {"z": 1}}, projection={"arr.$": 1}, return_document=ReturnDocument.AFTER)
    out["find_one_and_delete"] = db.t.find_one_and_delete({"_id": 2}, projection={"arr": {"$slice": 1}, "b": 1})
    out["find_one_and_replace_expr"] = db.t.find_one_and_replace({"_id": 3}, {"q": 1}, projection={"t": "$q"}, return_document=ReturnDocument.AFTER)
    out["aggregate_project_positional_like"] = list(db.t.aggregate([{"$match": {"_id": 1}}, {"$project": {"arr": {"$slice": ["$arr", 2]}}}]))
    return out


@case("g04.proj.command_forms")
def _cmd(client, db):
    db.t.insert_many(DOCS)
    out = {}
    for label, cmd in {"find_cmd": {"find": "t", "filter": {"_id": 1}, "projection": {"arr.$": 1}, "sort": {"_id": 1}}, "fields_legacy": {"find": "t", "filter": {"_id": 1}, "projection": {"b": 1}, "singleBatch": True},
                       "findAndModify": {"findAndModify": "t", "query": {"_id": 1, "arr": 4}, "update": {"$set": {"q": 1}}, "fields": {"arr.$": 1}, "new": True}}.items():
        try:
            r = db.command(cmd)
            out[label] = r.get("value") or r["cursor"]["firstBatch"]
        except OperationFailure as e:
            out[label] = {"err": (e.code, (e.details or {}).get("codeName"))}
    return out


# ---- generated specifications (fixed seeds): the same random projections on both servers
import random

PATHS = ["_id", "a", "a.x", "a.y", "b", "c", "c.d", "c.e", "c.f", "c.f.g", "s", "n", "arr", "mixed", "mixed.k", "empty", "nested", "nested.arr", "nested.docs", "nested.docs.v", "zz", "zz.y"]
VALUES = [1, 1, 1, 0, 0, True, False, "$b", "$c.d", "$a.x", {"$add": [1, 2]}, {"$slice": 2}, {"$slice": -1}, {"$slice": [1, 2]}, {"$elemMatch": {"x": 2}}, {"$elemMatch": {"$gt": 2}}, {"d": 1}, {"g": 0}, "lit", None, 7, {"$literal": 1}]
DOCS2 = DOCS + [
    {"_id": 5, "nested": {"arr": [1, [2, 3], {"k": 4}], "docs": [{"v": 1, "w": [1, 2]}, {"v": 2}, 3, [{"v": 5}]]}, "a": [[{"x": 1}], {"x": 2}], "c": 7, "zz": [1, 2, {"y": 1}]},
    {"_id": 6, "b": 1, "arr": [], "mixed": [[], [[1]]], "c": {"e": [[1, 2], [3]], "f": [{"g": 1}, {"g": 2, "h": 3}]}},
]


def fuzz(seed, n):
    @case(f"g04.proj.fuzz_{seed}")
    def _f(client, db):
        rnd = random.Random(seed)
        db.t.insert_many(DOCS2)
        out = {}
        for i in range(n):
            spec = {}
            for _ in range(rnd.choice([1, 1, 2, 2, 3, 4])):
                spec[rnd.choice(PATHS)] = rnd.choice(VALUES)
            special = [k for k, v in spec.items() if isinstance(v, dict) and ("$slice" in v or "$elemMatch" in v)]
            if any(k != o and (o.startswith(k + ".") or k.startswith(o + ".")) for k in special for o in spec):
                continue      # a $slice / $elemMatch sharing a path prefix with another field behaves unlike a plain collision (kept out of the generated set)
            try:
                r = list(db.t.find({}, spec).sort("_id", 1))
            except OperationFailure as e:
                r = {"err": (e.code, (e.details or {}).get("codeName"))}
            out[f"{i}:{spec}"] = r
        return out
    return _f


for _seed in (1, 2, 3, 4, 5, 6):
    fuzz(_seed, 250)


FILTER_POOL = [{"arr": {"$gt": 2}}, {"arr": 4}, {"a.x": 2}, {"a": {"$elemMatch": {"x": 2}}}, {"c.e": 2}, {"mixed": 1}, {"mixed.k": 1}, {"arr": {"$in": [8, 2]}}, {"arr": {"$ne": 1}}, {"arr": {"$size": 5}}, {"$or": [{"arr": 4}, {"b": 6}]},
               {"arr": {"$exists": True}}, {"b": 5}, {"a.y": {"$gt": 2}}, {"nested.docs.v": 2}, {"zz": 2}, {"arr": {"$all": [2, 3]}}, {"a.x": {"$in": [1, 3]}, "arr": {"$lt": 9}}, {"$and": [{"arr": {"$gt": 1}}, {"a.x": 2}]}, {"mixed": {"$type": "array"}},
               {"arr": {"$mod": [3, 0]}}, {"arr": {"$regex": "x"}}, {}, {"arr": {"$not": {"$lt": 2}}}, {"c.d": 1}, {"c": {"$elemMatch": {"d": 2}}}, {"nested.arr": 2}, {"zz.y": 1}, {"arr": {"$gte": 2, "$lte": 3}}, {"a.x": 2, "a.y": 4}]
POS = ["arr.$", "a.$", "c.$", "c.e.$", "mixed.$", "nested.arr.$", "nested.docs.$", "zz.$", "b.$", "a.x.$", "s.$"]


def pos_fuzz(seed):
    @case(f"g04.proj.positional_fuzz_{seed}")
    def _pf(client, db):
        rnd = random.Random(seed)
        db.t.insert_many(DOCS2)
        out = {}
        for i in range(400):
            flt = rnd.choice(FILTER_POOL)
            spec = {rnd.choice(POS): 1}
            if rnd.random() < 0.3:
                spec[rnd.choice(["b", "s", "_id", "arr2"])] = rnd.choice([1, 0])
            try:
                r = list(db.t.find(flt, spec).sort("_id", 1))
            except OperationFailure as e:
                r = {"err": (e.code, (e.details or {}).get("codeName"))}
            out[f"{i}:{spec}|{flt}"] = r
        return out
    return _pf


for _s in (99, 100, 101):
    pos_fuzz(_s)
