"""Item 5: aggregation gaps — trigonometric, bitwise and standard-deviation expressions, $lookup with let, the remaining stages."""
import math
from bson import Decimal128, Int64
from gaps_py import case
from pymongo.errors import OperationFailure


def ev_all(db, exprs, doc=None):
    db.t.drop()
    db.t.insert_one(doc or {"_id": 1, "x": 0.5, "n": 3, "s": "str", "arr": [1, 2, 3, 4], "nul": None, "big": Int64(2 ** 40), "dec": Decimal128("0.5")})
    out = {}
    for i, e in enumerate(exprs):
        try:
            r = db.t.aggregate([{"$project": {"_id": 0, "r": e}}]).next()
            out[f"{i}:{e}"] = r.get("r", "<missing>")
        except OperationFailure as ex:
            out[f"{i}:{e}"] = {"err": (ex.code, (ex.details or {}).get("codeName"))}
    return out


VALS = [0.5, 2, -2, 1, -1, 0, 0.0, -0.0, 10, 100, 1e300, 1e-300, math.inf, -math.inf, math.nan, "x", None, True, Int64(3), 3, [0.5], [0.5, 1], [], {"a": 1}, "$x", "$n", "$s", "$nul", "$nope", "$big", "$arr"]
UNARY = ["$sin", "$cos", "$tan", "$asin", "$acos", "$atan", "$sinh", "$cosh", "$tanh", "$asinh", "$acosh", "$atanh", "$degreesToRadians", "$radiansToDegrees"]

for _op in UNARY:
    case(f"g05.expr.{_op[1:]}")((lambda op: lambda client, db: ev_all(db, [{op: v} for v in VALS]))(_op))


@case("g05.expr.atan2")
def _a2(client, db):
    pairs = [[1, 1], [0, 0], [-1, -1], [1, 0], [0, -1], [math.inf, 1], [1, math.inf], [math.nan, 1], ["x", 1], [None, 1], [1, None], [1], [1, 2, 3], "$arr", ["$x", "$n"], [Int64(5), 2], [2.5, -0.5], [True, 1]]
    return ev_all(db, [{"$atan2": p} for p in pairs])


@case("g05.expr.bitwise")
def _bit(client, db):
    L = Int64
    vals = [[5, 3], [5, L(3)], [], [5], 5, [5, 1.5], [5, 2.0], [None, 1], ["a", 1], [1, 2, 4], [7, 2, 1], [L(2 ** 40), L(2 ** 40 + 1)], [-1, 255], [2147483647, 1], [L(2 ** 62), 1], [1, "$nope"], [1, "$nul"], [Decimal128("3"), 1], [True, 1], ["$n", "$n"], ["$big", 1], [[1], 1]]
    ex = [{op: v} for op in ("$bitAnd", "$bitOr", "$bitXor") for v in vals]
    ex += [{"$bitNot": v} for v in [5, [5], L(5), None, "a", [1, 2], 2.0, [], 0, -1, 2147483647, -2147483648, L(2 ** 40), True, "$nope", "$n", "$big", Decimal128("1")]]
    return ev_all(db, ex)


@case("g05.expr.stdDev_as_expression")
def _sd(client, db):
    vals = [[1, 2, 3, 4], [1], [], ["a", 1, 2], 1, [1, 2], {"$literal": [1, 2]}, [1, 2, None], [Decimal128("1"), Decimal128("3")], None, "$nope", "x", "$arr", ["$n", "$n", 1], [1e308, -1e308], [Int64(1), Int64(5), 2.5], [[1, 2], 3], [True, 1], "$x"]
    return ev_all(db, [{op: v} for op in ("$stdDevPop", "$stdDevSamp") for v in vals])


@case("g05.expr.trig_inside_pipeline_shapes")
def _shapes(client, db):
    db.t.insert_many([{"_id": i, "a": i * 0.25, "b": i} for i in range(1, 9)])
    return {
        "match_expr": [d["_id"] for d in db.t.aggregate([{"$match": {"$expr": {"$gt": [{"$sin": "$a"}, 0.5]}}}])],
        "group": list(db.t.aggregate([{"$group": {"_id": None, "s": {"$sum": {"$cos": "$a"}}, "m": {"$max": {"$atan2": ["$a", "$b"]}}}}])),
        "addFields": [d for d in db.t.aggregate([{"$addFields": {"r": {"$degreesToRadians": "$b"}, "bits": {"$bitXor": ["$b", 5]}}}, {"$limit": 3}])],
        "sort": [d["_id"] for d in db.t.aggregate([{"$sort": {"k": 1, "_id": 1}}, {"$set": {"k": {"$bitAnd": ["$b", 3]}}}])] if False else None,
        "find_projection": [d for d in db.t.find({}, {"t": {"$tanh": "$a"}, "n": {"$bitNot": "$b"}}).sort("_id", 1).limit(2)],
        "update_pipeline": [d for d in (db.t.update_one({"_id": 1}, [{"$set": {"z": {"$acosh": {"$add": ["$b", 1]}}}}]) and db.t.find({"_id": 1}))],
    }


import random


@case("g05.sum_avg.double_accumulation")
def _sum(client, db):
    rnd = random.Random(5)
    docs = [{"_id": i, "g": i % 3, "d": rnd.uniform(-1e6, 1e6) * rnd.choice([1, 1e-3, 1e-9]), "i": rnd.randint(-5, 5)} for i in range(400)]
    docs += [{"_id": 1000 + i, "g": 9, "d": 0.1} for i in range(100)] + [{"_id": 2000 + i, "g": 8, "d": 1e16 if i % 2 else -1e16 + 1.0} for i in range(40)]
    db.t.insert_many(docs)
    return {
        "group": list(db.t.aggregate([{"$group": {"_id": "$g", "s": {"$sum": "$d"}, "a": {"$avg": "$d"}, "si": {"$sum": "$i"}, "ai": {"$avg": "$i"}, "sd": {"$stdDevPop": "$d"}, "ss": {"$stdDevSamp": "$d"}}}, {"$sort": {"_id": 1}}])),
        "expr": list(db.t.aggregate([{"$group": {"_id": "$g", "arr": {"$push": "$d"}}}, {"$project": {"s": {"$sum": "$arr"}, "a": {"$avg": "$arr"}}}, {"$sort": {"_id": 1}}])),
        "window": [d["w"] for d in db.t.aggregate([{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"w": {"$sum": "$d", "window": {"documents": ["unbounded", "current"]}}}}}, {"$match": {"_id": {"$in": [10, 399, 1050]}}}])],
    }


@case("g05.sum_avg.integer_types_and_overflow")
def _sumint(client, db):
    from bson import Int64
    big = 2 ** 62
    db.t.insert_many([{"_id": 1, "g": "a", "v": 2147483647}, {"_id": 2, "g": "a", "v": 1}, {"_id": 3, "g": "b", "v": Int64(big)}, {"_id": 4, "g": "b", "v": Int64(big)}, {"_id": 5, "g": "b", "v": Int64(big)}, {"_id": 6, "g": "c", "v": 5}, {"_id": 7, "g": "c", "v": 2.5}, {"_id": 8, "g": "d", "v": "x"}, {"_id": 9, "g": "e", "v": -2147483648}, {"_id": 10, "g": "e", "v": -1}])
    return list(db.t.aggregate([{"$group": {"_id": "$g", "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}, {"$sort": {"_id": 1}}]))


def run_pipes(db, pipes, setup=None):
    out = {}
    for i, (coll, p) in enumerate(pipes):
        try:
            out[f"{i}:{p}"] = list(db[coll].aggregate(p))
        except OperationFailure as ex:
            out[f"{i}:{p}"] = {"err": (ex.code, (ex.details or {}).get("codeName"))}
    return out


@case("g05.lookup.let_variables")
def _lk(client, db):
    db.a.insert_many([{"_id": 1, "k": 1, "m": None, "arr": [1, 2]}, {"_id": 2, "k": 2}, {"_id": 3, "k": None}, {"_id": 4}])
    db.b.insert_many([{"_id": 10, "x": 1}, {"_id": 11, "x": 2}, {"_id": 12, "x": None}, {"_id": 13}, {"_id": 14, "x": [1, 2]}])
    L = lambda let, pipe, **kw: [{"$lookup": {"from": "b", "let": let, "pipeline": pipe, "as": "j", **kw}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]
    pipes = [
        ("a", L({"v": "$k"}, [{"$match": {"$expr": {"$eq": ["$x", "$$v"]}}}])),
        ("a", L({"v": "$m"}, [{"$match": {"$expr": {"$eq": ["$x", "$$v"]}}}])),
        ("a", L({"v": "$nope"}, [{"$match": {"$expr": {"$eq": ["$x", "$$v"]}}}])),
        ("a", L({"v": "$nope"}, [{"$match": {"$expr": {"$eq": ["$$v", None]}}}])),
        ("a", L({"v": "$nope"}, [{"$match": {"$expr": {"$eq": [{"$type": "$$v"}, "missing"]}}}])),
        ("a", L({"v": "$nope"}, [{"$project": {"t": {"$type": "$$v"}, "w": "$$v"}}])),
        ("a", L({"v": "$k", "w": {"$add": ["$k", 1]}}, [{"$match": {"$expr": {"$in": ["$x", ["$$v", "$$w"]]}}}])),
        ("a", L({}, [{"$match": {"x": 1}}])),
        ("a", L({"v": "$arr"}, [{"$match": {"$expr": {"$in": ["$x", "$$v"]}}}])),
        ("a", L({"v": "$$ROOT"}, [{"$project": {"r": "$$v"}}, {"$limit": 1}])),
        ("a", L({"v": "$k"}, [{"$addFields": {"vv": "$$v"}}, {"$match": {"vv": {"$exists": True}}}])),
        ("a", L({"V": "$k"}, [{"$match": {}}])),
        ("a", L({"v": "$k"}, [{"$match": {"x": "$$v"}}])),
        ("a", L({"v": "$k"}, [{"$match": {"$expr": {"$eq": ["$x", "$$undefinedvar"]}}}])),
        ("a", L({"v": "$k"}, [{"$lookup": {"from": "b", "let": {"w": "$$v"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$x", "$$w"]}}}], "as": "inner"}}, {"$project": {"inner._id": 1}}])),
        ("a", L({"v": "$k"}, [{"$match": {"$expr": {"$eq": ["$x", "$$v"]}}}], localField="k", foreignField="x")),
        ("a", [{"$lookup": {"from": "b", "localField": "k", "foreignField": "x", "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "localField": "nope", "foreignField": "x", "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "localField": "m", "foreignField": "nope", "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "localField": "arr", "foreignField": "x", "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "nonexistent", "localField": "k", "foreignField": "x", "as": "j"}}, {"$project": {"j": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "pipeline": [{"$match": {"x": 1}}], "as": "j"}}, {"$project": {"j._id": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "localField": "k", "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "let": 5, "pipeline": [], "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "let": {"v": "$k"}, "pipeline": 5, "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "let": {"v": "$k"}, "pipeline": [{"$out": "zz"}], "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "as": "j.deep.path", "pipeline": [{"$match": {"x": 1}}]}}, {"$project": {"j": 1}}, {"$sort": {"_id": 1}}, {"$limit": 2}]),
        ("a", [{"$lookup": {"from": "b", "let": {"v": "$k"}, "pipeline": [{"$documents": [{"x": 1}]}], "as": "j"}}]),
        ("a", [{"$lookup": {"from": "b", "let": {"v": "$k"}, "pipeline": [{"$group": {"_id": "$$v", "n": {"$sum": 1}}}], "as": "j"}}, {"$project": {"j": 1}}, {"$sort": {"_id": 1}}]),
        ("a", [{"$lookup": {"from": "b", "let": {"v": "$k"}, "pipeline": [{"$sort": {"_id": -1}}, {"$limit": 1}, {"$project": {"c": {"$ifNull": ["$$v", "d"]}}}], "as": "j"}}, {"$sort": {"_id": 1}}]),
    ]
    return run_pipes(db, pipes)


@case("g05.stage.changeStreamSplitLargeEvent_outside_change_stream")
def _split(client, db):
    db.t.insert_one({"_id": 1})
    return run_pipes(db, [("t", [{"$changeStreamSplitLargeEvent": {}}]), ("t", [{"$match": {}}, {"$changeStreamSplitLargeEvent": {}}]), ("t", [{"$changeStreamSplitLargeEvent": 1}])])


@case("g05.stage.changeStreamSplitLargeEvent_in_change_stream", rs=True)
def _split2(client, db):
    db.create_collection("c")
    st = db.c.watch([{"$changeStreamSplitLargeEvent": {}}], full_document="updateLookup", max_await_time_ms=500)
    db.c.insert_one({"_id": 1, "v": "x"})
    db.c.update_one({"_id": 1}, {"$set": {"v": "y"}})
    got = []
    for _ in range(6):
        e = st.try_next()
        if e:
            got.append({k: e[k] for k in ("operationType", "documentKey", "fullDocument", "updateDescription") if k in e} | {"hasSplit": "splitEvent" in e})
    st.close()
    return got
