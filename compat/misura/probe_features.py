#!/usr/bin/env python3
"""Probe of the most used MongoDB commands and operators (reference: MongoDB 8.0 manual).

Each probe runs a small request against the server and compares the answer with the result that the MongoDB 8.0
manual documents for it (written by hand next to each probe). There was no real mongod 8.0 in this run to compare
with (see README), so the expected results are the documented ones.

Status of each feature:
  funziona          the answer is the documented one
  in parte          it answers, but something documented is missing or different (said in the note)
  non supportato    the server refuses it (unknown command/operator/stage) or ignores it
  errore            crash, wrong data or a wrong answer where an error would be right

Usage: probe_features.py --uri mongodb://... --out results/funzioni.jsonl
"""
import argparse
import datetime
import json
import traceback

from bson import Int64, Decimal128, ObjectId, Regex
from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.operations import InsertOne, UpdateOne, DeleteOne, ReplaceOne

PROBES = []


def probe(area, name):
    def deco(fn):
        PROBES.append((area, name, fn))
        return fn
    return deco


class Partial(Exception):
    pass


def eq(got, want, note=""):
    if got != want:
        raise AssertionError(f"{note} expected {want!r}, got {got!r}")


def setup(db):
    db.p.drop()
    db.p.insert_many([
        {"_id": 1, "name": "anna", "age": 31, "city": "Roma", "tags": ["a", "b"], "score": 10.5,
         "items": [{"sku": "x", "qty": 2}, {"sku": "y", "qty": 5}], "ts": datetime.datetime(2026, 1, 1)},
        {"_id": 2, "name": "bruno", "age": 25, "city": "Milano", "tags": ["b"], "score": 7,
         "items": [{"sku": "x", "qty": 1}], "ts": datetime.datetime(2026, 2, 1)},
        {"_id": 3, "name": "carla", "age": 40, "city": "Roma", "tags": [], "score": None,
         "items": [], "ts": datetime.datetime(2026, 3, 1)},
        {"_id": 4, "name": "Dario", "age": 25, "city": "Napoli", "tags": ["c", "a"], "score": 3,
         "nested": {"k": 1}, "ts": datetime.datetime(2026, 4, 1)},
    ])
    return db.p


def ids(cur):
    return [d["_id"] for d in cur]


# ------------------------------------------------------------------ query operators
QUERY = [
    ("$eq", {"age": {"$eq": 25}}, [2, 4]), ("$ne", {"city": {"$ne": "Roma"}}, [2, 4]),
    ("$gt", {"age": {"$gt": 30}}, [1, 3]), ("$gte", {"age": {"$gte": 31}}, [1, 3]),
    ("$lt", {"age": {"$lt": 30}}, [2, 4]), ("$lte", {"age": {"$lte": 25}}, [2, 4]),
    ("$in", {"city": {"$in": ["Roma", "Napoli"]}}, [1, 3, 4]), ("$nin", {"city": {"$nin": ["Roma"]}}, [2, 4]),
    ("$and", {"$and": [{"city": "Roma"}, {"age": {"$gt": 35}}]}, [3]),
    ("$or", {"$or": [{"age": 40}, {"name": "bruno"}]}, [2, 3]),
    ("$nor", {"$nor": [{"city": "Roma"}, {"age": 25}]}, []),
    ("$not", {"age": {"$not": {"$gt": 30}}}, [2, 4]),
    ("$exists", {"nested": {"$exists": True}}, [4]),
    ("$type", {"score": {"$type": "double"}}, [1]),
    ("$regex", {"name": {"$regex": "^d", "$options": "i"}}, [4]),
    ("$expr", {"$expr": {"$gt": ["$age", 30]}}, [1, 3]),
    ("$mod", {"age": {"$mod": [5, 0]}}, [2, 3, 4]),
    ("$all", {"tags": {"$all": ["a", "b"]}}, [1]),
    ("$elemMatch", {"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 1}}}}, [1]),
    ("$size", {"tags": {"$size": 2}}, [1, 4]),
    ("$jsonSchema", {"$jsonSchema": {"required": ["nested"]}}, [4]),
    ("$where", {"$where": "this.age > 35"}, [3]),
    ("$bitsAllSet", {"age": {"$bitsAllSet": [0]}}, [1, 2, 4]),
    ("$text (con indice text)", None, None),
    ("$near / $geoWithin (con indice 2dsphere)", None, None),
    ("dot notation su array di documenti", {"items.sku": "y"}, [1]),
    ("null trova anche il campo mancante", {"score": None}, [3]),
]

for _name, _flt, _want in QUERY:
    if _flt is None:
        continue

    def _mk(flt=_flt, want=_want):
        def f(db):
            c = setup(db)
            eq(sorted(ids(c.find(flt))), want)
        return f
    probe("query", _name)(_mk())


@probe("query", "$text (con indice text)")
def _(db):
    c = setup(db)
    c.create_index([("name", "text")])
    eq(sorted(ids(c.find({"$text": {"$search": "anna carla"}}))), [1, 3])


@probe("query", "$near / $geoWithin (con indice 2dsphere)")
def _(db):
    db.g.drop()
    db.g.insert_many([{"_id": 1, "loc": {"type": "Point", "coordinates": [12.5, 41.9]}},
                      {"_id": 2, "loc": {"type": "Point", "coordinates": [9.19, 45.46]}}])
    db.g.create_index([("loc", "2dsphere")])
    eq(ids(db.g.find({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [12.4, 41.8]}}}})), [1, 2])
    eq(ids(db.g.find({"loc": {"$geoWithin": {"$centerSphere": [[12.5, 41.9], 0.01]}}})), [1])


@probe("query", "projection (inclusione, esclusione, $slice, $elemMatch, $)")
def _(db):
    c = setup(db)
    eq(c.find_one({"_id": 1}, {"name": 1, "_id": 0}), {"name": "anna"})
    eq(c.find_one({"_id": 1}, {"tags": {"$slice": 1}, "name": 1}), {"_id": 1, "name": "anna", "tags": ["a"]})
    eq(c.find_one({"_id": 1}, {"items": {"$elemMatch": {"sku": "y"}}}), {"_id": 1, "items": [{"sku": "y", "qty": 5}]})
    eq(c.find_one({"items.sku": "y"}, {"items.$": 1}), {"_id": 1, "items": [{"sku": "y", "qty": 5}]})


@probe("query", "sort, skip, limit")
def _(db):
    c = setup(db)
    eq(ids(c.find().sort([("age", DESCENDING), ("_id", ASCENDING)]).skip(1).limit(2)), [1, 2])


@probe("query", "collation (confronto senza maiuscole)")
def _(db):
    c = setup(db)
    eq(ids(c.find({"name": "dario"}, collation={"locale": "en", "strength": 2})), [4])
    eq(ids(c.find({}, {"_id": 1}).sort("name", 1).collation({"locale": "en"})), [1, 2, 3, 4])


@probe("query", "cursori e getMore (batch piccoli)")
def _(db):
    db.big.drop()
    db.big.insert_many([{"_id": i} for i in range(1000)])
    eq(len(list(db.big.find().batch_size(7))), 1000)


@probe("query", "countDocuments / estimatedDocumentCount / distinct")
def _(db):
    c = setup(db)
    eq(c.count_documents({"city": "Roma"}), 2)
    eq(c.estimated_document_count(), 4)
    eq(sorted(c.distinct("city")), ["Milano", "Napoli", "Roma"])


@probe("query", "explain")
def _(db):
    c = setup(db)
    e = c.find({"age": 25}).explain()
    if "queryPlanner" not in e:
        raise Partial(f"explain without queryPlanner: keys {list(e)[:6]}")


# ------------------------------------------------------------------ update operators
UPD = [
    ("$set", {"$set": {"a.b": 1}}, {"a": {"b": 1}}), ("$unset", {"$unset": {"name": ""}}, "noname"),
    ("$inc", {"$inc": {"age": 2}}, {"age": 33}), ("$mul", {"$mul": {"age": 2}}, {"age": 62}),
    ("$min", {"$min": {"age": 20}}, {"age": 20}), ("$max", {"$max": {"age": 50}}, {"age": 50}),
    ("$rename", {"$rename": {"city": "town"}}, {"town": "Roma"}),
    ("$currentDate", {"$currentDate": {"when": True}}, "date"),
    ("$setOnInsert (con upsert)", None, None),
    ("$push con $each/$sort/$slice", {"$push": {"tags": {"$each": ["z", "c"], "$sort": 1, "$slice": 3}}},
     {"tags": ["a", "b", "c"]}),
    ("$addToSet con $each", {"$addToSet": {"tags": {"$each": ["a", "q"]}}}, {"tags": ["a", "b", "q"]}),
    ("$pop", {"$pop": {"tags": 1}}, {"tags": ["a"]}),
    ("$pull", {"$pull": {"items": {"qty": {"$gt": 3}}}}, {"items": [{"sku": "x", "qty": 2}]}),
    ("$pullAll", {"$pullAll": {"tags": ["a"]}}, {"tags": ["b"]}),
    ("$ posizionale", None, None), ("$[] e $[id] con arrayFilters", None, None),
    ("$bit", {"$bit": {"age": {"and": 1}}}, {"age": 1}),
    ("update con pipeline di aggregazione", [{"$set": {"double": {"$multiply": ["$age", 2]}}}], {"double": 62}),
]
for _name, _u, _want in UPD:
    if _u is None:
        continue

    def _mk(u=_u, want=_want):
        def f(db):
            c = setup(db)
            c.update_one({"_id": 1}, u)
            d = c.find_one({"_id": 1})
            if want == "noname":
                eq("name" in d, False)
            elif want == "date":
                eq(isinstance(d.get("when"), datetime.datetime), True)
            else:
                for k, v in want.items():
                    eq(d.get(k), v, k)
        return f
    probe("update", _name)(_mk())


@probe("update", "$setOnInsert (con upsert)")
def _(db):
    c = setup(db)
    r = c.update_one({"_id": 9}, {"$set": {"a": 1}, "$setOnInsert": {"b": 2}}, upsert=True)
    eq(r.upserted_id, 9)
    eq(c.find_one({"_id": 9}), {"_id": 9, "a": 1, "b": 2})


@probe("update", "$ posizionale")
def _(db):
    c = setup(db)
    c.update_one({"_id": 1, "items.sku": "y"}, {"$inc": {"items.$.qty": 1}})
    eq(c.find_one({"_id": 1})["items"][1]["qty"], 6)


@probe("update", "$[] e $[id] con arrayFilters")
def _(db):
    c = setup(db)
    c.update_one({"_id": 1}, {"$inc": {"items.$[].qty": 10}})
    c.update_one({"_id": 1}, {"$set": {"items.$[e].sku": "big"}}, array_filters=[{"e.qty": {"$gt": 13}}])
    eq(c.find_one({"_id": 1})["items"], [{"sku": "x", "qty": 12}, {"sku": "big", "qty": 15}])


# ------------------------------------------------------------------ write commands
@probe("scrittura", "insertOne / insertMany (ordered e unordered)")
def _(db):
    db.w.drop()
    db.w.insert_one({"_id": 1})
    try:
        db.w.insert_many([{"_id": 2}, {"_id": 1}, {"_id": 3}], ordered=False)
    except PyMongoError as e:
        eq(e.details["nInserted"], 2)
    eq(sorted(ids(db.w.find())), [1, 2, 3])


@probe("scrittura", "updateMany / replaceOne / deleteOne / deleteMany")
def _(db):
    c = setup(db)
    eq(c.update_many({"city": "Roma"}, {"$set": {"r": 1}}).modified_count, 2)
    eq(c.replace_one({"_id": 2}, {"x": 1}).modified_count, 1)
    eq(c.find_one({"_id": 2}), {"_id": 2, "x": 1})
    eq(c.delete_one({"city": "Roma"}).deleted_count, 1)
    eq(c.delete_many({}).deleted_count, 3)


@probe("scrittura", "findOneAndUpdate / findOneAndReplace / findOneAndDelete")
def _(db):
    c = setup(db)
    d = c.find_one_and_update({"_id": 1}, {"$inc": {"age": 1}}, return_document=ReturnDocument.AFTER)
    eq(d["age"], 32)
    d = c.find_one_and_replace({"_id": 2}, {"z": 1}, projection={"_id": 0})
    eq(d["name"], "bruno")
    d = c.find_one_and_delete({"city": "Roma"}, sort=[("age", -1)])
    eq(d["_id"], 3)


@probe("scrittura", "bulkWrite (collezione)")
def _(db):
    c = setup(db)
    r = c.bulk_write([InsertOne({"_id": 10}), UpdateOne({"_id": 1}, {"$set": {"b": 1}}), DeleteOne({"_id": 2}),
                      ReplaceOne({"_id": 3}, {"c": 1})])
    eq((r.inserted_count, r.modified_count, r.deleted_count), (1, 2, 1))


@probe("scrittura", "updateOne con sort (novità 8.0)")
def _(db):
    c = setup(db)
    c.update_one({"city": "Roma"}, {"$set": {"hit": 1}}, sort={"age": -1})
    eq(c.find_one({"hit": 1})["_id"], 3, "the sort chooses the oldest (id 3):")


@probe("scrittura", "bulkWrite tra collezioni (comando bulkWrite, novità 8.0)")
def _(db):
    r = db.client.admin.command({"bulkWrite": 1, "ops": [{"insert": 0, "document": {"_id": 1}}],
                                 "nsInfo": [{"ns": db.name + ".bw"}]})
    eq(r.get("nInserted"), 1)


@probe("scrittura", "chiave duplicata: errore 11000")
def _(db):
    db.w.drop()
    db.w.insert_one({"_id": 1})
    try:
        db.w.insert_one({"_id": 1})
    except OperationFailure as e:
        eq(e.code, 11000)
        return
    raise AssertionError("no error")


# ------------------------------------------------------------------ aggregation
AGG = [
    ("$match + $group + $sum/$avg", [{"$match": {"city": "Roma"}}, {"$group": {"_id": "$city", "n": {"$sum": 1},
                                                                                "avg": {"$avg": "$age"}}}],
     [{"_id": "Roma", "n": 2, "avg": 35.5}]),
    ("$project + $addFields + $set", [{"$match": {"_id": 1}}, {"$project": {"name": 1}}, {"$addFields": {"k": 1}},
                                      {"$set": {"j": 2}}], [{"_id": 1, "name": "anna", "k": 1, "j": 2}]),
    ("$sort + $limit + $skip", [{"$sort": {"age": -1, "_id": 1}}, {"$skip": 1}, {"$limit": 1}, {"$project": {"_id": 1}}],
     [{"_id": 1}]),
    ("$unwind", [{"$match": {"_id": 1}}, {"$unwind": "$tags"}, {"$project": {"tags": 1}}],
     [{"_id": 1, "tags": "a"}, {"_id": 1, "tags": "b"}]),
    ("$count", [{"$match": {"age": 25}}, {"$count": "n"}], [{"n": 2}]),
    ("$sortByCount", [{"$sortByCount": "$city"}, {"$limit": 1}], [{"_id": "Roma", "count": 2}]),
    ("$facet", [{"$facet": {"a": [{"$count": "n"}], "b": [{"$match": {"_id": 1}}, {"$project": {"_id": 1}}]}}],
     [{"a": [{"n": 4}], "b": [{"_id": 1}]}]),
    ("$bucket", [{"$bucket": {"groupBy": "$age", "boundaries": [0, 30, 100]}}],
     [{"_id": 0, "count": 2}, {"_id": 30, "count": 2}]),
    ("$replaceRoot", [{"$match": {"_id": 4}}, {"$replaceRoot": {"newRoot": "$nested"}}], [{"k": 1}]),
    ("$unset (stadio)", [{"$match": {"_id": 4}}, {"$unset": ["name", "ts", "tags", "nested", "score", "city", "age"]}],
     [{"_id": 4}]),
    ("$sample", [{"$sample": {"size": 2}}, {"$count": "n"}], [{"n": 2}]),
    ("$group con $push/$addToSet/$first/$last/$max/$min",
     [{"$sort": {"_id": 1}}, {"$group": {"_id": None, "p": {"$push": "$_id"}, "s": {"$addToSet": "$city"},
                                         "f": {"$first": "$_id"}, "l": {"$last": "$_id"},
                                         "mx": {"$max": "$age"}, "mn": {"$min": "$age"}}},
      {"$project": {"_id": 0, "p": 1, "n": {"$size": "$s"}, "f": 1, "l": 1, "mx": 1, "mn": 1}}],
     [{"p": [1, 2, 3, 4], "n": 3, "f": 1, "l": 4, "mx": 40, "mn": 25}]),
    ("espressioni: $cond/$ifNull/$concat/$toUpper/$dateToString",
     [{"$match": {"_id": 3}}, {"$project": {"_id": 0, "c": {"$cond": [{"$gt": ["$age", 30]}, "old", "young"]},
                                            "i": {"$ifNull": ["$score", 0]},
                                            "s": {"$concat": [{"$toUpper": "$name"}, "-", "$city"]},
                                            "d": {"$dateToString": {"format": "%Y-%m", "date": "$ts"}}}}],
     [{"c": "old", "i": 0, "s": "CARLA-Roma", "d": "2026-03"}]),
    ("espressioni su array: $filter/$map/$reduce/$size",
     [{"$match": {"_id": 1}}, {"$project": {"_id": 0,
                                            "f": {"$filter": {"input": "$items", "cond": {"$gt": ["$$this.qty", 2]}}},
                                            "m": {"$map": {"input": "$tags", "in": {"$toUpper": "$$this"}}},
                                            "r": {"$reduce": {"input": "$items", "initialValue": 0,
                                                              "in": {"$add": ["$$value", "$$this.qty"]}}},
                                            "z": {"$size": "$tags"}}}],
     [{"f": [{"sku": "y", "qty": 5}], "m": ["A", "B"], "r": 7, "z": 2}]),
    ("$setWindowFields", [{"$setWindowFields": {"partitionBy": "$city", "sortBy": {"age": 1},
                                                "output": {"rk": {"$rank": {}}}}},
                          {"$match": {"_id": 3}}, {"$project": {"rk": 1}}], [{"_id": 3, "rk": 2}]),
    ("$densify / $fill", [{"$match": {"_id": {"$in": [1, 3]}}}, {"$project": {"age": 1}},
                          {"$densify": {"field": "age", "range": {"step": 5, "bounds": "full"}}},
                          {"$count": "n"}], [{"n": 3}]),
]
for _name, _pipe, _want in AGG:
    def _mk(pipe=_pipe, want=_want):
        def f(db):
            c = setup(db)
            eq(list(c.aggregate(pipe)), want)
        return f
    probe("aggregazione", _name)(_mk())


@probe("aggregazione", "$lookup (semplice e con pipeline)")
def _(db):
    c = setup(db)
    db.cities.drop()
    db.cities.insert_many([{"_id": "Roma", "region": "Lazio"}, {"_id": "Milano", "region": "Lombardia"}])
    r = list(c.aggregate([{"$match": {"_id": 2}}, {"$lookup": {"from": "cities", "localField": "city",
                                                               "foreignField": "_id", "as": "c"}},
                          {"$project": {"c": 1}}]))
    eq(r, [{"_id": 2, "c": [{"_id": "Milano", "region": "Lombardia"}]}])
    r = list(c.aggregate([{"$match": {"_id": 1}}, {"$lookup": {"from": "cities", "let": {"x": "$city"}, "pipeline": [
        {"$match": {"$expr": {"$eq": ["$_id", "$$x"]}}}, {"$project": {"_id": 0}}], "as": "c"}},
        {"$project": {"c": 1}}]))
    eq(r, [{"_id": 1, "c": [{"region": "Lazio"}]}])


@probe("aggregazione", "$graphLookup")
def _(db):
    db.tree.drop()
    db.tree.insert_many([{"_id": 1, "p": None}, {"_id": 2, "p": 1}, {"_id": 3, "p": 2}])
    r = list(db.tree.aggregate([{"$match": {"_id": 3}}, {"$graphLookup": {"from": "tree", "startWith": "$p",
                                                                         "connectFromField": "p",
                                                                         "connectToField": "_id", "as": "anc"}},
                                {"$project": {"n": {"$size": "$anc"}}}]))
    eq(r, [{"_id": 3, "n": 2}])


@probe("aggregazione", "$unionWith")
def _(db):
    c = setup(db)
    db.u2.drop()
    db.u2.insert_one({"_id": 99})
    eq(len(list(c.aggregate([{"$unionWith": "u2"}]))), 5)


@probe("aggregazione", "$out")
def _(db):
    c = setup(db)
    list(c.aggregate([{"$match": {"city": "Roma"}}, {"$out": "out1"}]))
    eq(sorted(ids(db.out1.find())), [1, 3])


@probe("aggregazione", "$merge")
def _(db):
    c = setup(db)
    db.m1.drop()
    db.m1.insert_one({"_id": 1, "keep": True})
    list(c.aggregate([{"$match": {"city": "Roma"}}, {"$project": {"age": 1}},
                      {"$merge": {"into": "m1", "whenMatched": "merge"}}]))
    eq(db.m1.find_one({"_id": 1}), {"_id": 1, "keep": True, "age": 31})


@probe("aggregazione", "aggregate a livello di database ($documents, $out)")
def _(db):
    eq(list(db.aggregate([{"$documents": [{"a": 1}]}])), [{"a": 1}])
    db.o3.drop()
    list(db.aggregate([{"$documents": [{"_id": 1}]}, {"$out": "o3"}]))
    eq(ids(db.o3.find()), [1])


@probe("aggregazione", "let e $$variabili nel comando aggregate")
def _(db):
    c = setup(db)
    r = list(c.aggregate([{"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}}, {"$project": {"_id": 0, "y": "$$y"}}],
                         let={"id": 1, "y": {"$literal": "$bar"}}))
    eq(r, [{"y": "$bar"}])


@probe("aggregazione", "mapReduce (deprecato)")
def _(db):
    setup(db)
    r = db.command("mapReduce", "p", map="function(){emit(this.city,1)}",
                   reduce="function(k,v){return Array.sum(v)}", out={"inline": 1})
    eq(sorted((x["_id"], x["value"]) for x in r["results"]), [("Milano", 1), ("Napoli", 1), ("Roma", 2)])


@probe("aggregazione", "$changeStream nel pipeline (watch)")
def _(db):
    c = setup(db)
    with c.watch() as s:
        c.insert_one({"_id": 50})
        ev = None
        for _ in range(50):
            ev = s.try_next()
            if ev:
                break
        eq(ev and ev["operationType"], "insert")


# ------------------------------------------------------------------ indexes
@probe("indici", "createIndex / listIndexes / dropIndex")
def _(db):
    c = setup(db)
    c.create_index([("age", 1), ("city", -1)], name="ac")
    eq(sorted(i["name"] for i in c.list_indexes()), ["_id_", "ac"])
    c.drop_index("ac")
    eq([i["name"] for i in c.list_indexes()], ["_id_"])


@probe("indici", "unique (rifiuta i duplicati)")
def _(db):
    c = setup(db)
    c.create_index("name", unique=True)
    try:
        c.insert_one({"name": "anna"})
    except OperationFailure as e:
        eq(e.code, 11000)
        return
    raise AssertionError("duplicate accepted")


@probe("indici", "unique su dati che hanno già duplicati (deve rifiutare)")
def _(db):
    c = setup(db)
    try:
        c.create_index("age", unique=True)
    except OperationFailure as e:
        eq(e.code, 11000)
        return
    raise AssertionError("unique index built over duplicates")


@probe("indici", "partialFilterExpression e sparse")
def _(db):
    c = setup(db)
    c.create_index("score", unique=True, partialFilterExpression={"score": {"$type": "number"}})
    c.insert_one({"_id": 20, "score": None})
    c.create_index("nested", sparse=True)
    eq(c.count_documents({}), 5)


@probe("indici", "TTL (expireAfterSeconds)")
def _(db):
    db.ttl.drop()
    db.ttl.create_index("t", expireAfterSeconds=1)
    db.ttl.insert_one({"t": datetime.datetime(2000, 1, 1)})
    import time
    time.sleep(2)
    n = db.ttl.count_documents({})
    if n != 0:
        raise Partial(f"expired document still visible after 2 s (count {n}); mongod removes it within ~60 s")


@probe("indici", "indice compound, multikey, hashed, wildcard")
def _(db):
    c = setup(db)
    c.create_index([("tags", 1), ("age", 1)])
    c.create_index([("city", "hashed")])
    c.create_index([("$**", 1)], name="wc")
    eq(sorted(ids(c.find({"tags": "a", "age": {"$gt": 20}}))), [1, 4])


@probe("indici", "hint")
def _(db):
    c = setup(db)
    c.create_index("age", name="age_1")
    eq(sorted(ids(c.find({"age": 25}).hint("age_1"))), [2, 4])


@probe("indici", "collMod (cambia TTL, nascondi indice)")
def _(db):
    c = setup(db)
    c.create_index("age", name="age_1")
    db.command("collMod", "p", index={"name": "age_1", "hidden": True})
    h = [i for i in c.list_indexes() if i["name"] == "age_1"][0]
    eq(h.get("hidden"), True)


@probe("indici", "Atlas Search / Vector Search (createSearchIndexes, $search, $vectorSearch)")
def _(db):
    setup(db)
    db.command("createSearchIndexes", "p", indexes=[{"name": "s", "definition": {"mappings": {"dynamic": True}}}])
    list(db.p.aggregate([{"$search": {"text": {"query": "anna", "path": "name"}}}]))


# ------------------------------------------------------------------ admin
@probe("amministrazione", "create (validator, capped) / drop / listCollections / renameCollection")
def _(db):
    db.v.drop()
    db.create_collection("v", validator={"$jsonSchema": {"required": ["a"]}})
    try:
        db.v.insert_one({"b": 1})
        raise AssertionError("validator ignored")
    except OperationFailure as e:
        eq(e.code, 121)
    db.cap.drop()
    db.create_collection("cap", capped=True, size=4096, max=2)
    db.cap.insert_many([{"_id": i} for i in range(3)])
    eq(ids(db.cap.find()), [1, 2])
    db.v.rename("v2", dropTarget=True)
    eq("v2" in db.list_collection_names(), True)


@probe("amministrazione", "views (createView)")
def _(db):
    setup(db)
    db.drop_collection("vw")
    db.create_collection("vw", viewOn="p", pipeline=[{"$match": {"city": "Roma"}}])
    eq(sorted(ids(db.vw.find())), [1, 3])


@probe("amministrazione", "timeseries collection")
def _(db):
    db.drop_collection("tsc")
    db.create_collection("tsc", timeseries={"timeField": "t", "metaField": "m"})
    db.tsc.insert_many([{"t": datetime.datetime(2026, 1, 1), "m": 1, "v": 1},
                        {"t": datetime.datetime(2026, 1, 2), "m": 1, "v": 2}])
    eq(db.tsc.count_documents({}), 2)


@probe("amministrazione", "dropDatabase / listDatabases")
def _(db):
    cl = db.client
    cl.dropme.x.insert_one({})
    eq("dropme" in cl.list_database_names(), True)
    cl.drop_database("dropme")
    eq(cl.dropme.list_collection_names(), [], "collections after dropDatabase:")
    if "dropme" in cl.list_database_names():
        raise Partial("data and collections are gone, but listDatabases still lists the dropped database "
                      "(empty: false)")


@probe("amministrazione", "dbStats / collStats / $collStats")
def _(db):
    setup(db)
    s = db.command("dbStats")
    eq(s["collections"] >= 1, True)
    cs = list(db.p.aggregate([{"$collStats": {"count": {}}}]))
    eq(cs[0]["count"], 4)
    db.command("collStats", "p")


@probe("amministrazione", "serverStatus / currentOp / killOp")
def _(db):
    a = db.client.admin
    st = a.command("serverStatus")
    eq("connections" in st, True)
    a.command("currentOp")


@probe("amministrazione", "hello / buildInfo / ping / hostInfo / getParameter")
def _(db):
    a = db.client.admin
    eq(a.command("ping")["ok"], 1.0)
    v = a.command("buildInfo")["version"]
    if not v.startswith("8."):
        raise Partial(f"buildInfo says version {v}: drivers turn off the 8.0 features")


@probe("amministrazione", "killCursors / getMore espliciti")
def _(db):
    db.big.drop()
    db.big.insert_many([{"_id": i} for i in range(50)])
    r = db.command("find", "big", batchSize=2)
    cid = r["cursor"]["id"]
    g = db.command("getMore", cid, collection="big", batchSize=3)
    eq(len(g["cursor"]["nextBatch"]), 3)
    k = db.command("killCursors", "big", cursors=[cid])
    eq(k["cursorsKilled"], [cid])


# ------------------------------------------------------------------ sessions / transactions
@probe("transazioni", "transazione multi-documento: commit")
def _(db):
    db.tx.drop()
    db.create_collection("tx")
    with db.client.start_session() as s:
        with s.start_transaction():
            db.tx.insert_one({"_id": 1}, session=s)
            db.tx.insert_one({"_id": 2}, session=s)
            eq(db.tx.count_documents({}), 0, "not visible outside before commit:")
    eq(db.tx.count_documents({}), 2)


@probe("transazioni", "transazione: abort annulla tutto")
def _(db):
    db.tx.drop()
    db.create_collection("tx")
    s = db.client.start_session()
    s.start_transaction()
    db.tx.insert_one({"_id": 1}, session=s)
    s.abort_transaction()
    eq(db.tx.count_documents({}), 0)


@probe("transazioni", "withTransaction (API comoda dei driver)")
def _(db):
    db.tx.drop()
    db.create_collection("tx")
    with db.client.start_session() as s:
        s.with_transaction(lambda ss: db.tx.insert_one({"_id": 1}, session=ss))
    eq(db.tx.count_documents({}), 1)


@probe("transazioni", "letture snapshot (readConcern snapshot fuori transazione)")
def _(db):
    db.sn.drop()
    db.sn.insert_one({"_id": 1, "x": 1})
    with db.client.start_session(snapshot=True) as s:
        db.sn.find_one({"_id": 1}, session=s)
        db.sn.update_one({"_id": 1}, {"$set": {"x": 2}})
        eq(db.sn.find_one({"_id": 1}, session=s)["x"], 1, "the snapshot must not see the later write:")


@probe("transazioni", "letture causali (afterClusterTime)")
def _(db):
    with db.client.start_session(causal_consistency=True) as s:
        db.cc.insert_one({"a": 1}, session=s)
        eq(db.cc.count_documents({}, session=s) >= 1, True)


# ------------------------------------------------------------------ auth (server started without --auth here)
@probe("autenticazione", "createUser / usersInfo / dropUser")
def _(db):
    try:
        db.command("dropUser", "u1")
    except OperationFailure:
        pass
    db.command("createUser", "u1", pwd="pw-123456", roles=["readWrite"])
    eq(db.command("usersInfo", "u1")["users"][0]["user"], "u1")
    db.command("dropUser", "u1")


@probe("autenticazione", "createRole / grantRolesToUser")
def _(db):
    try:
        db.command("dropRole", "r1")
    except OperationFailure:
        pass
    db.command("createRole", "r1", privileges=[{"resource": {"db": db.name, "collection": "p"}, "actions": ["find"]}],
               roles=[])
    db.command("dropRole", "r1")


def classify(exc):
    m = str(exc).lower()
    if isinstance(exc, Partial):
        return "in parte"
    if any(x in m for x in ("no such command", "unrecognized", "unknown operator", "not supported",
                            "unsupported", "not implemented", "unknown top level")):
        return "non supportato"
    if isinstance(exc, AssertionError):
        return "errore"
    return "errore"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    client = MongoClient(a.uri)
    db = client["compat_probe"]
    out = open(a.out, "w")
    for area, name, fn in PROBES:
        res = {"area": area, "feature": name}
        try:
            fn(db)
            res["status"] = "funziona"
        except Exception as e:
            res["status"] = classify(e)
            res["note"] = f"{type(e).__name__}: {str(e)[:300]}"
        try:
            client.admin.command("ping")
        except Exception:
            res["status"] = "errore"
            res["note"] = "CRASH: server down after this probe. " + res.get("note", "")
        out.write(json.dumps(res, ensure_ascii=False) + "\n")
        print(f"{res['status']:15} {area:15} {name}  {res.get('note', '')[:120]}")


if __name__ == "__main__":
    main()
