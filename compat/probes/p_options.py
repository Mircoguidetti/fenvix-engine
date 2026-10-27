"""One differential probe per documented command option: the same command document is sent to real mongod and to the engine on identical
data and the (normalised) replies plus the resulting collection state are compared. Option name -> inventory id `opt:<command>.<field>`.
Ids whose option is valid only in special circumstances (tailable, timeseries, ...) use the smallest request that real mongod accepts."""
import datetime, json
from bson import Int64, Timestamp
from probe_diff import case
import run_py

DROP = {"operationTime", "$clusterTime", "$db", "electionId", "opTime", "lastCommittedOpTime", "$configTime", "$topologyTime"}


def scrub(x, dbname):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            if k in DROP:
                continue
            if k == "id" and isinstance(v, (int, Int64)) and isinstance(x.get("ns"), str):
                out[k] = bool(v); continue
            if k in ("$recordId",):
                out[k] = "<rid>"; continue
            if k == "uuid":
                continue      # collection uuid in listCollections info: tracked by its own probe (compat.listCollections.info_uuid)
            out[k] = scrub(v, dbname)
        return out
    if isinstance(x, list):
        return [scrub(v, dbname) for v in x]
    if isinstance(x, str):
        return x.replace(dbname, "$DB")
    return x


def setup(db, extra_index=True):
    db.t.insert_many([{"_id": i, "a": i, "s": f"str{i}", "arr": [i, i + 1], "k": i % 3} for i in range(10)])
    if extra_index:
        db.t.create_index("a", name="a_1")


def state(db):
    names = db.list_collection_names()
    specs = {}
    for cn in ("t", "c"):
        if cn in names:
            specs[cn] = sorted(({k: v for k, v in i.items() if k not in ("ns", "v")} for i in db[cn].list_indexes()), key=lambda i: i["name"])
    return {"docs": list(db.t.find().sort("_id", 1)) if "t" in names else None, "colls": sorted(n for n in names if not n.startswith("system.")), "index_specs": specs}


def run_cmd(db, doc, norm=None, pre=None, after=True):
    if pre:
        pre(db)
    r = db.command(doc)
    if norm == "explain":
        r = run_py.reduce_explain(r["queryPlanner"] and r, False) if "queryPlanner" in r else {"keys": sorted(k for k in r if k not in DROP)}
    if norm == "keys":
        r = sorted(k for k in r if k not in DROP)
    if norm == "ok":
        r = {"ok": r["ok"]}
    num = lambda v: float(v) if isinstance(v, (int, float, Int64)) and not isinstance(v, bool) else v
    if norm == "collstats":
        r = {k: num(r.get(k)) for k in ("ns", "count", "nindexes", "scaleFactor", "ok", "capped")}
    if norm == "dbstats":
        r = {k: num(r.get(k)) for k in ("db", "collections", "objects", "indexes", "scaleFactor", "views", "ok")}
    if norm == "validate":
        r = {k: num(r.get(k)) for k in ("ns", "valid", "nrecords", "nIndexes", "errors", "warnings", "ok")}
    out = {"reply": scrub(r, db.name)}
    if after:
        out["state"] = scrub(state(db), db.name)
    return out




def strip(doc, field):
    import copy
    d = copy.deepcopy(doc)
    sub = [k for k in ("updates[].", "deletes[].", "indexes[].") if field.startswith(k)]
    if sub:
        for it in d.get(sub[0][:-3], []):
            it.pop(field[len(sub[0]):], None)
    elif "." in field:
        a, b = field.split(".", 1)
        if isinstance(d.get(a), dict):
            d[a].pop(b, None)
    else:
        d.pop(field, None)
    return d


def O(cmd, field, doc, norm=None, pre=None, ids=None, fail="partial", expect_error=False, db_name=None, after=True):
    rid = ids or [f"opt:{cmd}.{field}"]

    def mk_f(dd):
        def f(client, db):
            setup(db) if not pre else None
            d = db if db_name is None else client[db_name]
            return run_cmd(d, dd, norm, pre, after)
        return f
    ctrl = mk_f(strip(doc, field)) if field != cmd and field not in ("query", "filter", "updates", "deletes", "documents", "pipeline", "key") else None
    case(f"compat.opt.{cmd}.{field}", rid, fail=fail, expect_error=expect_error, control=ctrl, sort_keys=True)(mk_f(doc))


def D(**kw):
    return kw


# ------------------------------------------------------------------ find
FILT = {"a": {"$gte": 3}}
O("find", "find", D(find="t"))
O("find", "filter", D(find="t", filter={"a": 3}))
O("find", "sort", D(find="t", sort={"a": -1}))
O("find", "projection", D(find="t", projection={"a": 1, "_id": 0}, sort={"a": 1}))
O("find", "hint", D(find="t", hint={"a": 1}, filter=FILT))
O("find", "skip", D(find="t", skip=7, sort={"a": 1}))
O("find", "limit", D(find="t", limit=3, sort={"a": 1}))
O("find", "batchSize", D(find="t", batchSize=2, sort={"a": 1}))
O("find", "singleBatch", D(find="t", batchSize=2, singleBatch=True, sort={"a": 1}))
O("find", "comment", D(find="t", comment="probe", sort={"a": 1}))
O("find", "maxTimeMS", D(find="t", maxTimeMS=5000, sort={"a": 1}))
O("find", "readConcern", D(find="t", readConcern={"level": "local"}, sort={"a": 1}))
O("find", "max", D(find="t", hint={"a": 1}, max={"a": 5}, sort={"a": 1}))
O("find", "min", D(find="t", hint={"a": 1}, min={"a": 5}, sort={"a": 1}))
O("find", "returnKey", D(find="t", hint={"a": 1}, returnKey=True, sort={"a": 1}))
O("find", "showRecordId", D(find="t", showRecordId=True, sort={"a": 1}))
O("find", "tailable", D(find="t", tailable=True), expect_error=True)
O("find", "awaitData", D(find="t", tailable=True, awaitData=True), expect_error=True)
O("find", "noCursorTimeout", D(find="t", noCursorTimeout=True, sort={"a": 1}))
O("find", "allowPartialResults", D(find="t", allowPartialResults=True, sort={"a": 1}))
O("find", "collation", D(find="t", filter={"s": "STR1"}, collation={"locale": "en", "strength": 2}), fail="missing")
O("find", "allowDiskUse", D(find="t", allowDiskUse=True, sort={"a": -1}))
O("find", "let", D(find="t", filter={"$expr": {"$eq": ["$a", "$$x"]}}, let={"x": 4}))

# ------------------------------------------------------------------ aggregate
P = [{"$match": {"a": {"$gte": 2}}}, {"$sort": {"a": 1}}, {"$limit": 4}]
O("aggregate", "aggregate", D(aggregate="t", pipeline=P, cursor={}))
O("aggregate", "pipeline", D(aggregate="t", pipeline=[{"$group": {"_id": "$k", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}], cursor={}))
O("aggregate", "explain", D(aggregate="t", pipeline=P, explain=True), norm="explain")
O("aggregate", "allowDiskUse", D(aggregate="t", pipeline=P, cursor={}, allowDiskUse=True))
O("aggregate", "cursor", D(aggregate="t", pipeline=P, cursor={"batchSize": 2}))
O("aggregate", "maxTimeMS", D(aggregate="t", pipeline=P, cursor={}, maxTimeMS=5000))
O("aggregate", "bypassDocumentValidation", D(aggregate="t", pipeline=P + [{"$out": "o"}], cursor={}, bypassDocumentValidation=True))
O("aggregate", "readConcern", D(aggregate="t", pipeline=P, cursor={}, readConcern={"level": "local"}))
O("aggregate", "collation", D(aggregate="t", pipeline=[{"$match": {"s": "STR1"}}], cursor={}, collation={"locale": "en", "strength": 2}), fail="missing")
O("aggregate", "hint", D(aggregate="t", pipeline=P, cursor={}, hint={"a": 1}))
O("aggregate", "comment", D(aggregate="t", pipeline=P, cursor={}, comment="probe"))
O("aggregate", "writeConcern", D(aggregate="t", pipeline=P + [{"$out": "o"}], cursor={}, writeConcern={"w": 1}))
O("aggregate", "let", D(aggregate="t", pipeline=[{"$match": {"$expr": {"$eq": ["$a", "$$x"]}}}], cursor={}, let={"x": 4}))

# ------------------------------------------------------------------ insert
DOCS = [{"_id": 100, "a": 100}, {"_id": 101, "a": 101}]
O("insert", "insert", D(insert="t", documents=DOCS))
O("insert", "documents", D(insert="t", documents=DOCS + [{"_id": 3, "a": 3}], ordered=False))
O("insert", "ordered", D(insert="t", documents=[{"_id": 3}, {"_id": 102}], ordered=True))
O("insert", "maxTimeMS", D(insert="t", documents=DOCS, maxTimeMS=5000))
O("insert", "writeConcern", D(insert="t", documents=DOCS, writeConcern={"w": 1}))
O("insert", "bypassDocumentValidation", D(insert="t", documents=DOCS, bypassDocumentValidation=True))
O("insert", "comment", D(insert="t", documents=DOCS, comment="probe"))

# ------------------------------------------------------------------ update
U1 = {"q": {"a": 1}, "u": {"$set": {"v": 1}}}
O("update", "update", D(update="t", updates=[U1]))
O("update", "updates", D(update="t", updates=[U1, {"q": {"a": 2}, "u": {"$inc": {"v": 5}}}]))
O("update", "ordered", D(update="t", updates=[{"q": {"a": 1}, "u": {"$bogus": 1}}, U1], ordered=False))
O("update", "maxTimeMS", D(update="t", updates=[U1], maxTimeMS=5000))
O("update", "writeConcern", D(update="t", updates=[U1], writeConcern={"w": 1}))
O("update", "bypassDocumentValidation", D(update="t", updates=[U1], bypassDocumentValidation=True))
O("update", "comment", D(update="t", updates=[U1], comment="probe"))
O("update", "let", D(update="t", updates=[{"q": {"$expr": {"$eq": ["$a", "$$x"]}}, "u": {"$set": {"v": 1}}}], let={"x": 4}))
O("update", "updates[].q", D(update="t", updates=[{"q": {"a": {"$gt": 7}}, "u": {"$set": {"v": 1}}, "multi": True}]))
O("update", "updates[].u", D(update="t", updates=[{"q": {"a": 3}, "u": {"a": 3, "replaced": True}}]))
O("update", "updates[].c", D(update="t", updates=[{"q": {"a": 3}, "u": [{"$set": {"v": "$$c"}}], "c": {"c": 42}}]))
O("update", "updates[].upsert", D(update="t", updates=[{"q": {"a": 500}, "u": {"$set": {"v": 1}}, "upsert": True}]))
O("update", "updates[].multi", D(update="t", updates=[{"q": {"k": 1}, "u": {"$set": {"v": 1}}, "multi": True}]))
O("update", "updates[].collation", D(update="t", updates=[{"q": {"s": "STR3"}, "u": {"$set": {"v": 1}}, "collation": {"locale": "en", "strength": 2}}]), fail="missing")
O("update", "updates[].arrayFilters", D(update="t", updates=[{"q": {"a": 3}, "u": {"$set": {"arr.$[x]": 0}}, "arrayFilters": [{"x": {"$gte": 4}}]}]))
O("update", "updates[].hint", D(update="t", updates=[{"q": {"a": 3}, "u": {"$set": {"v": 1}}, "hint": {"a": 1}}]))

# ------------------------------------------------------------------ delete
O("delete", "delete", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}]))
O("delete", "deletes", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}, {"q": {"a": {"$gt": 7}}, "limit": 0}]))
O("delete", "comment", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}], comment="probe"))
O("delete", "let", D(delete="t", deletes=[{"q": {"$expr": {"$eq": ["$a", "$$x"]}}, "limit": 1}], let={"x": 4}))
O("delete", "ordered", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}], ordered=False))
O("delete", "writeConcern", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}], writeConcern={"w": 1}))
O("delete", "maxTimeMS", D(delete="t", deletes=[{"q": {"a": 1}, "limit": 1}], maxTimeMS=5000))
O("delete", "deletes[].q", D(delete="t", deletes=[{"q": {"k": 2}, "limit": 0}]))
O("delete", "deletes[].limit", D(delete="t", deletes=[{"q": {"k": 2}, "limit": 1}]))
O("delete", "deletes[].collation", D(delete="t", deletes=[{"q": {"s": "STR3"}, "limit": 1, "collation": {"locale": "en", "strength": 2}}]), fail="missing")
O("delete", "deletes[].hint", D(delete="t", deletes=[{"q": {"a": 3}, "limit": 1, "hint": {"a": 1}}]))

# ------------------------------------------------------------------ findAndModify
O("findAndModify", "findAndModify", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}))
O("findAndModify", "query", D(findAndModify="t", query={"a": {"$gt": 4}}, sort={"a": 1}, update={"$set": {"v": 1}}))
O("findAndModify", "sort", D(findAndModify="t", query={"k": 1}, sort={"a": -1}, update={"$set": {"v": 1}}))
O("findAndModify", "remove", D(findAndModify="t", query={"a": 1}, remove=True))
O("findAndModify", "update", D(findAndModify="t", query={"a": 1}, update=[{"$set": {"v": "$a"}}]))
O("findAndModify", "new", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, new=True))
O("findAndModify", "fields", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, new=True, fields={"v": 1}))
O("findAndModify", "upsert", D(findAndModify="t", query={"a": 500}, update={"$set": {"v": 1}}, upsert=True, new=True))
O("findAndModify", "bypassDocumentValidation", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, bypassDocumentValidation=True))
O("findAndModify", "writeConcern", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, writeConcern={"w": 1}))
O("findAndModify", "maxTimeMS", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, maxTimeMS=5000))
O("findAndModify", "collation", D(findAndModify="t", query={"s": "STR3"}, update={"$set": {"v": 1}}, collation={"locale": "en", "strength": 2}), fail="missing")
O("findAndModify", "arrayFilters", D(findAndModify="t", query={"a": 3}, update={"$set": {"arr.$[x]": 0}}, arrayFilters=[{"x": {"$gte": 4}}], new=True))
O("findAndModify", "hint", D(findAndModify="t", query={"a": 3}, update={"$set": {"v": 1}}, hint={"a": 1}))
O("findAndModify", "comment", D(findAndModify="t", query={"a": 1}, update={"$set": {"v": 1}}, comment="probe"))
O("findAndModify", "let", D(findAndModify="t", query={"$expr": {"$eq": ["$a", "$$x"]}}, update={"$set": {"v": 1}}, let={"x": 4}))

# ------------------------------------------------------------------ count / distinct
O("count", "count", D(count="t"))
O("count", "query", D(count="t", query={"a": {"$gt": 4}}))
O("count", "limit", D(count="t", limit=3))
O("count", "skip", D(count="t", skip=4))
O("count", "hint", D(count="t", query={"a": {"$gt": 4}}, hint={"a": 1}))
O("count", "readConcern", D(count="t", readConcern={"level": "local"}))
O("count", "maxTimeMS", D(count="t", maxTimeMS=5000))
O("count", "collation", D(count="t", query={"s": "STR3"}, collation={"locale": "en", "strength": 2}), fail="missing")
O("count", "comment", D(count="t", comment="probe"))
O("distinct", "distinct", D(distinct="t", key="k"))
O("distinct", "key", D(distinct="t", key="arr"))
O("distinct", "query", D(distinct="t", key="k", query={"a": {"$gt": 4}}))
O("distinct", "readConcern", D(distinct="t", key="k", readConcern={"level": "local"}))
O("distinct", "collation", D(distinct="t", key="s", collation={"locale": "en", "strength": 2}, query={"s": {"$in": ["STR3", "str3"]}}), fail="missing")
O("distinct", "comment", D(distinct="t", key="k", comment="probe"))


# ------------------------------------------------------------------ getMore / killCursors
def gm(extra):
    def f(client, db):
        setup(db)
        r = db.command({"find": "t", "batchSize": 2, "sort": {"a": 1}})
        cid = r["cursor"]["id"]
        doc = {"getMore": cid, "collection": "t", **extra}
        out = db.command(doc)
        return {"batch": [d["a"] for d in out["cursor"]["nextBatch"]], "more": bool(out["cursor"]["id"])}
    return f


for _f, _x in (("getMore", {}), ("collection", {}), ("batchSize", {"batchSize": 3}), ("comment", {"comment": "probe"})):
    case(f"compat.opt.getMore.{_f}", [f"opt:getMore.{_f}"])(gm(_x))
case("compat.opt.getMore.maxTimeMS", ["opt:getMore.maxTimeMS"], expect_error=True)(gm({"maxTimeMS": 500}))


def kc(extra):
    def f(client, db):
        setup(db)
        r = db.command({"find": "t", "batchSize": 2, "sort": {"a": 1}})
        out = db.command({"killCursors": "t", "cursors": [r["cursor"]["id"]], **extra})
        return {k: (len(v) if isinstance(v, list) else v) for k, v in out.items() if k in ("cursorsKilled", "cursorsNotFound", "cursorsAlive", "cursorsUnknown")}
    return f


for _f, _x in (("killCursors", {}), ("cursors", {}), ("comment", {"comment": "probe"})):
    case(f"compat.opt.killCursors.{_f}", [f"opt:killCursors.{_f}"])(kc(_x))

# ------------------------------------------------------------------ create
def mk(cmd_doc, tag, ids):
    def f(client, db):
        db.command(cmd_doc)
        c = db.list_collections(filter={"name": "c"}).next() if db.list_collection_names() else None
        return {"options": scrub(c.get("options") if c else None, db.name), "type": c.get("type") if c else None,
                "idx": sorted(i["name"] for i in db.c.list_indexes()) if c else None}
    def ctrl(client, db):
        db.command({"create": "c"})
        c = db.list_collections(filter={"name": "c"}).next()
        return {"options": scrub(c.get("options"), db.name), "type": c.get("type"), "idx": sorted(i["name"] for i in db.c.list_indexes())}
    case(f"compat.opt.create.{tag}", ids, control=ctrl, sort_keys=True)(f)


mk({"create": "c"}, "create", ["opt:create.create"])
mk({"create": "c", "capped": True, "size": 4096}, "capped", ["opt:create.capped", "opt:create.size"])
mk({"create": "c", "capped": True, "size": 4096, "max": 5}, "max", ["opt:create.max"])
mk({"create": "c", "timeseries": {"timeField": "t"}}, "timeseries.timeField", ["opt:create.timeseries.timeField"])
mk({"create": "c", "timeseries": {"timeField": "t", "metaField": "m"}}, "timeseries.metaField", ["opt:create.timeseries.metaField"])
mk({"create": "c", "timeseries": {"timeField": "t", "granularity": "minutes"}}, "timeseries.granularity", ["opt:create.timeseries.granularity"])
mk({"create": "c", "timeseries": {"timeField": "t", "bucketMaxSpanSeconds": 3600, "bucketRoundingSeconds": 3600}}, "timeseries.bucket", ["opt:create.timeseries.bucketMaxSpanSeconds", "opt:create.timeseries.bucketRoundingSeconds"])
mk({"create": "c", "timeseries": {"timeField": "t"}, "expireAfterSeconds": 3600}, "expireAfterSeconds", ["opt:create.expireAfterSeconds"])
mk({"create": "c", "clusteredIndex": {"key": {"_id": 1}, "unique": True}}, "clusteredIndex", ["opt:create.clusteredIndex", "idxtype:clustered index"])
mk({"create": "c", "changeStreamPreAndPostImages": {"enabled": True}}, "changeStreamPreAndPostImages", ["opt:create.changeStreamPreAndPostImages", "cs:pre- and post-images (changeStreamPreAndPostImages)"])
mk({"create": "c", "storageEngine": {"wiredTiger": {"configString": "block_compressor=zlib"}}}, "storageEngine", ["opt:create.storageEngine"])
mk({"create": "c", "validator": {"$jsonSchema": {"bsonType": "object", "required": ["a"]}}}, "validator", ["opt:create.validator"])
mk({"create": "c", "validator": {"a": {"$exists": True}}, "validationLevel": "moderate", "validationAction": "warn"}, "validationLevel", ["opt:create.validationLevel", "opt:create.validationAction"])
mk({"create": "c", "indexOptionDefaults": {"storageEngine": {"wiredTiger": {"configString": "block_compressor=zlib"}}}}, "indexOptionDefaults", ["opt:create.indexOptionDefaults"])
mk({"create": "c", "writeConcern": {"w": 1}}, "writeConcern", ["opt:create.writeConcern"])
mk({"create": "c", "comment": "probe"}, "comment", ["opt:create.comment"])


def view(client, db):
    db.t.insert_many([{"_id": i, "a": i} for i in range(5)])
    db.command({"create": "vw", "viewOn": "t", "pipeline": [{"$match": {"a": {"$gte": 2}}}]})
    return {"docs": list(db.vw.find().sort("_id", 1)), "info": scrub(db.list_collections(filter={"name": "vw"}).next(), db.name)}


case("compat.opt.create.viewOn_pipeline", ["opt:create.viewOn", "opt:create.pipeline"])(view)
mk({"create": "c", "encryptedFields": {"fields": [{"path": "p", "bsonType": "string", "keyId": None}]}}, "encryptedFields", ["opt:create.encryptedFields"])

# ------------------------------------------------------------------ createIndexes (index specification options)
def ci(spec, tag, ids, seed=True, fail="partial", expect_error=False):
    def f(client, db):
        if seed:
            db.t.insert_many([{"_id": i, "a": i, "s": f"str{i} word", "loc": [i, i], "g": {"type": "Point", "coordinates": [i, i]}, "k": i % 3} for i in range(6)])
        r = db.command({"createIndexes": "t", "indexes": [spec]})
        idx = [{k: v for k, v in i.items() if k != "ns"} for i in db.t.list_indexes()]
        return {"reply": {k: v for k, v in r.items() if k in ("ok", "numIndexesBefore", "numIndexesAfter", "note")}, "indexes": scrub(sorted(idx, key=lambda i: i["name"]), db.name)}
    def ctrl(client, db):
        db.t.insert_many([{"_id": i, "a": i, "s": f"str{i} word", "loc": [i, i], "g": {"type": "Point", "coordinates": [i, i]}, "k": i % 3} for i in range(6)])
        db.command({"createIndexes": "t", "indexes": [{k: v for k, v in spec.items() if k in ("key", "name")}]})
        idx = [{k: v for k, v in i.items() if k != "ns"} for i in db.t.list_indexes()]
        return {"reply": {}, "indexes": scrub(sorted(idx, key=lambda i: i["name"]), db.name)}
    case(f"compat.opt.createIndexes.{tag}", ids, fail=fail, expect_error=expect_error, sort_keys=True)(f)


ci({"key": {"a": 1}, "name": "a_1"}, "key_name", ["opt:createIndexes.indexes[].key", "opt:createIndexes.indexes[].name"])
ci({"key": {"a": 1}, "name": "a_u", "unique": True}, "unique", ["opt:createIndexes.indexes[].unique"])
ci({"key": {"a": 1}, "name": "a_p", "partialFilterExpression": {"a": {"$gt": 2}}}, "partial", ["opt:createIndexes.indexes[].partialFilterExpression"])
ci({"key": {"a": 1}, "name": "a_s", "sparse": True}, "sparse", ["opt:createIndexes.indexes[].sparse"])
ci({"key": {"a": 1}, "name": "a_ttl", "expireAfterSeconds": 3600}, "ttl", ["opt:createIndexes.indexes[].expireAfterSeconds"])
ci({"key": {"a": 1}, "name": "a_h", "hidden": True}, "hidden", ["opt:createIndexes.indexes[].hidden", "idxprop:hidden index"])
ci({"key": {"a": 1}, "name": "a_se", "storageEngine": {"wiredTiger": {"configString": "block_compressor=zlib"}}}, "storageEngine", ["opt:createIndexes.indexes[].storageEngine"])
ci({"key": {"s": "text"}, "name": "s_t", "weights": {"s": 5}}, "weights", ["opt:createIndexes.indexes[].weights"])
ci({"key": {"s": "text"}, "name": "s_t2", "default_language": "spanish"}, "default_language", ["opt:createIndexes.indexes[].default_language"])
ci({"key": {"s": "text"}, "name": "s_t3", "language_override": "lang"}, "language_override", ["opt:createIndexes.indexes[].language_override"])
ci({"key": {"s": "text"}, "name": "s_t4", "textIndexVersion": 3}, "textIndexVersion", ["opt:createIndexes.indexes[].textIndexVersion"])
ci({"key": {"g": "2dsphere"}, "name": "g_2ds", "2dsphereIndexVersion": 3}, "2dsphereIndexVersion", ["opt:createIndexes.indexes[].2dsphereIndexVersion"])
ci({"key": {"loc": "2d"}, "name": "loc_2d", "bits": 20}, "bits", ["opt:createIndexes.indexes[].bits"])
ci({"key": {"loc": "2d"}, "name": "loc_2d2", "min": -100, "max": 100}, "min_max", ["opt:createIndexes.indexes[].min", "opt:createIndexes.indexes[].max"])
ci({"key": {"s": 1}, "name": "s_c", "collation": {"locale": "en", "strength": 2}}, "collation", ["opt:createIndexes.indexes[].collation"], fail="missing")
ci({"key": {"$**": 1}, "name": "wc", "wildcardProjection": {"a": 1}}, "wildcardProjection", ["opt:createIndexes.indexes[].wildcardProjection"])


def ci_top(extra, tag, ids):
    def f(client, db):
        db.t.insert_many([{"_id": i, "a": i} for i in range(4)])
        r = db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}], **extra})
        return {"reply": {k: v for k, v in r.items() if k in ("ok", "numIndexesBefore", "numIndexesAfter")}}
    case(f"compat.opt.createIndexes.{tag}", ids)(f)


ci_top({}, "createIndexes", ["opt:createIndexes.createIndexes", "opt:createIndexes.indexes"])
ci_top({"writeConcern": {"w": 1}}, "writeConcern", ["opt:createIndexes.writeConcern"])
ci_top({"commitQuorum": "votingMembers"}, "commitQuorum", ["opt:createIndexes.commitQuorum"])
ci_top({"comment": "probe"}, "comment", ["opt:createIndexes.comment"])

# ------------------------------------------------------------------ dropIndexes / listIndexes / listCollections / listDatabases / rename
O("dropIndexes", "dropIndexes", D(dropIndexes="t", index="a_1"))
O("dropIndexes", "index", D(dropIndexes="t", index="*"))
O("dropIndexes", "writeConcern", D(dropIndexes="t", index="a_1", writeConcern={"w": 1}))
O("dropIndexes", "comment", D(dropIndexes="t", index="a_1", comment="probe"))
O("listIndexes", "listIndexes", D(listIndexes="t"), norm="keys")
O("listIndexes", "cursor.batchSize", D(listIndexes="t", cursor={"batchSize": 1}))
O("listIndexes", "comment", D(listIndexes="t", comment="probe"))
O("listCollections", "filter", D(listCollections=1, filter={"name": "t"}))
O("listCollections", "nameOnly", D(listCollections=1, nameOnly=True))
O("listCollections", "authorizedCollections", D(listCollections=1, nameOnly=True, authorizedCollections=True))
O("listCollections", "comment", D(listCollections=1, comment="probe"))
def ld(extra, tag, shape):
    def f(client, db):
        setup(db)
        r = client.admin.command({"listDatabases": 1, **extra})
        dbs = r["databases"]
        if shape == "names":
            return {"names": sorted(d["name"] for d in dbs if d["name"] in ("admin", db.name))[:5], "n_filtered": len([d for d in dbs if d["name"] not in ("admin", "config", "local") and not d["name"].startswith("cp_")]) >= 0,
                    "all_match": all(d["name"] == "admin" for d in dbs)}
        return {"keys": sorted({k for d in dbs for k in d}), "ok": r["ok"]}
    case(f"compat.opt.listDatabases.{tag}", [f"opt:listDatabases.{tag}"], sort_keys=True,
         control=None)(f)


ld({"filter": {"name": "admin"}}, "filter", "names")
ld({"nameOnly": True}, "nameOnly", "keys")
ld({"nameOnly": True, "authorizedDatabases": True}, "authorizedDatabases", "keys")
ld({"nameOnly": True, "comment": "probe"}, "comment", "keys")


@case("compat.listCollections.info_uuid", ["cmd:listCollections"])
def _lc_uuid(client, db):
    db.t.insert_one({"_id": 1})
    info = db.list_collections(filter={"name": "t"}).next()
    return {"has_info_uuid": "uuid" in info.get("info", {}), "keys": sorted(info), "info_keys": sorted(info.get("info", {}))}


def rn(extra, tag, ids):
    def f(client, db):
        setup(db)
        db.o.insert_one({"_id": 1})
        r = client.admin.command({"renameCollection": f"{db.name}.t", "to": f"{db.name}.o", **extra})
        return {"ok": r["ok"], "colls": sorted(db.list_collection_names()), "o": list(db.o.find().sort("_id", 1)) if "o" in db.list_collection_names() else None}
    case(f"compat.opt.renameCollection.{tag}", ids, expect_error=(tag == "to_exists"))(f)


rn({"dropTarget": True}, "dropTarget", ["opt:renameCollection.dropTarget", "opt:renameCollection.renameCollection", "opt:renameCollection.to"])
rn({}, "to_exists", [])
rn({"dropTarget": True, "writeConcern": {"w": 1}}, "writeConcern", ["opt:renameCollection.writeConcern"])
rn({"dropTarget": True, "comment": "probe"}, "comment", ["opt:renameCollection.comment"])

# ------------------------------------------------------------------ mapReduce (server-side JavaScript)
MAP = "function(){ emit(this.k, this.a); }"
RED = "function(k, v){ return Array.sum(v); }"
O("mapReduce", "mapReduce", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}), ids=["opt:mapReduce.mapReduce", "opt:mapReduce.map", "opt:mapReduce.reduce", "opt:mapReduce.out"], fail="missing")
O("mapReduce", "query", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, query={"a": {"$gt": 3}}), fail="missing")
O("mapReduce", "sort", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, sort={"a": -1}), fail="missing")
O("mapReduce", "limit", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, limit=5), fail="missing")
O("mapReduce", "finalize", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, finalize="function(k, v){ return v * 2; }"), fail="missing")
O("mapReduce", "scope", D(mapReduce="t", map="function(){ emit(this.k, this.a * m); }", reduce=RED, out={"inline": 1}, scope={"m": 3}), fail="missing")
O("mapReduce", "jsMode", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, jsMode=True), fail="missing")
O("mapReduce", "verbose", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, verbose=True), norm="keys", fail="missing")
O("mapReduce", "bypassDocumentValidation", D(mapReduce="t", map=MAP, reduce=RED, out="mro", bypassDocumentValidation=True), fail="missing")
O("mapReduce", "collation", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, collation={"locale": "en"}), fail="missing")
O("mapReduce", "maxTimeMS", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, maxTimeMS=5000), fail="missing")
O("mapReduce", "writeConcern", D(mapReduce="t", map=MAP, reduce=RED, out="mro", writeConcern={"w": 1}), fail="missing")
O("mapReduce", "comment", D(mapReduce="t", map=MAP, reduce=RED, out={"inline": 1}, comment="probe"), fail="missing")

# ------------------------------------------------------------------ explain / stats
O("explain", "explain", D(explain={"find": "t", "filter": {"a": 3}}, verbosity="queryPlanner"), norm="explain")
O("explain", "verbosity", D(explain={"find": "t", "filter": {"a": 3}}, verbosity="executionStats"), norm="explain")
O("explain", "comment", D(explain={"find": "t", "filter": {"a": 3}}, comment="probe"), norm="explain")
O("collStats", "collStats", D(collStats="t"), norm="collstats")
O("collStats", "scale", D(collStats="t", scale=1024), norm="collstats")
O("dbStats", "dbStats", D(dbStats=1), norm="dbstats")
O("dbStats", "scale", D(dbStats=1, scale=1024), norm="dbstats")
O("dbStats", "freeStorage", D(dbStats=1, freeStorage=1), norm="dbstats")
O("validate", "validate", D(validate="t"), norm="validate")
O("validate", "full", D(validate="t", full=True), norm="validate")
O("validate", "repair", D(validate="t", repair=True), norm="validate", expect_error=True)
O("validate", "metadata", D(validate="t", metadata=True), norm="validate")
O("validate", "checkBSONConformance", D(validate="t", checkBSONConformance=True), norm="validate")
O("drop", "drop", D(drop="t"))
O("drop", "writeConcern", D(drop="t", writeConcern={"w": 1}))
O("drop", "comment", D(drop="t", comment="probe"))
O("dropDatabase", "writeConcern", D(dropDatabase=1, writeConcern={"w": 1}), after=True, norm="ok")
O("dropDatabase", "comment", D(dropDatabase=1, comment="probe"), after=True, norm="ok")

# ------------------------------------------------------------------ collMod
O("collMod", "validator", D(collMod="t", validator={"a": {"$exists": True}}), norm="ok")
O("collMod", "validationLevel", D(collMod="t", validator={"a": {"$exists": True}}, validationLevel="moderate", validationAction="warn"), ids=["opt:collMod.validationLevel", "opt:collMod.validationAction"], norm="ok")
O("collMod", "index", D(collMod="t", index={"name": "a_1", "hidden": True}), ids=["opt:collMod.index", "opt:collMod.index.name", "opt:collMod.index.hidden"], norm="ok")
O("collMod", "index.expireAfterSeconds", D(collMod="t", index={"keyPattern": {"ttl": 1}, "expireAfterSeconds": 100}), pre=lambda db: (db.t.insert_many([{"_id": 1, "ttl": datetime.datetime(2030, 1, 1)}]), db.t.create_index("ttl", expireAfterSeconds=3600)), ids=["opt:collMod.index.keyPattern", "opt:collMod.index.expireAfterSeconds"], norm="ok")
O("collMod", "index.unique", D(collMod="t", index={"name": "a_1", "unique": True}), pre=lambda db: (db.t.insert_many([{"_id": i, "a": i} for i in range(3)]), db.t.create_index("a", name="a_1", prepareUnique=True) if False else db.t.create_index("a", name="a_1")), ids=["opt:collMod.index.unique"], norm="ok")
O("collMod", "index.prepareUnique", D(collMod="t", index={"name": "a_1", "prepareUnique": True}), ids=["opt:collMod.index.prepareUnique"], norm="ok")
O("collMod", "viewOn", D(collMod="vw", viewOn="t", pipeline=[{"$match": {"a": 1}}]), pre=lambda db: (setup(db), db.command({"create": "vw", "viewOn": "t"})), ids=["opt:collMod.viewOn", "opt:collMod.pipeline"], norm="ok")
O("collMod", "cappedSize", D(collMod="c", cappedSize=8192, cappedMax=10), pre=lambda db: db.command({"create": "c", "capped": True, "size": 4096}), ids=["opt:collMod.cappedSize", "opt:collMod.cappedMax"], norm="ok")
O("collMod", "changeStreamPreAndPostImages", D(collMod="t", changeStreamPreAndPostImages={"enabled": True}), norm="ok")
O("collMod", "expireAfterSeconds", D(collMod="c", expireAfterSeconds=7200), pre=lambda db: db.command({"create": "c", "timeseries": {"timeField": "t"}, "expireAfterSeconds": 3600}), norm="ok")

# ------------------------------------------------------------------ hello / sessions / transactions envelope fields
def hello(extra, tag, ids):
    def f(client, db):
        r = client.admin.command({"hello": 1, **extra})
        return {k: r.get(k) for k in ("isWritablePrimary", "maxBsonObjectSize", "maxWriteBatchSize", "minWireVersion", "maxWireVersion", "readOnly", "ok",
                                      "logicalSessionTimeoutMinutes", "compression", "saslSupportedMechs") if k in r}
    case(f"compat.opt.hello.{tag}", ids)(f)


hello({}, "plain", [])
hello({"saslSupportedMechs": "admin.nouser"}, "saslSupportedMechs", ["opt:hello.saslSupportedMechs"])
hello({"comment": "probe"}, "comment", ["opt:hello.comment"])
hello({"compression": ["zlib", "snappy"]}, "compression", ["opt:hello.compression"])


@case("compat.opt.hello.client", ["opt:hello.client"])
def _hello_client(client, db):
    from pymongo import MongoClient
    host, port = client.address
    c = MongoClient(host, port, appname="probe-app", driver=__import__("pymongo.driver_info", fromlist=["DriverInfo"]).DriverInfo("probe", "1.0"), serverSelectionTimeoutMS=8000)
    try:
        return {"ping": c.admin.command("ping")["ok"], "hello_ok": c.admin.command("hello")["ok"]}
    finally:
        c.close()


@case("compat.opt.createIndexes.several_indexes_in_one_call", ["opt:createIndexes.indexes", "cmd:createIndexes"], sort_keys=True)
def _multi_idx(client, db):
    db.t.insert_many([{"_id": i, "a": i, "b": i, "c": i} for i in range(4)])
    r = db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"b": -1}, "name": "b_1"}, {"key": {"c": 1, "a": 1}, "name": "ca"}]})
    return {"after": r["numIndexesAfter"], "listed": sorted(i["name"] for i in db.t.list_indexes())}
