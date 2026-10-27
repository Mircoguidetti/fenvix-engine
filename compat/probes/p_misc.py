"""Remaining features without suite tests: projection operators, window functions, bulk writes, Stable API, cursors, server-side JavaScript, diagnostics."""
import datetime, uuid
from bson import Binary, Int64
from pymongo import MongoClient, InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany
from pymongo.server_api import ServerApi
from probe_diff import case


def seed(db, n=10):
    db.t.insert_many([{"_id": i, "a": i, "g": i % 3, "s": f"s{i}", "arr": [{"x": i, "y": "p"}, {"x": i + 1, "y": "q"}], "nums": [i, i + 1, i + 2]} for i in range(n)])


@case("compat.proj.positional", ["proj:$"])
def _pos(client, db):
    seed(db)
    return list(db.t.find({"nums": {"$gte": 5}}, {"nums.$": 1}).sort("_id", 1))


@case("compat.proj.elemMatch", ["proj:$elemMatch"])
def _em(client, db):
    seed(db)
    return list(db.t.find({}, {"arr": {"$elemMatch": {"y": "q"}}}).sort("_id", 1)) + list(db.t.find({}, {"nums": {"$elemMatch": {"$gt": 6}}}).sort("_id", 1))


@case("compat.proj.expressions_in_find_projection", ["proj:aggregation expressions in find projection"], fail="missing")
def _pe(client, db):
    seed(db)
    return list(db.t.find({}, {"x": {"$add": ["$a", 1]}, "u": {"$toUpper": "$s"}, "n": {"$size": "$nums"}}).sort("_id", 1))


@case("compat.stage.changeStreamSplitLargeEvent", ["stage:$changeStreamSplitLargeEvent"], rs=True)
def _split(client, db):
    st = db.c.watch([{"$changeStreamSplitLargeEvent": {}}], max_await_time_ms=300)
    try:
        db.c.insert_one({"_id": 1, "a": 1})
        out = []
        for _ in range(8):
            e = st.try_next()
            if e: out.append({k: v for k, v in e.items() if k not in ("_id", "clusterTime", "wallTime", "ns")})
            if out: break
        return out
    finally:
        st.close()


@case("compat.match.sampleRate", ["expr:$sampleRate"])
def _sr(client, db):
    seed(db)
    return {"all": db.t.count_documents({"$sampleRate": 1.0}), "none": db.t.count_documents({"$sampleRate": 0.0}),
            "agg": len(list(db.t.aggregate([{"$match": {"$sampleRate": 1}}])))}


def window(op, spec, tag, extra=None):
    def f(client, db):
        seed(db)
        out = {"output": {"r": {op: spec, "window": {"documents": ["unbounded", "current"]}}}, "sortBy": {"a": 1}, "partitionBy": "$g"}
        out["output"]["r"].update(extra or {})
        return list(db.t.aggregate([{"$setWindowFields": out}, {"$project": {"_id": 1, "r": 1}}, {"$sort": {"_id": 1}}]))
    case(f"compat.window.{tag}", [f"win:{op}"])(f)


window("$top", {"sortBy": {"a": -1}, "output": "$s"}, "top")
window("$bottom", {"sortBy": {"a": -1}, "output": "$s"}, "bottom")
window("$topN", {"sortBy": {"a": -1}, "output": "$s", "n": 2}, "topN")
window("$bottomN", {"sortBy": {"a": -1}, "output": "$s", "n": 2}, "bottomN")
window("$firstN", {"input": "$a", "n": 2}, "firstN")
window("$lastN", {"input": "$a", "n": 2}, "lastN")
window("$maxN", {"input": "$a", "n": 2}, "maxN")
window("$minN", {"input": "$a", "n": 2}, "minN")


def raw_txn(db, client, finish, extra_finish):
    lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
    base = {"lsid": lsid, "txnNumber": Int64(int(__import__("time").time() * 1000)), "autocommit": False}
    db.command({"insert": "t", "documents": [{"_id": 1}], **base, "startTransaction": True})
    r = client.admin.command({finish: 1, **base, **extra_finish})
    return {"ok": r["ok"], "n": db.t.count_documents({})}


@case("compat.txn.commit_raw_fields", ["opt:commitTransaction.writeConcern", "opt:commitTransaction.comment"], rs=True)
def _craw(client, db):
    return raw_txn(db, client, "commitTransaction", {"writeConcern": {"w": "majority"}, "comment": "probe"})


@case("compat.txn.abort_raw_fields", ["opt:abortTransaction.writeConcern", "opt:abortTransaction.comment"], rs=True)
def _araw(client, db):
    return raw_txn(db, client, "abortTransaction", {"writeConcern": {"w": "majority"}, "comment": "probe"})


# ------------------------------------------------------------------ bulk write
def bulk(ops, ordered=True, seed_docs=True):
    def f(client, db):
        if seed_docs:
            db.t.insert_many([{"_id": i, "a": i, "g": i % 2} for i in range(6)])
        try:
            r = db.t.bulk_write(ops, ordered=ordered)
            res = {"ins": r.inserted_count, "match": r.matched_count, "mod": r.modified_count, "del": r.deleted_count, "ups": r.upserted_count, "upserted": sorted(r.upserted_ids)}
        except Exception as e:  # noqa
            import run_py
            res = run_py.err_result(e, False)
        return {"result": res, "docs": list(db.t.find().sort("_id", 1))}
    return f


for _tag, _ops, _ids, _ord in (
    ("insertOne", [InsertOne({"_id": 100, "a": 100})], ["bulk:insertOne"], True),
    ("updateOne", [UpdateOne({"a": 1}, {"$set": {"b": 1}}), UpdateOne({"a": 99}, {"$set": {"b": 1}}, upsert=True)], ["bulk:updateOne"], True),
    ("updateMany", [UpdateMany({"g": 1}, {"$inc": {"a": 10}})], ["bulk:updateMany"], True),
    ("replaceOne", [ReplaceOne({"a": 2}, {"a": 2, "replaced": True})], ["bulk:replaceOne"], True),
    ("deleteOne", [DeleteOne({"g": 0})], ["bulk:deleteOne"], True),
    ("deleteMany", [DeleteMany({"g": 1})], ["bulk:deleteMany"], True),
    ("ordered_stops_at_error", [InsertOne({"_id": 100}), InsertOne({"_id": 1}), InsertOne({"_id": 101})], ["bulk:ordered"], True),
    ("unordered_continues_after_error", [InsertOne({"_id": 100}), InsertOne({"_id": 1}), InsertOne({"_id": 101})], ["bulk:ordered"], False),
    ("mixed", [InsertOne({"_id": 200}), UpdateOne({"_id": 200}, {"$set": {"z": 1}}), DeleteOne({"_id": 0}), ReplaceOne({"_id": 1}, {"r": 1}), UpdateMany({}, {"$set": {"all": 1}})], [], True)):
    _opids = sorted({f"py:pymongo.operations.{type(o).__name__}" for o in _ops})
    case(f"compat.bulk.{_tag}", _ids + _opids + ["py:pymongo.collection.Collection.bulk_write", "node:Collection.bulkWrite"][:1], expect_error=False)(bulk(_ops, _ord))


# ------------------------------------------------------------------ Stable API
def sapi(**kw):
    def f(client, db):
        host, port = client.address
        c = MongoClient(host, port, server_api=ServerApi("1", **kw), directConnection=True, serverSelectionTimeoutMS=8000)
        try:
            d = c[db.name]
            out = {"ping": c.admin.command("ping")["ok"]}
            d.t.insert_one({"_id": 1})
            out["find"] = list(d.t.find())
            for name, fn in (("count_cmd", lambda: d.command({"count": "t"})["n"]), ("distinct", lambda: d.t.distinct("_id")),
                             ("aggregate", lambda: list(d.t.aggregate([{"$match": {}}]))), ("collStats", lambda: d.command({"collStats": "t"})["ok"]),
                             ("dbStats", lambda: d.command({"dbStats": 1})["ok"]), ("createIndexes", lambda: d.t.create_index("a")),
                             ("listIndexes", lambda: len(list(d.t.list_indexes())))):
                try:
                    out[name] = fn()
                except Exception as e:  # noqa
                    out[name] = {"code": getattr(e, "code", None), "codeName": (getattr(e, "details", None) or {}).get("codeName")}
            return out
        finally:
            c.close()
    return f


case("compat.stableapi.version_only", ["stableapi:apiVersion"])(sapi())
case("compat.stableapi.apiStrict", ["stableapi:apiStrict"])(sapi(strict=True))
case("compat.stableapi.apiDeprecationErrors", ["stableapi:apiDeprecationErrors"])(sapi(deprecation_errors=True))


# ------------------------------------------------------------------ cursors
@case("compat.cursor.tailable_capped", ["cursor:tailable cursor"], fail="missing")
def _tail(client, db):
    db.create_collection("cap", capped=True, size=4096)
    db.cap.insert_many([{"_id": i} for i in range(3)])
    cur = db.cap.find(cursor_type=__import__("pymongo").CursorType.TAILABLE)
    first = [d["_id"] for d in cur]
    db.cap.insert_one({"_id": 3})
    more = [d["_id"] for d in cur]
    return {"first": first, "more": more, "alive": cur.alive}


@case("compat.cursor.tailable_await", ["cursor:awaitData"], fail="missing")
def _await(client, db):
    import time
    db.create_collection("cap", capped=True, size=4096)
    db.cap.insert_many([{"_id": i} for i in range(2)])
    cur = db.cap.find(cursor_type=__import__("pymongo").CursorType.TAILABLE_AWAIT).max_await_time_ms(200)
    first = [d["_id"] for d in cur]
    t0 = time.time()
    empty = [d["_id"] for d in cur]
    waited = (time.time() - t0) >= 0.15
    return {"first": first, "empty_after_wait": empty, "waited_for_data": waited}


@case("compat.cursor.noCursorTimeout", ["cursor:noCursorTimeout", "cursor:noCursorTimeout (method)"])
def _nct(client, db):
    seed(db, 30)
    cur = db.t.find({}, no_cursor_timeout=True, batch_size=5).sort("_id", 1)
    try:
        return [d["_id"] for d in cur]
    finally:
        cur.close()


@case("compat.cursor.allowPartialResults", ["cursor:allowPartialResults"])
def _apr(client, db):
    seed(db, 8)
    return [d["_id"] for d in db.t.find({}, allow_partial_results=True).sort("_id", 1)]


# ------------------------------------------------------------------ server-side JavaScript
@case("compat.js.function_args_body_lang", ["js:$function.body", "js:$function.args", "js:$function.lang"])
def _fn(client, db):
    seed(db, 5)
    return list(db.t.aggregate([{"$project": {"r": {"$function": {"body": "function(a, b){ return a + '-' + b.length; }", "args": ["$s", "$nums"], "lang": "js"}}}}, {"$sort": {"_id": 1}}]))


@case("compat.js.accumulator_all_fields", ["js:$accumulator.init", "js:$accumulator.initArgs", "js:$accumulator.accumulate", "js:$accumulator.accumulateArgs",
                                           "js:$accumulator.merge", "js:$accumulator.finalize", "js:$accumulator.lang"])
def _acc(client, db):
    seed(db, 9)
    return list(db.t.aggregate([{"$group": {"_id": "$g", "r": {"$accumulator": {
        "init": "function(k){ return {n: 0, sum: k}; }", "initArgs": ["$g"], "accumulate": "function(st, v){ return {n: st.n + 1, sum: st.sum + v}; }",
        "accumulateArgs": ["$a"], "merge": "function(a, b){ return {n: a.n + b.n, sum: a.sum + b.sum}; }", "finalize": "function(st){ return st.sum * 10 + st.n; }", "lang": "js"}}}},
        {"$sort": {"_id": 1}}]))


@case("compat.js.where_this_and_functions", ["qop:$where"])
def _where(client, db):
    seed(db, 8)
    return {"fn": [d["_id"] for d in db.t.find({"$where": "function(){ return this.a % 2 == 0 && this.nums.length == 3; }"}).sort("_id", 1)],
            "str": [d["_id"] for d in db.t.find({"$where": "this.a > 5"}).sort("_id", 1)]}


@case("compat.js.where_obj_alias", ["qop:$where"])
def _where_obj(client, db):
    seed(db, 8)
    return [d["_id"] for d in db.t.find({"$where": "obj.s == 's3'"}).sort("_id", 1)]


# ------------------------------------------------------------------ diagnostics
@case("compat.diag.connectionStatus", ["cmd:connectionStatus"])
def _cs(client, db):
    r = client.admin.command({"connectionStatus": 1})
    return {"authInfo": r["authInfo"], "ok": r["ok"]}


@case("compat.diag.whatsmyuri", ["cmd:whatsmyuri"])
def _wm(client, db):
    r = client.admin.command({"whatsmyuri": 1})
    return {"has_you": ":" in r["you"], "ok": r["ok"]}


# ------------------------------------------------------------------ authentication (SCRAM)
def auth_probe(mech):
    def f(client, db):
        import uuid as _u
        user = "probe_" + _u.uuid4().hex[:8]
        host, port = client.address
        client.admin.command("createUser", user, pwd="pw-" + user, roles=[], mechanisms=["SCRAM-SHA-1", "SCRAM-SHA-256"])
        try:
            c = MongoClient(host, port, username=user, password="pw-" + user, authSource="admin", authMechanism=mech, directConnection=True, serverSelectionTimeoutMS=8000)
            try:
                out = {"ping": c.admin.command("ping")["ok"]}
                bad = MongoClient(host, port, username=user, password="wrong", authSource="admin", authMechanism=mech, directConnection=True, serverSelectionTimeoutMS=8000)
                try:
                    bad.admin.command("ping"); out["wrong_password"] = "accepted"
                except Exception as e:  # noqa
                    out["wrong_password"] = getattr(e, "code", type(e).__name__)
                finally:
                    bad.close()
                return out
            finally:
                c.close()
        finally:
            try:
                client.admin.command("dropUser", user)
            except Exception:  # noqa
                pass
    return f


case("compat.auth.scram_sha_1", ["authmech:SCRAM-SHA-1"], fail="missing")(auth_probe("SCRAM-SHA-1"))
case("compat.auth.scram_sha_256", ["authmech:SCRAM-SHA-256"], fail="missing")(auth_probe("SCRAM-SHA-256"))


# ------------------------------------------------------------------ $natural sort / hint
def natural(fn):
    def f(client, db):
        db.t.insert_many([{"_id": i} for i in (30, 10, 20, 5, 25)])
        return fn(db)
    return f


case("compat.natural.sort", ["sorthint:$natural (sort)"])(natural(lambda db: {"asc": [d["_id"] for d in db.t.find().sort("$natural", 1)], "desc": [d["_id"] for d in db.t.find().sort("$natural", -1)]}))
case("compat.natural.hint", ["sorthint:$natural (hint)"])(natural(lambda db: {"fwd": [d["_id"] for d in db.t.find().hint([("$natural", 1)])], "rev": [d["_id"] for d in db.t.find().hint([("$natural", -1)])]}))


# ------------------------------------------------------------------ $out / $merge into another database
@case("compat.stage.out_other_database", ["stage:$out"])
def _out_db(client, db):
    seed(db, 5)
    other = db.name + "_other"
    try:
        db.t.aggregate([{"$match": {"a": {"$gte": 2}}}, {"$out": {"db": other, "coll": "o"}}]).close() if False else list(db.t.aggregate([{"$match": {"a": {"$gte": 2}}}, {"$out": {"db": other, "coll": "o"}}]))
        return sorted(d["_id"] for d in client[other].o.find())
    finally:
        client.drop_database(other)


@case("compat.stage.merge_other_database", ["stage:$merge"])
def _merge_db(client, db):
    seed(db, 5)
    other = db.name + "_other"
    try:
        list(db.t.aggregate([{"$match": {"a": {"$gte": 2}}}, {"$merge": {"into": {"db": other, "coll": "m"}}}]))
        return sorted(d["_id"] for d in client[other].m.find())
    finally:
        client.drop_database(other)


@case("compat.stage.out_and_merge_same_database", ["stage:$out", "stage:$merge"])
def _out_same(client, db):
    seed(db, 5)
    list(db.t.aggregate([{"$match": {"a": {"$gte": 2}}}, {"$out": "o"}]))
    list(db.t.aggregate([{"$match": {"a": {"$lt": 2}}}, {"$merge": {"into": "o", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}}]))
    return sorted(d["_id"] for d in db.o.find())


@case("compat.cmd.renameCollection_across_databases", ["cmd:renameCollection"])
def _rename_xdb(client, db):
    seed(db, 4)
    other = db.name + "_other"
    try:
        client.admin.command({"renameCollection": f"{db.name}.t", "to": f"{other}.t2"})
        return {"moved": sorted(d["_id"] for d in client[other].t2.find()), "left": db.list_collection_names()}
    finally:
        client.drop_database(other)
