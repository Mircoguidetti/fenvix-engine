"""pymongo public API members that the suite's harness never calls, each exercised against both servers.
Ids are `py:<anchor id of the pymongo 4.19 API reference>`."""
import datetime
import bson
from bson import DBRef, SON
from pymongo import IndexModel, ASCENDING, DESCENDING, CursorType, MongoClient
from pymongo.collation import Collation
from probe_diff import case


def P(*members):
    return [f"py:pymongo.{m}" if m.split(".")[0] in ("collection", "database", "mongo_client", "cursor", "command_cursor", "change_stream", "client_session", "operations")
            else f"py:{m}" for m in members]


def seed(db, n=8):
    db.t.insert_many([{"_id": i, "a": i, "g": i % 3, "s": f"s{i}", "arr": [i, i + 1]} for i in range(n)])


def dc(name, members, fn, **kw):
    case(f"compat.py.{name}", P(*members), **kw)(fn)


# ---- MongoClient
def _list_databases(client, db):
    seed(db)
    docs = [d for d in client.list_databases() if d["name"] == db.name]
    return {"found": len(docs), "keys": sorted(docs[0].keys()) if docs else None}


dc("MongoClient.list_databases", ["mongo_client.MongoClient.list_databases"], _list_databases)


def _server_info(client, db):
    r = client.server_info()
    return {"version_prefix": r["version"][:3], "ok": r["ok"], "has_gitVersion": "gitVersion" in r, "maxBsonObjectSize": r.get("maxBsonObjectSize")}


dc("MongoClient.server_info", ["mongo_client.MongoClient.server_info"], _server_info)


def _close(client, db):
    host, port = client.address
    c = MongoClient(host, port, directConnection=True, serverSelectionTimeoutMS=8000)
    c[db.name].t.insert_one({"_id": 1})
    c.close()
    return {"closed_ok": True, "count": db.t.count_documents({})}


dc("MongoClient.close", ["mongo_client.MongoClient.close"], _close)


def _get_default_database(client, db):
    host, port = client.address
    c = MongoClient(f"mongodb://{host}:{port}/{db.name}", directConnection=True, serverSelectionTimeoutMS=8000)
    try:
        d = c.get_default_database()
        d.t.insert_one({"_id": 1})
        d2 = c.get_database(db.name)
        return {"name_ok": d.name == db.name, "n": d.t.count_documents({}), "n2": d2.t.count_documents({})}
    finally:
        c.close()


dc("MongoClient.get_default_database", ["mongo_client.MongoClient.get_default_database", "mongo_client.MongoClient.get_database"], _get_default_database)


# ---- Database
def _db_aggregate(client, db):
    return list(db.aggregate([{"$documents": [{"x": 1}, {"x": 2}]}, {"$match": {"x": {"$gt": 1}}}]))


dc("Database.aggregate", ["database.Database.aggregate"], _db_aggregate)


def _cursor_command(client, db):
    seed(db)
    c = db.cursor_command({"find": "t", "filter": {"a": {"$gt": 4}}, "sort": {"a": 1}, "batchSize": 2})
    return [d["a"] for d in c]


dc("Database.cursor_command", ["database.Database.cursor_command"], _cursor_command)


def _dereference(client, db):
    db.t.insert_one({"_id": 5, "v": "x"})
    return db.dereference(DBRef("t", 5))


dc("Database.dereference", ["database.Database.dereference"], _dereference)


def _drop_collection(client, db):
    seed(db)
    db.drop_collection("t")
    return {"colls": db.list_collection_names()}


dc("Database.drop_collection", ["database.Database.drop_collection"], _drop_collection)


def _validate(client, db):
    seed(db)
    r = db.validate_collection("t")
    return {"valid": r["valid"], "nrecords": int(r["nrecords"]), "ns": r["ns"].replace(db.name, "$DB")}


dc("Database.validate_collection", ["database.Database.validate_collection"], _validate)


def _get_collection(client, db):
    from pymongo import WriteConcern
    c = db.get_collection("t", write_concern=WriteConcern(w=1))
    c.insert_one({"_id": 1})
    return {"n": db["t"].count_documents({}), "attr": db.t.name}


dc("Database.get_collection", ["database.Database.get_collection", "database.Database.__getitem__", "database.Database.__getattr__"], _get_collection)


# ---- Collection
def _create_indexes(client, db):
    seed(db)
    names = db.t.create_indexes([IndexModel([("a", ASCENDING)], name="a_1"), IndexModel([("g", DESCENDING), ("s", ASCENDING)], unique=False)])
    return {"names": names, "info": db.t.index_information()}


dc("Collection.create_indexes", ["collection.Collection.create_indexes", "collection.Collection.index_information", "operations.IndexModel"], _create_indexes)


def _drop_indexes(client, db):
    seed(db)
    db.t.create_index("a"); db.t.create_index("g")
    db.t.drop_indexes()
    return list(db.t.index_information())


dc("Collection.drop_indexes", ["collection.Collection.drop_indexes"], _drop_indexes)


def _rename(client, db):
    seed(db)
    db.t.rename("t2")
    return {"colls": sorted(db.list_collection_names()), "n": db.t2.count_documents({})}


dc("Collection.rename", ["collection.Collection.rename"], _rename)


def _options(client, db):
    db.create_collection("c", capped=True, size=4096)
    return {"options": db.c.options()}


dc("Collection.options", ["collection.Collection.options"], _options, fail="missing")


def _raw_batches(client, db):
    seed(db)
    f = list(db.t.find_raw_batches({}, sort=[("_id", 1)], batch_size=3))
    a = list(db.t.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=3))
    dec = lambda bs: [d["_id"] for b in bs for d in bson.decode_all(b)]
    return {"find": dec(f), "agg": dec(a)}


dc("Collection.raw_batches", ["collection.Collection.find_raw_batches", "collection.Collection.aggregate_raw_batches"], _raw_batches)


def _bulk_api(client, db):
    from pymongo import InsertOne, UpdateOne, DeleteOne
    r = db.t.bulk_write([InsertOne({"_id": 1}), UpdateOne({"_id": 1}, {"$set": {"a": 1}}), DeleteOne({"_id": 2})])
    return {"ins": r.inserted_count, "mod": r.modified_count, "docs": list(db.t.find())}


dc("Collection.bulk_write", ["collection.Collection.bulk_write", "operations.InsertOne", "operations.UpdateOne", "operations.DeleteOne"], _bulk_api)


# ---- Cursor
def cur(name, members, build, fail="partial"):
    def f(client, db):
        seed(db, 12)
        return build(db)
    dc(f"Cursor.{name}", [f"cursor.Cursor.{m}" for m in members], f, fail=fail)


cur("sort_limit_skip", ["sort", "limit", "skip"], lambda db: [d["a"] for d in db.t.find().sort("a", -1).skip(2).limit(4)])
cur("batch_size", ["batch_size"], lambda db: [d["a"] for d in db.t.find().sort("a", 1).batch_size(3)])
cur("hint", ["hint"], lambda db: (db.t.create_index("a"), [d["a"] for d in db.t.find({"a": {"$gt": 3}}).hint([("a", 1)])])[1])
cur("comment_max_time", ["comment", "max_time_ms", "allow_disk_use"], lambda db: [d["a"] for d in db.t.find().sort("a", 1).comment("c").max_time_ms(5000).allow_disk_use(True)])
cur("clone_rewind", ["clone", "rewind"], lambda db: (lambda c: [[d["a"] for d in c], (c.rewind(), [d["a"] for d in c])[1], [d["a"] for d in c.clone()]])(db.t.find().sort("a", 1).limit(3)))
cur("distinct", ["distinct"], lambda db: sorted(db.t.find({"a": {"$gt": 2}}).distinct("g")))
cur("explain", ["explain"], lambda db: sorted(k for k in db.t.find({"a": 3}).explain() if k in ("queryPlanner", "ok", "serverInfo")))
cur("to_list", ["to_list"], lambda db: [d["a"] for d in db.t.find().sort("a", 1).limit(5).to_list()] if hasattr(db.t.find(), "to_list") else "no to_list")
cur("where", ["where"], lambda db: [d["a"] for d in db.t.find().where("this.a > 8").sort("a", 1)])
cur("collation", ["collation"], lambda db: [d["_id"] for d in db.t.find({"s": "S3"}).collation(Collation("en", strength=2))], fail="missing")
cur("min_max", ["min", "max"], lambda db: (db.t.create_index("a"), [d["a"] for d in db.t.find().hint([("a", 1)]).min([("a", 3)]).max([("a", 7)])])[1], fail="missing")
cur("close", ["close"], lambda db: (lambda c: (next(c), c.close(), c.alive)[2])(db.t.find().batch_size(2)))
cur("add_remove_option", ["add_option", "remove_option"], lambda db: (lambda c: [c.add_option(2), c.remove_option(2), [d["a"] for d in c.sort("a", 1)]][2])(db.t.find()))
cur("max_await_time", ["max_await_time_ms"], lambda db: [d["a"] for d in db.t.find().sort("a", 1).max_await_time_ms(200)])


# ---- CommandCursor (list_indexes / aggregate / list_collections)
def _cc(client, db):
    seed(db, 9)
    db.t.create_index("a"); db.t.create_index("g")
    c = db.t.aggregate([{"$sort": {"a": 1}}], batchSize=2)
    first = next(c)["a"]
    tn = c.try_next()
    rest = [d["a"] for d in c]
    c2 = db.t.list_indexes()
    names = sorted(i["name"] for i in c2)
    return {"first": first, "try_next": tn["a"] if tn else None, "rest": rest, "names": names}


dc("CommandCursor", ["command_cursor.CommandCursor.next", "command_cursor.CommandCursor.try_next"], _cc)


def _cc_close(client, db):
    seed(db, 9)
    c = db.t.aggregate([{"$sort": {"a": 1}}], batchSize=2)
    next(c); c.close()
    return {"alive": c.alive}


dc("CommandCursor.close", ["command_cursor.CommandCursor.close"], _cc_close)


# ---- Change stream classes: same behaviour for collection/database/cluster streams
def _cs_members(client, db):
    st = db.c.watch(max_await_time_ms=300)
    db.c.insert_one({"_id": 1})
    e = None
    for _ in range(5):
        e = st.try_next()
        if e: break
    tok = st.resume_token
    alive = st.alive
    st.close()
    return {"op": e["operationType"] if e else None, "token_is_doc": isinstance(tok, dict) and "_data" in tok, "alive": alive, "closed": not st.alive}


for _cls in ("CollectionChangeStream", "DatabaseChangeStream", "ClusterChangeStream", "ChangeStream"):
    pass
dc("ChangeStream.members", [f"change_stream.{c}.{m}" for c in ("ChangeStream", "CollectionChangeStream") for m in ("try_next", "close", "resume_token", "alive")], _cs_members, rs=True)


def _cs_db(client, db):
    st = db.watch(max_await_time_ms=300)
    db.c.insert_one({"_id": 1})
    e = next(iter([x for x in [st.try_next() for _ in range(5)] if x]), None)
    st.close()
    return {"op": e["operationType"] if e else None, "ns": (e["ns"]["coll"] if e else None)}


dc("Database.watch", ["database.Database.watch"] + [f"change_stream.DatabaseChangeStream.{m}" for m in ("try_next", "close")], _cs_db, rs=True)


def _cs_client(client, db):
    st = client.watch(max_await_time_ms=300)
    db.c.insert_one({"_id": 1})
    e = next(iter([x for x in [st.try_next() for _ in range(8)] if x and x.get("ns", {}).get("db") == db.name]), None)
    st.close()
    return {"op": e["operationType"] if e else None}


dc("MongoClient.watch", ["mongo_client.MongoClient.watch"] + [f"change_stream.ClusterChangeStream.{m}" for m in ("try_next", "close")], _cs_client, rs=True)


# ---- sessions
def _session_members(client, db):
    with client.start_session() as s:
        db.t.insert_one({"_id": 1}, session=s)
        return {"has_id": bool(s.session_id), "in_txn": s.in_transaction, "ended": s.has_ended, "client_is": s.client is client,
                "opts": s.options.causal_consistency}


dc("ClientSession.members", ["client_session.ClientSession.session_id", "client_session.ClientSession.in_transaction", "client_session.ClientSession.has_ended",
                             "client_session.ClientSession.client", "client_session.ClientSession.options"], _session_members, rs=True)


def _cluster_time_ops(client, db):
    with client.start_session() as s:
        db.t.insert_one({"_id": 1}, session=s)
        return {"cluster_time": s.cluster_time is not None, "operation_time": s.operation_time is not None}


dc("ClientSession.times", ["client_session.ClientSession.cluster_time", "client_session.ClientSession.operation_time"], _cluster_time_ops, rs=True)
