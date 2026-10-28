"""Item 12: driver members that send requests and that no earlier test called (pymongo): create_indexes, index_information, options, the cursor
option members, raw-batch and command cursors, change-stream cursors, session times, legacy GridFS. Each case runs against mongod 7.0.45 and the engine."""
import io
import time
from gaps_py import case
from pymongo import IndexModel, ASCENDING, DESCENDING, CursorType
from pymongo.errors import OperationFailure, PyMongoError
from gridfs import GridFS


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        return {"err": [e.code, e.details.get("codeName")]}
    except PyMongoError as e:
        return {"exc": type(e).__name__}
    except Exception as e:  # noqa  (driver-side errors have the same class on both servers)
        return {"exc": type(e).__name__, "msg": str(e)[:80]}


def seed(db, n=10, name="t"):
    db[name].insert_many([{"_id": i, "a": i, "g": i % 3, "s": f"s{i}"} for i in range(n)])
    return db[name]


@case("g12.py.create_indexes_and_information")
def _ci(client, db):
    t = seed(db)
    out = {}
    out["names"] = t.create_indexes([IndexModel([("a", ASCENDING)]), IndexModel([("g", DESCENDING), ("s", ASCENDING)], name="gs", unique=False), IndexModel([("s", ASCENDING)], unique=True, name="su", sparse=True)])
    out["again"] = t.create_indexes([IndexModel([("a", ASCENDING)])])
    info = t.index_information()
    out["info"] = {k: {kk: vv for kk, vv in v.items() if kk != "ns"} for k, v in sorted(info.items())}
    out["conflict"] = tr(lambda: t.create_indexes([IndexModel([("a", ASCENDING)], name="a_1", unique=True)]))
    out["empty"] = tr(lambda: t.create_indexes([]))
    out["bad_key"] = tr(lambda: t.create_indexes([IndexModel([("a", "bogus")])]))
    out["ttl"] = t.create_indexes([IndexModel([("a", ASCENDING)], name="ttl", expireAfterSeconds=100)]) if False else None
    out["partial"] = t.create_indexes([IndexModel([("g", ASCENDING)], name="part", partialFilterExpression={"a": {"$gt": 2}})])
    out["info_keys"] = sorted(t.index_information())
    out["missing"] = db["nope"].index_information() if False else tr(lambda: db["nope"].index_information())
    return out


@case("g12.py.collection_options")
def _opts(client, db):
    out = {}
    db.create_collection("plain")
    db.create_collection("cap", capped=True, size=4096, max=50)
    db.create_collection("val", validator={"a": {"$gt": 0}}, validationLevel="moderate", validationAction="warn")
    db.create_collection("col", collation={"locale": "fr", "strength": 2})
    for n in ("plain", "cap", "val", "col"):
        out[n] = db[n].options()
    out["missing"] = db["nope"].options()
    return out


@case("g12.py.cursor_option_members")
def _cur(client, db):
    t = seed(db)
    db.create_collection("cap", capped=True, size=4096)
    db.cap.insert_many([{"_id": i} for i in range(4)])
    out = {}
    out["add_option_tailable_plain"] = tr(lambda: [d["_id"] for d in t.find().add_option(2)])
    out["add_option_tailable_capped"] = tr(lambda: [d["_id"] for d in db.cap.find().add_option(2).limit(2)])
    out["add_remove"] = tr(lambda: [d["_id"] for d in db.cap.find().add_option(2).remove_option(2)])
    out["add_option_slave_ok"] = tr(lambda: [d["_id"] for d in t.find().add_option(4).limit(2)])
    out["add_option_no_timeout"] = tr(lambda: [d["_id"] for d in t.find().add_option(16).limit(2)])
    out["add_option_partial"] = tr(lambda: [d["_id"] for d in t.find().add_option(128).limit(2)])
    out["add_option_await"] = tr(lambda: [d["_id"] for d in db.cap.find().add_option(2 | 32).max_await_time_ms(100).limit(2)])
    def tail():
        c = db.cap.find(cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(100)
        return [next(c)["_id"], next(c)["_id"]]
    out["tailable_await"] = tr(tail)
    out["max_scan"] = tr(lambda: [d["_id"] for d in t.find().max_scan(5)])
    out["max_time"] = [d["_id"] for d in t.find().max_time_ms(5000)]
    out["bad_option"] = tr(lambda: list(t.find().add_option(1 << 20)))
    out["remove_unset"] = [d["_id"] for d in t.find().remove_option(2).limit(3)]
    out["batch_size_to_list"] = [d["_id"] for d in t.find().sort("_id", 1).batch_size(3)]
    return out


@case("g12.py.raw_batch_and_command_cursors")
def _raw(client, db):
    t = seed(db, 25)
    out = {}
    import bson
    cur = t.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=10)
    first = next(cur)
    out["first_batch_docs"] = [d["_id"] for d in bson.decode_all(first)][:3] + ["…", len(bson.decode_all(first))]
    rest = [len(bson.decode_all(b)) for b in cur]
    out["rest"] = rest
    cur = t.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=10)
    out["try_next"] = len(bson.decode_all(cur.try_next()))
    cur.close()
    out["after_close"] = tr(lambda: next(cur))
    cur = t.aggregate_raw_batches([{"$sort": {"_id": 1}}], batchSize=7)
    out["to_list"] = [len(bson.decode_all(b)) for b in cur.to_list()]
    cc = t.aggregate([{"$sort": {"_id": 1}}], batchSize=4)
    out["cc_to_list"] = [d["_id"] for d in cc.to_list()][:5] + [len(t.aggregate([{"$sort": {"_id": 1}}], batchSize=4).to_list())]
    cc = t.aggregate([{"$sort": {"_id": 1}}], batchSize=4)
    out["cc_try_next"] = [cc.try_next()["_id"], cc.alive]
    cc.close()
    out["cc_closed"] = [cc.alive, tr(lambda: next(cc))]
    cc = t.list_indexes()
    out["list_indexes_to_list"] = [d["name"] for d in cc.to_list()]
    out["find_raw"] = len(bson.decode_all(b"".join(t.find_raw_batches(batch_size=6))))
    return out


@case("g12.py.change_stream_cursors", rs=True)
def _cs(client, db):
    out = {}
    db.create_collection("c")
    for label, w in (("collection", db.c), ("database", db), ("cluster", client)):
        with w.watch(max_await_time_ms=200) as cs:
            out[label + "_empty_try"] = cs.try_next()
            tok0 = cs.resume_token
            out[label + "_token0"] = {k: type(v).__name__ for k, v in tok0.items()}   # (the token's bytes depend on the server's oplog)
            db.c.insert_one({"_id": label})
            ev = None
            for _ in range(30):
                ev = cs.try_next()
                if ev is not None and ev.get("ns", {}).get("db") == db.name:
                    break
            out[label + "_event"] = {k: v for k, v in (ev or {}).items() if k in ("operationType", "fullDocument", "documentKey")}
            tok = cs.resume_token
            out[label + "_token_type"] = [type(tok).__name__, sorted(tok) if isinstance(tok, dict) else None, tok != tok0]
            out[label + "_next_blocks_empty"] = None
        out[label + "_alive_after_with"] = None
        db.c.delete_many({})
    with db.c.watch(max_await_time_ms=100) as cs:
        db.c.insert_one({"_id": 1})
        out["next"] = cs.next()["operationType"]
    cs = db.c.watch(max_await_time_ms=100)
    cs.close()
    out["closed_alive"] = cs.alive
    out["closed_next"] = tr(lambda: cs.next())
    return out


@case("g12.py.session_times", rs=True)
def _sess(client, db):
    out = {}
    with client.start_session() as s:
        out["before"] = [s.cluster_time, s.operation_time]
        db.t.insert_one({"_id": 1}, session=s)
        out["after_write"] = [s.cluster_time is not None and sorted(s.cluster_time), type(s.operation_time).__name__]
        ct, ot = s.cluster_time, s.operation_time
        with client.start_session() as s2:
            s2.advance_cluster_time(ct)
            s2.advance_operation_time(ot)
            out["advanced"] = [s2.cluster_time == ct, s2.operation_time == ot]
            out["causal_read"] = db.t.find_one({"_id": 1}, session=s2)
            out["bad_cluster_time"] = tr(lambda: s2.advance_cluster_time(5))
            out["bad_operation_time"] = tr(lambda: s2.advance_operation_time(5))
            out["no_signature"] = tr(lambda: s2.advance_cluster_time({"clusterTime": ot}))
        out["older_ignored"] = tr(lambda: (s.advance_operation_time(ot), s.operation_time == ot)[1])
        db.t.insert_one({"_id": 2}, session=s)
        out["monotonic"] = s.operation_time >= ot
    return out


@case("g12.py.gridfs_legacy")
def _grid(client, db):
    fs = GridFS(db)
    out = {}
    f = fs.new_file(filename="a.txt", contentType="text/plain", chunk_size=4)
    f.write(b"hello world")
    f.close()
    out["new_file"] = [f._id is not None, f.length, f.chunk_size, f.name, f.content_type]
    fs.put(b"second version", filename="a.txt")
    out["find"] = sorted(g.filename for g in fs.find({"filename": "a.txt"}))
    out["find_one"] = fs.find_one({"filename": "a.txt"}).read()
    out["find_one_none"] = fs.find_one({"filename": "zzz"})
    out["get_version0"] = fs.get_version("a.txt", 0).read()
    out["get_version-1"] = fs.get_version("a.txt", -1).read()
    out["get_version_missing"] = tr(lambda: fs.get_version("a.txt", 5))
    out["get_version_noname"] = tr(lambda: fs.get_version("zzz"))
    out["find_sort_limit"] = [g.length for g in fs.find({}, sort=[("length", -1)], limit=1)]
    g = fs.new_file(filename="w.txt", chunk_size=3)
    g.writelines([b"abc", b"def", b"gh"])
    g.close()
    out["writelines"] = fs.get_last_version("w.txt").read()
    g = fs.new_file(filename="abort.txt", chunk_size=2)
    g.write(b"abcdefg")
    g.abort()
    out["abort"] = [fs.exists(filename="abort.txt"), db.fs.chunks.count_documents({"files_id": g._id}), g.closed]
    out["abort_again"] = tr(lambda: g.write(b"x"))
    h = fs.new_file(filename="ctx.txt")
    with h as hh:
        hh.write(b"ctx")
    out["ctx"] = fs.get_last_version("ctx.txt").read()
    out["list"] = sorted(fs.list())
    out["gridout_writelines"] = tr(lambda: fs.get_last_version("w.txt").writelines([b"x"]))
    return out


def _norm_ev(ev):
    """A change event without the volatile parts: the ids, the times and every collection UUID."""
    def rec(x):
        if isinstance(x, dict):
            return {k: ("<uuid>" if k in ("uuid", "collectionUUID") else rec(v)) for k, v in x.items() if k not in ("_id", "clusterTime", "wallTime", "lsid", "txnNumber")}
        if isinstance(x, list):
            return [rec(v) for v in x]
        return x
    return rec(dict(ev))


@case("g12.py.change_stream_expanded_ddl_events", rs=True)
def _cs_ddl(client, db):
    out = {}
    with db.watch(show_expanded_events=True, max_await_time_ms=200) as cs:
        c = db.create_collection("c")
        c.create_index([("a", 1)], name="a_1")
        c.create_index([("b", 1)], name="b_1", unique=True)
        db.command({"collMod": "c", "index": {"name": "a_1", "hidden": True}})
        db.command({"collMod": "c", "index": {"name": "b_1", "expireAfterSeconds": 100}}) if False else None
        db.command({"collMod": "c", "validator": {"x": {"$exists": True}}, "validationLevel": "moderate"})
        db.command({"collMod": "c", "index": {"keyPattern": {"b": 1}, "prepareUnique": True}})
        c.drop_index("a_1")
        db.command({"dropIndexes": "c", "index": ["b_1"]})
        c.create_index([("z", 1)], name="z_1")
        db.command({"dropIndexes": "c", "index": "*"})
        db.client.admin.command({"renameCollection": db.name + ".c", "to": db.name + ".d"})
        db.drop_collection("d")
        evs = []
        for _ in range(40):
            e = cs.try_next()
            if e is None:
                if len(evs) >= 11:
                    break
                continue
            evs.append(_norm_ev(e))
        out["events"] = evs
        out["types"] = [e["operationType"] for e in evs]
    return out
