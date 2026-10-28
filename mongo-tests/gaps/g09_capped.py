"""Item 9: capped collections (create, eviction, limits, options, errors, transactions, collMod, convertToCapped,
cloneCollectionAsCapped), tailable cursors, dataSize."""
import re
import time
from gaps_py import case
from pymongo.errors import OperationFailure
import bson


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        return {"err": [e.code, e.details.get("codeName"), re.sub(r"UUID\(\"[0-9a-f-]+\"\)", "UUID(<u>)", e.details.get("errmsg") or "")]}


def opts(db, n):
    return next(db.list_collections(filter={"name": n}), {}).get("options")


def ids(db, n, **kw):
    return [d["_id"] for d in db[n].find(**kw)]


@case("g09.capped.create_options")
def _create(client, db):
    out = {}
    specs = {"size": {"capped": True, "size": 4096}, "max": {"capped": True, "size": 100000, "max": 5}, "tiny": {"capped": True, "size": 100}, "size5000": {"capped": True, "size": 5000},
             "nocap_size": {"size": 1000}, "no_size": {"capped": True}, "size0": {"capped": True, "size": 0}, "neg": {"capped": True, "size": -5}, "big": {"capped": True, "size": 1 << 60},
             "maxbig": {"capped": True, "size": 4096, "max": 1 << 40}, "capped_str": {"capped": "x", "size": 5000}, "size_str": {"capped": True, "size": "x"}, "max_nocap": {"max": 5},
             "capped_num": {"capped": 1, "size": 4096}, "capped_false": {"capped": False}, "size_double": {"capped": True, "size": 4096.0}, "max0": {"capped": True, "size": 4096, "max": 0}, "max_neg": {"capped": True, "size": 4096, "max": -1}}
    for name, s in specs.items():
        out[name] = [tr(lambda: db.command({"create": "c_" + name, **s})), opts(db, "c_" + name)]
    out["recreate_same"] = tr(lambda: db.command({"create": "c_size", "capped": True, "size": 4096}))
    out["recreate_other"] = tr(lambda: db.command({"create": "c_size", "capped": True, "size": 8192}))
    return out


@case("g09.capped.eviction_by_size")
def _evict_size(client, db):
    db.command({"create": "c", "capped": True, "size": 4096})
    for i in range(200):
        db.c.insert_one({"_id": i, "p": "y" * 30})
    got = ids(db, "c")
    st = db.command("collStats", "c")
    return {"n": len(got), "first": got[:3], "last": got[-3:], "stats": {k: st.get(k) for k in ("capped", "size", "count", "max", "maxSize", "nindexes")}, "docsize": len(bson.encode({"_id": 0, "p": "y" * 30}))}


@case("g09.capped.eviction_by_max")
def _evict_max(client, db):
    db.command({"create": "c", "capped": True, "size": 100000, "max": 5})
    for i in range(30):
        db.c.insert_one({"_id": i, "p": "x" * 10})
    st = db.command("collStats", "c")
    return {"ids": ids(db, "c"), "count": db.c.count_documents({}), "est": db.c.estimated_document_count(), "stats": {k: st.get(k) for k in ("capped", "size", "count", "max", "maxSize")}}


@case("g09.capped.insert_many_batches")
def _many(client, db):
    db.command({"create": "c", "capped": True, "size": 2048, "max": 20})
    db.c.insert_many([{"_id": i, "p": "z" * 50} for i in range(100)])
    a = ids(db, "c")
    db.c.insert_many([{"_id": 1000 + i} for i in range(5)])
    return {"a": a, "b": ids(db, "c"), "sorted_rev": ids(db, "c", sort=[("$natural", -1)])[:3]}


@case("g09.capped.single_doc_over_cap")
def _big(client, db):
    db.command({"create": "c", "capped": True, "size": 1000})
    db.c.insert_one({"_id": 1})
    r = tr(lambda: db.c.insert_one({"_id": "big", "p": "b" * 5000}).acknowledged)
    return {"r": r, "ids": ids(db, "c"), "count": db.c.count_documents({})}


@case("g09.capped.update_delete")
def _upd(client, db):
    db.command({"create": "c", "capped": True, "size": 4096})
    for i in range(10):
        db.c.insert_one({"_id": i, "p": "y" * 30})
    out = {}
    out["same_size"] = tr(lambda: db.c.update_one({"_id": 3}, {"$set": {"p": "z" * 30}}).modified_count)
    out["bigger"] = tr(lambda: db.c.update_one({"_id": 3}, {"$set": {"p": "z" * 31}}).modified_count)
    out["smaller"] = tr(lambda: db.c.update_one({"_id": 4}, {"$set": {"p": "z" * 5}}).modified_count)
    out["delete_one"] = tr(lambda: db.c.delete_one({"_id": 5}).deleted_count)
    out["replace"] = tr(lambda: db.c.replace_one({"_id": 6}, {"_id": 6, "p": "q" * 30}).modified_count)
    out["fam_remove"] = tr(lambda: db.command({"findAndModify": "c", "query": {"_id": 7}, "remove": True})["value"])
    out["delete_many"] = tr(lambda: db.c.delete_many({"_id": {"$gte": 8}}).deleted_count)
    out["ids"] = ids(db, "c")
    out["stats"] = {k: v for k, v in db.command("collStats", "c").items() if k in ("size", "count")}
    return out


@case("g09.capped.indexes")
def _idx(client, db):
    db.command({"create": "c", "capped": True, "size": 4096})
    out = {"secondary": tr(lambda: db.c.create_index("p")), "ttl": tr(lambda: db.c.create_index("q", expireAfterSeconds=10)),
           "unique": tr(lambda: db.c.create_index("u", unique=True)), "drop_all": tr(lambda: db.command({"dropIndexes": "c", "index": "*"}))}
    for i in range(50):
        db.c.insert_one({"_id": i, "p": i % 5, "u": i})
    db.c.create_index("p", name="p_1")
    for i in range(50, 150):
        db.c.insert_one({"_id": i, "p": i % 5, "u": i})
    out["n"] = db.c.count_documents({})
    out["by_index"] = db.c.count_documents({"p": 2})
    out["via_find"] = len(list(db.c.find({"p": 2}).hint("p_1")))
    out["dup"] = tr(lambda: db.c.insert_one({"_id": 149}))
    return out


@case("g09.capped.list_and_stats")
def _list(client, db):
    db.command({"create": "c", "capped": True, "size": 5000, "max": 10})
    db.command({"create": "plain"})
    for i in range(15):
        db.c.insert_one({"_id": i})
    lc = {d["name"]: d.get("options") for d in sorted(db.list_collections(), key=lambda d: d["name"])}
    st = db.command("collStats", "c")
    return {"lc": lc, "stats": {k: st.get(k) for k in ("capped", "max", "maxSize", "count", "size", "avgObjSize")}, "plain": db.command("collStats", "plain").get("capped"),
            "dbstats_objects": db.command("dbStats")["objects"]}


@case("g09.capped.drop_rename")
def _dr(client, db):
    db.command({"create": "c", "capped": True, "size": 1000, "max": 3})
    for i in range(10):
        db.c.insert_one({"_id": i})
    out = {"rename": tr(lambda: db.c.rename("d").name), "opts": opts(db, "d")}
    for i in range(10, 14):
        db.d.insert_one({"_id": i})
    out["ids"] = ids(db, "d")
    db.d.drop()
    db.command({"create": "d"})
    for i in range(10):
        db.d.insert_one({"_id": i})
    out["plain_after_drop"] = db.d.count_documents({})
    return out


@case("g09.capped.transactions", rs=True)
def _txn(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    db.cap.insert_one({"_id": 1})
    out = {}

    def run(name, f):
        def go():
            with client.start_session() as s:
                with s.start_transaction():
                    return f(s)
        out[name] = tr(go)
    run("insert", lambda s: db.cap.insert_one({"_id": 2}, session=s).acknowledged)
    run("read", lambda s: db.cap.count_documents({}, session=s))
    run("update", lambda s: db.cap.update_one({"_id": 1}, {"$set": {"a": 1}}, session=s).modified_count)
    run("delete", lambda s: db.cap.delete_one({"_id": 1}, session=s).deleted_count)
    out["ids"] = ids(db, "cap")
    return out


@case("g09.capped.collmod")
def _collmod(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    for i in range(10):
        db.cap.insert_one({"_id": i})
    db.nc.insert_one({"_id": 1})
    out = {}
    for name, c in {"noncapped": {"collMod": "nc", "cappedSize": 8192}, "size0": {"collMod": "cap", "cappedSize": 0}, "size_str": {"collMod": "cap", "cappedSize": "x"}, "size_big": {"collMod": "cap", "cappedSize": 1 << 60},
                    "max_neg": {"collMod": "cap", "cappedMax": -1}, "max0": {"collMod": "cap", "cappedMax": 0}, "both": {"collMod": "cap", "cappedSize": 10000, "cappedMax": 5}, "max3": {"collMod": "cap", "cappedMax": 3}}.items():
        out[name] = [tr(lambda: db.command(c)), opts(db, "cap"), db.cap.count_documents({})]
    db.cap.insert_one({"_id": 100})
    out["after_insert"] = ids(db, "cap")
    return out


@case("g09.capped.convert")
def _convert(client, db):
    db.nc.insert_many([{"_id": i, "p": "x" * 20} for i in range(10)])
    db.nc.create_index("p", name="p_1")
    db.command({"create": "cap", "capped": True, "size": 4096})
    for i in range(10):
        db.cap.insert_one({"_id": i})
    db.cap.command if False else None
    db.command({"collMod": "cap", "cappedMax": 3})
    out = {}
    out["capped_again"] = [tr(lambda: db.command({"convertToCapped": "cap", "size": 8192})), opts(db, "cap")]
    out["missing"] = tr(lambda: db.command({"convertToCapped": "nope", "size": 8192}))
    out["no_size"] = tr(lambda: db.command({"convertToCapped": "nc"}))
    out["size0"] = tr(lambda: db.command({"convertToCapped": "nc", "size": 0}))
    out["trim"] = [tr(lambda: db.command({"convertToCapped": "nc", "size": 100})), opts(db, "nc"), ids(db, "nc"), [i["name"] for i in db.nc.list_indexes()]]
    db.nc.insert_one({"_id": 50})
    out["after"] = ids(db, "nc")
    db.big.insert_many([{"_id": i} for i in range(20)])
    out["fits"] = [tr(lambda: db.command({"convertToCapped": "big", "size": 100000})), ids(db, "big")[:3], db.big.count_documents({})]
    return out


@case("g09.capped.clone")
def _clone(client, db):
    db.nc2.insert_many([{"_id": i, "p": "x" * 20} for i in range(5)])
    db.nc2.create_index("p")
    out = {}
    out["ok"] = [tr(lambda: db.command({"cloneCollectionAsCapped": "nc2", "toCollection": "cl", "size": 5000})), opts(db, "cl"), ids(db, "cl"), [i["name"] for i in db.cl.list_indexes()]]
    out["existing"] = tr(lambda: db.command({"cloneCollectionAsCapped": "nc2", "toCollection": "cl", "size": 5000}))
    out["missing"] = tr(lambda: db.command({"cloneCollectionAsCapped": "nope", "toCollection": "cl2", "size": 5000}))
    out["no_size"] = tr(lambda: db.command({"cloneCollectionAsCapped": "nc2", "toCollection": "cl3"}))
    out["no_to"] = tr(lambda: db.command({"cloneCollectionAsCapped": "nc2", "size": 5000}))
    out["small"] = [tr(lambda: db.command({"cloneCollectionAsCapped": "nc2", "toCollection": "cl4", "size": 100})), ids(db, "cl4"), db.nc2.count_documents({})]
    return out


@case("g09.tailable.find_getmore")
def _tail(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    for i in range(5):
        db.cap.insert_one({"_id": 10 + i, "v": i})
    r = db.command({"find": "cap", "filter": {}, "tailable": True, "batchSize": 2})
    cid = r["cursor"]["id"]
    out = {"first": [d["_id"] for d in r["cursor"]["firstBatch"]], "open": cid != 0, "ns": r["cursor"]["ns"]}
    seq = []
    for _ in range(5):
        g = db.command({"getMore": cid, "collection": "cap", "batchSize": 2})
        seq.append([g["cursor"]["id"] == cid, [d["_id"] for d in g["cursor"]["nextBatch"]]])
    out["seq"] = seq
    db.cap.insert_one({"_id": 99})
    g = db.command({"getMore": cid, "collection": "cap", "batchSize": 2})
    out["after_insert"] = [g["cursor"]["id"] == cid, [d["_id"] for d in g["cursor"]["nextBatch"]]]
    out["filter"] = (lambda c: [[d["_id"] for d in c["firstBatch"]], c["id"] != 0])(db.command({"find": "cap", "filter": {"v": {"$gt": 2}}, "tailable": True})["cursor"])
    out["kill"] = db.command({"killCursors": "cap", "cursors": [cid]})["cursorsKilled"] == [cid]
    return out


@case("g09.tailable.await_data")
def _await(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    db.cap.insert_one({"_id": 1})
    r = db.command({"find": "cap", "filter": {}, "tailable": True, "awaitData": True, "batchSize": 100})
    cid = r["cursor"]["id"]
    t = time.time()
    g = db.command({"getMore": cid, "collection": "cap", "maxTimeMS": 500})
    el = time.time() - t
    return {"first": len(r["cursor"]["firstBatch"]), "open": cid != 0, "empty": g["cursor"]["nextBatch"], "same": g["cursor"]["id"] == cid, "waited": 0.35 < el < 1.5,
            "maxtime_plain": tr(lambda: db.command({"getMore": db.command({"find": "cap", "tailable": True})["cursor"]["id"], "collection": "cap", "maxTimeMS": 5}))}


@case("g09.tailable.errors")
def _terr(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    db.cap.insert_one({"_id": 1, "v": 1})
    db.nc.insert_one({"_id": 1})
    out = {}
    for name, c in {"sort": {"find": "cap", "tailable": True, "sort": {"v": 1}}, "natural_sort": {"find": "cap", "tailable": True, "sort": {"$natural": 1}}, "limit": {"find": "cap", "tailable": True, "limit": 3},
                    "single": {"find": "cap", "tailable": True, "singleBatch": True}, "await_alone": {"find": "cap", "awaitData": True}, "non_capped": {"find": "nc", "tailable": True},
                    "skip": {"find": "cap", "tailable": True, "skip": 2}, "projection": {"find": "cap", "tailable": True, "projection": {"v": 1}}}.items():
        r = tr(lambda: db.command(c))
        out[name] = r if "err" in r else {"ok": True, "id": r["cursor"]["id"] != 0, "n": len(r["cursor"]["firstBatch"])}
    out["agg_no_tail"] = tr(lambda: list(db.cap.aggregate([{"$match": {}}], batchSize=1)))
    return out


@case("g09.tailable.lost_position")
def _lost(client, db):
    db.command({"create": "cap2", "capped": True, "size": 4096})
    for i in range(10):
        db.cap2.insert_one({"_id": i, "p": "x" * 200})
    r = db.command({"find": "cap2", "tailable": True, "batchSize": 2})
    cid = r["cursor"]["id"]
    for i in range(60):
        db.cap2.insert_one({"_id": 100 + i, "p": "y" * 200})
    return tr(lambda: db.command({"getMore": cid, "collection": "cap2", "batchSize": 2}))


@case("g09.tailable.pymongo_cursor")
def _pycur(client, db):
    db.command({"create": "cap", "capped": True, "size": 4096})
    for i in range(5):
        db.cap.insert_one({"_id": i})
    cur = db.cap.find({}, cursor_type=2, batch_size=2)
    got = [next(cur)["_id"] for _ in range(5)]
    alive = cur.alive
    db.cap.insert_one({"_id": 5})
    more = [d["_id"] for d in cur]
    return {"got": got, "alive": alive, "more": more}


@case("g09.datasize")
def _datasize(client, db):
    db.nc2.insert_many([{"_id": i} for i in range(5)])
    out = {}
    for name, c in {"plain": {"dataSize": f"{db.name}.nc2"}, "missing": {"dataSize": f"{db.name}.nope"}, "nodot": {"dataSize": "nodot"}, "range": {"dataSize": f"{db.name}.nc2", "keyPattern": {"_id": 1}, "min": {"_id": 1}, "max": {"_id": 3}},
                    "estimate": {"dataSize": f"{db.name}.nc2", "estimate": True}}.items():
        r = tr(lambda: db.command(c))
        if "err" not in r:
            r.pop("millis", None)
        out[name] = r
    out["other_db"] = (lambda r: {k: v for k, v in r.items() if k != "millis"})(client.admin.command({"dataSize": f"{db.name}.nc2"}))
    return out
