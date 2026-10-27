"""Transactions, retryable writes, sessions and the OP_MSG envelope fields, on a single-node replica set (real side)."""
import uuid
from bson import Binary, Int64, Timestamp
from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import PyMongoError
from probe_diff import case
import time
T0 = int(time.time() * 1000)     # pymongo reuses pooled implicit sessions: use a txnNumber larger than any it has used


@case("compat.txn.transient_error_label_on_conflict", ["txn:TransientTransactionError"], rs=True)
def _transient(client, db):
    db.t.insert_one({"_id": 1, "v": 0})
    s1, s2 = client.start_session(), client.start_session()
    s1.start_transaction(); s2.start_transaction()
    db.t.update_one({"_id": 1}, {"$set": {"v": 1}}, session=s1)
    try:
        db.t.update_one({"_id": 1}, {"$set": {"v": 2}}, session=s2)
        return {"conflict": False}
    except PyMongoError as e:
        return {"conflict": True, "labels": sorted(e.details.get("errorLabels", [])) if getattr(e, "details", None) else None, "code": getattr(e, "code", None),
                "has_label": e.has_error_label("TransientTransactionError")}
    finally:
        s1.abort_transaction(); s2.end_session(); s1.end_session()


@case("compat.txn.options_read_write_concern", ["txn:startTransaction", "txn:autocommit"], rs=True)
def _opts(client, db):
    with client.start_session() as s:
        s.start_transaction(read_concern=ReadConcern("snapshot"), write_concern=WriteConcern("majority"))
        db.t.insert_one({"_id": 1}, session=s)
        n = db.t.count_documents({}, session=s)
        s.commit_transaction()
    return {"n_in_txn": n, "n_after": db.t.count_documents({})}


@case("compat.txn.commit_maxTimeMS", ["txn:maxTimeMS (commitTransaction)"], rs=True)
def _commit_max(client, db):
    with client.start_session() as s:
        s.start_transaction(max_commit_time_ms=5000)
        db.t.insert_one({"_id": 1}, session=s)
        s.commit_transaction()
    return {"n": db.t.count_documents({})}


@case("compat.txn.commit_retry_idempotent", ["txn:commitTransaction"], rs=True)
def _commit_retry(client, db):
    with client.start_session() as s:
        s.start_transaction()
        db.t.insert_one({"_id": 1}, session=s)
        s.commit_transaction()
        s.commit_transaction()     # driver re-sends commitTransaction (documented retry behaviour)
    return {"n": db.t.count_documents({})}


@case("compat.txn.lifetime_limit_parameter", ["txn:transactionLifetimeLimitSeconds"], rs=True)
def _lifetime(client, db):
    r = client.admin.command({"getParameter": 1, "transactionLifetimeLimitSeconds": 1})
    return {"transactionLifetimeLimitSeconds": r.get("transactionLifetimeLimitSeconds")}


@case("compat.sessions.retryable_write_replay", ["sess:txnNumber (retryable writes)", "sess:retryWrites"], rs=True)
def _retry(client, db):
    lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
    cmd = {"insert": "t", "documents": [{"_id": 1, "x": 1}], "lsid": lsid, "txnNumber": Int64(T0)}
    r1 = db.command(cmd)
    r2 = db.command(cmd)        # same lsid + txnNumber: must not insert again
    cmd2 = {"update": "t", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"x": 1}}}], "lsid": lsid, "txnNumber": Int64(T0 + 1)}
    u1 = db.command(cmd2); u2 = db.command(cmd2)
    return {"n1": r1["n"], "n2": r2["n"], "count": db.t.count_documents({}), "x": db.t.find_one({"_id": 1})["x"], "u": [u1["n"], u2["n"], u1["nModified"], u2["nModified"]]}


@case("compat.sessions.retryWrites_option", ["sess:retryWrites"], rs=True)
def _rw(client, db):
    from pymongo import MongoClient
    host, port = client.address
    c = MongoClient(host, port, retryWrites=True, directConnection=True, serverSelectionTimeoutMS=8000)
    try:
        t = c[db.name].t
        t.insert_one({"_id": 1}); t.update_one({"_id": 1}, {"$set": {"a": 1}}); t.find_one_and_update({"_id": 1}, {"$inc": {"a": 1}})
        t.bulk_write([__import__("pymongo").InsertOne({"_id": 2})])
        return {"docs": list(t.find().sort("_id", 1))}
    finally:
        c.close()


@case("compat.sessions.causal_consistency", ["sess:causal consistency", "sess:$clusterTime"], rs=True)
def _causal(client, db):
    with client.start_session(causal_consistency=True) as s:
        db.t.insert_one({"_id": 1}, session=s)
        r = db.t.find_one({"_id": 1}, session=s)
        return {"read_own_write": r, "has_operation_time": s.operation_time is not None, "has_cluster_time": s.cluster_time is not None}


@case("compat.sessions.explicit_lsid_find", ["sess:lsid (logical session id)"], rs=True)
def _lsid(client, db):
    db.t.insert_one({"_id": 1})
    lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
    r = db.command({"find": "t", "lsid": lsid, "filter": {}})
    e = db.command({"endSessions": [lsid]}) if False else client.admin.command({"endSessions": [lsid]})
    return {"docs": r["cursor"]["firstBatch"], "endSessions": e["ok"]}


@case("compat.sessions.startSession_command", ["cmd:startSession"], rs=True)
def _start(client, db):
    r = client.admin.command({"startSession": 1})
    return {"keys": sorted(k for k in r if k not in ("operationTime", "$clusterTime")), "ok": r["ok"]}


@case("compat.sessions.refresh_kill_sessions", ["cmd:refreshSessions", "cmd:killSessions"], rs=True)
def _rk(client, db):
    lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
    db.command({"find": "t", "lsid": lsid, "filter": {}})
    a = client.admin.command({"refreshSessions": [lsid]})
    b = client.admin.command({"killSessions": [lsid]})
    return {"refresh": a["ok"], "kill": b["ok"]}
