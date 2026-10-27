"""Item 10: cluster time. A replica set stamps every reply with $clusterTime and operationTime; sessions use them for causal consistency;
readConcern.afterClusterTime is checked against the current cluster time. (A standalone sends neither.)"""
import re
from bson import Timestamp
from gaps_py import case
from pymongo.errors import OperationFailure


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        return {"err": [e.code, e.details.get("codeName"), re.sub(r"Timestamp\(\d+, \d+\)", "Timestamp(T)", e.details.get("errmsg") or "")]}


def fields(r):
    ct = r.get("$clusterTime")
    out = {"has_clusterTime": ct is not None, "has_operationTime": "operationTime" in r}
    if ct is not None:
        sig = ct.get("signature", {})
        out.update({"ct_keys": sorted(ct), "sig_keys": sorted(sig), "hash_len": len(sig.get("hash", b"")), "keyId": int(sig.get("keyId", -1)), "ct_type": type(ct["clusterTime"]).__name__,
                    "op_type": type(r.get("operationTime")).__name__, "op_le_ct": r.get("operationTime") is not None and r["operationTime"] <= ct["clusterTime"]})
    return out


def reply_fields(client, db):
    db.t.insert_one({"_id": 1})
    return {"ping": fields(db.command("ping")), "insert": fields(db.command({"insert": "t", "documents": [{"_id": 2}]})), "find": fields(db.command({"find": "t", "filter": {}})),
            "error": fields(tr(lambda: db.command({"find": "t", "filter": {"$bogus": 1}})) if False else _err_reply(db)), "hello": fields(client.admin.command("hello"))}


def _err_reply(db):
    try:
        db.command({"find": "t", "filter": {"$bogus": 1}})
    except OperationFailure as e:
        return e.details
    return {}


case("g10.cluster.reply_fields", rs=True)(reply_fields)
case("g10.cluster.reply_fields_sa", sa=True)(reply_fields)


@case("g10.cluster.session_times", rs=True)
def _session_times(client, db):
    out = {}
    with client.start_session(causal_consistency=True) as s:
        out["before"] = [s.operation_time is None, s.cluster_time is None]
        db.t.insert_one({"_id": 1}, session=s)
        w = s.operation_time
        out["after_write"] = [w is not None, s.cluster_time is not None, type(w).__name__]
        db.t.insert_one({"_id": 2}, session=s)
        w2 = s.operation_time
        out["write_advances"] = w2 > w
        r = list(db.t.find({}, session=s).sort("_id", 1))
        out["read_own_writes"] = [d["_id"] for d in r]
        out["read_time"] = s.operation_time >= w2
        out["cluster_ge_op"] = s.cluster_time["clusterTime"] >= s.operation_time
        with client.start_session(causal_consistency=True) as s2:
            s2.advance_cluster_time(s.cluster_time)
            s2.advance_operation_time(s.operation_time)
            out["other_session_reads"] = db.t.count_documents({}, session=s2)
    return out


def rc_table(client, db, rs):
    db.t.insert_one({"_id": 1})
    if rs:
        with client.start_session() as s:
            db.t.insert_one({"_id": 2}, session=s)
            ts = s.operation_time
    else:
        ts = Timestamp(1700000000, 1)
    cases = {
        "none": None, "level_local": {"level": "local"}, "level_majority": {"level": "majority"}, "level_available": {"level": "available"}, "level_snapshot": {"level": "snapshot"}, "level_linearizable": {"level": "linearizable"},
        "level_bad": {"level": "nope"}, "level_int": {"level": 5}, "unknown": {"bogus": 1}, "act": {"afterClusterTime": ts}, "act_local": {"afterClusterTime": ts, "level": "local"},
        "act_majority": {"afterClusterTime": ts, "level": "majority"}, "act_available": {"afterClusterTime": ts, "level": "available"}, "act_snapshot": {"afterClusterTime": ts, "level": "snapshot"},
        "act_linearizable": {"afterClusterTime": ts, "level": "linearizable"}, "act_zero": {"afterClusterTime": Timestamp(0, 0)}, "act_int": {"afterClusterTime": 5},
        "act_past": {"afterClusterTime": Timestamp(ts.time - 10, 1)}, "act_future": {"afterClusterTime": Timestamp(ts.time + 100000, 0), "maxTimeMS": 300},
        "act_and_aot": {"afterClusterTime": ts, "afterOpTime": {"ts": ts, "t": 1}}, "empty": {},
    }
    out = {}
    for name, rc in cases.items():
        for cmd_name, cmd in (("find", {"find": "t", "filter": {"_id": 1}}), ("count", {"count": "t"}), ("aggregate", {"aggregate": "t", "pipeline": [], "cursor": {}}), ("distinct", {"distinct": "t", "key": "_id"})):
            c = dict(cmd)
            if rc is not None:
                rc = dict(rc)
                mt = rc.pop("maxTimeMS", None)
                c["readConcern"] = rc
                if mt:
                    c["maxTimeMS"] = mt
            r = tr(lambda: db.command(c))
            if "err" in r:
                out[f"{name}.{cmd_name}"] = r
            else:
                cur = r.get("cursor")
                out[f"{name}.{cmd_name}"] = {"ok": True, "n": r.get("n"), "values": r.get("values"), "docs": len(cur["firstBatch"]) if cur else None, "atClusterTime": bool(cur and "atClusterTime" in cur)}
    return out


case("g10.cluster.read_concern_rs", rs=True)(lambda client, db: rc_table(client, db, True))
case("g10.cluster.read_concern_sa", sa=True)(lambda client, db: rc_table(client, db, False))
