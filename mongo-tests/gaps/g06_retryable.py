"""Item 6: retryable writes. A replay of a write with the same session, txnNumber and statement id must not run twice and
returns the stored result (plus retriedStmtIds / retriedStmtId); older numbers are refused; transactions share the number space.
All cases run on the replica-set pair (a standalone mongod refuses txnNumber)."""
import threading
import time

from bson import Int64
from gaps_py import case
from pymongo.errors import OperationFailure

_N = [0]


def base():
    """A transaction number above anything the driver's pooled sessions used before."""
    _N[0] += 1000
    return int(time.time_ns() // 1000) + _N[0]


def norm(r):
    if isinstance(r, tuple):
        return r
    out = {}
    for k in ("n", "nModified", "upserted", "retriedStmtIds", "lastErrorObject", "value", "retriedStmtId"):
        if k in r:
            out[k] = r[k]
    if "writeErrors" in r:
        out["writeErrors"] = [(e["index"], e["code"]) for e in r["writeErrors"]]
    return out


def w(db, s, cmd, num, **extra):
    c = dict(cmd)
    c["txnNumber"] = Int64(num)
    c.update(extra)
    try:
        return norm(db.command(c, session=s))
    except OperationFailure as e:
        return ("ERR", e.code, e.details.get("codeName"))


def ins(coll, docs, **kw):
    return {"insert": coll, "documents": docs, **kw}


def upd(coll, specs, **kw):
    return {"update": coll, "updates": specs, **kw}


def dele(coll, specs, **kw):
    return {"delete": coll, "deletes": specs, **kw}


@case("g06.insert.replay_returns_stored_result_and_does_not_insert_twice", rs=True)
def _insert(client, db):
    s = client.start_session(); b = base()
    a = w(db, s, ins("t", [{"_id": 1}, {"_id": 2}]), b)
    r = w(db, s, ins("t", [{"_id": 1}, {"_id": 2}]), b)
    return {"first": a, "replay": r, "count": db.t.count_documents({})}


@case("g06.insert.replay_ignores_the_body", rs=True)
def _insert_body(client, db):
    s = client.start_session(); b = base()
    w(db, s, ins("t", [{"_id": 1, "v": "orig"}]), b)
    r = w(db, s, ins("t", [{"_id": 99, "v": "other"}]), b)
    return {"replay": r, "docs": sorted(d["_id"] for d in db.t.find())}


@case("g06.insert.replay_in_other_collection_and_database", rs=True)
def _insert_ns(client, db):
    s = client.start_session(); b = base()
    w(db, s, ins("t", [{"_id": 1}]), b)
    r1 = w(db, s, ins("u", [{"_id": 1}]), b)
    r2 = w(client[db.name + "_other"], s, ins("t", [{"_id": 1}]), b)
    res = {"coll": r1, "db": r2, "u": db.u.count_documents({}), "other": client[db.name + "_other"].t.count_documents({})}
    client.drop_database(db.name + "_other")
    return res


@case("g06.insert.batch_replayed_with_more_statements", rs=True)
def _insert_more(client, db):
    s = client.start_session(); b = base()
    a = w(db, s, ins("t", [{"_id": 20}, {"_id": 21}], stmtIds=[0, 1]), b)
    r = w(db, s, ins("t", [{"_id": 20}, {"_id": 21}, {"_id": 22}], stmtIds=[0, 1, 2]), b)
    return {"first": a, "replay": r, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.insert.stmtId_offsets_the_batch", rs=True)
def _insert_stmt_id(client, db):
    s = client.start_session(); b = base()
    w(db, s, ins("t", [{"_id": 1}, {"_id": 2}], stmtId=10), b)
    r = w(db, s, ins("t", [{"_id": 8}, {"_id": 9}, {"_id": 3}], stmtId=9), b)
    return {"replay": r, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.insert.unordered_error_statement_runs_again", rs=True)
def _insert_unordered(client, db):
    db.t.insert_one({"_id": 1})
    s = client.start_session(); b = base()
    docs = [{"_id": 1}, {"_id": 50}, {"_id": 51}]
    a = w(db, s, ins("t", docs, ordered=False), b)
    r = w(db, s, ins("t", docs, ordered=False), b)
    return {"first": a, "replay": r, "count": db.t.count_documents({})}


@case("g06.insert.ordered_error_stops_and_is_not_recorded", rs=True)
def _insert_ordered(client, db):
    db.t.insert_one({"_id": 2})
    s = client.start_session(); b = base()
    docs = [{"_id": 1}, {"_id": 2}, {"_id": 3}]
    a = w(db, s, ins("t", docs), b)
    r = w(db, s, ins("t", docs), b)
    return {"first": a, "replay": r, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.update.replay_does_not_apply_twice", rs=True)
def _update(client, db):
    db.t.insert_one({"_id": 1, "x": 0})
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 1}, "u": {"$inc": {"x": 1}}}]
    a = w(db, s, upd("t", sp), b)
    r = w(db, s, upd("t", sp), b)
    r2 = w(db, s, upd("t", [{"q": {"_id": 1}, "u": {"$inc": {"x": 100}}}]), b)
    return {"first": a, "replay": r, "body_changed": r2, "x": db.t.find_one({"_id": 1})["x"]}


@case("g06.update.upsert_replay_returns_the_upserted_id", rs=True)
def _update_upsert(client, db):
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 5}, "u": {"$set": {"a": 1}}, "upsert": True}]
    a = w(db, s, upd("t", sp), b)
    r = w(db, s, upd("t", sp), b)
    return {"first": a, "replay": r, "count": db.t.count_documents({})}


@case("g06.update.batch_of_three_mixed", rs=True)
def _update_batch(client, db):
    db.t.insert_many([{"_id": 50, "z": 0}, {"_id": 51, "z": 5}])
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 50}, "u": {"$set": {"z": 1}}}, {"q": {"_id": 99}, "u": {"$set": {"z": 1}}, "upsert": True}, {"q": {"_id": 51}, "u": {"$set": {"z": 0}}}]
    a = w(db, s, upd("t", sp), b)
    r = w(db, s, upd("t", sp), b)
    return {"first": a, "replay": r, "docs": list(db.t.find().sort("_id"))}


@case("g06.update.statements_that_changed_nothing_run_again", rs=True)
def _update_noop(client, db):
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 7}, "u": {"$set": {"a": 1}}}]
    a = w(db, s, upd("t", sp), b)
    db.t.insert_one({"_id": 7})
    r = w(db, s, upd("t", sp), b)
    r2 = w(db, s, upd("t", sp), b)
    return {"first": a, "after_insert": r, "again": r2, "doc": db.t.find_one({"_id": 7})}


@case("g06.update.unchanged_match_runs_again", rs=True)
def _update_nochange(client, db):
    db.t.insert_one({"_id": 1, "v": 1})
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 1}, "u": {"$set": {"v": 1}}}]
    a = w(db, s, upd("t", sp), b)
    db.t.update_one({"_id": 1}, {"$set": {"v": 2}})
    r = w(db, s, upd("t", sp), b)
    return {"first": a, "replay": r, "v": db.t.find_one({"_id": 1})["v"]}


@case("g06.update.replacement_replay", rs=True)
def _update_replace(client, db):
    db.t.insert_one({"_id": 4, "v": 1})
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 4}, "u": {"v": 9}}]
    a = w(db, s, upd("t", sp), b)
    db.t.update_one({"_id": 4}, {"$set": {"other": 1}})
    r = w(db, s, upd("t", sp), b)
    return {"first": a, "replay": r, "doc": db.t.find_one({"_id": 4})}


@case("g06.delete.replay_does_not_delete_again", rs=True)
def _delete(client, db):
    db.t.insert_many([{"_id": i} for i in range(4)])
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 2}, "limit": 1}]
    a = w(db, s, dele("t", sp), b)
    db.t.insert_one({"_id": 2, "again": 1})
    r = w(db, s, dele("t", sp), b)
    r2 = w(db, s, dele("t", [{"q": {"_id": 3}, "limit": 1}]), b)
    return {"first": a, "replay": r, "body_changed": r2, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.delete.nothing_deleted_runs_again", rs=True)
def _delete_none(client, db):
    s = client.start_session(); b = base()
    sp = [{"q": {"_id": 9}, "limit": 1}]
    a = w(db, s, dele("t", sp), b)
    db.t.insert_one({"_id": 9})
    r = w(db, s, dele("t", sp), b)
    return {"first": a, "after_insert": r, "count": db.t.count_documents({})}


@case("g06.multi_and_limit0_are_refused", rs=True)
def _multi(client, db):
    db.t.insert_many([{"_id": i} for i in range(3)])
    s = client.start_session(); b = base()
    a = w(db, s, upd("t", [{"q": {}, "u": {"$set": {"k": 1}}, "multi": True}]), b)
    b2 = w(db, s, dele("t", [{"q": {}, "limit": 0}]), b + 1)
    c = w(db, s, upd("t", [{"q": {"_id": 0}, "u": {"$set": {"k": 1}}}, {"q": {}, "u": {"$set": {"k": 2}}, "multi": True}, {"q": {"_id": 1}, "u": {"$set": {"k": 1}}}], ordered=False), b + 2)
    return {"update_multi": a, "delete_limit0": b2, "unordered": c, "docs": list(db.t.find().sort("_id"))}


@case("g06.findAndModify.remove_replay_returns_the_removed_document", rs=True)
def _fam_remove(client, db):
    db.t.insert_many([{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}])
    s = client.start_session(); b = base()
    cmd = {"findAndModify": "t", "query": {"_id": 1}, "remove": True}
    a = w(db, s, cmd, b)
    r = w(db, s, cmd, b)
    return {"first": a, "replay": r, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.findAndModify.update_replay_returns_stored_image", rs=True)
def _fam_update(client, db):
    db.t.insert_one({"_id": 5, "a": 1})
    s = client.start_session(); b = base()
    pre = {"findAndModify": "t", "query": {"_id": 5}, "update": {"$inc": {"a": 1}}, "new": False}
    post = {"findAndModify": "t", "query": {"_id": 5}, "update": {"$inc": {"a": 1}}, "new": True}
    a = w(db, s, pre, b)
    r = w(db, s, pre, b)
    c = w(db, s, post, b + 1)
    d = w(db, s, post, b + 1)
    return {"pre": a, "pre_replay": r, "post": c, "post_replay": d, "a": db.t.find_one({"_id": 5})["a"]}


@case("g06.findAndModify.upsert_replay", rs=True)
def _fam_upsert(client, db):
    s = client.start_session(); b = base()
    cmd = {"findAndModify": "t", "query": {"_id": 77}, "update": {"$set": {"q": 1}}, "upsert": True, "new": True}
    a = w(db, s, cmd, b)
    r = w(db, s, cmd, b)
    pre = {"findAndModify": "t", "query": {"_id": 78}, "update": {"$set": {"q": 1}}, "upsert": True}
    c = w(db, s, pre, b + 1)
    d = w(db, s, pre, b + 1)
    return {"new": a, "new_replay": r, "pre": c, "pre_replay": d, "count": db.t.count_documents({})}


@case("g06.findAndModify.fields_are_applied_to_the_stored_image", rs=True)
def _fam_fields(client, db):
    db.t.insert_one({"_id": 77, "q": 1, "r": 2})
    s = client.start_session(); b = base()
    cmd = {"findAndModify": "t", "query": {"_id": 77}, "update": {"$set": {"q": 2}}, "fields": {"q": 1}, "new": True}
    a = w(db, s, cmd, b)
    r = w(db, s, cmd, b)
    return {"first": a, "replay": r}


@case("g06.findAndModify.no_match_runs_again", rs=True)
def _fam_nomatch(client, db):
    s = client.start_session(); b = base()
    cmd = {"findAndModify": "t", "query": {"_id": 1000}, "update": {"$set": {"q": 2}}}
    a = w(db, s, cmd, b)
    db.t.insert_one({"_id": 1000})
    r = w(db, s, cmd, b)
    return {"first": a, "after_insert": r}


@case("g06.findAndModify.stmtId", rs=True)
def _fam_stmt(client, db):
    db.t.insert_one({"_id": 1, "v": 1})
    s = client.start_session(); b = base()
    cmd = {"findAndModify": "t", "query": {"_id": 1}, "update": {"$inc": {"v": 1}}, "new": True, "stmtId": 4}
    a = w(db, s, cmd, b)
    r = w(db, s, cmd, b)
    return {"first": a, "replay": r}


@case("g06.number.older_is_too_old", rs=True)
def _older(client, db):
    s = client.start_session(); b = base()
    a = w(db, s, ins("t", [{"_id": 1}]), b + 5)
    o = w(db, s, ins("t", [{"_id": 2}]), b + 4)
    u = w(db, s, upd("t", [{"q": {"_id": 1}, "u": {"$set": {"a": 1}}}]), b + 4)
    f = w(db, s, {"findAndModify": "t", "query": {}, "remove": True}, b + 1)
    same = w(db, s, ins("t", [{"_id": 1}]), b + 5)
    return {"first": a, "older_insert": o, "older_update": u, "older_fam": f, "same": same, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.number.sessions_are_independent", rs=True)
def _sessions(client, db):
    s1 = client.start_session(); s2 = client.start_session(); b = base()
    a = w(db, s1, ins("t", [{"_id": 1}]), b)
    c = w(db, s2, ins("t", [{"_id": 2}]), b)
    r = w(db, s2, ins("t", [{"_id": 2}]), b)
    return {"s1": a, "s2": c, "s2_replay": r, "count": db.t.count_documents({})}


@case("g06.number.validation", rs=True)
def _validate(client, db):
    s = client.start_session(); b = base()
    int32 = w(db, s, ins("t", [{"_id": 1}]), 1) if False else None
    try:
        db.command({"insert": "t", "documents": [{"_id": 1}], "txnNumber": 5}, session=s)
        int32 = "ok"
    except OperationFailure as e:
        int32 = (e.code, e.details.get("codeName"))
    try:
        db.command({"insert": "t", "documents": [{"_id": 1}], "txnNumber": "x"}, session=s)
        string = "ok"
    except OperationFailure as e:
        string = (e.code, e.details.get("codeName"))
    neg = w(db, s, ins("t", [{"_id": 1}]), -1)
    return {"int32": int32, "string": string, "negative": neg, "count": db.t.count_documents({})}


@case("g06.stmtIds.validation", rs=True)
def _stmt_validate(client, db):
    s = client.start_session(); b = base()
    both = w(db, s, ins("t", [{"_id": 1}], stmtIds=[0], stmtId=0), b)
    short = w(db, s, ins("t", [{"_id": 1}, {"_id": 2}], stmtIds=[0]), b + 1)
    long = w(db, s, ins("t", [{"_id": 1}], stmtId=Int64(1)), b + 2)
    badtype = w(db, s, ins("t", [{"_id": 1}], stmtIds=["a"]), b + 3)
    return {"both": both, "short": short, "long": long, "badtype": badtype, "count": db.t.count_documents({})}


@case("g06.command.not_retryable_with_txnNumber", rs=True)
def _not_retryable(client, db):
    s = client.start_session(); b = base()
    db.t.insert_one({"_id": 1})
    out = {}
    for name, cmd in (("find", {"find": "t"}), ("aggregate", {"aggregate": "t", "pipeline": [], "cursor": {}}), ("count", {"count": "t"}), ("drop", {"drop": "zz"}), ("create", {"create": "zz"}), ("createIndexes", {"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}]})):
        out[name] = w(db, s, cmd, b)
    return out


@case("g06.transactions.share_the_number_space", rs=True)
def _txn_space(client, db):
    s = client.start_session(); b = base()
    w(db, s, ins("t", [{"_id": 1}]), b + 10)
    lower = w(db, s, ins("t", [{"_id": 2}]), b + 3, autocommit=False, startTransaction=True)
    same = w(db, s, ins("t", [{"_id": 2}]), b + 10, autocommit=False, startTransaction=True)
    higher = w(db, s, ins("t", [{"_id": 3}]), b + 11, autocommit=False, startTransaction=True)
    commit = w(client.admin, s, {"commitTransaction": 1}, b + 11, autocommit=False)
    retry_lower = w(db, s, ins("t", [{"_id": 4}]), b + 10)
    retry_same = w(db, s, ins("t", [{"_id": 4}]), b + 11)
    higher_w = w(db, s, ins("t", [{"_id": 5}]), b + 12)
    return {"txn_lower": lower, "txn_same": same, "txn_higher": higher, "commit": commit, "write_lower": retry_lower, "write_same": retry_same, "write_higher": higher_w, "ids": sorted(d["_id"] for d in db.t.find())}


@case("g06.concurrent_replays_run_once", rs=True)
def _concurrent(client, db):
    db.t.insert_one({"_id": 1, "x": 0})
    s = client.start_session(); b = base()
    out = []

    def go():
        out.append(w(db, s, upd("t", [{"q": {"_id": 1}, "u": {"$inc": {"x": 1}}}]), b))

    ts = [threading.Thread(target=go) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    replayed = sum(1 for o in out if "retriedStmtIds" in o)
    return {"x": db.t.find_one({"_id": 1})["x"], "results": sorted({(o["n"], o["nModified"]) for o in out}), "replayed": replayed}


@case("g06.session_survives_endSessions", rs=True)
def _end(client, db):
    s = client.start_session(); b = base()
    a = w(db, s, ins("t", [{"_id": 700}]), b)
    client.admin.command({"endSessions": [s.session_id]})
    r = w(db, s, ins("t", [{"_id": 700}]), b)
    o = w(db, s, ins("t", [{"_id": 701}]), b - 1)
    return {"first": a, "replay": r, "older": o, "count": db.t.count_documents({})}


@case("g06.driver.retryable_helpers_send_txnNumber", rs=True)
def _driver(client, db):
    # pymongo's own insert_one/update_one/delete_one/find_one_and_* attach lsid + txnNumber + stmtId on a replica set.
    t = db.t
    t.insert_one({"_id": 1, "v": 0})
    t.update_one({"_id": 1}, {"$inc": {"v": 1}})
    t.find_one_and_update({"_id": 1}, {"$inc": {"v": 1}})
    t.insert_many([{"_id": i} for i in range(2, 6)])
    t.delete_one({"_id": 2})
    t.replace_one({"_id": 3}, {"w": 1})
    r = t.bulk_write([__import__("pymongo").InsertOne({"_id": 10}), __import__("pymongo").UpdateOne({"_id": 1}, {"$inc": {"v": 1}}), __import__("pymongo").DeleteOne({"_id": 4})])
    return {"docs": list(t.find().sort("_id")), "bulk": (r.inserted_count, r.modified_count, r.deleted_count)}


@case("g06.standalone.refuses_txnNumber", sa=True)
def _standalone(client, db):
    s = client.start_session(); b = base()
    a = w(db, s, ins("t", [{"_id": 1}]), b)
    c = w(db, s, {"find": "t"}, b)
    try:
        db.command({"insert": "t", "documents": [{"_id": 1}], "txnNumber": "x"}, session=s)
        t = "ok"
    except OperationFailure as e:
        t = (e.code, e.details.get("codeName"))
    return {"insert": a, "find": c, "string": t, "count": db.t.count_documents({})}
