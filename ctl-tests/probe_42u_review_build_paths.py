"""Reviewer probes, round 2 of fix-unique-txn: 29 write paths during a unique index build, on ours and on the reference mongod.

Usage: python probe_42u_review_build_paths.py ours|mongod [case ...]  (MONGO_SERVER_BIN picks the binary). Not a pytest file:
it prints one JSON line per case with a verdict (as-expected, DIFF, HOLE = a ready unique index holds a value twice).
"""
import sys, os, threading, time, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pymongo import MongoClient, InsertOne, UpdateOne, DeleteOne, ReplaceOne
from pymongo.collation import Collation
from pymongo.errors import OperationFailure, BulkWriteError, PyMongoError
from lib import Server, free_port, make_bucket

REAL = "mongodb://127.0.0.1:27020/?directConnection=true"
OURS_N, REAL_N = int(os.environ.get("OURS_N", "6000")), 300000


def code(f):
    try:
        f()
        return "ok"
    except BulkWriteError as e:
        return "bulk:" + ",".join(str(x["code"]) for x in e.details.get("writeErrors", []))
    except OperationFailure as e:
        return e.code


def seed(c, co, extra=None):
    n = REAL_N if c.address[1] == 27020 else OURS_N
    for i in range(0, n, 50000):
        co.insert_many([{"_id": 1000 + j, "k": j, "s": f"v{j}", "p": True} for j in range(i, min(n, i + 50000))], ordered=False)
    if extra:
        co.insert_many(extra)


def run(c, name, writes, spec=("k",), opts=None, extra=None, field="k", check_coll="c"):
    db = c["rv2_" + name]
    c.drop_database(db.name)
    co = db.c
    seed(c, co, extra)
    res = {}
    opts = opts or {}

    def build():
        try:
            if isinstance(spec, list):
                co.database.command("createIndexes", "c", indexes=spec)
            else:
                co.create_index([(f, 1) for f in spec], unique=True, **opts)
            res["build"] = "ok"
        except OperationFailure as e:
            res["build"] = e.code

    t = threading.Thread(target=build)
    t.start()
    time.sleep(float(os.environ.get("SLEEP", "0.15")) if c.address[1] != 27020 else 0.05)
    w = writes(db, co)
    alive = t.is_alive()
    t.join()
    co = db[check_coll]
    ix = [i for i in co.list_indexes() if i.get("unique") and i["name"] != "_id_"] if check_coll in db.list_collection_names() else []
    dups = []
    for i in ix:
        f = list(i["key"].keys())
        grp = {"_id": {x: "$" + x for x in f}, "ids": {"$addToSet": "$_id"}}
        pipe = []
        if "partialFilterExpression" in i:
            pipe.append({"$match": i["partialFilterExpression"]})
        pipe += [{"$unwind": {"path": "$" + f[0], "preserveNullAndEmptyArrays": True}}] if len(f) == 1 else []
        pipe += [{"$group": grp}, {"$match": {"ids.1": {"$exists": True}}}]
        kw = {"collation": i["collation"]} if "collation" in i else {}
        if kw:
            kw = {"collation": Collation(locale=i["collation"]["locale"], strength=i["collation"]["strength"])}
        d = list(co.aggregate(pipe, **kw))
        if d:
            dups.append((i["name"], d[:3]))
    valid = db.command("validate", check_coll)["valid"] if check_coll in db.list_collection_names() else None
    out = {"build": res.get("build"), "writes": w, "build_running_at_end_of_writes": alive,
           "unique_indexes": [i["name"] for i in ix], "dups_in_ready_unique": dups, "valid": valid}
    c.drop_database(db.name)
    return out


def W(*fs):
    return lambda db, co: [code(lambda f=f: f(db, co)) for f in fs]


def merge_src(db, co):
    db.src.insert_many([{"_id": 1, "k": -1}, {"_id": 2, "k": -1}])
    db.src.aggregate([{"$merge": {"into": "c", "whenNotMatched": "insert"}}])


def rename_target(db, co):
    db.src.insert_many([{"_id": 1, "k": -1}, {"_id": 2, "k": -1}])
    db.src.create_index("j")
    db.client.admin.command("renameCollection", f"{db.name}.src", to=f"{db.name}.c", dropTarget=True)


def drop_recreate(db, co):
    co.drop()
    co.insert_many([{"_id": 1, "k": -1}, {"_id": 2, "k": -1}])


CASES = {
    # expected: "fail" = build 11000 and no unique index; "ok" = build ok, index ready, no duplicate
    "fam_update_into_held": ("fail", dict(writes=W(lambda db, co: co.find_one_and_update({"_id": 1000}, {"$set": {"k": 1}})))),
    "fam_replace_into_held": ("fail", dict(writes=W(lambda db, co: co.find_one_and_replace({"_id": 1000}, {"k": 1})))),
    "fam_upsert_into_held": ("fail", dict(writes=W(lambda db, co: co.find_one_and_update({"_id": 5}, {"$set": {"k": 2}}, upsert=True)))),
    "fam_remove_then_insert": ("ok", dict(writes=W(lambda db, co: co.find_one_and_delete({"_id": 1000}), lambda db, co: co.insert_one({"_id": 1, "k": 0})))),
    "bulk_mixed_resolved": ("ok", dict(writes=W(lambda db, co: co.bulk_write([InsertOne({"_id": 1, "k": -1}), UpdateOne({"_id": 1000}, {"$set": {"k": -1}}), DeleteOne({"_id": 1})], ordered=False)))),
    "bulk_mixed_dup": ("fail", dict(writes=W(lambda db, co: co.bulk_write([InsertOne({"_id": 1, "k": -1}), UpdateOne({"_id": 1000}, {"$set": {"k": -1}}), ReplaceOne({"_id": 1001}, {"k": 7})], ordered=True)))),
    "bulk_swap_resolved": ("ok", dict(writes=W(lambda db, co: co.bulk_write([UpdateOne({"_id": 1000}, {"$set": {"k": -9}}), UpdateOne({"_id": 1001}, {"$set": {"k": 0}}), UpdateOne({"_id": 1000}, {"$set": {"k": 1}})])))),
    "transient_dup_same_command": ("ok", dict(writes=W(lambda db, co: co.bulk_write([UpdateOne({"_id": 1000}, {"$set": {"k": 1}}), UpdateOne({"_id": 1001}, {"$set": {"k": 0}})])))),
    "update_many_reverse_transient": ("ok", dict(writes=W(lambda db, co: co.update_many({"_id": {"$lt": 1010}}, [{"$set": {"k": {"$subtract": [9, "$k"]}}}])))),
    "update_many_inc_dup": ("fail", dict(writes=W(lambda db, co: co.update_many({"_id": {"$lt": 1010}}, {"$inc": {"k": 1}})))),
    "delete_reinsert_value": ("ok", dict(writes=W(lambda db, co: co.delete_one({"_id": 1000}), lambda db, co: co.insert_one({"_id": 1, "k": 0})))),
    "delete_reinsert_value_twice": ("fail", dict(writes=W(lambda db, co: co.delete_one({"_id": 1000}), lambda db, co: co.insert_one({"_id": 1, "k": 0}), lambda db, co: co.insert_one({"_id": 2, "k": 0})))),
    "delete_reinsert_same_id_other_value": ("fail", dict(writes=W(lambda db, co: co.delete_one({"_id": 1000}), lambda db, co: co.insert_one({"_id": 1000, "k": 1})))),
    "dup_then_fixed_by_update": ("ok", dict(writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 0}), lambda db, co: co.update_one({"_id": 1000}, {"$set": {"k": -3}})))),
    "dup_three_then_one_removed": ("fail", dict(writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 0}), lambda db, co: co.insert_one({"_id": 2, "k": 0}), lambda db, co: co.delete_one({"_id": 1})))),
    "partial_enter_filter": ("fail", dict(opts={"partialFilterExpression": {"p": True}}, extra=[{"_id": 1, "k": 5, "p": False}],
                                          writes=W(lambda db, co: co.update_one({"_id": 1}, {"$set": {"p": True}})))),
    "partial_outside_filter": ("ok", dict(opts={"partialFilterExpression": {"p": True}}, writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 5, "p": False})))),
    "collation_case": ("fail", dict(spec=("s",), opts={"collation": Collation(locale="en", strength=2)}, writes=W(lambda db, co: co.insert_one({"_id": 1, "s": "V3"})))),
    "string_prefix_no_false_dup": ("ok", dict(spec=("s",), writes=W(lambda db, co: co.insert_one({"_id": 1, "s": "v1x"}), lambda db, co: co.insert_one({"_id": 2, "s": "v"})))),
    "multikey_overlap": ("fail", dict(writes=W(lambda db, co: co.insert_one({"_id": 1, "k": [-5, -6]}), lambda db, co: co.insert_one({"_id": 2, "k": [-6, -7]})))),
    "multikey_self_repeat": ("ok", dict(writes=W(lambda db, co: co.insert_one({"_id": 1, "k": [-5, -5, -8]})))),
    "compound_dup": ("fail", dict(spec=("k", "s"), writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 3, "s": "v3"})))),
    "compound_no_dup": ("ok", dict(spec=("k", "s"), writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 3, "s": "other"})))),
    "merge_into_two_dups": ("fail", dict(writes=W(merge_src))),
    "two_indexes_dup_on_second": ("fail", dict(spec=[{"key": {"k": 1}, "name": "k_1", "unique": True}, {"key": {"s": 1}, "name": "s_1", "unique": True}],
                                               writes=W(lambda db, co: co.insert_one({"_id": 1, "k": -1, "s": "v2"})))),
    "txn_write_dup": ("fail", dict(writes=lambda db, co: [code(lambda: txn_insert(db))])),
    "drop_recreate_during_build": ("any", dict(writes=W(drop_recreate))),
    "rename_droptarget_during_build": ("any", dict(writes=W(rename_target))),
    "convert_to_capped_during_build": ("any", dict(writes=W(lambda db, co: co.insert_one({"_id": 1, "k": 0}), lambda db, co: db.command("convertToCapped", "c", size=1 << 30)))),
}


def txn_insert(db):
    c = db.client
    with c.start_session() as s:
        def body(s):
            db.c.insert_one({"_id": 1, "k": 0}, session=s)
        s.with_transaction(body)


def judge(exp, out):
    bad_dup = bool(out["dups_in_ready_unique"])
    if bad_dup or out["valid"] is False:
        return "HOLE"
    if exp == "fail":
        return "as-expected" if out["build"] == 11000 and not out["unique_indexes"] else "DIFF"
    if exp == "ok":
        return "as-expected" if out["build"] == "ok" and out["unique_indexes"] else "DIFF"
    return "no-dup"


if __name__ == "__main__":
    which = sys.argv[1]
    only = sys.argv[2:] or list(CASES)
    if which == "mongod":
        c = MongoClient(REAL)
        srv = None
    else:
        port = free_port()
        bin_ = os.environ.get("MONGO_SERVER_BIN")
        srv = Server(make_bucket("rv2" + which), "rv2" + which, port=port, env={"MONGO_BUILD_CHUNK": "1"}, binary=bin_,
                     extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
        c = MongoClient(srv.uri, socketTimeoutMS=None)
    for name in only:
        exp, kw = CASES[name]
        try:
            out = run(c, name, **kw)
            print(json.dumps({"case": name, "verdict": judge(exp, out), **out}, default=str), flush=True)
        except Exception as e:
            print(json.dumps({"case": name, "verdict": "ERROR", "err": repr(e)[:300]}), flush=True)
    if srv:
        srv.term()
