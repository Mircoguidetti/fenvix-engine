"""Minimal reproductions of the Mastra differences, run against the target on localhost:27017.
Each probe prints one line: name, then what the target answered. Run it on mongod and on ours and compare."""
import datetime
import threading

import pymongo
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError

c = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")
db = c["probe_gaps"]
c.drop_database("probe_gaps")


def show(name, fn):
    try:
        print(f"{name}: {fn()!r}")
    except PyMongoError as e:
        print(f"{name}: ERROR {type(e).__name__} code={getattr(e, 'code', None)} {str(e)[:140]}")


# 1. $toLong / $convert of a Date (Mastra run fencing: generation seeded from {$toLong: '$$NOW'})
db.t.insert_one({"_id": 1, "d": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)})
show("toLong(date field)", lambda: db.t.aggregate([{"$project": {"_id": 0, "x": {"$toLong": "$d"}}}]).next())
show("convert(date->long)", lambda: db.t.aggregate([{"$project": {"_id": 0, "x": {"$convert": {"input": "$d", "to": "long"}}}}]).next())
show("toLong($$NOW) is long", lambda: type(db.t.aggregate([{"$project": {"_id": 0, "x": {"$toLong": "$$NOW"}}}]).next()["x"]).__name__)
show("upsert pipeline $max[1,$toLong $$NOW]", lambda: db.t.find_one_and_update(
    {"_id": "run1", "generation": {"$exists": False}},
    [{"$set": {"generation": {"$max": [1, {"$toLong": "$$NOW"}]}}}], upsert=True, return_document=True)["generation"] > 1)
show("toDecimal(date)", lambda: str(db.t.aggregate([{"$project": {"_id": 0, "x": {"$toDecimal": "$d"}}}]).next()["x"]))
show("toDouble(date)", lambda: db.t.aggregate([{"$project": {"_id": 0, "x": {"$toDouble": "$d"}}}]).next())


# 2. unique partial index, concurrent inserts of the same key, with and without transactions
def race(n, use_txn):
    coll = db[f"pages_{'txn' if use_txn else 'plain'}"]
    coll.create_index("activeKey", unique=True, partialFilterExpression={"activeKey": {"$type": "string"}})
    coll.insert_one({"_id": "seed"})
    barrier = threading.Barrier(n)
    out = []

    def one(i):
        try:
            barrier.wait()
            if use_txn:
                with c.start_session() as s:
                    def body(s):
                        coll.find_one({"activeKey": "p/install"}, session=s)
                        coll.insert_one({"_id": f"p{i}", "activeKey": "p/install"}, session=s)
                    s.with_transaction(body)
            else:
                coll.insert_one({"_id": f"p{i}", "activeKey": "p/install"})
            out.append("ok")
        except DuplicateKeyError:
            out.append("dup")
        except OperationFailure as e:
            out.append(f"err{e.code}")

    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return f"winners={out.count('ok')} docs_with_key={coll.count_documents({'activeKey': 'p/install'})} others={sorted(x for x in out if x != 'ok')}"


show("unique partial index, 4 concurrent plain inserts", lambda: race(4, False))
show("unique partial index, 4 concurrent transactions", lambda: race(4, True))


# 3. same with a plain unique index inside transactions
def race_plain_unique(n):
    coll = db["pages_u"]
    coll.create_index("k", unique=True)
    barrier = threading.Barrier(n)
    out = []

    def one(i):
        try:
            barrier.wait()
            with c.start_session() as s:
                s.with_transaction(lambda s: coll.insert_one({"_id": i, "k": "x"}, session=s))
            out.append("ok")
        except PyMongoError as e:
            out.append(type(e).__name__)

    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return f"winners={out.count('ok')} docs={coll.count_documents({'k': 'x'})}"


show("unique index, 4 concurrent transactions", lambda: race_plain_unique(4))
c.drop_database("probe_gaps")
