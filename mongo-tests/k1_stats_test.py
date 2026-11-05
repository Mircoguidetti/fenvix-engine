"""K1: statistics, serverStatus, explain and profiler for the Atlas test script.

Starts mongo-server (--store local, temporary directory) and checks:
  1. collStats / dbStats / $collStats counters equal a recount from the data (validate full makes them recount),
     after insert, update, replace, delete, upsert, transaction, capped collection, createIndex and dropIndex, TTL,
     rename and drop; dbStats dataSize no longer 0 under 2 GiB; the counters survive a restart; count with limit;
  2. serverStatus: process CPU that grows, wiredTiger.cache with the bytes in the shared cache and the store reads;
  3. explain: real executionTimeMillis;
  4. profiler: millis, storage.data.bytesRead and timeReadingMicros, keys and documents examined, plan summary,
     entries visible at once to a reader of system.profile.
Usage: python3 k1_stats_test.py [binary path]   (default: ./target/release/mongo-server)
"""
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import bson
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "target/release/mongo-server")
FAILS = []


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Srv:
    def __init__(self, d, extra=()):
        self.d, self.extra, self.p = d, list(extra), None
        self.port = free_port()

    def start(self):
        log = open(os.path.join(self.d, "server.log"), "a")
        self.p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{self.port}", "--store", "local", "--local-dir",
                                   os.path.join(self.d, "data"), "--ttl-sweep-secs", "1"] + self.extra,
                                  stdout=log, stderr=log)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        self.c = MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=5000)
        return self.c

    def stop(self):
        self.c.close()
        self.p.terminate()
        self.p.wait(30)


def recount(db, coll):
    """Counters read again from the data: validate full drops them, the next collStats reads everything."""
    db.command("validate", coll, full=True)
    return db.command("collStats", coll)


def same_stats(db, coll, label):
    a = db.command("collStats", coll)
    docs = list(db[coll].find({}))
    size = sum(len(bson.encode(d)) for d in docs)
    check(a["count"] == len(docs), f"{label}: count {a['count']} = documents {len(docs)}")
    check(a["size"] == size, f"{label}: size {a['size']} = BSON sum {size}")
    b = recount(db, coll)
    check(a["indexSizes"] == b["indexSizes"], f"{label}: indexSizes counted {dict(a['indexSizes'])} = recounted {dict(b['indexSizes'])}")
    check((a["count"], a["size"]) == (b["count"], b["size"]), f"{label}: count and size equal after the recount")


def main():
    d = tempfile.mkdtemp(prefix="k1-")
    srv = Srv(d)
    c = srv.start()
    db = c["k1"]
    rnd = random.Random(7)

    # ---- 1. counters
    db.create_collection("a")
    db["a"].create_index([("x", 1)], name="x_1")
    db["a"].insert_many([{"_id": i, "x": rnd.randrange(100), "t": "p" * rnd.randrange(1, 300), "arr": list(range(rnd.randrange(3)))} for i in range(500)])
    same_stats(db, "a", "insert")
    db["a"].create_index([("arr", 1)], name="arr_1")  # multikey, built over data: counted at the first collStats
    same_stats(db, "a", "createIndex over data")
    for i in range(0, 500, 3):
        db["a"].update_one({"_id": i}, {"$set": {"x": rnd.randrange(100), "t": "q" * rnd.randrange(1, 500)}, "$push": {"arr": i}})
    db["a"].replace_one({"_id": 1}, {"x": 5, "nuovo": True})
    db["a"].update_one({"_id": 10_000}, {"$set": {"x": 1}}, upsert=True)
    db["a"].delete_many({"x": {"$lt": 20}})
    db["a"].find_one_and_update({"_id": 2}, {"$set": {"t": "z" * 1000}})
    db["a"].find_one_and_delete({"_id": 4})
    same_stats(db, "a", "update/replace/upsert/delete/findAndModify")
    db["a"].drop_index("x_1")
    db["a"].create_index([("x", 1)], name="x_1")  # same name again: must not keep the old size
    db["a"].insert_one({"_id": "dopo", "x": 3})
    same_stats(db, "a", "dropIndex and createIndex with the same name")
    # implicit, no create
    db["b"].insert_many([{"v": "x" * i} for i in range(50)])
    same_stats(db, "b", "collection created by an insert")
    # transactions need a replica set: tested below with rs0
    # capped
    db.create_collection("cap", capped=True, size=4096)
    for i in range(200):
        db["cap"].insert_one({"i": i, "p": "c" * 40})
    same_stats(db, "cap", "capped (oldest removed by the limit)")
    # rename
    db["b"].rename("b2")
    same_stats(db, "b2", "rename")
    # TTL
    import datetime as dt
    db["ttl"].create_index("at", expireAfterSeconds=1)
    db["ttl"].insert_many([{"at": dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)} for _ in range(20)] + [{"at": dt.datetime(2100, 1, 1)}])
    time.sleep(3)
    same_stats(db, "ttl", "TTL")
    # dbStats dataSize under 2 GiB
    s = db.command("dbStats")
    tot = sum(db.command("collStats", n)["size"] for n in db.list_collection_names())
    check(s["dataSize"] > 0 and s["dataSize"] == tot, f"dbStats.dataSize {s['dataSize']} = sum of the sizes {tot} (not 0)")
    # drop and create again
    db["b2"].drop()
    db["b2"].insert_one({"k": 1})
    same_stats(db, "b2", "drop and again")
    # $collStats
    cs = list(db["a"].aggregate([{"$collStats": {"storageStats": {}, "count": {}}}]))[0]
    check(cs["count"] == db["a"].count_documents({}), "$collStats.count right")
    # count with limit
    n_all = db["a"].count_documents({})
    check(db.command("count", "a", query={}, limit=7)["n"] == 7, "count limit 7")
    check(db.command("count", "a", query={}, limit=7, skip=n_all - 3)["n"] == 3, "count limit 7 skip n-3 = 3")
    check(db.command("count", "a", query={"x": {"$gte": 50}}, limit=100000)["n"] == db["a"].count_documents({"x": {"$gte": 50}}), "count with a filter and a large limit")
    # restart: the counters stay (no recount needed)
    before = db.command("collStats", "a")
    srv.stop()
    c = srv.start()
    db = c["k1"]
    after = db.command("collStats", "a")
    check((before["count"], before["size"], before["indexSizes"]) == (after["count"], after["size"], after["indexSizes"]), "counters equal after the restart")
    same_stats(db, "a", "after the restart")

    # ---- 2. serverStatus
    s1 = c.admin.command("serverStatus")
    for _ in range(3):
        list(db["a"].find({}))
    s2 = c.admin.command("serverStatus")
    u1, u2 = s1["extra_info"]["user_time_us"], s2["extra_info"]["user_time_us"]
    check(u2 > u1 > 0, f"real process CPU, growing ({u1} -> {u2} us)")
    wc = s2["wiredTiger"]["cache"]
    check(wc["maximum bytes configured"] > 0, f"cache configured {wc['maximum bytes configured']} bytes")
    check(wc["bytes currently in the cache"] > 0, f"bytes in the cache {wc['bytes currently in the cache']}")
    check(wc["pages read into cache"] > 0 and wc["bytes read into cache"] > 0, f"store reads: {wc['pages read into cache']} GETs, {wc['bytes read into cache']} bytes")
    check("fenvix" in s2 and "s3" in s2["fenvix"], "fenvix section with the S3 counters")

    srv.stop()

    # ---- with a replica set: transaction, explain, profiler
    srv = Srv(d, ["--replica-set", "rs0"])
    c = srv.start()
    db = c["k1"]
    with c.start_session() as ses:
        with ses.start_transaction():
            db["a"].insert_one({"_id": "tx1", "x": 7}, session=ses)
            db["a"].delete_one({"_id": 3}, session=ses)
            db["a"].update_one({"_id": 5}, {"$set": {"t": "w" * 777}}, session=ses)
    same_stats(db, "a", "transaction")

    # ---- 3. explain
    db["big"].insert_many([{"i": i, "p": "x" * 200} for i in range(30000)])
    ex = db.command("explain", {"find": "big", "filter": {"p": "nessuno"}}, verbosity="executionStats")
    es = ex["executionStats"]
    check(es["executionTimeMillis"] > 0, f"explain executionTimeMillis {es['executionTimeMillis']} (was always 0)")
    check(es["totalDocsExamined"] == 30000, "explain: documents examined")

    # ---- 4. profiler
    db.command("profile", 2, slowms=0)
    t0 = time.perf_counter()
    list(db["big"].find({"p": "nessuno"}).comment("k1-cerca"))
    client_ms = (time.perf_counter() - t0) * 1000
    db.command("profile", 0)
    prof = list(db["system.profile"].find({"command.comment": "k1-cerca"}))
    check(len(prof) == 1, f"profiler: the entry is there at once ({len(prof)})")
    if prof:
        p = prof[0]
        check(isinstance(p.get("millis"), int) and p["millis"] <= client_ms + 1, f"profiler millis {p.get('millis')} <= client time {client_ms:.1f}")
        sd = p.get("storage", {}).get("data", {})
        check("bytesRead" in sd and "timeReadingMicros" in sd, f"profiler storage.data {sd}")
        check(p.get("docsExamined") == 30000, f"profiler docsExamined {p.get('docsExamined')}")
        check(p.get("planSummary") == "COLLSCAN", f"profiler planSummary {p.get('planSummary')}")
    db["big"].create_index("i")
    db.command("profile", 2, slowms=0)
    db["big"].find_one({"i": 77}, comment="k1-indice")
    db.command("profile", 0)
    p = db["system.profile"].find_one({"command.comment": "k1-indice"})
    check(p is not None and p.get("planSummary") == "IXSCAN { i: 1 }" and p.get("keysExamined") == 1, f"profiler with an index: {p and (p.get('planSummary'), p.get('keysExamined'), p.get('docsExamined'))}")
    # cold read: restart, the first find reads from the store
    srv.stop()
    c = srv.start()
    db = c["k1"]
    db.command("profile", 2, slowms=0)
    list(db["big"].find({"i": {"$gte": 100, "$lt": 200}}).comment("k1-fredda"))
    db.command("profile", 0)
    p = db["system.profile"].find_one({"command.comment": "k1-fredda"})
    sd = (p or {}).get("storage", {}).get("data", {})
    check(sd.get("bytesRead", 0) > 0 and sd.get("timeReadingMicros", 0) > 0, f"cold read: bytes and time read from the store {sd}")
    srv.stop()
    shutil.rmtree(d, ignore_errors=True)
    print(f"\n{len(FAILS)} failed")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
