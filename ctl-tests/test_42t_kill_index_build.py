"""SIGKILL during an index build with concurrent writes (fix round 2 of fix-unique-txn).

Each trial: a fresh collection with documents, a unique index build on `k` and a non-unique one on `g` started together (one
document per build chunk, so the builds run for seconds), 4 writers inserting new documents and updating existing ones on
both sides of the build cursor (some values collide on purpose), then SIGKILL at a random moment. After the restart the
build resumes (or its leftovers are removed), and the test checks:
- a unique index that exists holds no value twice;
- `validate` says every index is consistent with the data (no entry missing, none extra, no unique value held twice);
- every acknowledged insert is there (the writers only insert and update, never delete).
At least 20 trials (KILL_TRIALS, default 20).
"""
import os
import random
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, free_port, make_bucket

TRIALS = int(os.environ.get("KILL_TRIALS", "20"))
DOCS = 3000


def wait_builds_done(co, timeout=180):
    """Until no index of `co` is building: listIndexes shows only ready indexes, validate also shows building ones."""
    end = time.time() + timeout
    while time.time() < end:
        co.find_one()                       # loading the collection resumes an unfinished build
        ready = {i["name"] for i in co.list_indexes()}
        v = co.database.command("validate", co.name)
        if set(v["indexDetails"]) <= ready:
            return v
        time.sleep(0.2)
    raise AssertionError("index build did not finish after the restart")


def test_kill9_during_index_build_with_writes():
    port = free_port()
    srv = Server(make_bucket("buildkill"), "buildkill", port=port, env={"MONGO_BUILD_CHUNK": "1"},
                 extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    totals = {"acked": 0, "unique_kept": 0, "unique_failed": 0, "killed_while_building": 0}
    try:
        for trial in range(TRIALS):
            c = srv.client()
            co = c.bk[f"c{trial}"]
            co.insert_many([{"_id": 1000 + i, "k": i, "g": i % 7} for i in range(DOCS)])
            c.close()
            acked, lock, stop = [], threading.Lock(), threading.Event()
            r0 = random.Random(trial)
            dup_bias = r0.random() < 0.5          # half of the trials try to write duplicates, half only new values

            def build():
                cl = MongoClient(srv.uri, serverSelectionTimeoutMS=2000, socketTimeoutMS=60000)
                try:
                    cl.bk[co.name].create_indexes([__import__("pymongo").IndexModel("k", unique=True), __import__("pymongo").IndexModel("g")])
                except PyMongoError:
                    pass
                cl.close()

            def writer(w):
                cl = MongoClient(srv.uri, retryWrites=False, serverSelectionTimeoutMS=2000, socketTimeoutMS=5000)
                cc = cl.bk[co.name]
                r = random.Random(trial * 100 + w)
                i = 0
                while not stop.is_set():
                    i += 1
                    k = -r.randrange(30) if dup_bias else -(10 ** 6 + w * 10 ** 5 + i)
                    _id = r.choice([-1, 1]) * (10 ** 7 + w * 10 ** 6 + i)
                    try:
                        if r.random() < 0.6:
                            cc.insert_one({"_id": _id, "k": k, "g": r.randrange(7)})
                            with lock:
                                acked.append(_id)
                        else:
                            cc.update_one({"_id": 1000 + r.randrange(DOCS)}, {"$set": {"k": k, "g": r.randrange(7)}})
                    except PyMongoError:
                        pass
                    time.sleep(0.002)
                cl.close()

            bt = threading.Thread(target=build)
            ws = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
            bt.start()
            for t in ws:
                t.start()
            time.sleep(0.3 + r0.random() * 2.5)
            building = bt.is_alive()
            srv.kill9()
            stop.set()
            bt.join()
            for t in ws:
                t.join()
            srv.proc = None
            srv.start()
            c = srv.client()
            co = c.bk[co.name]
            v = wait_builds_done(co)
            assert v["valid"], (trial, v.get("errors"))
            names = {i["name"]: i for i in co.list_indexes()}
            if "k_1" in names:
                assert names["k_1"].get("unique")
                dups = list(co.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 1}}}]))
                assert dups == [], (trial, dups[:5])
                totals["unique_kept"] += 1
            else:
                totals["unique_failed"] += 1
            have = {d["_id"] for d in co.find({}, {"_id": 1})}
            lost = [a for a in acked if a not in have]
            assert lost == [], (trial, lost[:5])
            totals["acked"] += len(acked)
            totals["killed_while_building"] += building
            print(f"trial {trial}: killed {'during' if building else 'after'} the build, dup writes {dup_bias}, "
                  f"{len(acked)} acknowledged inserts, unique index {'kept' if 'k_1' in names else 'gone'}, valid, 0 duplicates, 0 lost")
            c.drop_database("bk")
            c.close()
        print("totals", totals)
        assert totals["killed_while_building"] >= TRIALS // 2, totals    # most kills must land inside the build
    finally:
        srv.term()
