"""Reviewer's tests for fix-long-commands (a plain find answered page by page, the F7 check reading the live lease).

Every answer is compared with mongod 7.0 (judge/setup.sh, replica set rs0 on 127.0.0.1:27020) where mongod has one.

- test_find_collation_kept_by_getmore: a find with an explicit collation, drained with small batches. Each getMore
  reads its page as a new command: does it still match under the find's collation?
- test_find_simple_collation_on_a_case_insensitive_collection: the other direction (collection default strength 2, the
  find asks for the simple collation).
- test_find_let_kept_by_getmore: a find whose filter reads a `let` variable.
- test_writes_between_getmores_and_a_reopen: deletes, updates and inserts acknowledged between getMores, then the
  database is closed and reopened in the middle of the cursor; no deleted document, no stale version, nothing twice,
  nothing untouched missing.
- test_takeover_and_reopen_in_the_middle_of_a_cursor: P1 loses the database to P2, P2 writes and closes it, P1 takes it
  back and the same cursor goes on (the owner's test_57 accepts a refusal; this one requires the answer).
- test_a_long_aggregate_is_refused_when_the_lease_is_lost: F7 must not get weaker for the commands that still read
  everything in one command ($group and count).
- test_drop_in_the_middle_of_a_cursor: mongod kills the cursor (QueryPlanKilled); recorded, not asserted.
- test_first_batch_size_cap: a find whose batchSize asks for more than a reply can hold; recorded, not asserted.
"""
import os
import tempfile
import threading
import time

from bson import SON
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, make_bucket

REAL = os.environ.get("REAL_URI", "mongodb://127.0.0.1:27020/?directConnection=true")


def real():
    return MongoClient(REAL, serverSelectionTimeoutMS=5000)


def drain(c, dbname, cmd, batch):
    db = c[dbname]
    r = db.command(cmd)
    out = list(r["cursor"]["firstBatch"])
    cid, coll = r["cursor"]["id"], cmd["find"]
    while cid:
        g = db.command(SON([("getMore", cid), ("collection", coll), ("batchSize", batch)]))
        out += g["cursor"]["nextBatch"]
        cid = g["cursor"]["id"]
    return out


def faults():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def names_docs(n):
    return [{"_id": i, "name": ["abc", "ABC", "Abc", "xyz"][i % 4], "k": i % 7} for i in range(n)]


def both(setup):
    srv = Server(make_bucket("r58"), "r58", idle=600).start()
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
    m = real()
    m.drop_database("r58")
    for cl in (c, m):
        setup(cl["r58"])
    return srv, c, m


def test_find_collation_kept_by_getmore():
    srv, c, m = both(lambda db: db.t.insert_many(names_docs(1000)))
    try:
        cmd = SON([("find", "t"), ("filter", {"name": "abc"}), ("collation", {"locale": "en", "strength": 2}), ("batchSize", 10)])
        ours = [d["_id"] for d in drain(c, "r58", cmd, 10)]
        ref = [d["_id"] for d in drain(m, "r58", cmd, 10)]
        print(f"collation strength 2, filter name 'abc': ours {len(ours)}, mongod {len(ref)}")
        assert sorted(ours) == sorted(ref), f"ours {len(ours)} documents, mongod {len(ref)}"
    finally:
        srv.term()


def test_find_simple_collation_on_a_case_insensitive_collection():
    def setup(db):
        db.create_collection("t", collation={"locale": "en", "strength": 2})
        db.t.insert_many(names_docs(1000))
    srv, c, m = both(setup)
    try:
        cmd = SON([("find", "t"), ("filter", {"name": "abc"}), ("collation", {"locale": "simple"}), ("batchSize", 10)])
        ours = [d["_id"] for d in drain(c, "r58", cmd, 10)]
        ref = [d["_id"] for d in drain(m, "r58", cmd, 10)]
        print(f"collection strength 2, find with the simple collation: ours {len(ours)}, mongod {len(ref)}")
        assert sorted(ours) == sorted(ref), f"ours {len(ours)} documents, mongod {len(ref)}"
    finally:
        srv.term()


def test_find_let_kept_by_getmore():
    srv, c, m = both(lambda db: db.t.insert_many(names_docs(1000)))
    try:
        cmd = SON([("find", "t"), ("filter", {"$expr": {"$eq": ["$name", "$$v"]}}), ("let", {"v": "xyz"}), ("batchSize", 10)])
        res = {}
        for label, cl in (("ours", c), ("mongod", m)):
            try:
                res[label] = sorted(d["_id"] for d in drain(cl, "r58", cmd, 10))
            except OperationFailure as e:
                res[label] = f"error {e.code}: {str(e)[:120]}"
        print(f"let v='xyz': ours {res['ours'] if isinstance(res['ours'], str) else len(res['ours'])}, mongod {len(res['mongod'])}")
        assert res["ours"] == res["mongod"], f"ours {res['ours'] if isinstance(res['ours'], str) else len(res['ours'])}, mongod {len(res['mongod'])}"
    finally:
        srv.term()


def test_writes_between_getmores_and_a_reopen():
    """Single process. Batches of 1000 over 20,000 documents; after the first batch: delete 2,000 ahead, update 2,000
    ahead (new field v), insert 500 new; close the database (tenantAdmin close); go on. The same on mongod."""
    n = 20_000
    srv, c, m = both(lambda db: db.t.insert_many([{"_id": i, "pad": "x" * 100} for i in range(n)]))
    try:
        res = {}
        for label, cl in (("ours", c), ("mongod", m)):
            db = cl["r58"]
            r = db.command(SON([("find", "t"), ("batchSize", 1000)]))
            got = list(r["cursor"]["firstBatch"])
            cid = r["cursor"]["id"]
            db.t.delete_many({"_id": {"$gte": 5000, "$lt": 7000}})
            db.t.update_many({"_id": {"$gte": 10_000, "$lt": 12_000}}, {"$set": {"v": 1}})
            db.t.insert_many([{"_id": 100_000 + i, "pad": "y"} for i in range(500)])
            if label == "ours":
                db.command("tenantAdmin", close=True)
            k = 0
            while cid:
                g = db.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 1000)]))
                got += g["cursor"]["nextBatch"]
                cid = g["cursor"]["id"]
                k += 1
                if label == "ours" and k == 3:
                    db.command("tenantAdmin", close=True)   # a second reopen, later in the scan
            ids = [d["_id"] for d in got]
            res[label] = ids
            assert len(ids) == len(set(ids)), f"{label}: a document came twice"
            assert not [i for i in ids if 5000 <= i < 7000], f"{label}: deleted documents returned"
            stale = [d["_id"] for d in got if 10_000 <= d["_id"] < 12_000 and d.get("v") != 1]
            assert not stale, f"{label}: {len(stale)} documents returned in their version before an acknowledged update"
            want = set(range(n)) - set(range(5000, 7000)) | set(range(100_000, 100_500))
            assert set(ids) == want, f"{label}: {len(want - set(ids))} missing, {len(set(ids) - want)} extra"
        print(f"ours {len(res['ours'])} documents, mongod {len(res['mongod'])}; same order: {res['ours'] == res['mongod']}")
        assert res["ours"] == res["mongod"]
    finally:
        srv.term()


def test_takeover_and_reopen_in_the_middle_of_a_cursor():
    bucket = make_bucket("r58tk")
    ff = faults()
    p1 = Server(bucket, "r58tk1", idle=600, holder="Q1", extra=["--lease-ttl-secs", "3"], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    p2 = Server(bucket, "r58tk2", idle=2, holder="Q2", extra=["--lease-ttl-secs", "3"]).start()
    try:
        c1 = MongoClient(p1.uri, serverSelectionTimeoutMS=5000, retryReads=False, retryWrites=False)
        c2 = MongoClient(p2.uri, serverSelectionTimeoutMS=5000, retryReads=False, retryWrites=False)
        n = 20_000
        for s in range(0, n, 5000):
            c1.d.t.insert_many([{"_id": i, "pad": "x" * 100} for i in range(s, s + 5000)])
        r = c1.d.command(SON([("find", "t"), ("batchSize", 1000)]))
        got, cid = list(r["cursor"]["firstBatch"]), r["cursor"]["id"]
        with open(ff, "w") as fh:
            fh.write("delay_put processes/Q1/_lease 4500\n")
        deadline = time.time() + 60
        while True:
            try:
                c2.d.t.delete_many({"_id": {"$gte": 5000, "$lt": 7000}})
                c2.d.t.update_many({"_id": {"$gte": 10_000, "$lt": 12_000}}, {"$set": {"v": 1}})
                c2.d.t.insert_many([{"_id": 100_000 + i, "pad": "y"} for i in range(500)])
                break
            except PyMongoError:
                assert time.time() < deadline, "P2 never took the database over"
                time.sleep(0.3)
        with open(ff, "w") as fh:
            fh.write("")
        # P2 closes the database when idle (2 s) and releases it; P1 takes it back on the next getMore
        refused, t0 = 0, time.time()
        while cid:
            try:
                g = c1.d.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 1000)]))
            except OperationFailure as e:
                refused += 1
                assert time.time() - t0 < 60, f"P1 never got the database back: {e}"
                time.sleep(0.5)
                continue
            got += g["cursor"]["nextBatch"]
            cid = g["cursor"]["id"]
        ids = [d["_id"] for d in got]
        takeovers = p1.log_text().count("takeover_from=Q2") + p1.log_text().count("LEASE acquired")
        print(f"P1 answered {len(ids)} documents after {refused} refused getMores; P1 lease acquisitions logged: {takeovers}")
        assert len(ids) == len(set(ids)), "a document came twice"
        assert not [i for i in ids if 5000 <= i < 7000], "deleted documents returned"
        stale = [d["_id"] for d in got if 10_000 <= d["_id"] < 12_000 and d.get("v") != 1]
        assert not stale, f"{len(stale)} documents returned in their version before P2's acknowledged update"
        want = set(range(n)) - set(range(5000, 7000)) | set(range(100_000, 100_500))
        assert set(ids) == want, f"{len(want - set(ids))} missing, {len(set(ids) - want)} extra"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff)


def test_a_long_aggregate_is_refused_when_the_lease_is_lost():
    """Lease ttl 3 s, 100,000 documents, every GET of the database slowed by 60 ms (as test_55); the process-lease renewal
    held back 4.5 s after 1 s. A $group and a count over everything must be refused (134), never answered."""
    ff = faults()
    srv = Server(make_bucket("r58lost"), "r58lost", idle=3, holder="L58", extra=["--lease-ttl-secs", "3"],
                 env={"MONGO_TEST_FAULTS_FILE": ff, "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=300000, retryReads=False)
        n = 100_000
        for s in range(0, n, 5000):
            c.d.t.insert_many([{"_id": i, "k": i % 7, "pad": "x" * 200} for i in range(s, s + 5000)])
        outs = []
        for what in ("group", "count"):
            deadline = time.time() + 60
            while time.time() < deadline and srv.log_text().count("LEASE writer_close tenant=d ") < (1 if what == "group" else 2):
                time.sleep(0.5)
            time.sleep(2)
            with open(ff, "w") as fh:
                fh.write("delay_get tenants/d/ 60\n")
            c.d.t.find_one({"_id": 0})

            def lapse():
                time.sleep(1.0)
                with open(ff, "a") as fh:
                    fh.write("delay_put processes/L58/_lease 4500\n")
            th = threading.Thread(target=lapse)
            th.start()
            t0 = time.time()
            try:
                if what == "group":
                    v = list(c.d.t.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}]))
                    out = f"answered {sum(x['n'] for x in v)}"
                else:
                    out = f"answered {c.d.t.count_documents({})}"
            except PyMongoError as e:
                out = f"refused after {time.time() - t0:.1f} s: {getattr(e, 'code', None)} {str(e)[:90]}"
            th.join()
            with open(ff, "w") as fh:
                fh.write("")
            outs.append(f"{what}: {out}")
        print("; ".join(outs) + f"; LEASE lost lines {srv.log_text().count('LEASE lost')}")
        for o in outs:
            assert "refused" in o and " 134 " in o, o
    finally:
        srv.term()
        os.unlink(ff)


def test_drop_in_the_middle_of_a_cursor():
    srv, c, m = both(lambda db: db.t.insert_many([{"_id": i} for i in range(5000)]))
    try:
        res = {}
        for label, cl in (("ours", c), ("mongod", m)):
            db = cl["r58"]
            r = db.command(SON([("find", "t"), ("batchSize", 100)]))
            cid = r["cursor"]["id"]
            db.t.drop()
            db.t.insert_many([{"_id": -i} for i in range(1, 51)])
            try:
                got, n = [], 0
                while cid:
                    g = db.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 1000)]))
                    got += g["cursor"]["nextBatch"]
                    cid = g["cursor"]["id"]
                res[label] = f"answered {len(got)} more documents ({sum(1 for d in got if d['_id'] < 0)} of the new collection)"
            except OperationFailure as e:
                res[label] = f"error {e.code} {e.details.get('codeName') if e.details else ''}"
        print(f"getMore after a drop and re-create: ours {res['ours']}; mongod {res['mongod']}")
    finally:
        srv.term()


def test_first_batch_size_cap():
    n = 300_000
    srv = Server(make_bucket("r58big"), "r58big", idle=600).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=300000)
        for s in range(0, n, 10_000):
            c.d.t.insert_many([{"_id": i, "pad": "x" * 200} for i in range(s, s + 10_000)])
        try:
            r = c.d.command(SON([("find", "t"), ("batchSize", n)]))
            out = f"firstBatch {len(r['cursor']['firstBatch'])} documents"
        except PyMongoError as e:
            out = f"error: {str(e)[:150]}"
        print(f"find batchSize {n} over {n} documents of about 230 bytes (about 69 MB): {out} (mongod: firstBatch capped at 16 MiB)")
    finally:
        srv.term()


def test_snapshot_read_concern_across_getmores():
    """A find with readConcern snapshot (outside a transaction) reads one point in time on mongod: documents deleted
    between its getMores are still returned. The server runs as replica set rs0 (as in the judge)."""
    srv = Server(make_bucket("r58snap"), "r58snap", idle=600)
    srv.extra += ["--replica-set", "rs0", "--advertise", f"localhost:{srv.port}"]
    srv.start()
    m = real()
    m.drop_database("r58")
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
        res = {}
        for label, cl in (("ours", c), ("mongod", m)):
            db = cl["r58"]
            db.t.insert_many([{"_id": i} for i in range(5000)])
            r = db.command(SON([("find", "t"), ("batchSize", 100), ("readConcern", {"level": "snapshot"})]))
            got, cid = [d["_id"] for d in r["cursor"]["firstBatch"]], r["cursor"]["id"]
            db.t.delete_many({"_id": {"$gte": 1000}})
            db.t.insert_many([{"_id": 10_000 + i} for i in range(100)])
            while cid:
                g = db.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 1000)]))
                got += [d["_id"] for d in g["cursor"]["nextBatch"]]
                cid = g["cursor"]["id"]
            res[label] = got
        print(f"snapshot find, 4,000 of 5,000 deleted and 100 inserted after the first batch: ours {len(res['ours'])}, mongod {len(res['mongod'])}")
        assert sorted(res["ours"]) == sorted(res["mongod"]), f"ours {len(res['ours'])}, mongod {len(res['mongod'])}"
    finally:
        srv.term()
