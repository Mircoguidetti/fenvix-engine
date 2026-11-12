"""Long reads under lease churn, a kill between batches, and a takeover in the middle of a cursor (fix-long-commands).

- test_scan_count_aggregate_under_lease_churn: T57_DOCS documents (default 200,000; the report also ran 1,000,000), lease
  ttl 3 s, and the process-lease renewals delayed past the lease now and then, so the lease lapses and comes back during
  the reads. A full find drained with getMore (a refused getMore is sent again on the same cursor), a count and an
  aggregate ($group) (a refused command is sent again). Fails when a document is missing or returned twice, or a count
  or a sum is wrong.
- test_kill_between_batches: a cursor's first batch, kill -9, restart: the cursor is gone (CursorNotFound), and a new
  find returns every document once.
- test_takeover_in_the_middle_of_a_cursor: a cursor on P1, P1 loses its lease, P2 takes the database over and deletes and
  inserts documents (acknowledged); P1's next getMore must not answer from the old state: refused, or answered with P2's
  writes visible (never a deleted document, never a duplicate).
"""
import os
import tempfile
import threading
import time

from bson import SON
from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure

from lib import Server, make_bucket

N = int(os.environ.get("T57_DOCS", "200000"))


def faults():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def load(c, n, coll="t"):
    for s in range(0, n, 10_000):
        c.d[coll].insert_many([{"_id": i, "k": i % 7, "pad": "x" * 100} for i in range(s, min(n, s + 10_000))])


def retry(fn, what, tries=60):
    last = None
    for _ in range(tries):
        try:
            return fn()
        except OperationFailure as e:
            if e.code != 134 and "retry" not in str(e):
                raise
            last = e
            time.sleep(0.5)
    raise AssertionError(f"{what}: refused {tries} times: {last}")


def test_scan_count_aggregate_under_lease_churn():
    ff = faults()
    srv = Server(make_bucket("churn"), "churn", idle=600, holder="C1", extra=["--lease-ttl-secs", "3"], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    stop = threading.Event()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=600000, retryReads=False)
        load(c, N)

        def churn():
            while not stop.is_set():
                time.sleep(4)
                with open(ff, "w") as fh:
                    fh.write("delay_put processes/C1/_lease 4500\n")
                time.sleep(3)
                with open(ff, "w") as fh:
                    fh.write("")
        th = threading.Thread(target=churn)
        th.start()
        want = {k: (len(range(k, N, 7)), sum(range(k, N, 7))) for k in range(7)}
        t_end, rounds, refused = time.time() + float(os.environ.get("T57_SECS", "40")), 0, [0]
        while time.time() < t_end or rounds == 0:
            rounds += 1
            t0 = time.time()
            r = retry(lambda: c.d.command(SON([("find", "t"), ("batchSize", 5000)])), "find")
            seen = [d["_id"] for d in r["cursor"]["firstBatch"]]
            cid, getmores = r["cursor"]["id"], 0
            while cid:
                def gm():
                    try:
                        return c.d.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 5000)]))
                    except OperationFailure:
                        refused[0] += 1
                        raise
                g = retry(gm, "getMore")
                getmores += 1
                seen += [d["_id"] for d in g["cursor"]["nextBatch"]]
                cid = g["cursor"]["id"]
            scan_s = time.time() - t0
            assert len(seen) == N and len(set(seen)) == N, f"round {rounds} scan: {len(seen)} returned, {len(set(seen))} distinct, of {N}"
            t1 = time.time()
            n = retry(lambda: c.d.t.count_documents({}), "count")
            count_s = time.time() - t1
            assert n == N, f"round {rounds} count {n}"
            t2 = time.time()
            groups = retry(lambda: {g["_id"]: (g["n"], g["s"]) for g in c.d.t.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}, "s": {"$sum": "$_id"}}}])}, "aggregate")
            agg_s = time.time() - t2
            assert groups == want, f"round {rounds}: aggregate sums differ"
        stop.set()
        th.join()
        losses = srv.log_text().count("LEASE lost") + srv.log_text().count("LEASE invalid")
        print(f"{N} documents, {rounds} rounds of scan + count + aggregate (last: scan {scan_s:.1f} s, {getmores} getMores; count {count_s:.1f} s; aggregate {agg_s:.1f} s); {refused[0]} getMores refused and sent again; lease lost or invalid {losses} times")
        assert losses > 0, "the lease never lapsed: the test did not reach the case"
    finally:
        stop.set()
        srv.term()
        os.unlink(ff)


def test_kill_between_batches():
    srv = Server(make_bucket("killb"), "killb", idle=600).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, retryReads=False)
        load(c, 20_000)
        r = c.d.command(SON([("find", "t"), ("batchSize", 1000)]))
        cid = r["cursor"]["id"]
        assert cid
        srv.kill9()
        srv.start()
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, retryReads=False)
        try:
            c.d.command(SON([("getMore", cid), ("collection", "t")]))
            raise AssertionError("the cursor survived a kill -9")
        except OperationFailure as e:
            print(f"getMore after the restart: {e.code} {str(e)[:80]}")
            assert e.code == 43, e
        ids = retry(lambda: [d["_id"] for d in c.d.t.find({}, {"_id": 1}).batch_size(1000)], "find after restart")
        assert sorted(ids) == list(range(20_000)) and len(ids) == 20_000
    finally:
        srv.term()


def test_takeover_in_the_middle_of_a_cursor():
    bucket = make_bucket("tkcur")
    ff = faults()
    p1 = Server(bucket, "tkcur1", idle=600, holder="P1", extra=["--lease-ttl-secs", "3"], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    p2 = Server(bucket, "tkcur2", idle=600, holder="P2", extra=["--lease-ttl-secs", "3"]).start()
    try:
        c1 = MongoClient(p1.uri, serverSelectionTimeoutMS=5000, retryReads=False, retryWrites=False)
        c2 = MongoClient(p2.uri, serverSelectionTimeoutMS=5000, retryReads=False, retryWrites=False)
        load(c1, 5_000)
        r = c1.d.command(SON([("find", "t"), ("batchSize", 1000)]))
        seen, cid = [d["_id"] for d in r["cursor"]["firstBatch"]], r["cursor"]["id"]
        # P1 loses its lease; P2 takes the database over and writes
        with open(ff, "w") as fh:
            fh.write("delay_put processes/P1/_lease 4500\n")
        deadline = time.time() + 40
        while True:
            try:
                c2.d.t.delete_many({"_id": {"$gte": 3000, "$lt": 3500}})
                c2.d.t.insert_many([{"_id": 10_000 + i, "k": 0} for i in range(100)])
                break
            except PyMongoError:
                assert time.time() < deadline, "P2 never took the database over"
                time.sleep(0.3)
        with open(ff, "w") as fh:
            fh.write("")
        outcomes = []
        while cid:
            try:
                g = c1.d.command(SON([("getMore", cid), ("collection", "t"), ("batchSize", 1000)]))
            except OperationFailure as e:
                outcomes.append(f"refused {e.code}")
                break
            seen += [d["_id"] for d in g["cursor"]["nextBatch"]]
            cid = g["cursor"]["id"]
        print(f"after the takeover: {outcomes or 'answered'}; {len(seen)} ids seen")
        stale = [i for i in seen if 3000 <= i < 3500 and seen.index(i) >= 1000]
        assert len(seen) == len(set(seen)), "a document came twice"
        assert not stale, f"P1 answered from the state before P2's acknowledged deletes: {len(stale)} deleted documents returned"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff)
