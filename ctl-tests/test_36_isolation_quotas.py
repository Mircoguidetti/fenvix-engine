"""Isolation between databases (fair.rs, S4): the unit of cost per operation, quotas per database, the JavaScript time
limit per command.

 1. every command is charged in points, by part: an insert charges bytes written and (in the background) S3 PUTs, a read
    charges bytes read; CPU is charged to both; the server reports the weights of the unit;
 2. a database's own quota (PUT /v1/databases/{db}/quota) makes its commands wait over the quota (quotaWaits grows),
    and beyond --db-quota-queue waiting commands a new one gets error 462 with the RetryableError label; another database
    without a quota is not slowed; GET and DELETE of the quota;
 3. --js-command-max-ms limits the total JavaScript time of one command across its calls (error 50 MaxTimeMSExpired),
    while each call alone stays far below internalQueryJavaScriptFnTimeoutMillis;
 4. --unit-weights with a bad value stops the server at start.
"""
import subprocess
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import BIN, Server, make_bucket


def client(srv, pool=64):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, retryWrites=False, retryReads=False, maxPoolSize=pool)


def dbm(srv, db):
    st, m = srv.admin.get("/v1/metrics/databases")
    assert st == 200
    return m["databases"].get(db, {}), m["server"]


def test_points_by_part():
    srv = Server(make_bucket("iso1"), "iso1", idle=600).start()
    try:
        c = client(srv)
        c.w.t.insert_one({"_id": 0})
        time.sleep(1)
        before, server = dbm(srv, "w")
        assert server["unitWeights"]["ptsPerS3Put"] > 0 and server["unitWeights"]["ptsPerCpuMs"] > 0
        for i in range(1, 51):
            c.w.t.insert_one({"_id": i, "pad": "x" * 1000})
        time.sleep(2)     # the WAL PUTs of the last inserts
        after, _ = dbm(srv, "w")
        assert after["bytesWritten"] - before["bytesWritten"] >= 50 * 1000
        assert after["s3Put"] > before["s3Put"], "inserts caused no S3 PUT charged to their database"
        assert after["pointsWritten"] > before["pointsWritten"] and after["pointsS3"] > before["pointsS3"]
        assert after["cpuUs"] > before["cpuUs"] and after["pointsCpu"] > before["pointsCpu"]
        parts = after["pointsCpu"] + after["pointsWritten"] + after["pointsRead"] + after["pointsS3"]
        assert abs(parts - after["points"]) < 1e-6 * max(1.0, after["points"]) + 1e-3, (parts, after["points"])
        r0 = after["bytesRead"]
        for i in range(1, 51):
            assert c.w.t.find_one({"_id": i})["_id"] == i
        after2, _ = dbm(srv, "w")
        assert after2["bytesRead"] - r0 >= 50 * 1000, "reads of 1 KB documents charged no bytes read"
        assert after2["pointsRead"] > after["pointsRead"]
    finally:
        srv.term()


def test_quota_waits_then_refuses_retryably():
    srv = Server(make_bucket("iso2"), "iso2", idle=600, extra=["--db-quota-queue", "4"]).start()
    try:
        c = client(srv)
        c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(200)])
        c.free.t.insert_one({"_id": 1})
        st, q = srv.admin.call("PUT", "/v1/databases/q/quota", {"pts_per_sec": 2000, "burst_pts": 2000})
        assert st == 200 and q["pts_per_sec"] == 2000 and q["burst_pts"] == 2000 and q["own"] is True, q
        st, q = srv.admin.get("/v1/databases/q/quota")
        assert st == 200 and q["pts_per_sec"] == 2000
        st, _ = srv.admin.call("PUT", "/v1/databases/q/quota", {"burst_pts": 5})
        assert st == 400
        errs, ok = [], [0]
        lock = threading.Lock()

        def w():
            cc = client(srv)
            for i in range(20):
                try:
                    list(cc.q.t.find({}))     # 200 KB read: tens of points each (bytes read and CPU)
                    with lock:
                        ok[0] += 1
                except OperationFailure as e:
                    with lock:
                        errs.append(e)
        ts = [threading.Thread(target=w) for _ in range(16)]
        [t.start() for t in ts]
        # meanwhile a database without a quota answers at once
        time.sleep(0.5)
        t0 = time.time()
        assert c.free.t.find_one({"_id": 1}) == {"_id": 1}
        assert time.time() - t0 < 1.0
        [t.join() for t in ts]
        assert errs, "16 clients far over a quota of 2000 points/s with a queue of 4 were never refused"
        e = errs[0]
        assert e.code == 462, e.details
        assert "RetryableError" in (e.details.get("errorLabels") or []), e.details
        assert "quota" in str(e)
        m, _ = dbm(srv, "q")
        assert m["quotaWaits"] > 0 and m["quotaRejections"] == len(errs), m
        assert m["quotaPtsPerSec"] == 2000
        f, _ = dbm(srv, "free")
        assert f["quotaRejections"] == 0 and f["quotaWaits"] == 0
        # the quota goes: no more waits
        st, q = srv.admin.call("DELETE", "/v1/databases/q/quota")
        assert st == 200 and q["own"] is False and q["pts_per_sec"] == 0
        w0 = dbm(srv, "q")[0]["quotaWaits"]
        for _ in range(20):
            list(c.q.t.find({}))
        assert dbm(srv, "q")[0]["quotaWaits"] == w0
    finally:
        srv.term()


def test_js_time_per_command():
    srv = Server(make_bucket("iso3"), "iso3", idle=600, extra=["--js-command-max-ms", "300"]).start()
    try:
        c = client(srv)
        c.j.t.insert_many([{"_id": i} for i in range(20)])
        # 20 documents x 50 ms of JavaScript each: 1 s in total, far below the 60 s per call
        try:
            list(c.j.t.find({"$where": "sleep(50) || true"}))
            assert False, "1 s of JavaScript passed a limit of 300 ms per command"
        except OperationFailure as e:
            assert e.code == 50 and "--js-command-max-ms" in str(e), e.details
        # a command within the limit still works, and the next command starts with a full budget
        assert len(list(c.j.t.find({"$where": "this._id < 3"}))) == 3
    finally:
        srv.term()


def test_bad_unit_weights_stop_the_server():
    p = subprocess.run([BIN, "--listen", "127.0.0.1:0", "--store", "memory", "--unit-weights", "1,2,3"], capture_output=True, timeout=30)
    assert p.returncode != 0
    assert b"unit-weights" in p.stderr + p.stdout
