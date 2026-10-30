"""Safety limits per database and per-database metrics (fair.rs, round3).

 1. with the defaults (10 000 commands at once, 100 000 per second per database) a busy client is never refused;
 2. a tiny --db-max-rate shows what a client gets when a limit trips: error 462 IngressRequestRateLimitExceeded with a
    message naming the flag, the database's safetyRejections metric grows, and other databases are not affected;
 3. --db-max-inflight the same way;
 4. GET /v1/metrics/databases reports commands, rate, CPU time, queue waits and the server's scheduling lag.
"""
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, retryWrites=False, retryReads=False, maxPoolSize=64)


def hammer(srv, db, threads, n):
    errs, ok = [], [0]
    lock = threading.Lock()

    def w():
        c = client(srv)
        for i in range(n):
            try:
                c[db].t.find_one({"_id": i})
                with lock:
                    ok[0] += 1
            except OperationFailure as e:
                with lock:
                    errs.append(e)
    ts = [threading.Thread(target=w) for _ in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return ok[0], errs


def test_defaults_never_refuse_a_busy_client():
    srv = Server(make_bucket("fair1"), "fair1", idle=600).start()
    try:
        client(srv).d.t.insert_many([{"_id": i} for i in range(100)])
        ok, errs = hammer(srv, "d", 32, 200)
        assert not errs, errs[:3]
        assert ok == 32 * 200
    finally:
        srv.term()


def test_rate_limit_trips_with_a_clear_error_and_a_metric():
    srv = Server(make_bucket("fair2"), "fair2", idle=600, extra=["--db-max-rate", "200"]).start()
    try:
        c = client(srv)
        c.d.t.insert_many([{"_id": i} for i in range(100)])
        c.other.t.insert_one({"_id": 1})
        ok, errs = hammer(srv, "d", 32, 100)
        assert errs, "3200 commands in a few seconds never passed a limit of 200 per second"
        e = errs[0]
        assert e.code == 462 and "IngressRequestRateLimitExceeded" in str(e.details.get("codeName")), e.details
        assert "--db-max-rate" in str(e) and "runaway" in str(e)
        # another database is not affected
        assert client(srv).other.t.find_one({"_id": 1}) == {"_id": 1}
        st, m = srv.admin.get("/v1/metrics/databases")
        assert st == 200
        assert m["databases"]["d"]["safetyRejections"] == len(errs)
        assert m["databases"]["other"]["safetyRejections"] == 0
    finally:
        srv.term()


def test_inflight_limit_trips():
    srv = Server(make_bucket("fair3"), "fair3", idle=600, extra=["--db-max-inflight", "2"]).start()
    try:
        c = client(srv)
        c.d.t.insert_many([{"_id": i, "pad": "x" * 2000} for i in range(3000)])
        errs = []

        def scan():
            try:
                list(client(srv).d.t.find({"$where": "sleep(200) || true"}).limit(1))
            except OperationFailure as e:
                errs.append(e)
        ts = [threading.Thread(target=scan) for _ in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        assert any(e.code == 462 and "--db-max-inflight" in str(e) for e in errs), [str(e)[:120] for e in errs]
    finally:
        srv.term()


def test_metrics_endpoint():
    srv = Server(make_bucket("fair4"), "fair4", idle=600).start()
    try:
        c = client(srv)
        for i in range(50):
            c.m.t.insert_one({"_id": i})
        st, m = srv.admin.get("/v1/metrics/databases")
        assert st == 200
        d = m["databases"]["m"]
        for k in ("commands", "commandsPerSec10s", "commandsPerSec60s", "cpuMs", "queueWaits", "queueWaitMs", "safetyRejections", "bytesWrittenSinceOpen", "state"):
            assert k in d, k
        assert d["commands"] >= 50 and d["commandsPerSec10s"] > 0 and d["state"] == "writer"
        for k in ("schedulingLagMs", "saturated", "queuedCommands", "dbMaxInflight", "dbMaxRate", "fairLagThresholdMs"):
            assert k in m["server"], k
        assert m["server"]["dbMaxRate"] == 100000 and m["server"]["dbMaxInflight"] == 10000
        # without the token: refused
        import urllib.request, urllib.error
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{srv.admin_port}/v1/metrics/databases", timeout=5)
            assert False, "metrics served without the admin token"
        except urllib.error.HTTPError as e:
            assert e.code == 401
    finally:
        srv.term()
