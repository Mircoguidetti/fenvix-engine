"""Review of PR #34 (C8 fix), the cost of the two new mechanisms, measured:

1. a repeated commit on a process that does not remember the session lists the session's hints and then OPENS every
   database named there (one `dbs.lock(db)` each), taking its lease: what does that cost when a session committed in
   many databases;
2. the cleanup of the expired session records travels inside the batch of a write (`session_sweep_ops`, up to
   SWEEP_SCAN = 10,000 records per pass, every TTL/3 per database): what does the unlucky write pay.
"""
import os
import time
import uuid

from bson import Binary, Int64
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket
import test_41_commit_retry as T

N_DBS = int(os.environ.get("RV_DBS", "8"))


def _srv(tag, bucket=None, holder=None, env=None, extra=()):
    port = free_port()
    kw = {"holder": holder} if holder else {}
    return Server(bucket or make_bucket(tag), tag, port=port, env=env,
                  extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"] + list(extra), **kw).start()


def _stats(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=20000).admin.command("tenantStats")


def _res(f):
    try:
        r = f()
        return r.get("ok") if isinstance(r, dict) else r
    except OperationFailure as e:
        return e.code
    except (OSError, TypeError) as e:
        return f"no reply ({type(e).__name__})"


def test_repeated_commit_on_another_process_opens_every_database_of_the_session():
    bucket = make_bucket("rv41c")
    P1 = _srv("rv41c1", bucket=bucket, holder="p1")
    P2 = _srv("rv41c2", bucket=bucket, holder="p2")
    try:
        r1 = T.Raw(P1)
        lsid = T._lsid()
        for n in range(1, N_DBS + 1):
            db = f"d{n}"
            r1[db].command({"insert": "c", "documents": [{"_id": n}], "lsid": lsid, "txnNumber": Int64(n), "startTransaction": True, "autocommit": False})
            assert T._commit(r1, lsid, n)["ok"] == 1.0
        sess = lsid["id"].hex()
        hints = [k for k, _ in list_prefix(bucket, f"txn-hints/{sess}/")]
        assert len(hints) == N_DBS, hints
        # P1 goes away cleanly: its databases are free, so P2 can take them
        r1.close()
        P1.term()
        time.sleep(1)
        before = _stats(P2)
        t0 = time.time()
        r2 = T.Raw(P2)
        got = _res(lambda: T._commit(r2, lsid, N_DBS))
        took = time.time() - t0
        after = _stats(P2)
        d = lambda k: after["requests"][k] - before["requests"][k]
        out = {"answer": got, "seconds": round(took, 2), "hints": len(hints),
               "entries": [before["entries"], after["entries"]], "leasesHeld": [before["leasesHeld"], after["leasesHeld"]],
               "list": d("list"), "get": d("get"), "head": d("head"), "put": d("put")}
        print("repeated commit on another process:", out)
        assert got == 1.0, out
        # the answer is right; the cost is what this test reports (no threshold asserted here)
    finally:
        P1.kill9()
        P2.kill9()


def _fill_sessions(srv, n, db="ct"):
    """n session records in `db`, one retryable write each (a distinct lsid)."""
    cl = MongoClient(srv.uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=120000)
    d = cl[db]
    for i in range(n):
        lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
        d.command({"insert": "c", "documents": [{"_id": f"s{i}"}], "lsid": lsid, "txnNumber": Int64(1)})
    return cl


def _write_latencies(cl, db, tag, secs):
    out = []
    t0 = time.time()
    i = 0
    while time.time() - t0 < secs:
        i += 1
        s = time.perf_counter()
        cl[db].command({"insert": "c", "documents": [{"_id": f"{tag}{i}"}]})
        out.append(time.perf_counter() - s)
    out.sort()
    return out


def test_session_record_cleanup_inside_a_write_batch():
    """Session TTL 4 s (test flag) so the cleanup is due every ~1.3 s with thousands of expired records; compared with a
    server with the normal TTL (no pass due), same work. Reported: p50, p99 and the worst write of each."""
    n = int(os.environ.get("RV_SESSIONS", "3000"))
    res = {}
    for tag, env in (("sweeping", {"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_SESSION_TTL_MS": "4000"}), ("quiet", None)):
        srv = _srv(f"rv41s{tag[:4]}", env=env)
        try:
            cl = _fill_sessions(srv, n)
            time.sleep(5)                                     # the records of `sweeping` are expired now
            lat = _write_latencies(cl, "ct", tag, 20)
            pct = lambda x: round(lat[min(len(lat) - 1, int(len(lat) * x))] * 1000, 1)
            st = _stats(srv)
            res[tag] = {"writes": len(lat), "p50_ms": pct(0.5), "p99_ms": pct(0.99), "max_ms": round(lat[-1] * 1000, 1),
                        "put": st["requests"]["put"], "get": st["requests"]["get"]}
        finally:
            srv.kill9()
    print(f"session cleanup with {n} expired records:", res)
