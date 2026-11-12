"""c2-fix-hangs: two hangs found by integration-3's full ctl run, one test each. Both servers run with sealed runs on
(their own environment), whatever mode the judge runs in.

 1. test_compactor_upkeep_does_not_starve_the_quota (ctl test_36 hung): with sealed runs and the compactor on, a
    database's compactor keeps a SlateDB compactor and worker polling the store (every 20 ms and 10 ms), runs its GC, and
    makes the writer read SlateDB's manifest every 2 s: the server counted about 370 GETs a second on an idle database
    (MEASURED; each request is counted twice, so about 185 real ones), 74 000 points against a quota of 2 000. All of it drew the database's quota, so the bucket never refilled and every command
    of the database waited forever. Now that upkeep is counted in the database's points but does not draw its quota.
    Fails when, after 3 s idle under a quota, the bucket is not positive or five small reads do not all answer within
    their socket timeout; also checks that the upkeep is still counted (points grow while idle).
 2. test_publish_cut_is_bounded_when_wal_puts_hang (ctl test_31 hung): the server's publish of our manifest runs inside a
    write, under the database's exclusive lock, and starts with a WAL flush (the cut). With the store not answering the
    WAL PUTs, SlateDB retries them without end and the flush never ended: the write, and every later command on the
    database, waited forever, with or without the compactor. Now the cut's flush is bounded by the F6 PUT timeout
    (--sealed-put-timeout-ms) and the publish fails before writing anything. Fails when a relaxed write (acknowledged from
    memory, as without sealed runs) waits more than the bound plus a margin while WAL PUTs hang; also checks that a
    durable write is NOT acknowledged while its WAL PUT hangs, and is there after the store answers and a restart.
"""
import os
import sys
import time

from pymongo import MongoClient, WriteConcern
from pymongo.errors import AutoReconnect, NetworkTimeout

from lib import Server, make_bucket, MINIO

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402

SEALED = {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
          "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}


def test_compactor_upkeep_does_not_starve_the_quota():
    env = dict(SEALED, FENVIX_SEALED_COMPACTOR="small")
    srv = Server(make_bucket("c2q"), "c2q", idle=600, extra=["--db-quota-queue", "4"], env=env).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=15000, retryReads=False, retryWrites=False)
        c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(200)])
        t0 = time.time()
        while c.admin.command("serverStatus")["fenvix"]["compactor"]["running"] < 1:
            assert time.time() - t0 < 30, "the compactor of the database did not start"
            time.sleep(0.2)
        assert len(list(c.q.t.find({}))) == 200      # the first, cold read of the runs, before the quota
        st, q = srv.admin.call("PUT", "/v1/databases/q/quota", {"pts_per_sec": 2000, "burst_pts": 2000})
        assert st == 200, q
        p0 = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]["points"]
        time.sleep(3)
        p1 = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]["points"]
        bal = srv.admin.get("/v1/databases/q/quota")[1]["balance_pts"]
        print(f"idle 3 s under the quota: {p1 - p0:.0f} points counted, balance {bal:.0f}")
        assert p1 > p0, "the compactor's upkeep is no longer counted in the database's points"
        assert bal > 0, f"the idle database's quota bucket is empty ({bal:.0f}): the compactor's upkeep drew it"
        t0 = time.time()
        for _ in range(5):
            assert len(list(c.q.t.find({}))) == 200
        print(f"5 reads under the quota in {time.time() - t0:.2f} s")
    finally:
        srv.kill9()


def test_publish_cut_is_bounded_when_wal_puts_hang():
    bound_s = 3
    env = dict(SEALED, FENVIX_SEALED_COMPACTOR="0")     # the hang needed no compactor
    host, port = MINIO.split("//")[1].split(":")
    px = PathProxy((host, int(port)))
    bucket = make_bucket("c2cut")
    db = "cut"
    srv = Server(bucket, "c2cut", idle=600, endpoint=px.endpoint, env=env,
                 extra=["--sealed-put-timeout-ms", str(bound_s * 1000)]).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=8 * bound_s * 1000, retryWrites=False)
        c[db].t.insert_one({"_id": "first"})
        st, r = srv.admin.put(f"/v1/databases/{db}/durability", {"relaxed_flush_ms": 10000})
        assert st == 200, r
        relaxed = c[db].get_collection("t", write_concern=WriteConcern(w=1, j=False))
        relaxed.insert_one({"_id": "before"})
        px.hold("PUT", f"/tenants/{db}/wal/")
        # with --sealed-runs 2 a publish is due every second write batch: four writes reach two publishes
        for i in range(4):
            t0 = time.time()
            relaxed.insert_one({"_id": f"held-{i}"})
            took = time.time() - t0
            print(f"relaxed write {i} with WAL PUTs held: {took:.1f} s")
            assert took < bound_s + 5, f"a relaxed write waited {took:.1f} s with WAL PUTs held (bound {bound_s} s)"
        assert px.held > 0, "no WAL PUT was held: the test did not reach the case"
        # a durable write is acknowledged only once its WAL object is stored: not while the store holds it
        durable = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=2 * bound_s * 1000, retryWrites=False)
        acked = True
        try:
            durable[db].get_collection("t", write_concern=WriteConcern(w=1, j=True)).insert_one({"_id": "durable"})
        except (NetworkTimeout, AutoReconnect):
            acked = False
        assert not acked, "a durable write was acknowledged while its WAL PUT was held"
        durable.close()
        px.release()
        t0 = time.time()
        c[db].get_collection("t", write_concern=WriteConcern(w=1, j=True)).insert_one({"_id": "after"})
        print(f"durable write after the store answers: {time.time() - t0:.1f} s")
        st = c.admin.command("serverStatus")["fenvix"]
        print("sealed runs:", {k: v for k, v in st.items() if "publish" in k.lower()} or st.get("sealedRuns"))
        srv.term()
    finally:
        px.release()
        srv.kill9()
        px.close()
    srv2 = Server(bucket, "c2cut2", idle=600, env=env).start()
    try:
        ids = {d["_id"] for d in srv2.client()[db].t.find({}, {"_id": 1})}
        assert {"first", "after"} <= ids, ids
        print(f"after a restart: {len(ids)} documents")
    finally:
        srv2.term()


def test_each_request_is_charged_once():
    """Fix round 2: every S3 request of a database was charged twice to its points, because the server stacks counting
    stores (the lifecycle's on the process-wide one) and each reported the request. An independent counting proxy in
    front of MinIO is the ground truth: over a cold scan (sealed runs on, the first scan after a publish reads the runs
    from the store) and over a series of durable inserts, the database's GET-class and PUT-class counts must grow by what
    the proxy saw for its objects, multipart pieces apart (a request or two of background work in flight at a snapshot may fall on either side).
    Failed on 9fa25cd: 772 GETs counted for 386 at the proxy."""
    import proxy as counting_proxy
    env = dict(SEALED, FENVIX_SEALED_COMPACTOR="0")     # no compactor: no store polling during the windows
    px = counting_proxy.CountingProxy()
    srv = Server(make_bucket("c2once"), "c2once", idle=600, endpoint=px.endpoint, env=env).start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, retryWrites=False)
        c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(3000)])
        time.sleep(3)

        def server_counts():
            d = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]
            return d["s3Get"], d["s3Put"]

        def proxy_counts():
            n = px.snapshot()["by_prefix"].get("tenants/q", {})
            gets = sum(v for k, v in n.items() if k in ("GET", "GET_RANGE", "HEAD"))
            # multipart pieces (create, part, complete: the sealed runs a publish writes) are not charged to the points
            # at all, a separate gap (c2-fix-hangs report, fix round 2): compared apart
            puts = sum(v for k, v in n.items() if k in ("PUT", "COPY", "LIST"))
            multipart = sum(v for k, v in n.items() if k in ("PUT_PART", "MP_CREATE", "MP_COMPLETE"))
            return gets, puts, multipart

        g0, p0 = server_counts()
        px.reset()
        assert len(list(c.q.t.find({}))) == 3000
        for i in range(20):
            c.q.t.insert_one({"_id": f"w{i}"})            # durable: one WAL PUT each, or a few shared
        g1, p1 = server_counts()
        pg, pp, pm = proxy_counts()
        print(f"GET-class: server {g1 - g0}, proxy {pg}; PUT, COPY and LIST: server {p1 - p0}, proxy {pp}; multipart pieces at the proxy {pm}")
        assert pg >= 50, f"the scan made only {pg} GETs at the proxy: the test did not reach a cold read"
        assert pp >= 5, f"the inserts made only {pp} PUTs at the proxy"
        assert abs((g1 - g0) - pg) <= 3, f"GET-class charged {g1 - g0} times for {pg} requests at the proxy"
        assert abs((p1 - p0) - pp) <= 3, f"PUT-class charged {p1 - p0} times for {pp} requests at the proxy"
    finally:
        srv.kill9()
        px.stop()
