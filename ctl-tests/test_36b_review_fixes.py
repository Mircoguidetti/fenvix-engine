"""Corrections of the review of S4 (C7): three errors the reviewer proved, one test each.

 A. `getMore` passes through its database's quota and the fair queue like the other commands (it was exempt). The
    cursors that wait on purpose (tailable awaitData, change streams) stay exempt. With a quota, a cursor read in
    batches makes its getMores wait (quotaWaits grows during the getMores) and its commands are counted.
 B. An S3 request made while a command runs is charged to the database of the command: the reads of a branch made
    with fork "clone" read the parent's objects (tenants/{parent}/...) and are charged to the branch, not to the parent.
 D. The flush gate (`--fair-max-flushing`) holds a database's place only while its data is encoded, not while the
    WAL PUT waits for S3: with the gate engaged (forced by `--fair-lag-ms` tiny) and 1 place, four databases that write
    at once take as long as one alone (before, they waited behind each other's PUTs).
"""
import threading
import time

from pymongo import MongoClient

from lib import Server, make_bucket


def client(srv, pool=64):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, retryWrites=False, retryReads=False, maxPoolSize=pool)


def dbm(srv, db):
    st, m = srv.admin.get("/v1/metrics/databases")
    assert st == 200
    return m["databases"].get(db, {}), m["server"]


def test_A_getmore_pays_quota():
    srv = Server(make_bucket("c7a"), "c7a", idle=600).start()
    try:
        c = client(srv)
        c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(3000)])
        assert srv.admin.call("PUT", "/v1/databases/q/quota", {"pts_per_sec": 50, "burst_pts": 50})[0] == 200
        time.sleep(1.5)
        b0, _ = dbm(srv, "q")
        cur = c.q.t.find({}, batch_size=10)
        first = next(cur)     # the find: it reads every document once (the cursor keeps them in memory)
        b1, _ = dbm(srv, "q")
        t0 = time.time()
        n = 1 + sum(1 for _ in cur)
        dt = time.time() - t0
        b2, _ = dbm(srv, "q")
        assert n == 3000 and first["_id"] == 0
        getmores = b2["commands"] - b1["commands"]
        waits = b2["quotaWaits"] - b1["quotaWaits"]
        print(f"find: {b1['points'] - b0['points']:.0f} pt; {getmores} getMore: {b2['points'] - b1['points']:.0f} pt in {dt:.2f} s, quota waits {waits}")
        assert getmores >= 299, "the getMores are not counted as commands of their database"
        assert waits > 0, "no getMore waited for the quota of its database"
    finally:
        srv.term()


def test_A_awaitdata_getmore_stays_exempt():
    """A change stream's getMore waits on purpose: it does not take a quota place (nor wait for one)."""
    srv = Server(make_bucket("c7a2"), "c7a2", idle=600, extra=["--replica-set", "rs0"]).start()
    try:
        c = client(srv)
        c.q.t.insert_one({"_id": 0})
        assert srv.admin.call("PUT", "/v1/databases/q/quota", {"pts_per_sec": 1, "burst_pts": 1})[0] == 200
        with c.q.t.watch(max_await_time_ms=200) as cs:
            b0, _ = dbm(srv, "q")
            t0 = time.time()
            for _ in range(5):
                cs.try_next()
            dt = time.time() - t0
            b1, _ = dbm(srv, "q")
        print(f"5 change-stream getMores in {dt:.2f} s, quota waits {b1['quotaWaits'] - b0['quotaWaits']}")
        assert b1["quotaWaits"] == b0["quotaWaits"]
        assert dt < 5, "the change stream's getMores waited for the quota"
    finally:
        srv.term()


def test_B_clone_reads_charged_to_branch():
    bucket = make_bucket("c7b")
    srv = Server(bucket, "c7b", idle=600).start()
    try:
        c = client(srv)
        c.p.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(3000)])
        st, _ = srv.admin.post("/v1/databases/p/fork", {"target": "child"})
        assert st == 201
        srv.restart()   # cold cache: the branch's reads must go to S3
        c = client(srv)
        assert c.p.t.find_one({"_id": 1})
        assert c.child.t.find_one({"_id": 1})
        time.sleep(1)
        p0, _ = dbm(srv, "p")
        k0, _ = dbm(srv, "child")
        n = sum(1 for _ in c.child.t.find({}).sort("_id", -1))
        time.sleep(1)
        p1, _ = dbm(srv, "p")
        k1, _ = dbm(srv, "child")
        print(f"branch read {n} docs: parent GET {p0['s3Get']} -> {p1['s3Get']}, branch GET {k0['s3Get']} -> {k1['s3Get']}")
        assert n == 3000
        assert p1["s3Get"] == p0["s3Get"], "the branch's reads were charged to the parent"
        assert k1["s3Get"] > k0["s3Get"], "the branch's reads were charged to nobody"
    finally:
        srv.term()


DBS = ("a", "b", "c", "d")


def test_D_flush_place_not_held_during_put():
    """Every PUT is slowed by 300 ms (test-only `MONGO_TEST_PUT_DELAY_MS`); the gate is always engaged and has one flush
    place. One insert alone takes about 600 ms (it makes two PUTs, measured). Four inserts on four databases at once must
    take as long as one alone: the place is given back when the WAL PUT starts. With the place held during the PUT
    (before the correction) they waited for each other: about 100 ms more in most rounds (measured)."""
    env = {"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_PUT_DELAY_MS": "300"}
    srv = Server(make_bucket("c7d"), "c7d", idle=600, env=env, extra=["--fair-lag-ms", "0.001", "--fair-max-flushing", "1"]).start()
    try:
        c = client(srv)
        for d in DBS:
            c[d].t.insert_one({"_id": -1})
        time.sleep(1)
        st, m = srv.admin.get("/v1/metrics/databases")
        assert m["server"]["saturated"], "the gate did not engage (the test needs it)"
        single = []
        for i in range(5):
            t = time.time()
            c.a.t.insert_one({"_id": 100 + i})
            single.append(time.time() - t)
        pair = []
        for rnd in range(5):
            done = {}

            def w(d):
                t = time.time()
                c[d].t.insert_one({"_id": rnd, "x": 1})
                done[d] = time.time() - t

            th = [threading.Thread(target=w, args=(d,)) for d in DBS]
            for t in th:
                t.start()
            for t in th:
                t.join()
            pair.append(max(done.values()))
        single.sort()
        pair.sort()
        print("one insert alone (s):", [round(t, 3) for t in single], "slowest of four at once (s):", [round(t, 3) for t in pair])
        assert pair[2] < single[2] + 0.05, (single, pair)
    finally:
        srv.term()
