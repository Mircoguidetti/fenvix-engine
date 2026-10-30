"""Review round 2, defects 2 and 3 (dropDatabase).

2. dropDatabase (wire) forgets everything erase forgets: a database created again with the same name inherits neither
   retention, nor relaxed durability, nor an IP allow list. Before the fix the drop forgot only the lease, the WAL state
   and the counters: the new database kept the old retention window and relaxed durability (in memory and in the stored
   markers) and the old allow list.
3. dropDatabase of a fork served by another process: P1 serves the fork f, P2 receives dropDatabase("f"). The drop must fail
   before it touches anything: the pin on the source stays, and a cleanup of the source does not delete files f reads.
   Before the fix P2 released the source's pin first and only then failed on the lease.
"""
import time

from pymongo import MongoClient, WriteConcern
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, list_prefix


def state(srv, db):
    st, d = srv.admin.get(f"/v1/databases/{db}/durability")
    st2, r = srv.admin.get(f"/v1/databases/{db}/restore-points")
    st3, a = srv.admin.get(f"/v1/databases/{db}/ip-allowlist")
    return {"relaxed_flush_ms": d.get("relaxed_flush_ms") if st == 200 else f"http {st}",
            "retention": r.get("retention") if st2 == 200 else f"http {st2}",
            "allowlist": a.get("entries") if st3 == 200 else f"http {st3}"}


def test_drop_then_recreate_inherits_no_settings():
    bucket = make_bucket("dropset")
    srv = Server(bucket, "dropset", idle=600, holder="DS").start()
    try:
        c = srv.client()
        c["r"]["c"].insert_one({"_id": 0})
        assert srv.admin.put("/v1/databases/r/retention", {"window_secs": 3600})[0] == 200
        assert srv.admin.put("/v1/databases/r/durability", {"relaxed_flush_ms": 2000})[0] == 200
        st, r = srv.admin.put("/v1/databases/r/ip-allowlist", {"entries": ["127.0.0.1/32"]})
        assert st == 200, r
        print("before the drop:", state(srv, "r"))
        c.drop_database("r")
        c["r"]["c"].insert_one({"_id": 1})                   # the same name again: a new database
        after = state(srv, "r")
        coll = c["r"].get_collection("c", write_concern=WriteConcern(w=1, j=False))
        before = c.admin.command("tenantStats")["groupCommit"]["relaxedAcks"]
        for i in range(2, 22):
            coll.insert_one({"_id": i})
        relaxed = c.admin.command("tenantStats")["groupCommit"]["relaxedAcks"] - before
        markers = [k for k, _ in list_prefix(bucket, "control/retention/r")] + [k for k, _ in list_prefix(bucket, "control/durability/r")]
        print("after drop and recreate:", after, "| relaxed acknowledgements:", relaxed, "| stored markers left:", markers)
        assert not after["relaxed_flush_ms"] and relaxed == 0, "the recreated database inherited relaxed durability"
        assert not (after["retention"] or {}).get("window_secs"), "the recreated database inherited the retention window"
        assert not after["allowlist"], "the recreated database inherited the IP allow list"
        assert not markers, f"markers of the dropped database left: {markers}"
    finally:
        srv.term()


def _fork_setup(tag, p2_env=None, ttl=None):
    from lib import fill
    bucket = make_bucket(tag)
    extra = ["--lease-ttl-secs", str(ttl)] if ttl else []
    p1 = Server(bucket, f"{tag}1", idle=900, holder=f"{tag.upper()}1", extra=extra).start()
    p2 = Server(bucket, f"{tag}2", idle=900, holder=f"{tag.upper()}2", extra=extra, env=p2_env or {}).start()
    c1 = p1.client()
    fill(c1, "src", "c", 1500)
    c1["src"].command("tenantAdmin", close=True)
    assert p1.admin.post("/v1/databases/src/fork", {"target": "f"})[0] == 201
    return bucket, p1, p2, c1


def _checkpoints(p1):
    return p1.admin.get("/v1/databases/src/checkpoints")[1]["checkpoints"]


def _pins(p1):
    return [x for x in _checkpoints(p1) if x["expires_at_ms"] is None]


def _drop(c2):
    try:
        c2.drop_database("f")
        return "succeeded"
    except OperationFailure as e:
        return f"refused ({e.code})"


def _source_cleanup_then_read_fork(bucket, p1, c1):
    """Compact and collect the source (after the clone's ephemeral 5-minute checkpoint expired), then read the fork cold."""
    from lib import fill, wait_until
    now = lambda: time.time() * 1000
    wait_until(lambda: all(x["expires_at_ms"] is None or x["expires_at_ms"] < now() for x in _checkpoints(p1)), 400, every=5, what="expiry of the clone's ephemeral checkpoint")
    base = c1.admin.command("tenantStats")["maintenance"]
    for s in range(7):
        fill(c1, "src", "c", 200, start=10000 + s * 200)
        c1["src"].command("tenantAdmin", close=True)
        time.sleep(0.3)
    wait_until(lambda: c1.admin.command("tenantStats")["maintenance"]["compactRuns"] > base["compactRuns"], 120, what="compaction of the source")
    after_compaction = {k for k, _ in list_prefix(bucket, "tenants/src/")}
    wait_until(lambda: after_compaction - {k for k, _ in list_prefix(bucket, "tenants/src/")}, 200, every=2, what="garbage collection of the source")
    time.sleep(2)
    try:
        c1["f"].command("tenantAdmin", close=True)
        ids = sorted(d["_id"] for d in c1["f"]["c"].find({}, {"_id": 1}))
        return len(ids), ids == list(range(1500))
    except OperationFailure as e:
        return f"error: {str(e)[:120]}", False


def test_drop_of_a_fork_served_elsewhere_is_refused_at_once():
    """P1 serves the fork f; P2 receives dropDatabase("f"). The command reads f's collections first, which takes f's
    lease, so P2 is refused before the drop starts, on the code before and after the fix alike."""
    bucket, p1, p2, c1 = _fork_setup("dropfa")
    try:
        assert c1["f"]["c"].count_documents({}) == 1500          # P1 serves the fork (it holds its lease)
        assert len(_pins(p1)) == 1
        dropped = _drop(MongoClient(p2.uri, serverSelectionTimeoutMS=5000, retryWrites=False))
        left = len(_pins(p1))
        print(f"dropDatabase('f') on P2: {dropped}; pins on the source after it: {left} (before: 1)")
        assert dropped.startswith("refused") and left == 1
    finally:
        p1.term()
        p2.term()


def test_drop_of_a_fork_whose_ownership_is_lost_during_the_drop_touches_nothing():
    """P2 serves the fork f and receives dropDatabase("f"). During the drop P2 loses f: the reads of the drop are slow
    (test-only delay on f's manifest GET) and P2's process lease renewals fail, so P1 takes f over and serves it. When the
    drop reaches its first change, P2 no longer owns f: it must stop there. The pin on the source must stay and a cleanup of
    the source must not delete files f reads. Before the fix the drop released the source's pin (and the fork markers)
    before it checked ownership, after its slow reads."""
    import tempfile, threading, os
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket, p1, p2, c1 = _fork_setup("dropfb", p2_env={"MONGO_TEST_FAULTS_FILE": ff.name}, ttl=4)
    try:
        c1["f"].command("tenantAdmin", close=True)                # P1 closes f: its lease goes, P2 can take it
        c2 = MongoClient(p2.uri, serverSelectionTimeoutMS=5000, retryWrites=False, retryReads=False)
        t0 = time.time()
        while True:
            try:
                assert c2["f"]["c"].count_documents({}) == 1500   # P2 serves the fork
                break
            except OperationFailure:
                assert time.time() - t0 < 30, "P2 never took the fork"
                time.sleep(0.5)
        assert len(_pins(p1)) == 1
        with open(ff.name, "w") as f:
            f.write("delay_put processes/DROPFB2/_lease 15000\ndelay_get tenants/f/manifest 12000\n")
        out = {}
        th = threading.Thread(target=lambda: out.update(r=_drop(c2)))
        th.start()
        took, t0 = None, time.time()
        while took is None and time.time() - t0 < 30:
            try:
                assert c1["f"]["c"].count_documents({}) == 1500
                took = round(time.time() - t0, 1)
            except OperationFailure:
                time.sleep(0.3)
        th.join()
        with open(ff.name, "w") as f:
            f.write("")
        left = len(_pins(p1))
        print(f"P1 took f over {took} s after the drop started; dropDatabase('f') on P2: {out.get('r')}; pins on the source after it: {left} (before: 1)")
        assert took is not None, "P1 never took the fork over (the test needs it)"
        n, ok = _source_cleanup_then_read_fork(bucket, p1, c1)
        print(f"after the source's compaction and cleanup the fork reads {n} of 1500 documents, ids intact: {ok}")
        assert left == 1, "the drop released the source's pin after P2 lost the fork"
        assert n == 1500 and ok, "the source's cleanup deleted files the fork still reads"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff.name)
