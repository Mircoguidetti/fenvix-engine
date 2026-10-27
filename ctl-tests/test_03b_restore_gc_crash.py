"""Operation 3, harder cases: restore after compaction and garbage collection (and the negative control without WAL
retention), kill -9 during a restore, and concurrent operations on the same database."""
import random
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until, fill, dump_db
from test_03_restore import GAP, listing, now_ms, restore

EXTRA = ["--retention-interval-secs", "100000", "--retention-prune-secs", "3"]


def gc_scenario(srv, bucket, db):
    """Enable retention; one write session with three states (only the first point exists, so the later states are
    reached through the WAL); then more sessions, compaction and a GC pass. Returns the recorded states."""
    c = srv.client()
    c[db]["c"].insert_many([{"_id": i, "v": 0} for i in range(400)])
    srv.admin.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 100000})
    states = []

    def mark():
        time.sleep(0.05)
        states.append((now_ms(), dump_db(c, db)))
        time.sleep(GAP)

    c[db]["c"].update_many({"_id": {"$lt": 200}}, {"$set": {"v": 1}})
    mark()
    c[db]["c"].insert_many([{"_id": 1000 + i, "v": 2} for i in range(100)])
    mark()
    c[db]["c"].delete_many({"_id": {"$gte": 300, "$lt": 400}})
    mark()
    c[db].command("tenantAdmin", close=True)
    base = c.admin.command("tenantStats")["maintenance"]
    for s in range(6):  # more sessions: L0 SSTs pile up, a compaction rewrites them
        c[db]["c"].insert_many([{"_id": 5000 + s * 100 + i} for i in range(100)])
        c[db].command("tenantAdmin", close=True)
        time.sleep(0.3)
    wait_until(lambda: c.admin.command("tenantStats")["maintenance"]["compactRuns"] > base["compactRuns"], 60, what="compaction")
    before = {k for k, _ in list_prefix(bucket, f"tenants/{db}/")}
    wait_until(lambda: before - {k for k, _ in list_prefix(bucket, f"tenants/{db}/")}, 150, every=2, what="GC pass")
    time.sleep(2)
    deleted = before - {k for k, _ in list_prefix(bucket, f"tenants/{db}/")}
    return states, deleted


def test_restore_after_compaction_and_gc_uses_retained_wal_and_ssts():
    bucket = make_bucket("rgc")
    srv = Server(bucket, "rgc", idle=300, extra=EXTRA).start()
    try:
        states, deleted = gc_scenario(srv, bucket, "g")
        print("objects collected by GC:", len(deleted), "of which WAL:", len([k for k in deleted if "/wal/" in k]), "SST:", len([k for k in deleted if "/compacted/" in k]))
        assert deleted
        c = srv.client()
        for i in (0, 2, 1):
            t, want = states[i]
            st, r = restore(srv, "g", t)
            assert st == 200, r
            assert dump_db(c, "g") == want, f"state {i}"
            assert r["from_point"]["wal_objects_replayed_after_it"] >= 1
    finally:
        srv.term()


def test_negative_control_without_wal_retention_the_restore_is_refused():
    """The same scenario with the WAL retention switched off: the garbage collector deletes the WAL objects the restore
    needs, and the restore answers 409 retention_gap (it never returns a wrong state)."""
    bucket = make_bucket("rgc0")
    srv = Server(bucket, "rgc0", idle=300, extra=EXTRA, env={"CTL_DISABLE_WAL_RETENTION": "1"}).start()
    try:
        states, deleted = gc_scenario(srv, bucket, "g")
        assert [k for k in deleted if "/wal/" in k], "the GC must have collected WAL objects for the control to mean anything"
        c = srv.client()
        now_state = dump_db(c, "g")
        t, want = states[0]
        st, r = restore(srv, "g", t)
        print("without WAL retention:", st, r)
        assert st == 409 and r["error"]["code"] == "retention_gap", r
        assert dump_db(c, "g") == now_state, "a refused restore must change nothing"
        assert list_prefix(bucket, "control/staging/g/") == []
        assert srv.admin.get("/v1/databases/g")[1]["operation"] is None
    finally:
        srv.term()


# ---- kill -9 ----------------------------------------------------------------------------------------

POINTS = ["restore.after_intent", "restore.after_staging", "restore.mid_apply"]


@pytest.mark.parametrize("point", POINTS)
def test_kill9_at_each_step_finishes_after_restart(point):
    bucket = make_bucket("rcrash")
    srv = Server(bucket, "rcr", idle=300, extra=EXTRA, env={"CTL_FAILPOINT": point}).start()
    try:
        c = srv.client()
        fill(c, "r", "c", 4000)
        c["r"]["c"].create_index([("grp", 1)])
        srv.admin.put("/v1/databases/r/retention", {"window_secs": 3600, "interval_secs": 100000})
        time.sleep(0.3)
        t = now_ms()
        want = dump_db(c, "r")
        time.sleep(GAP)
        c["r"]["c"].update_many({}, {"$set": {"grp": 99, "extra": "z"}})   # every document and index entry changes
        c["r"]["c"].insert_many([{"_id": 90000 + i} for i in range(500)])
        res = {}
        th = threading.Thread(target=lambda: res.update(r=_post(srv, "r", t)))
        th.start()
        wait_until(lambda: f"FAILPOINT {point} reached" in srv.log_text(), 60, what="failpoint")
        srv.kill9()
        th.join(timeout=60)
        srv.env = {}
        srv.proc = None
        srv.start()
        log = srv.log_text()
        assert "ctl recovery: restore of r was interrupted" in log and "ctl recovery: restore of r finished" in log, log[-2000:]
        c = srv.client()
        assert dump_db(c, "r") == want
        assert list_prefix(bucket, "control/staging/r/") == [] and list_prefix(bucket, "control/ops/r.json") == []
        pins = [x for x in srv.admin.get("/v1/databases/r/checkpoints")[1]["checkpoints"] if x["expires_at_ms"] is None]
        assert pins == [], "the scratch database's pin was left in the restored database"
        assert srv.admin.get("/v1/databases/r")[1]["operation"] is None
        # usable afterwards
        c["r"]["c"].insert_one({"_id": -1})
        assert c["r"]["c"].count_documents({}) == 4001
    finally:
        srv.term()


def _post(srv, db, t):
    try:
        return restore(srv, db, t)
    except Exception as e:
        return (0, str(e))


def test_random_kill_loop():
    """Restore with a kill -9 at a random moment, 8 times. After the restart the database is either exactly the state
    restored to (the restore had been started, so recovery finished it) or exactly the state before (killed before
    it started); never a mix."""
    bucket = make_bucket("rcrash2")
    srv = Server(bucket, "rcr2", idle=300, extra=EXTRA).start()
    rnd = random.Random(21)
    try:
        c = srv.client()
        fill(c, "r", "c", 3000)
        srv.admin.put("/v1/databases/r/retention", {"window_secs": 3600, "interval_secs": 100000})
        time.sleep(0.3)
        t = now_ms()
        want = dump_db(c, "r")
        time.sleep(GAP)
        outcomes = {"restored": 0, "untouched": 0}
        for i in range(8):
            c = srv.client()
            c["r"]["c"].update_many({}, {"$set": {"v": i}})
            c["r"]["c"].insert_many([{"_id": 10000 + i * 100 + j} for j in range(100)])
            before = dump_db(c, "r")
            res = {}
            th = threading.Thread(target=lambda: res.update(r=_post(srv, "r", t)))
            th.start()
            time.sleep(rnd.uniform(0.0, 1.2))
            srv.kill9()
            th.join(timeout=60)
            srv.proc = None
            srv.start()
            got = dump_db(srv.client(), "r")
            if got == want:
                outcomes["restored"] += 1
            elif got == before:
                outcomes["untouched"] += 1
            else:
                raise AssertionError(f"iteration {i}: the database is neither the restored state nor the state before")
            assert list_prefix(bucket, "control/staging/r/") == []
            assert list_prefix(bucket, "control/ops/r.json") == []
        print("random kill outcomes", outcomes)
        assert outcomes["restored"] >= 2
    finally:
        srv.term()


# ---- concurrency --------------------------------------------------------------------------------------

@pytest.fixture()
def env():
    bucket = make_bucket("rconc")
    srv = Server(bucket, "rco", idle=300, extra=["--retention-interval-secs", "2", "--retention-prune-secs", "3"]).start()
    yield srv, bucket
    srv.term()


def setup_states(srv, db):
    c = srv.client()
    c[db]["c"].insert_many([{"_id": i, "v": 0} for i in range(500)])
    srv.admin.put(f"/v1/databases/{db}/retention", {"window_secs": 3600, "interval_secs": 2})
    time.sleep(0.3)
    t1 = now_ms()
    s1 = dump_db(c, db)
    time.sleep(GAP)
    c[db]["c"].update_many({}, {"$set": {"v": 1}})
    c[db]["c"].insert_many([{"_id": 1000 + i} for i in range(50)])
    time.sleep(0.2)
    t2 = now_ms()
    s2 = dump_db(c, db)
    time.sleep(GAP)
    c[db]["c"].delete_many({"_id": {"$lt": 100}})
    return c, (t1, s1), (t2, s2), dump_db(c, db)


def test_restore_while_clients_write(env):
    srv, bucket = env
    c, (t1, s1), _, _ = setup_states(srv, "w")
    stop = threading.Event()
    acked = []
    errs = []

    def writer():
        cc = srv.client()
        i = 100000
        while not stop.is_set():
            try:
                cc["w"]["c"].insert_one({"_id": i})
                acked.append((time.time(), i))
            except Exception as e:
                errs.append(e)
            i += 1

    th = threading.Thread(target=writer)
    th.start()
    time.sleep(1.0)
    started = time.time()
    st, r = restore(srv, "w", t1)
    finished = time.time()
    time.sleep(0.7)
    stop.set()
    th.join()
    assert st == 200 and not errs, (r, errs[:2])
    ids = sorted(d["_id"] for d in c["w"]["c"].find({"_id": {"$gte": 100000}}))
    pre = [i for ts, i in acked if ts < started]
    post = [i for ts, i in acked if ts > finished]
    assert pre and post
    # writes acknowledged before the restore began (and after t1) are gone; writes after it are all there; the writes
    # that were waiting for the lock during the restore were applied after it
    assert not set(pre) & set(ids)
    assert set(post) <= set(ids)
    assert ids == list(range(ids[0], ids[-1] + 1)), "the surviving writes are one contiguous run"
    base = {k: v for k, v in dump_db(c, "w")["c"].items() if not (isinstance(eval(k), int) and eval(k) >= 100000)}
    assert base == s1["c"]


def test_two_restores_at_once_are_serialised(env):
    srv, bucket = env
    c, (t1, s1), (t2, s2), _ = setup_states(srv, "two")
    out = {}
    ts = [threading.Thread(target=lambda k=k, t=t: out.update({k: restore(srv, "two", t)})) for k, t in (("a", t1), ("b", t2))]
    [x.start() for x in ts]
    [x.join() for x in ts]
    assert out["a"][0] == 200 and out["b"][0] == 200, out
    got = dump_db(c, "two")
    assert got == s1 or got == s2
    assert srv.admin.get("/v1/databases/two")[1]["operation"] is None and list_prefix(bucket, "control/staging/two/") == []


def test_restore_and_erase_at_once(env):
    srv, bucket = env
    for i in range(4):
        db = f"re{i}"
        c, (t1, s1), _, _ = setup_states(srv, db)
        out = {}
        a = threading.Thread(target=lambda: out.update(r=restore(srv, db, t1)))
        b = threading.Thread(target=lambda: out.update(e=srv.admin.delete(f"/v1/databases/{db}")))
        a.start(); b.start(); a.join(); b.join()
        assert out["e"][0] == 200, out
        assert out["r"][0] in (200, 404), out
        for p in (f"tenants/{db}/", f"control/dbs/{db}/", f"control/staging/{db}/", f"control/retention/{db}", f"control/ops/{db}.json"):
            assert list_prefix(bucket, p) == [], (p, out)


def test_restore_and_fork_at_once(env):
    srv, bucket = env
    for i in range(3):
        db = f"rf{i}"
        c, (t1, s1), _, cur = setup_states(srv, db)
        out = {}
        a = threading.Thread(target=lambda: out.update(r=restore(srv, db, t1)))
        b = threading.Thread(target=lambda: out.update(f=srv.admin.post(f"/v1/databases/{db}/fork", {"target": f"{db}k"})))
        a.start(); b.start(); a.join(); b.join()
        assert out["r"][0] == 200 and out["f"][0] == 201, out
        fk = dump_db(c, f"{db}k")
        assert fk == cur or fk == s1, "a fork taken during a restore must be the state before or after it, never a mix"
        assert dump_db(c, db) == s1


def test_restore_and_export_at_once(env):
    srv, bucket = env
    import tempfile
    import export_client as xc
    import bson
    from lib import TOKEN
    db = "rx"
    c, (t1, s1), _, cur = setup_states(srv, db)
    out = {}
    a = threading.Thread(target=lambda: out.update(r=restore(srv, db, t1)))
    b = threading.Thread(target=lambda: out.update(x=srv.admin.post(f"/v1/databases/{db}/exports")))
    a.start(); b.start(); a.join(); b.join()
    assert out["r"][0] == 200 and out["x"][0] == 201
    dest = tempfile.mkdtemp(prefix="dump-")
    ver = xc.download(f"http://127.0.0.1:{srv.admin_port}", TOKEN, out["x"][1]["id"], dest)
    assert all(v == "ok" for v in ver.values())
    got = {repr(d["_id"]): d for d in bson.decode_all(open(f"{dest}/{db}/c.bson", "rb").read())}
    assert got == cur["c"] or got == s1["c"]
