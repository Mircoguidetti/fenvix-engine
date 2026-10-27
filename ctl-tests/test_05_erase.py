"""Operation 5: erase (delete every object of a database, proven by listing the prefixes)."""
import random
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, prefix_bytes, wait_until, fill, dump_db

ALL_PREFIXES = lambda db: [f"tenants/{db}/", f"ttl-hints/{db}/", f"control/dbs/{db}/", f"control/pins/{db}", f"control/usage/{db}/",
                           f"control/forks/{db}/", f"control/exports/{db}/", f"control/ops/{db}.json"]


def objects_of(bucket, db):
    out = []
    for p in ALL_PREFIXES(db):
        out += list_prefix(bucket, p)
    return out


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("erase")
    srv = Server(bucket, "erase", idle=300, extra=["--usage-flush-secs", "1"]).start()
    yield srv, bucket
    srv.term()


def mk(srv, name, n=500):
    c = srv.client()
    fill(c, name, "c", n)
    c[name]["c"].create_index([("grp", 1)])
    c[name]["d"].insert_many([{"_id": i} for i in range(50)])
    return c


def test_erase_deletes_every_object_and_proves_it(env):
    srv, bucket = env
    c = mk(srv, "victim", 3000)
    c["victim"].command("tenantAdmin", close=True)
    time.sleep(2.5)  # usage flushed: control/usage/victim exists
    srv.admin.get("/v1/databases/victim")
    assert len(objects_of(bucket, "victim")) > 5
    others = {d: objects_of(bucket, d) for d in ()}
    st, r = srv.admin.delete("/v1/databases/victim")
    assert st == 200, r
    assert r["erased"] and r["objects_deleted"] > 5 and r["proof"]["remaining_objects"] == 0
    assert objects_of(bucket, "victim") == []          # listed straight from the bucket, not through the server
    assert [k for k, _ in list_prefix(bucket, "") if "victim" in k] == []
    assert srv.admin.get("/v1/databases/victim")[0] == 404
    assert "victim" not in [d["name"] for d in c.list_databases()]
    assert srv.admin.delete("/v1/databases/victim")[0] == 404
    print("erase cost", r["cost"], "deleted", r["objects_deleted"], "ms", r["elapsed_ms"])


def test_erase_leaves_other_databases_alone(env):
    srv, bucket = env
    c = mk(srv, "keep1", 300)
    mk(srv, "gone1", 300)
    mk(srv, "keep2", 300)
    before = {d: sorted(objects_of(bucket, d)) for d in ("keep1", "keep2")}
    ref = {d: dump_db(c, d) for d in ("keep1", "keep2")}
    assert srv.admin.delete("/v1/databases/gone1")[0] == 200
    for d in ("keep1", "keep2"):
        assert dump_db(c, d) == ref[d]
    assert objects_of(bucket, "gone1") == []
    names = [d["name"] for d in c.list_databases()]
    assert "keep1" in names and "keep2" in names and "gone1" not in names


def test_name_can_be_reused_and_starts_empty(env):
    srv, bucket = env
    c = mk(srv, "reuse", 100)
    meta = srv.admin.get("/v1/databases/reuse")[1]
    assert srv.admin.delete("/v1/databases/reuse")[0] == 200
    st, d = srv.admin.post("/v1/databases", {"name": "reuse"})
    assert st == 201 and d["created_at_ms"] > meta["created_at_ms"]
    assert c["reuse"].list_collection_names() == []
    c["reuse"]["c"].insert_one({"_id": 1})
    assert c["reuse"]["c"].count_documents({}) == 1


def test_erase_awake_and_asleep(env):
    srv, bucket = env
    c = mk(srv, "awake1", 100)
    assert srv.admin.get("/v1/databases/awake1")[1]["state"] == "awake"
    assert srv.admin.delete("/v1/databases/awake1")[0] == 200 and objects_of(bucket, "awake1") == []
    c = mk(srv, "asleep1", 100)
    c["asleep1"].command("tenantAdmin", close=True)
    assert srv.admin.get("/v1/databases/asleep1")[1]["state"] == "asleep"
    assert srv.admin.delete("/v1/databases/asleep1")[0] == 200 and objects_of(bucket, "asleep1") == []


def test_erase_refused_while_forks_read_it_then_allowed(env):
    srv, bucket = env
    c = mk(srv, "par", 300)
    assert srv.admin.post("/v1/databases/par/fork", {"target": "kid"})[0] == 201
    st, r = srv.admin.delete("/v1/databases/par")
    assert st == 409 and r["error"]["code"] == "has_forks" and "kid" in r["error"]["message"], r
    assert c["par"]["c"].count_documents({}) == 300 and c["kid"]["c"].count_documents({}) == 300
    assert len(objects_of(bucket, "par")) > 0
    # erasing the fork releases its pin in the parent; then the parent can go
    assert srv.admin.delete("/v1/databases/kid")[0] == 200
    pins = [x for x in srv.admin.get("/v1/databases/par/checkpoints")[1]["checkpoints"] if x["expires_at_ms"] is None]
    assert pins == []
    assert srv.admin.delete("/v1/databases/par")[0] == 200 and objects_of(bucket, "par") == []


def test_independent_copy_fork_does_not_block_erase(env):
    srv, bucket = env
    c = mk(srv, "par2", 300)
    assert srv.admin.post("/v1/databases/par2/fork", {"target": "kid2", "mode": "copy"})[0] == 201
    assert srv.admin.delete("/v1/databases/par2")[0] == 200
    assert objects_of(bucket, "par2") == []
    assert c["kid2"]["c"].count_documents({}) == 300
    c["kid2"].command("tenantAdmin", close=True)
    assert c["kid2"]["c"].count_documents({}) == 300


def test_erase_errors(env):
    srv, _ = env
    assert srv.admin.delete("/v1/databases/never-existed")[0] == 404
    assert srv.admin.delete("/v1/databases/bad.name")[0] == 400
    assert srv.admin.delete("/v1/databases/admin")[0] == 400
    assert srv.admin.delete("/v1/databases/x", token=None)[0] == 401


def test_concurrent_erases_one_wins(env):
    srv, bucket = env
    mk(srv, "race", 200)
    out = []

    def go():
        out.append(srv.admin.delete("/v1/databases/race")[0])

    ts = [threading.Thread(target=go) for _ in range(5)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(out) == [200, 404, 404, 404, 404], out
    assert objects_of(bucket, "race") == []


def test_concurrent_erase_and_fork_are_serialised(env):
    srv, bucket = env
    for i in range(6):
        src = f"ef{i}"
        mk(srv, src, 200)
        res = {}
        t1 = threading.Thread(target=lambda: res.update(f=srv.admin.post(f"/v1/databases/{src}/fork", {"target": f"{src}_k"})[0]))
        t2 = threading.Thread(target=lambda: res.update(e=srv.admin.delete(f"/v1/databases/{src}")[0]))
        t1.start(); t2.start(); t1.join(); t2.join()
        assert (res["f"], res["e"]) in [(201, 409), (404, 200)], res
        if res["f"] == 201:
            assert srv.client()[f"{src}_k"]["c"].count_documents({}) == 200 and objects_of(bucket, src) != []
        else:
            assert objects_of(bucket, src) == [] and objects_of(bucket, f"{src}_k") == []


def test_erase_while_clients_write(env):
    srv, bucket = env
    c = mk(srv, "busy", 100)
    stop = threading.Event()
    acked = []

    def writer():
        cc = srv.client()
        i = 10000
        while not stop.is_set():
            try:
                cc["busy"]["c"].insert_one({"_id": i})
                acked.append((time.time(), i))
            except Exception:
                pass
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.8)
    st, r = srv.admin.delete("/v1/databases/busy")
    t_done = time.time()
    time.sleep(0.5)
    stop.set()
    t.join()
    assert st == 200, r
    # after the erase the writer's inserts re-created a database holding only post-erase documents
    after = [i for ts, i in acked if ts > t_done]
    ids = sorted(d["_id"] for d in c["busy"]["c"].find({}))
    assert all(i >= 10000 for i in ids) and not any(i < 10000 for i in ids)
    assert set(after) <= set(ids)
    assert srv.admin.delete("/v1/databases/busy")[0] == 200 and objects_of(bucket, "busy") == []


# ---- kill -9 ---------------------------------------------------------------------------------------

POINTS = ["erase.after_intent", "erase.after_manifests", "erase.after_tenant", "erase.after_control"]


@pytest.mark.parametrize("fork_first", [False, True])
@pytest.mark.parametrize("point", POINTS)
def test_kill9_at_each_step_finishes_after_restart(point, fork_first):
    bucket = make_bucket("erasecrash")
    srv = Server(bucket, "ec", idle=300, env={"CTL_FAILPOINT": point}, extra=["--usage-flush-secs", "1"]).start()
    try:
        c = srv.client()
        fill(c, "victim", "c", 2000)
        c["victim"]["c"].create_index([("grp", 1)])
        c["victim"].command("tenantAdmin", close=True)
        fill(c, "bystander", "c", 300)
        ref = dump_db(c, "bystander")
        target = "victim"
        if fork_first:  # the victim is itself a shallow fork: erase must release its pin
            srv.admin.post("/v1/databases/victim/fork", {"target": "victim2"})
            target = "victim2"
        res = {}
        th = threading.Thread(target=lambda: res.update(r=_del(srv, target)))
        th.start()
        wait_until(lambda: f"FAILPOINT {point} reached" in srv.log_text(), 30, what="failpoint")
        srv.kill9()
        th.join(timeout=30)
        srv.env = {}
        srv.proc = None
        srv.start()
        log = srv.log_text()
        assert f"ctl recovery: erase of {target} was interrupted" in log and f"ctl recovery: erase of {target} finished" in log, log[-1500:]
        assert objects_of(bucket, target) == [], objects_of(bucket, target)
        assert srv.admin.get(f"/v1/databases/{target}")[0] == 404
        assert target not in [d["name"] for d in srv.client().list_databases()]
        assert dump_db(srv.client(), "bystander") == ref
        if fork_first:
            pins = [x for x in srv.admin.get("/v1/databases/victim/checkpoints")[1]["checkpoints"] if x["expires_at_ms"] is None]
            assert pins == [], "the erased fork's pin was left in its source"
            assert srv.client()["victim"]["c"].count_documents({}) == 2000
            assert srv.admin.delete("/v1/databases/victim")[0] == 200 and objects_of(bucket, "victim") == []
    finally:
        srv.term()


def _del(srv, db):
    try:
        return srv.admin.delete(f"/v1/databases/{db}", timeout=20)
    except Exception as e:
        return (0, str(e))


def test_random_kill_loop():
    """Erase a database with a few hundred objects, kill -9 at a random moment, 12 times; after each restart the
    database is gone without a trace and the others are intact."""
    bucket = make_bucket("erasecrash2")
    srv = Server(bucket, "ec2", idle=300).start()
    random.seed(11)
    c = srv.client()
    fill(c, "bystander", "c", 200)
    ref = dump_db(c, "bystander")
    killed_mid = 0
    outcomes = {"finished_by_recovery_or_before_kill": 0, "killed_before_start_intact": 0}

    def populate(c):
        # many small objects: every insert_many is one WAL object, and the database is closed now and then so SSTs are written
        for s in range(30):
            c["victim"][f"c{s}"].insert_many([{"_id": j, "pad": "y" * 200} for j in range(40)])
            if s % 5 == 4:
                c["victim"].command("tenantAdmin", close=True)
        c["victim"].command("tenantAdmin", close=True)

    try:
        # how long does an erase of this database take? (three un-killed ones)
        times = []
        for _ in range(3):
            populate(c)
            st, r = srv.admin.delete("/v1/databases/victim")
            assert st == 200
            times.append(r["elapsed_ms"] / 1000.0)
        typical = sum(times) / len(times)
        print("typical erase time", typical, "s")
        for i in range(12):
            c = srv.client()
            populate(c)
            n_before = len(objects_of(bucket, "victim"))
            res = {}
            th = threading.Thread(target=lambda: res.update(r=_del(srv, "victim")))
            th.start()
            time.sleep(random.uniform(0.1 * typical, 1.1 * typical))
            srv.kill9()
            th.join(timeout=30)
            left = len(objects_of(bucket, "victim"))
            if 0 < left:
                killed_mid += 1
            srv.proc = None
            srv.start()
            if res["r"][0] == 200:
                assert objects_of(bucket, "victim") == [], "an erase that was acknowledged must be complete"
            now = objects_of(bucket, "victim")
            outcomes["killed_before_start_intact" if now else "finished_by_recovery_or_before_kill"] += 1
            if now:
                # killed before the erase had written its intent: the database is whole and the erase can be repeated
                assert srv.client()["victim"]["c0"].count_documents({}) == 40
                assert list_prefix(bucket, "control/ops/victim.json") == []
                assert sum(srv.client()["victim"][f"c{s}"].count_documents({}) for s in range(30)) == 1200
                assert srv.admin.delete("/v1/databases/victim")[0] == 200
            assert objects_of(bucket, "victim") == [], f"iteration {i}: {objects_of(bucket, 'victim')}"
            assert srv.admin.get("/v1/databases/victim")[0] == 404
            assert dump_db(srv.client(), "bystander") == ref
        print("kills that found objects left (before the restart):", killed_mid, "of 12;", outcomes)
        assert killed_mid >= 3, "the kills did not land inside the erase often enough to prove anything"
    finally:
        srv.term()
