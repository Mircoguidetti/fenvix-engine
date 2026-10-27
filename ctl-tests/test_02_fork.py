"""Operation 2: fork (instant copy as a new database)."""
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, prefix_bytes, wait_until, fill, dump_db


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("fork")
    srv = Server(bucket, "fork", idle=300).start()
    yield srv, bucket
    srv.term()


def mk_source(srv, name, n=1000):
    c = srv.client()
    fill(c, name, "users", n)
    c[name]["orders"].insert_many([{"_id": i, "user": i % 50, "total": i * 3} for i in range(300)])
    c[name]["users"].create_index([("grp", 1)])
    c[name]["orders"].create_index([("user", 1), ("total", -1)], name="u_t")
    return c


def indexes(c, db):
    return {cn: sorted(i["name"] for i in c[db][cn].list_indexes()) for cn in sorted(c[db].list_collection_names())}


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_fork_contents_equal(env, mode):
    srv, bucket = env
    c = mk_source(srv, f"s_{mode}")
    st, r = srv.admin.post(f"/v1/databases/s_{mode}/fork", {"target": f"f_{mode}", "mode": mode})
    assert st == 201, r
    assert r["forked_from"]["database"] == f"s_{mode}" and r["forked_from"]["mode"] == mode and r["created_via"] == "fork"
    assert dump_db(c, f"s_{mode}") == dump_db(c, f"f_{mode}")
    assert indexes(c, f"s_{mode}") == indexes(c, f"f_{mode}")
    # queries through the fork's indexes work
    assert c[f"f_{mode}"]["users"].count_documents({"grp": 3}) == 100
    assert c[f"f_{mode}"]["orders"].find_one({"user": 7, "total": {"$gte": 0}}, sort=[("total", -1)]) is not None
    g = srv.admin.get(f"/v1/databases/f_{mode}")[1]
    assert g["forked_from"]["database"] == f"s_{mode}"


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_writes_never_cross(env, mode):
    srv, _ = env
    c = mk_source(srv, f"w_{mode}", 200)
    assert srv.admin.post(f"/v1/databases/w_{mode}/fork", {"target": f"wf_{mode}", "mode": mode})[0] == 201
    s, f = c[f"w_{mode}"]["users"], c[f"wf_{mode}"]["users"]
    base = dump_db(c, f"w_{mode}")
    # writes to the source
    s.insert_one({"_id": 5000, "only": "source"})
    s.update_one({"_id": 1}, {"$set": {"name": "changed-in-source"}})
    s.delete_one({"_id": 2})
    c[f"w_{mode}"]["orders"].drop()
    # writes to the fork
    f.insert_one({"_id": 6000, "only": "fork"})
    f.update_one({"_id": 3}, {"$set": {"name": "changed-in-fork"}})
    f.delete_one({"_id": 4})
    c[f"wf_{mode}"]["extra"].insert_one({"_id": 1})
    assert f.find_one({"_id": 5000}) is None and f.find_one({"_id": 1})["name"] == "user1" and f.find_one({"_id": 2}) is not None
    assert s.find_one({"_id": 6000}) is None and s.find_one({"_id": 3})["name"] == "user3" and s.find_one({"_id": 4}) is not None
    assert c[f"wf_{mode}"]["orders"].count_documents({}) == 300
    assert "extra" not in c[f"w_{mode}"].list_collection_names()
    # untouched documents are identical on both sides
    for i in range(10, 200):
        assert s.find_one({"_id": i}) == f.find_one({"_id": i}) == base["users"][repr(i)]
    # still true after both sides slept and woke again (everything flushed to their own SSTs)
    for db in (f"w_{mode}", f"wf_{mode}"):
        c[db].command("tenantAdmin", close=True)
    assert s.find_one({"_id": 5000})["only"] == "source" and f.find_one({"_id": 6000})["only"] == "fork"
    assert f.find_one({"_id": 5000}) is None and s.find_one({"_id": 6000}) is None


def test_clone_copies_no_data(env):
    srv, bucket = env
    c = srv.client()
    fill(c, "big", "c", 20000)
    c["big"].command("tenantAdmin", close=True)
    src_bytes = prefix_bytes(bucket, "tenants/big/")
    st, r = srv.admin.post("/v1/databases/big/fork", {"target": "big_clone"})
    assert st == 201
    cost = r["fork"]["cost"]
    fork_bytes = prefix_bytes(bucket, "tenants/big_clone/")
    print("clone cost", cost, "source bytes", src_bytes, "fork own bytes", fork_bytes, "ms", r["fork"]["elapsed_ms"])
    assert src_bytes > 1_000_000
    assert cost["put_bytes"] < 20_000 and cost["get_bytes"] < 20_000, cost  # manifests only: no document data moves
    assert fork_bytes < 20_000
    assert cost["copy"] == 0
    # copy mode: server-side COPY requests, no bytes through the process, bytes duplicated in the bucket
    st, r2 = srv.admin.post("/v1/databases/big/fork", {"target": "big_copy", "mode": "copy"})
    assert st == 201
    cost2 = r2["fork"]["cost"]
    print("copy cost", cost2, "fork bytes", prefix_bytes(bucket, "tenants/big_copy/"), "ms", r2["fork"]["elapsed_ms"])
    assert cost2["copy"] >= 3 and cost2["put_bytes"] < 20_000
    assert abs(prefix_bytes(bucket, "tenants/big_copy/") - src_bytes) < 2000  # lease/marker only
    assert c["big_clone"]["c"].count_documents({}) == 20000 == c["big_copy"]["c"].count_documents({})


def test_fork_of_awake_source_includes_every_acknowledged_write(env):
    srv, _ = env
    c = srv.client()
    c["live"]["c"].insert_one({"_id": 0})
    for i in range(1, 300):
        c["live"]["c"].insert_one({"_id": i})  # acknowledged one by one, source stays open as a writer
    assert srv.admin.post("/v1/databases/live/fork", {"target": "live_f"})[0] == 201
    assert c["live_f"]["c"].count_documents({}) == 300


def test_fork_chain(env):
    srv, bucket = env
    c = mk_source(srv, "g0", 300)
    assert srv.admin.post("/v1/databases/g0/fork", {"target": "g1"})[0] == 201
    c["g1"]["users"].insert_one({"_id": 9001})
    assert srv.admin.post("/v1/databases/g1/fork", {"target": "g2"})[0] == 201
    c["g2"]["users"].insert_one({"_id": 9002})
    assert c["g2"]["users"].count_documents({}) == 302 and c["g1"]["users"].count_documents({}) == 301 and c["g0"]["users"].count_documents({}) == 300
    assert c["g2"]["users"].find_one({"_id": 9001}) and not c["g0"]["users"].find_one({"_id": 9001})
    # g2 reads g0's objects too, so both g0 and g1 record it as a dependent
    assert list_prefix(bucket, "control/forks/g0/") and list_prefix(bucket, "control/forks/g1/")


def test_errors(env):
    srv, _ = env
    a = srv.admin
    mk_source(srv, "e_src", 10)
    assert a.post("/v1/databases/nope/fork", {"target": "t"})[0] == 404
    assert a.post("/v1/databases/e_src/fork", {"target": "e_src"})[0] == 400
    assert a.post("/v1/databases/e_src/fork", {})[0] == 400
    assert a.post("/v1/databases/e_src/fork", {"target": "bad/name"})[0] == 400
    assert a.post("/v1/databases/e_src/fork", {"target": "t", "mode": "magic"})[0] == 400
    assert a.post("/v1/databases/e_src/fork", {"target": "e_t1"})[0] == 201
    assert a.post("/v1/databases/e_src/fork", {"target": "e_t1"})[0] == 409
    assert a.post("/v1/databases/e_src/fork", {"target": "e_src"})[0] == 400
    # a shallow fork cannot be copied (the copy would read the ancestor's objects without being recorded)
    st, r = a.post("/v1/databases/e_t1/fork", {"target": "e_t2", "mode": "copy"})
    assert st == 409 and r["error"]["code"] == "source_is_shallow_fork", r


def test_concurrent_forks_of_one_source(env):
    srv, _ = env
    c = mk_source(srv, "cc", 500)
    results = {}

    def go(i):
        results[i] = srv.admin.post("/v1/databases/cc/fork", {"target": f"cc_f{i}"})

    ts = [threading.Thread(target=go, args=(i,)) for i in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(r[0] == 201 for r in results.values()), results
    ref = dump_db(c, "cc")
    for i in range(6):
        assert dump_db(c, f"cc_f{i}") == ref


def test_concurrent_forks_into_one_target(env):
    srv, _ = env
    mk_source(srv, "ct", 100)
    results = []

    def go():
        results.append(srv.admin.post("/v1/databases/ct/fork", {"target": "ct_same"})[0])

    ts = [threading.Thread(target=go) for _ in range(5)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(results) == [201, 409, 409, 409, 409], results
    assert srv.client()["ct_same"]["users"].count_documents({}) == 100


def test_fork_while_source_is_written(env):
    srv, _ = env
    c = srv.client()
    c["hot"]["c"].insert_one({"_id": 0})
    stop = threading.Event()
    last = [0]

    def writer():
        i = 1
        cc = srv.client()
        while not stop.is_set():
            cc["hot"]["c"].insert_one({"_id": i})
            last[0] = i
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(1.0)
    before = last[0]
    st, r = srv.admin.post("/v1/databases/hot/fork", {"target": "hot_f"})
    after = last[0]
    time.sleep(0.5)
    stop.set()
    t.join()
    assert st == 201, r
    ids = sorted(d["_id"] for d in c["hot_f"]["c"].find({}))
    # a consistent prefix of the writes: no holes, contains everything acknowledged before the fork started
    assert ids == list(range(len(ids))), "fork has holes"
    assert before <= ids[-1] <= after + 1
    assert c["hot"]["c"].count_documents({}) > len(ids)  # the source kept going


def test_fork_then_compaction_and_gc_keep_the_fork_readable(env):
    """The source is written in many sessions after the fork, so it is compacted and its old SSTs are collected;
    the fork must still read every document it was forked with (it pins them with a checkpoint)."""
    srv, bucket = env
    c = srv.client()
    for i in range(300):
        pass
    fill(c, "cg", "c", 1500)
    c["cg"].command("tenantAdmin", close=True)
    assert srv.admin.post("/v1/databases/cg/fork", {"target": "cg_f"})[0] == 201
    base = c.admin.command("tenantStats")["maintenance"]
    for s in range(7):
        fill(c, "cg", "c", 200, start=10000 + s * 200)
        c["cg"].command("tenantAdmin", close=True)
        time.sleep(0.3)
    wait_until(lambda: c.admin.command("tenantStats")["maintenance"]["compactRuns"] > base["compactRuns"], 60, what="compaction of the source")
    objs_after_compaction = {k for k, _ in list_prefix(bucket, "tenants/cg/")}
    wait_until(lambda: c.admin.command("tenantStats")["maintenance"]["gcRuns"] > base["gcRuns"], 120, what="garbage collection")
    time.sleep(2)
    objs_after_gc = {k for k, _ in list_prefix(bucket, "tenants/cg/")}
    deleted = objs_after_compaction - objs_after_gc
    print("objects of the source deleted by GC after compaction:", len(deleted))
    assert deleted, "the GC pass collected nothing, so the test proves nothing"
    assert c["cg"]["c"].count_documents({}) == 1500 + 1400
    f = c["cg_f"]["c"]
    assert f.count_documents({}) == 1500
    assert sorted(d["_id"] for d in f.find({}, {"_id": 1})) == list(range(1500))
    assert f.find_one({"_id": 777})["name"] == "user777"


def test_mongo_dropdatabase_respects_forks(env):
    """The wire-protocol dropDatabase must not delete objects a fork still reads, and dropping a fork must release
    the pin it holds in its source."""
    from pymongo.errors import OperationFailure
    srv, bucket = env
    c = mk_source(srv, "dd_src", 200)
    assert srv.admin.post("/v1/databases/dd_src/fork", {"target": "dd_fork"})[0] == 201
    with pytest.raises(OperationFailure) as e:
        c.drop_database("dd_src")
    assert "forks" in str(e.value)
    assert c["dd_src"]["users"].count_documents({}) == 200 and c["dd_fork"]["users"].count_documents({}) == 200
    pins = lambda: [x for x in srv.admin.get("/v1/databases/dd_src/checkpoints")[1]["checkpoints"] if x["expires_at_ms"] is None]
    assert len(pins()) == 1
    c.drop_database("dd_fork")
    assert pins() == [] and list_prefix(bucket, "control/forks/dd_src/") == [] and list_prefix(bucket, "tenants/dd_fork/") == []
    c.drop_database("dd_src")
    assert list_prefix(bucket, "tenants/dd_src/") == []
