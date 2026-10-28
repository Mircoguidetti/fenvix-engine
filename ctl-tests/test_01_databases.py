"""Operation 1: create, list, get database (admin interface)."""
import time

import pytest

from lib import Server, make_bucket, list_prefix, prefix_bytes, wait_until, fill, TOKEN, Admin


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("dbs")
    srv = Server(bucket, "dbs", idle=3).start()
    yield srv, bucket
    srv.term()


def test_auth_required(env):
    srv, _ = env
    a = srv.admin
    assert a.get("/v1/databases", token=None)[0] == 401
    assert a.get("/v1/databases", token="wrong-token-0123456789abcdef")[0] == 401
    assert a.get("/v1/databases", token=TOKEN[:-1])[0] == 401
    assert a.post("/v1/databases", {"name": "x"}, token=None)[0] == 401
    assert a.get("/v1/databases")[0] == 200
    assert TOKEN not in srv.log_text()


def test_create_get(env):
    srv, bucket = env
    a = srv.admin
    st, d = a.post("/v1/databases", {"name": "alpha"})
    assert st == 201, d
    assert d["name"] == "alpha" and d["keep_awake"] is False and d["forked_from"] is None
    assert d["created_via"] == "admin" and d["created_at"].endswith("Z")
    assert d["state"] in ("awake", "asleep")
    st, g = a.get("/v1/databases/alpha")
    assert st == 200
    # storage_bytes is the sum of the object sizes under the tenant prefix, listed independently of the server
    objs = list_prefix(bucket, "tenants/alpha/")
    assert objs and g["storage_bytes"] == sum(s for _, s in objs) and g["storage_objects"] == len(objs)
    assert g["last_active_at"] is not None


def test_create_conflict_and_validation(env):
    srv, _ = env
    a = srv.admin
    assert a.post("/v1/databases", {"name": "alpha"})[0] == 409
    for bad in ["", "a/b", "a.b", "a b", "a$b", "x" * 64, "admin", "local", "config", "a\"b"]:
        st, d = a.post("/v1/databases", {"name": bad})
        assert st == 400, (bad, st, d)
    assert a.post("/v1/databases", {})[0] == 400
    assert a.get("/v1/databases/nope")[0] == 404
    assert a.get("/v1/databases/bad.name")[0] == 400


def test_created_database_is_visible_to_mongo(env):
    srv, _ = env
    c = srv.client()
    names = [d["name"] for d in c.list_databases()]
    assert "alpha" in names
    c["alpha"]["c"].insert_one({"_id": 1})
    assert c["alpha"]["c"].count_documents({}) == 1


def test_implicit_database_is_discovered(env):
    srv, _ = env
    c = srv.client()
    t0 = time.time() * 1000
    c["implicit"]["c"].insert_one({"_id": 1})
    st, g = srv.admin.get("/v1/databases/implicit")
    assert st == 200 and g["created_via"] == "discovered"
    assert abs(g["created_at_ms"] - t0) < 15000  # oldest object of the tenant
    st, lst = srv.admin.get("/v1/databases")
    names = [d["name"] for d in lst["databases"]]
    assert "implicit" in names and "alpha" in names


def test_state_awake_asleep_awake(env):
    srv, _ = env
    c = srv.client()
    c["sleepy"]["c"].insert_one({"_id": 1})
    assert srv.admin.get("/v1/databases/sleepy")[1]["state"] == "awake"
    wait_until(lambda: srv.admin.get("/v1/databases/sleepy")[1]["state"] == "asleep", timeout=20, what="idle close")
    g = srv.admin.get("/v1/databases/sleepy")[1]
    assert g["state"] == "asleep" and g["holder"] is None
    assert c["sleepy"]["c"].count_documents({}) == 1  # wakes it
    assert srv.admin.get("/v1/databases/sleepy")[1]["state"] == "awake"


def test_last_active_moves_with_commands(env):
    srv, _ = env
    c = srv.client()
    c["active"]["c"].insert_one({"_id": 1})
    g1 = srv.admin.get("/v1/databases/active")[1]
    time.sleep(1.2)
    c["active"]["c"].find_one({})
    g2 = srv.admin.get("/v1/databases/active")[1]
    assert g2["last_active_at_ms"] > g1["last_active_at_ms"] + 900


def test_list_with_storage(env):
    srv, bucket = env
    st, lst = srv.admin.get("/v1/databases?storage=1")
    assert st == 200
    for d in lst["databases"]:
        assert d["storage_bytes"] == prefix_bytes(bucket, f"tenants/{d['name']}/")
    st, lst = srv.admin.get("/v1/databases")
    assert all(d["storage_bytes"] is None for d in lst["databases"])


def test_awake_in_another_process(env):
    """A second server process on the same bucket sees a database that the first one has open as awake."""
    srv, bucket = env
    other = Server(bucket, "dbs2", idle=60).start()
    try:
        srv.client()["shared"]["c"].insert_one({"_id": 1})
        g = other.admin.get("/v1/databases/shared")[1]
        assert g["state"] == "awake" and g["holder"] == srv.holder, g
    finally:
        other.term()


def test_registry_survives_restart(env):
    srv, _ = env
    before = srv.admin.get("/v1/databases/alpha")[1]
    srv.restart()
    after = srv.admin.get("/v1/databases/alpha")[1]
    assert after["created_at_ms"] == before["created_at_ms"] and after["created_via"] == "admin"
    assert srv.client()["alpha"]["c"].count_documents({}) == 1
