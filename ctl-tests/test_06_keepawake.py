"""Operation 6: keep awake (pin / unpin; the lease is renewed and survives restarts)."""
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until, fill, dump_db

IDLE = 3


@pytest.fixture()
def env():
    bucket = make_bucket("pin")
    srv = Server(bucket, "pin", idle=IDLE, extra=["--pin-lease-ttl-secs", "6"]).start()
    yield srv, bucket
    srv.term()


def state(srv, db):
    return srv.admin.get(f"/v1/databases/{db}?storage=0")[1]


def mk(srv, db, n=100):
    c = srv.client()
    fill(c, db, "c", n)
    return c


def test_pinned_database_does_not_sleep_unpinned_one_does(env):
    srv, _ = env
    c = mk(srv, "pinned")
    mk(srv, "plain")
    wakes0 = c["pinned"].command("tenantAdmin")["wakes"]
    st, r = srv.admin.put("/v1/databases/pinned/keep-awake")
    assert st == 200 and r["keep_awake"] is True and r["keep_awake_lease"]["holder"] == srv.holder
    time.sleep(IDLE * 4 + 2)  # four idle periods with no traffic at all
    assert state(srv, "pinned")["state"] == "awake"
    assert state(srv, "plain")["state"] == "asleep"
    assert c["pinned"].command("tenantAdmin")["wakes"] == wakes0, "the pinned database was closed and woken again"
    assert state(srv, "pinned")["keep_awake"] is True and state(srv, "plain")["keep_awake"] is False


def test_pinning_a_sleeping_database_wakes_it(env):
    srv, _ = env
    c = mk(srv, "nap")
    c["nap"].command("tenantAdmin", close=True)
    assert state(srv, "nap")["state"] == "asleep"
    assert srv.admin.put("/v1/databases/nap/keep-awake")[0] == 200
    wait_until(lambda: state(srv, "nap")["state"] == "awake", 5, what="wake on pin")
    time.sleep(IDLE * 2 + 1)
    assert state(srv, "nap")["state"] == "awake"
    assert c["nap"]["c"].count_documents({}) == 100


def test_unpin_lets_it_sleep(env):
    srv, bucket = env
    mk(srv, "tmp")
    srv.admin.put("/v1/databases/tmp/keep-awake")
    time.sleep(IDLE + 1)
    assert state(srv, "tmp")["state"] == "awake"
    st, r = srv.admin.delete("/v1/databases/tmp/keep-awake")
    assert st == 200 and r["keep_awake"] is False and r["keep_awake_lease"] is None
    assert list_prefix(bucket, "control/pins/tmp") == []
    wait_until(lambda: state(srv, "tmp")["state"] == "asleep", IDLE * 3 + 3, what="sleep after unpin")
    assert srv.admin.delete("/v1/databases/tmp/keep-awake")[0] == 200  # idempotent


def test_lease_is_renewed(env):
    srv, _ = env
    mk(srv, "lease")
    srv.admin.put("/v1/databases/lease/keep-awake")
    a = state(srv, "lease")["keep_awake_lease"]
    time.sleep(13)  # ttl 6 s, renewed every 2 s
    b = state(srv, "lease")["keep_awake_lease"]
    assert b["renewals"] >= a["renewals"] + 3, (a, b)
    assert b["expires_at_ms"] > a["expires_at_ms"] + 8000
    assert b["holder"] == srv.holder
    assert b["expires_at_ms"] > time.time() * 1000, "the lease must be valid now"


def test_pin_survives_graceful_restart(env):
    srv, _ = env
    c = mk(srv, "gr")
    srv.admin.put("/v1/databases/gr/keep-awake")
    srv.restart()
    wait_until(lambda: state(srv, "gr")["state"] == "awake", 10, what="re-woken after restart")
    assert state(srv, "gr")["keep_awake"] is True
    time.sleep(IDLE * 2 + 1)
    assert state(srv, "gr")["state"] == "awake"
    assert srv.client()["gr"]["c"].count_documents({}) == 100


def test_pin_survives_kill9_same_holder(env):
    srv, _ = env
    c = mk(srv, "k9")
    srv.admin.put("/v1/databases/k9/keep-awake")
    srv.kill9()
    srv.proc = None
    srv.start()
    wait_until(lambda: state(srv, "k9")["state"] == "awake", 15, what="re-woken after kill -9")
    time.sleep(IDLE * 2)
    assert state(srv, "k9")["state"] == "awake" and state(srv, "k9")["keep_awake"] is True
    assert srv.client()["k9"]["c"].count_documents({}) == 100


def test_pin_taken_over_by_another_process_after_kill9(env):
    srv, bucket = env
    c = mk(srv, "take")
    srv.admin.put("/v1/databases/take/keep-awake")
    srv.kill9()
    t0 = time.time()
    other = Server(bucket, "pin2", idle=IDLE, holder="another-holder", extra=["--pin-lease-ttl-secs", "6"]).start()
    try:
        wait_until(lambda: other.admin.get("/v1/databases/take?storage=0")[1]["keep_awake_lease"]["holder"] == "another-holder", 30, what="pin takeover")
        took = time.time() - t0
        wait_until(lambda: other.admin.get("/v1/databases/take?storage=0")[1]["state"] == "awake", 30, what="wake by the new holder")
        print("pin taken over after", round(took, 1), "s (lease ttl 6 s)")
        time.sleep(IDLE * 2)
        assert other.admin.get("/v1/databases/take?storage=0")[1]["state"] == "awake"
        assert other.client()["take"]["c"].count_documents({}) == 100
    finally:
        other.term()


def test_pin_released_at_clean_shutdown_so_the_successor_does_not_wait(env):
    srv, bucket = env
    mk(srv, "handover")
    srv.admin.put("/v1/databases/handover/keep-awake")
    srv.term()
    other = Server(bucket, "pin3", idle=IDLE, holder="successor", extra=["--pin-lease-ttl-secs", "60"]).start()
    try:
        t0 = time.time()
        wait_until(lambda: other.admin.get("/v1/databases/handover?storage=0")[1]["keep_awake_lease"]["holder"] == "successor", 15, what="handover")
        assert time.time() - t0 < 12  # not the 60 s ttl
    finally:
        other.term()


def test_errors_and_auth(env):
    srv, _ = env
    assert srv.admin.put("/v1/databases/ghost/keep-awake")[0] == 404
    assert srv.admin.delete("/v1/databases/ghost/keep-awake")[0] == 404
    mk(srv, "a1")
    assert srv.admin.put("/v1/databases/a1/keep-awake", token=None)[0] == 401
    assert srv.admin.put("/v1/databases/bad.name/keep-awake")[0] == 400


def test_erase_removes_the_pin_and_it_does_not_come_back(env):
    srv, bucket = env
    mk(srv, "gone")
    srv.admin.put("/v1/databases/gone/keep-awake")
    assert srv.admin.delete("/v1/databases/gone")[0] == 200
    time.sleep(4)  # several agent ticks
    for p in ("tenants/gone/", "control/pins/gone", "control/dbs/gone/", "control/ops/gone.json"):
        assert list_prefix(bucket, p) == [], p


def test_fork_of_a_pinned_source_leaves_it_awake(env):
    srv, _ = env
    c = mk(srv, "psrc", 300)
    srv.admin.put("/v1/databases/psrc/keep-awake")
    assert srv.admin.post("/v1/databases/psrc/fork", {"target": "pfork"})[0] == 201
    wait_until(lambda: state(srv, "psrc")["state"] == "awake", 8, what="source re-woken after the fork")
    assert state(srv, "pfork")["keep_awake"] is False
    time.sleep(IDLE * 2)
    assert state(srv, "psrc")["state"] == "awake"
    assert c["pfork"]["c"].count_documents({}) == 300


def test_pinned_database_keeps_serving_under_load_and_after_idle(env):
    srv, _ = env
    c = mk(srv, "load", 50)
    srv.admin.put("/v1/databases/load/keep-awake")
    stop = threading.Event()
    errs = []

    def w():
        cc = srv.client()
        i = 1000
        while not stop.is_set():
            try:
                cc["load"]["c"].insert_one({"_id": i})
            except Exception as e:
                errs.append(e)
            i += 1
            time.sleep(0.05)

    t = threading.Thread(target=w)
    t.start()
    time.sleep(IDLE * 2)
    stop.set()
    t.join()
    assert not errs
    time.sleep(IDLE * 3)
    assert state(srv, "load")["state"] == "awake"
