"""Operation 2, crash safety: kill -9 during a fork (at every step, and at random moments)."""
import random
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until, fill, dump_db

POINTS = ["fork.after_intent", "fork.after_markers", "fork.after_clone", "fork.after_marker", "fork.after_meta"]


def cp_ids(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/checkpoints")
    assert st == 200, r
    # slatedb also leaves an ephemeral checkpoint with a 5 minute expiry per clone; only checkpoints that never
    # expire can hold objects forever
    return sorted(c["id"] for c in r["checkpoints"] if c["expires_at_ms"] is None)


def assert_fork_absent(srv, bucket, target, ancestors=("src",)):
    assert srv.admin.get(f"/v1/databases/{target}")[0] == 404
    for prefix in [f"tenants/{target}/", f"ttl-hints/{target}/", f"control/dbs/{target}/", f"control/ops/{target}.json"] + [f"control/forks/{a}/{target}" for a in ancestors]:
        assert list_prefix(bucket, prefix) == [], (prefix, list_prefix(bucket, prefix))
    assert target not in [d["name"] for d in srv.client().list_databases()]


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_kill9_at_each_step_rolls_back(point, mode):
    bucket = make_bucket("forkcrash")
    srv = Server(bucket, "fc", idle=300, env={"CTL_FAILPOINT": point}).start()
    try:
        c = srv.client()
        fill(c, "src", "c", 800)
        c["src"]["c"].create_index([("grp", 1)])
        c["src"].command("tenantAdmin", close=True)
        ref = dump_db(c, "src")
        pins_before = cp_ids(srv, "src")
        res = {}
        th = threading.Thread(target=lambda: res.update(r=srv.admin.post("/v1/databases/src/fork", {"target": "kid", "mode": mode}, timeout=20)) or None)
        th.start()
        wait_until(lambda: f"FAILPOINT {point} reached" in srv.log_text(), 20, what="failpoint")
        srv.kill9()
        th.join(timeout=30)
        # restart: the interrupted fork is rolled back before any client is served
        srv.env = {}
        srv.start()
        log = srv.log_text()
        assert "ctl recovery: fork of kid was interrupted" in log and "ctl recovery: fork of kid finished" in log, log[-2000:]
        assert_fork_absent(srv, bucket, "kid", ancestors=("src",))
        # the source is intact, has no leaked pin, and is writable
        c = srv.client()
        assert dump_db(c, "src") == ref
        assert cp_ids(srv, "src") == pins_before, "a checkpoint pinned by the failed fork was left in the source"
        c["src"]["c"].insert_one({"_id": 99999})
        c["src"]["c"].delete_one({"_id": 99999})
        # and the fork can be retried
        st, r = srv.admin.post("/v1/databases/src/fork", {"target": "kid", "mode": mode})
        assert st == 201, r
        assert dump_db(c, "kid") == ref
    finally:
        srv.term()


def test_takeover_by_another_process_after_kill9():
    """The process that died held the source's lease; a different process (other holder id) can recover and serve
    the source once the lease has expired."""
    bucket = make_bucket("forkcrash2")
    srv = Server(bucket, "fc2", idle=300, env={"CTL_FAILPOINT": "fork.after_clone"}).start()
    c = srv.client()
    fill(c, "src", "c", 300)
    c["src"].command("tenantAdmin", close=True)
    res = {}
    th = threading.Thread(target=lambda: res.update(r=srv.admin.post("/v1/databases/src/fork", {"target": "kid"}, timeout=20)))
    th.start()
    wait_until(lambda: "FAILPOINT fork.after_clone reached" in srv.log_text(), 20, what="failpoint")
    srv.kill9()
    th.join(timeout=30)
    other = Server(bucket, "fc2b", idle=300, holder="someone-else").start()
    try:
        assert_fork_absent(other, bucket, "kid")
        # refused (retryable) while the dead process's lease is valid, served once it has expired (ttl 6 s)
        n = wait_until(lambda: other.client()["src"]["c"].count_documents({}), 20, what="takeover after lease expiry")
        assert n == 300
    finally:
        other.term()


def test_random_kill_loop():
    """Kill -9 at random moments of a fork, 15 times in a row; after each restart the fork is either complete and
    equal to the source or absent without a trace, and the source never loses a document."""
    bucket = make_bucket("forkcrash3")
    srv = Server(bucket, "fc3", idle=300).start()
    c = srv.client()
    fill(c, "src", "c", 3000)
    c["src"]["c"].create_index([("grp", 1)])
    c["src"].command("tenantAdmin", close=True)
    ref = dump_db(c, "src")
    random.seed(7)
    outcomes = {"complete": 0, "absent": 0}
    try:
        for i in range(15):
            res = {}
            th = threading.Thread(target=lambda: res.update(r=_post(srv, i)))
            th.start()
            time.sleep(random.uniform(0.0, 0.25))
            srv.kill9()
            th.join(timeout=30)
            srv.proc = None
            srv.start()
            c = srv.client()
            st, d = srv.admin.get(f"/v1/databases/k{i}")
            if st == 200:
                assert dump_db(c, f"k{i}") == ref, "a fork that exists must be complete"
                outcomes["complete"] += 1
            else:
                assert st == 404
                assert_fork_absent(srv, bucket, f"k{i}", ancestors=("src",))
                outcomes["absent"] += 1
            assert dump_db(c, "src") == ref
            assert srv.admin.get("/v1/databases/src/checkpoints")[0] == 200
        print("random kill outcomes", outcomes)
        assert sum(outcomes.values()) == 15
        # every surviving fork pins the source; nothing else does
        pins = cp_ids(srv, "src")
        assert len(pins) == outcomes["complete"], (pins, outcomes)
    finally:
        srv.term()


def _post(srv, i):
    try:
        return srv.admin.post("/v1/databases/src/fork", {"target": f"k{i}"}, timeout=10)
    except Exception as e:  # connection reset by the kill
        return ("error", str(e))
