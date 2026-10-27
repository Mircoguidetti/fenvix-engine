"""Operation 7: usage per database (storage bytes, requests per day, object-store GET/PUT counts, wake events)."""
import json
import time

import pytest

from lib import Server, make_bucket, list_prefix, prefix_bytes, wait_until, fill, s3, MINIO
from proxy import CountingProxy

IDLE = 3


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("usage")
    proxy = CountingProxy()
    srv = Server(bucket, "usage", idle=IDLE, extra=["--usage-flush-secs", "1"], endpoint=proxy.endpoint).start()
    yield srv, bucket, proxy
    srv.term()
    proxy.stop()


def usage(srv, db, **q):
    qs = "&".join(f"{k}={v}" for k, v in q.items())
    st, r = srv.admin.get(f"/v1/databases/{db}/usage" + ("?" + qs if qs else ""))
    assert st == 200, r
    return r


def today(r):
    return r["days"][-1]


def test_commands_per_day_are_counted_exactly(env):
    srv, _, _ = env
    c = srv.client()
    db = c["u1"]
    for i in range(3):
        db.command({"insert": "c", "documents": [{"_id": i}]})
    for _ in range(5):
        db.command({"find": "c", "filter": {}})
    db.command({"count": "c"})
    db.command({"update": "c", "updates": [{"q": {"_id": 0}, "u": {"$set": {"x": 1}}}]})
    db.command({"delete": "c", "deletes": [{"q": {"_id": 1}, "limit": 1}]})
    c["other_db"].command({"insert": "c", "documents": [{"_id": 1}]})
    time.sleep(1.5)
    r = usage(srv, "u1")
    d = today(r)
    assert d["requests"]["by_command"] == {"insert": 3, "find": 5, "count": 1, "update": 1, "delete": 1}, d["requests"]
    assert d["requests"]["commands"] == 11
    assert r["totals"]["commands"] == 11
    # another database's commands are not mixed in
    r2 = usage(srv, "other_db")
    assert today(r2)["requests"]["by_command"] == {"insert": 1}


def test_object_store_counts_match_an_independent_proxy(env):
    srv, bucket, proxy = env
    c = srv.client()
    db = "u2"
    proxy.reset()
    fill(c, db, "c", 3000)
    c[db].command("tenantAdmin", close=True)
    for _ in range(3):
        c[db]["c"].count_documents({"grp": 3})
        c[db]["c"].find_one({"_id": 17})
        c[db].command("tenantAdmin", close=True)
    c[db]["c"].insert_many([{"_id": 10000 + i, "pad": "z" * 500} for i in range(500)])
    c[db].command("tenantAdmin", close=True)
    time.sleep(1.5)
    r = usage(srv, db, storage=0)
    ours = today(r)["object_store"]
    truth = proxy.snapshot()
    pre = truth["by_prefix"]
    mine = {k: v for k, v in pre.items() if k.startswith(f"tenants/{db}")}
    kinds = {}
    for v in mine.values():
        for k, n in v.items():
            kinds[k] = kinds.get(k, 0) + n
    print("ours", ours, "proxy", kinds)
    assert ours["get"] == kinds.get("GET", 0)
    assert ours["get_range"] == kinds.get("GET_RANGE", 0)
    assert ours["head"] == kinds.get("HEAD", 0)
    assert ours["put"] == kinds.get("PUT", 0) + kinds.get("PUT_PART", 0) + kinds.get("MP_CREATE", 0) + kinds.get("MP_COMPLETE", 0)
    assert ours["list"] == kinds.get("LIST", 0)
    assert ours["get_tier_requests"] == ours["get"] + ours["get_range"] + ours["head"] > 20
    assert ours["put_tier_requests"] == ours["put"] + ours["copy"] + ours["list"]
    assert ours["put_bytes"] > 100_000 and ours["get_bytes"] > 10_000
    bb = {k: v for k, v in truth["bytes_by_prefix"].items() if k.startswith(f"tenants/{db}")}
    sent = sum(v.get("PUT:in", 0) + v.get("PUT_PART:in", 0) for v in bb.values())
    got = sum(v.get("GET:out", 0) + v.get("GET_RANGE:out", 0) for v in bb.values())
    assert ours["put_bytes"] == sent, (ours["put_bytes"], sent)
    assert ours["get_bytes"] == got, (ours["get_bytes"], got)


def test_wake_events_with_first_request_latency(env):
    srv, _, _ = env
    c = srv.client()
    db = "u3"
    fill(c, db, "c", 500)
    lat = []
    for i in range(3):
        c[db].command("tenantAdmin", close=True)
        wait_until(lambda: srv.admin.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 5, what="asleep")
        t0 = time.time()
        c[db]["c"].find_one({"_id": 1})
        lat.append((time.time() - t0) * 1000)
    time.sleep(1.5)
    r = usage(srv, db, storage=0)
    w = today(r)["wakes"]
    # the first insert created the database (a wake of its own), then three wakes by `find`
    cmds = [e["command"] for e in w["events"]]
    assert w["count"] == 4 and cmds == ["insert", "find", "find", "find"], w
    for e, client_ms in zip(w["events"][1:], lat):
        assert 1 <= e["first_request_latency_ms"] <= client_ms + 5, (e, client_ms)
        assert e["first_request_latency_ms"] >= client_ms * 0.5 - 20, (e, client_ms)  # the server-side view is most of what the client saw
    assert r["totals"]["wakes"] == 4
    # a request to an awake database is not a wake
    c[db]["c"].find_one({"_id": 2})
    c[db]["c"].find_one({"_id": 3})
    time.sleep(1.5)
    assert usage(srv, db, storage=0)["totals"]["wakes"] == 4


def test_storage_bytes(env):
    srv, bucket, _ = env
    c = srv.client()
    fill(c, "u4", "c", 4000)
    c["u4"].command("tenantAdmin", close=True)
    time.sleep(2.5)
    r = usage(srv, "u4")
    assert r["storage_bytes_now"] == prefix_bytes(bucket, "tenants/u4/")
    sampled = today(r)["storage_bytes_sampled"]
    assert sampled is not None and 0 < sampled["bytes"] <= r["storage_bytes_now"]
    # grows with data
    fill(c, "u4", "c", 4000, start=4000)
    c["u4"].command("tenantAdmin", close=True)
    r2 = usage(srv, "u4")
    assert r2["storage_bytes_now"] > r["storage_bytes_now"] and r2["storage_bytes_now"] == prefix_bytes(bucket, "tenants/u4/")


def test_last_active_and_listing(env):
    srv, _, _ = env
    c = srv.client()
    c["u5"]["c"].insert_one({"_id": 1})
    a = srv.admin.get("/v1/databases/u5?storage=0")[1]["last_active_at_ms"]
    time.sleep(1.1)
    c["u5"]["c"].find_one({})
    b = srv.admin.get("/v1/databases/u5?storage=0")[1]["last_active_at_ms"]
    assert b >= a + 1000


def test_counters_survive_restart_and_kill9(env):
    srv, bucket, proxy = env
    c = srv.client()
    for i in range(20):
        c["u6"].command({"insert": "c", "documents": [{"_id": i}]})
    time.sleep(2.5)  # flushed
    before = today(usage(srv, "u6", storage=0))["requests"]["commands"]
    assert before == 20
    srv.restart()
    c = srv.client()
    for i in range(20, 25):
        c["u6"].command({"insert": "c", "documents": [{"_id": i}]})
    time.sleep(1.5)
    r = usage(srv, "u6", storage=0)
    assert today(r)["requests"]["commands"] == 25, today(r)["requests"]
    assert today(r)["processes"] == 2  # one record per process incarnation
    # kill -9: at most the last flush interval (1 s) is lost
    for i in range(25, 35):
        c["u6"].command({"insert": "c", "documents": [{"_id": i}]})
    time.sleep(2.0)
    srv.kill9()
    srv.proc = None
    srv.start()
    after = today(usage(srv, "u6", storage=0))["requests"]["commands"]
    assert after == 35, after  # all ten had been flushed
    c = srv.client()
    c["u6"].command({"insert": "c", "documents": [{"_id": 99}]})
    c["u6"].command({"insert": "c", "documents": [{"_id": 100}]})
    srv.kill9()  # inside the flush interval: these two may be lost
    srv.proc = None
    srv.start()
    lost = 37 - today(usage(srv, "u6", storage=0))["requests"]["commands"]
    print("commands lost by a kill -9 within the flush interval:", lost)
    assert 0 <= lost <= 2


def test_usage_errors_and_auth(env):
    srv, _, _ = env
    a = srv.admin
    assert a.get("/v1/databases/nope/usage")[0] == 404
    assert a.get("/v1/databases/u1/usage", token=None)[0] == 401
    assert a.get("/v1/databases/u1/usage?from=2026-13-40")[0] == 400
    assert a.get("/v1/databases/u1/usage?from=2026-10-27&to=2026-10-01")[0] == 400
    assert a.get("/v1/databases/bad.name/usage")[0] == 400


def test_several_processes_and_days_are_summed(env):
    """Usage files from other processes and other days are summed per day; a range selects days."""
    srv, bucket, _ = env
    c = srv.client()
    c["u7"]["c"].insert_one({"_id": 1})
    time.sleep(1.5)
    client = s3()

    def put(day, name, commands, gets):
        doc = {"db": "u7", "day": day, "holder": name, "nonce": 1, "object_store": {"get": gets, "get_range": 0, "head": 0, "put": 2, "copy": 0, "list": 1, "delete": 0, "get_bytes": 1000, "put_bytes": 500},
               "commands": commands, "by_command": {"find": commands}, "wake_total": 1, "wakes": [{"at_ms": 1000, "first_request_latency_ms": 12, "command": "find"}], "storage": {"sampled_at_ms": 5, "bytes": 777}}
        client.put_object(Bucket=bucket, Key=f"control/usage/u7/{day}/{name}.json", Body=json.dumps(doc).encode())

    put("2020-01-01", "procA-00000001", 10, 4)
    put("2020-01-01", "procB-00000002", 5, 6)
    put("2020-01-02", "procA-00000001", 7, 1)
    r = usage(srv, "u7", **{"from": "2020-01-01", "to": "2020-01-02", "storage": 0})
    days = {d["day"]: d for d in r["days"]}
    assert days["2020-01-01"]["requests"]["commands"] == 15 and days["2020-01-01"]["object_store"]["get"] == 10 and days["2020-01-01"]["processes"] == 2
    assert days["2020-01-01"]["wakes"]["count"] == 2 and len(days["2020-01-01"]["wakes"]["events"]) == 2
    assert days["2020-01-02"]["requests"]["commands"] == 7
    assert r["totals"]["commands"] == 22 and r["totals"]["object_store"]["get"] == 11 and r["totals"]["wakes"] == 3
    r = usage(srv, "u7", **{"from": "2020-01-02", "to": "2020-01-02", "storage": 0})
    assert [d["day"] for d in r["days"]] == ["2020-01-02"] and r["totals"]["commands"] == 7


def test_two_server_processes_on_one_bucket():
    bucket = make_bucket("usage2")
    a = Server(bucket, "ua", idle=IDLE, extra=["--usage-flush-secs", "1"]).start()
    b = Server(bucket, "ub", idle=IDLE, extra=["--usage-flush-secs", "1"]).start()
    try:
        ca = a.client()
        ca["shared"]["c"].insert_one({"_id": 0})
        for i in range(1, 5):
            ca["shared"].command({"insert": "c", "documents": [{"_id": i}]})
        wait_until(lambda: a.admin.get("/v1/databases/shared?storage=0")[1]["state"] == "asleep", 10, what="a lets go")
        cb = b.client()
        for i in range(5, 8):
            cb["shared"].command({"insert": "c", "documents": [{"_id": i}]})
        time.sleep(2)
        for srv in (a, b):
            r = usage(srv, "shared", storage=0)
            assert r["totals"]["commands"] == 8, r["totals"]
            assert today(r)["processes"] == 2
    finally:
        a.term()
        b.term()


def test_day_rollover():
    """The counters of a database are split at UTC midnight: run a server whose day clock crosses it 6 s after start."""
    bucket = make_bucket("usage3")
    DAY = 86_400_000
    off = DAY - (int(time.time() * 1000) % DAY) - 6000
    srv = Server(bucket, "ud", idle=IDLE, extra=["--usage-flush-secs", "1"], env={"CTL_USAGE_DAY_OFFSET_MS": str(off)}).start()
    try:
        c = srv.client()
        for i in range(4):
            c["roll"].command({"insert": "c", "documents": [{"_id": i}]})
        time.sleep(7.5)
        for i in range(4, 7):
            c["roll"].command({"insert": "c", "documents": [{"_id": i}]})
        time.sleep(1.5)
        r = srv.admin.get("/v1/databases/roll/usage?storage=0")[1]
        assert len(r["days"]) == 2, r["days"]
        assert [d["requests"]["commands"] for d in r["days"]] == [4, 3]
        assert r["days"][0]["day"] < r["days"][1]["day"]
    finally:
        srv.term()
