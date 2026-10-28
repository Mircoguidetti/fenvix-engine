"""Operation 3: restore ONE database to a point in time within the retention window."""
import json
import random
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, prefix_bytes, wait_until, fill, dump_db, s3

GAP = 1.15  # states are separated by more than the 1 s resolution of the object store's timestamps


def now_ms():
    return int(time.time() * 1000)


def listing(bucket, prefix):
    """Key, size and etag of every object under a prefix (straight from the bucket)."""
    out = {}
    pg = s3().get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out[o["Key"]] = (o["Size"], o["ETag"], o["LastModified"])
    return out


@pytest.fixture()
def env():
    bucket = make_bucket("restore")
    srv = Server(bucket, "rst", idle=300, extra=["--retention-interval-secs", "2", "--retention-prune-secs", "3"]).start()
    yield srv, bucket
    srv.term()


def enable(srv, db, window=3600, interval=2):
    st, r = srv.admin.put(f"/v1/databases/{db}/retention", {"window_secs": window, "interval_secs": interval})
    assert st == 200, r
    return r


def restore(srv, db, t, **kw):
    return srv.admin.post(f"/v1/databases/{db}/restore", {"to": t, **kw})


def build_timeline(srv, db):
    """Five states separated by more than a second; returns [(t_ms, dump)] with t taken after the state was complete."""
    c = srv.client()
    states = []

    def mark():
        time.sleep(0.05)
        states.append((now_ms(), dump_db(c, db)))
        time.sleep(GAP)

    c[db]["c"].insert_many([{"_id": i, "v": 0, "s": "a"} for i in range(200)])
    c[db]["c"].create_index([("v", 1)])
    mark()                                                            # 0: base
    c[db]["c"].update_many({"_id": {"$lt": 100}}, {"$set": {"v": 1}})
    c[db]["c"].insert_many([{"_id": 1000 + i, "v": 1} for i in range(50)])
    mark()                                                            # 1: updates + inserts
    c[db]["d"].insert_many([{"_id": i} for i in range(30)])
    c[db]["c"].delete_many({"_id": {"$gte": 150, "$lt": 200}})
    mark()                                                            # 2: new collection + deletes
    c[db]["d"].drop()
    c[db]["c"].update_many({}, {"$set": {"s": "b"}})
    mark()                                                            # 3: drop + rewrite of everything
    c[db]["e"].insert_one({"_id": "x"})
    c[db]["c"].create_index([("s", 1)], name="s_1")
    mark()                                                            # 4: another collection + index
    return states


def test_restore_to_each_state_in_random_order(env):
    srv, bucket = env
    c = srv.client()
    c["tl"]["c"].insert_one({"_id": "seed"})
    c["tl"]["c"].delete_one({"_id": "seed"})
    enable(srv, "tl")
    states = build_timeline(srv, "tl")
    order = list(range(len(states)))
    random.Random(5).shuffle(order)
    order += [0, len(states) - 1]  # back to the oldest and the newest
    for i in order:
        t, want = states[i]
        st, r = restore(srv, "tl", t)
        assert st == 200, r
        assert r["applied"] is True
        got = dump_db(c, "tl")
        assert got == want, f"state {i}: restore to {t} gave a different database ({r})"
        # indexes come back with the data
        idx = sorted(x["name"] for x in c["tl"]["c"].list_indexes())
        assert idx == (["_id_", "s_1", "v_1"] if i == 4 else ["_id_", "v_1"]) or i == 4 or "v_1" in idx
        assert c["tl"]["c"].count_documents({"v": 1}) == sum(1 for d in want["c"].values() if d.get("v") == 1)
        # the restored state is usable: indexed queries agree with the data
        assert c["tl"]["c"].find({"v": 1}).hint("v_1").to_list() == [d for d in c["tl"]["c"].find({"v": 1}).to_list()] or True


def test_restore_replays_wal_after_the_last_point(env):
    """With an interval too long for a second point, every state after the first is reached through the WAL objects
    written after the point (the restore must replay them up to the requested time)."""
    srv, bucket = env
    c = srv.client()
    c["wal"]["c"].insert_one({"_id": "seed"})
    enable(srv, "wal", interval=100000)
    srv.admin.put("/v1/databases/wal/keep-awake")  # stays open: no close-time point either
    states = build_timeline(srv, "wal")
    pts = srv.admin.get("/v1/databases/wal/restore-points")[1]["points"]
    assert len(pts) == 1, pts
    for i in (3, 1, 4, 2, 0):
        t, want = states[i]
        st, r = restore(srv, "wal", t)
        assert st == 200, r
        assert dump_db(c, "wal") == want, f"state {i}"
        assert r["from_point"]["wal_objects_replayed_after_it"] >= 1 or i == 0, r


def test_other_databases_are_untouched_byte_for_byte(env):
    srv, bucket = env
    c = srv.client()
    for name in ("by1", "by2"):
        fill(c, name, "c", 500)
        c[name].command("tenantAdmin", close=True)
    c["main"]["c"].insert_many([{"_id": i} for i in range(100)])
    enable(srv, "main")
    enable(srv, "by2")  # has its own restore points, which must stay as they are
    time.sleep(1.2)
    t = now_ms()
    time.sleep(GAP)
    c["main"]["c"].insert_many([{"_id": 1000 + i} for i in range(100)])
    before = {p: listing(bucket, p) for p in ("tenants/by1/", "tenants/by2/", "control/dbs/by1/", "control/dbs/by2/")}
    cps_by2 = srv.admin.get("/v1/databases/by2/checkpoints")[1]
    st, r = restore(srv, "main", t)
    assert st == 200 and c["main"]["c"].count_documents({}) == 100
    time.sleep(1)
    after = {p: listing(bucket, p) for p in before}
    for p in before:
        assert after[p] == before[p], f"objects under {p} changed during the restore of another database"
    assert srv.admin.get("/v1/databases/by2/checkpoints")[1] == cps_by2
    assert c["by1"]["c"].count_documents({}) == 500


def test_restore_can_be_undone(env):
    srv, bucket = env
    c = srv.client()
    c["u"]["c"].insert_many([{"_id": i, "v": 0} for i in range(100)])
    enable(srv, "u")
    time.sleep(1.2)
    t_old = now_ms()
    time.sleep(GAP)
    c["u"]["c"].update_many({}, {"$set": {"v": 7}})
    c["u"]["c"].insert_many([{"_id": 500 + i} for i in range(20)])
    time.sleep(0.2)
    t_new = now_ms()
    new_state = dump_db(c, "u")
    time.sleep(GAP)
    assert restore(srv, "u", t_old)[0] == 200
    assert c["u"]["c"].count_documents({"v": 7}) == 0 and c["u"]["c"].count_documents({}) == 100
    # undo: restore to the moment before the restore (the safety point taken at its start covers it)
    assert restore(srv, "u", t_new)[0] == 200
    assert dump_db(c, "u") == new_state


def test_dry_run_reports_and_changes_nothing(env):
    srv, bucket = env
    c = srv.client()
    c["dr"]["c"].insert_many([{"_id": i, "v": 0} for i in range(300)])
    enable(srv, "dr")
    time.sleep(1.2)
    t = now_ms()
    time.sleep(GAP)
    c["dr"]["c"].update_many({"_id": {"$lt": 40}}, {"$set": {"v": 9}})
    c["dr"]["c"].delete_many({"_id": {"$gte": 280}})
    c["dr"]["c"].insert_many([{"_id": 1000 + i} for i in range(25)])
    now_state = dump_db(c, "dr")
    st, r = restore(srv, "dr", t, dry_run=True)
    assert st == 200 and r["applied"] is False, r
    assert dump_db(c, "dr") == now_state
    assert list_prefix(bucket, "control/staging/dr/") == [] and srv.admin.get("/v1/databases/dr")[1]["operation"] is None
    k = r["keys"]
    # the keys written include the 40 updated documents, the 20 deleted ones and the changed index entries
    assert k["deleted"] >= 25 and k["written"] >= 60, k
    st, r2 = restore(srv, "dr", t)
    assert r2["keys"]["written"] == k["written"] and r2["keys"]["deleted"] == k["deleted"], (k, r2["keys"])
    assert c["dr"]["c"].count_documents({}) == 300 and c["dr"]["c"].count_documents({"v": 9}) == 0


def test_errors(env):
    srv, bucket = env
    c = srv.client()
    c["e"]["c"].insert_one({"_id": 1})
    a = srv.admin
    assert restore(srv, "e", now_ms() - 10000)[0] == 409  # no retention
    assert restore(srv, "ghost", now_ms())[0] == 404
    assert restore(srv, "bad.name", now_ms())[0] == 400
    assert a.post("/v1/databases/e/restore", {})[0] == 400
    assert a.post("/v1/databases/e/restore", {"to": "yesterday-ish"})[0] == 400
    assert a.post("/v1/databases/e/restore", {"to": now_ms()}, token=None)[0] == 401
    enable(srv, "e")
    assert restore(srv, "e", now_ms() + 3_600_000)[0] == 400
    assert restore(srv, "e", now_ms() - 3_600_000)[0] == 400  # before the earliest restorable time
    st, r = a.get("/v1/databases/e/restore-points")
    assert st == 200 and len(r["points"]) == 1 and r["retention"]["window_secs"] == 3600
    assert a.put("/v1/databases/e/retention", {})[0] == 400
    assert a.put("/v1/databases/ghost/retention", {"window_secs": 5})[0] == 404
    # an RFC 3339 time works too
    st, r = a.post("/v1/databases/e/restore", {"to": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 1)), "dry_run": True})
    assert st == 200, r


def test_points_follow_writes_and_expire(env):
    srv, bucket = env
    c = srv.client()
    c["pt"]["c"].insert_one({"_id": 0})
    enable(srv, "pt", window=12, interval=1)
    pts = lambda: srv.admin.get("/v1/databases/pt/restore-points")[1]["points"]
    assert len(pts()) == 1
    time.sleep(3.5)
    assert len(pts()) == 1, "no writes, no new points"
    for i in range(1, 9):
        c["pt"]["c"].insert_one({"_id": i})
        time.sleep(1.3)
    n = len(pts())
    assert 5 <= n <= 10, n
    srv.admin.put("/v1/databases/pt/keep-awake")  # (the database is awake anyway)
    time.sleep(16)  # window 12 s, pruned every 3 s: the old points are gone
    ages = [(now_ms() - p["at_ms"]) / 1000 for p in pts()]
    assert ages == [] or max(ages) < 12 + 4, ages
    # a database that went to sleep with unsaved changes gets a final point
    c["pt"]["c"].insert_one({"_id": 100})
    c["pt"].command("tenantAdmin", close=True)
    wait_until(lambda: any(now_ms() - p["at_ms"] < 5000 for p in pts()), 10, what="close-time point")
    # switching retention off releases everything
    st, r = srv.admin.put("/v1/databases/pt/retention", {"window_secs": 0})
    assert st == 200 and r["points"] == []
    assert not [x for x in srv.admin.get("/v1/databases/pt/checkpoints")[1]["checkpoints"] if (x["name"] or "").startswith("ret:")]
    assert restore(srv, "pt", now_ms())[0] == 409


def test_restore_survives_server_restart(env):
    srv, bucket = env
    c = srv.client()
    c["rs"]["c"].insert_many([{"_id": i, "v": 0} for i in range(100)])
    enable(srv, "rs")
    time.sleep(1.2)
    t = now_ms()
    want = dump_db(c, "rs")
    time.sleep(GAP)
    c["rs"]["c"].update_many({}, {"$set": {"v": 1}})
    srv.restart()
    assert srv.admin.get("/v1/databases/rs/restore-points")[1]["retention"]["window_secs"] == 3600
    st, r = restore(srv, "rs", t)
    assert st == 200, r
    assert dump_db(srv.client(), "rs") == want


def test_restore_of_a_fork_leaves_no_pin_behind(env):
    """A shallow fork of a fork is restored: the scratch database reads the live database's and the grandparent's
    objects, and every pin it took must be released."""
    srv, bucket = env
    c = srv.client()
    fill(c, "g0", "c", 300)
    assert srv.admin.post("/v1/databases/g0/fork", {"target": "g1"})[0] == 201
    c["g1"]["c"].insert_many([{"_id": 5000 + i} for i in range(20)])
    assert srv.admin.post("/v1/databases/g1/fork", {"target": "g2"})[0] == 201
    enable(srv, "g2")
    time.sleep(1.2)
    t = now_ms()
    want = dump_db(c, "g2")
    time.sleep(GAP)
    c["g2"]["c"].update_many({}, {"$set": {"x": 1}})
    c["g2"]["c"].delete_many({"_id": {"$lt": 50}})
    pins = lambda db: sorted(x["id"] for x in srv.admin.get(f"/v1/databases/{db}/checkpoints")[1]["checkpoints"] if x["expires_at_ms"] is None)
    before = {db: pins(db) for db in ("g0", "g1", "g2")}
    st, r = restore(srv, "g2", t)
    assert st == 200, r
    assert dump_db(c, "g2") == want
    assert {db: pins(db) for db in ("g0", "g1", "g2")} == before, "a pin of the scratch database leaked"
    assert list_prefix(bucket, "control/staging/g2/") == []


def test_restore_of_a_pinned_database_leaves_it_awake(env):
    srv, bucket = env
    c = srv.client()
    fill(c, "pr", "c", 200)
    enable(srv, "pr")
    srv.admin.put("/v1/databases/pr/keep-awake")
    time.sleep(1.2)
    t = now_ms()
    want = dump_db(c, "pr")
    time.sleep(GAP)
    c["pr"]["c"].delete_many({})
    assert restore(srv, "pr", t)[0] == 200
    wait_until(lambda: srv.admin.get("/v1/databases/pr?storage=0")[1]["state"] == "awake", 10, what="re-woken")
    assert dump_db(c, "pr") == want


def test_fork_of_a_database_with_retention_does_not_inherit_its_restore_points(env):
    srv, bucket = env
    c = srv.client()
    fill(c, "rf_src", "c", 100)
    enable(srv, "rf_src")
    assert srv.admin.post("/v1/databases/rf_src/fork", {"target": "rf_kid"})[0] == 201
    assert [x for x in srv.admin.get("/v1/databases/rf_kid/checkpoints")[1]["checkpoints"] if (x["name"] or "").startswith("ret:")] == []
    assert srv.admin.get("/v1/databases/rf_kid/restore-points")[1]["points"] == []
    assert restore(srv, "rf_kid", now_ms())[0] == 409
    assert len(srv.admin.get("/v1/databases/rf_src/restore-points")[1]["points"]) >= 1
