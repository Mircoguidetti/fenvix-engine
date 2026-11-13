"""P-big-speed (2026-11-12), fixes 1-4 seen from a client, on the LangGraph saver's own shapes.

The server runs with `FENVIX_SIM_S3=25,0,10000`: every object-store GET waits 25 ms, so a trip (a wave of GETs sent
together, serverStatus fenvix.waiter.trips) is a real wait and two GETs one after the other are two trips.

  - fix 3, the customer's box: after a restart, a customer's FIRST request (the saver's resume: the newest checkpoint,
    then its pending writes) costs one trip; every NEXT request of that customer (list, last 10, time travel, writes,
    the resume again) sends no GET; every answer equals what was written. Control (`--customer-box false`): the same
    FIRST request costs more than one trip.
  - fix 4: a save (an upsert) as the first request of a customer costs at most one trip, and the next save sends no GET.
    The read before the upsert stays: `$setOnInsert` keeps a first value (the saver's pending writes rely on it), so
    the server must know whether the document exists. Shown here: the second upsert keeps the first value.
  - fix 2: a find with a limit on a unique index runs no tie-completion scan; on a non-unique index it does (control).
  - fix 1: `--memory-limit-mb 1024` gives 256 MiB of caches (WiredTiger's rule at 1 GiB), reported in serverStatus.
"""
import time

import pytest
from bson import ObjectId

from lib import Server, make_bucket

SIM = {"FENVIX_SIM_S3": "25,0,10000"}
THREADS = 24
STEPS = 12


def fx(c):
    return c.admin.command("serverStatus")["fenvix"]


def gets(c):
    s = fx(c)["s3"]
    return s["get"] + s["getRange"]


def trips(c):
    return fx(c)["waiter"]["trips"]


def tid(i):
    return f"user{i:03d}-thread{i % 3}"


def load(db):
    db.checkpoints.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1)], unique=True)
    db.checkpoint_writes.create_index([("thread_id", 1), ("checkpoint_ns", 1), ("checkpoint_id", 1), ("task_id", 1), ("idx", 1)], unique=True)
    want = {}
    for i in range(THREADS):
        t = tid(i)
        for s in range(STEPS):
            cid = f"1ef{s:05d}-{i:04d}"
            body = ("message %d of %s " % (s, t)) * (40 + s * 8)
            db.checkpoints.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid},
                                      {"$set": {"parent_checkpoint_id": None, "type": "json", "checkpoint": body, "metadata": {"step": s}}}, upsert=True)
            for k in range(3):
                db.checkpoint_writes.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid, "task_id": f"task{k}", "task_path": "", "idx": k},
                                                {"$setOnInsert": {"channel": "messages", "type": "json", "value": f"{t}:{s}:{k}"}}, upsert=True)
            want[(t, cid)] = body
    return want


def resume(db, t):
    """The saver's get_tuple: the newest checkpoint, then its pending writes."""
    doc = next(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1))
    ws = list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": doc["checkpoint_id"]}))
    return doc, ws


def check_resume(want, t, doc, ws):
    cids = sorted(c for (tt, c) in want if tt == t)
    assert doc["checkpoint_id"] == cids[-1] and doc["checkpoint"] == want[(t, cids[-1])], f"{t}: wrong newest checkpoint"
    assert sorted(w["value"] for w in ws) == [f"{t}:{STEPS - 1}:{k}" for k in range(3)], f"{t}: wrong pending writes"


def warm_up_db(c, db):
    """Open the database (a request of a customer not measured) and let the maps pinned at open settle: the first
    request after a restart also opens the database, which is not a customer's cost."""
    db.other.find_one({"x": 1})
    resume(db, tid(THREADS - 1))
    last = None
    for _ in range(100):
        f = fx(c)["customerBox"]
        cur = (f["sstMapsPinned"], f["runPinsStarted"], fx(c)["s3"]["get"])
        if cur == last:
            return
        last = cur
        time.sleep(0.3)


@pytest.fixture(scope="module")
def loaded():
    bucket = make_bucket("bigspeed")
    s = Server(bucket, "bigspeed", idle=600, env=SIM).start()
    c = s.client()
    want = load(c.app)
    c.close()
    s.term()
    return bucket, want


def test_first_request_one_trip_next_requests_no_get(loaded):
    bucket, want = loaded
    s = Server(bucket, "bigspeed", idle=600, env=SIM).start()
    try:
        c = s.client()
        db = c.app
        warm_up_db(c, db)
        firsts, nexts = [], []
        for i in range(0, THREADS - 1, 2):
            t = tid(i)
            t0, g0 = trips(c), gets(c)
            doc, ws = resume(db, t)
            firsts.append((t, trips(c) - t0, gets(c) - g0))
            check_resume(want, t, doc, ws)
            g1 = gets(c)
            # the NEXT requests of the same customer
            hist = list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)]))
            assert [d["checkpoint_id"] for d in hist] == sorted((cc for (tt, cc) in want if tt == t), reverse=True)
            last10 = list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=10))
            assert len(last10) == 10
            old = sorted(cc for (tt, cc) in want if tt == t)[3]
            tt_doc = db.checkpoints.find_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": old})
            assert tt_doc["checkpoint"] == want[(t, old)]
            assert len(list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": old}))) == 3
            doc2, ws2 = resume(db, t)
            check_resume(want, t, doc2, ws2)
            nexts.append((t, gets(c) - g1))
        print("FIRST (thread, trips, GETs):", firsts)
        print("NEXT (thread, GETs):", nexts)
        assert all(tr == 1 for _, tr, _ in firsts), f"a FIRST request took more than one trip: {firsts}"
        assert all(g == 0 for _, g in nexts), f"a NEXT request sent a GET: {nexts}"
        box = fx(c)["customerBox"]
        assert box["warms"] >= len(firsts) and box["warmErrors"] == 0
    finally:
        s.term()


def test_control_without_the_box_first_request_takes_more_trips(loaded):
    bucket, want = loaded
    s = Server(bucket, "bigspeed", idle=600, env=SIM, extra=["--customer-box", "false"]).start()
    try:
        c = s.client()
        db = c.app
        warm_up_db(c, db)
        firsts = []
        for i in range(1, THREADS - 1, 2):
            t = tid(i)
            t0 = trips(c)
            doc, ws = resume(db, t)
            firsts.append(trips(c) - t0)
            check_resume(want, t, doc, ws)
        print("control FIRST trips:", firsts)
        assert max(firsts) > 1, f"control: the cold resume took {firsts} trips without the box (the test could not fail)"
    finally:
        s.term()


def test_save_first_one_trip_next_no_get_and_set_on_insert_keeps_the_first(loaded):
    bucket, want = loaded
    s = Server(bucket, "bigspeed", idle=600, env=SIM).start()
    try:
        c = s.client()
        db = c.app
        warm_up_db(c, db)
        # the database's writer opens at its first write (a per-database cost, not a customer's: in the bench the database
        # is always being written): a write of another customer first
        # (and our manifest's announcement is refreshed inside a write at most every T_pin, another per-database cost):
        # writes of another customer to both collections first, right before the measured save
        other = tid(THREADS - 1)
        db.checkpoints.update_one({"thread_id": other, "checkpoint_ns": "", "checkpoint_id": "1ef99999-other"}, {"$set": {"checkpoint": "x"}}, upsert=True)
        db.checkpoint_writes.update_one({"thread_id": other, "checkpoint_ns": "", "checkpoint_id": "1ef99999-other", "task_id": "t", "task_path": "", "idx": 0},
                                        {"$setOnInsert": {"value": "o"}}, upsert=True)
        t = tid(5)
        t0, g0 = trips(c), gets(c)
        db.checkpoints.update_one({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": "1ef99999-new"},
                                  {"$set": {"type": "json", "checkpoint": "new", "metadata": {}}}, upsert=True)
        first = (trips(c) - t0, gets(c) - g0)
        g1 = gets(c)
        f = {"thread_id": t, "checkpoint_ns": "", "checkpoint_id": "1ef99999-new", "task_id": "t", "task_path": "", "idx": 0}
        db.checkpoint_writes.update_one(f, {"$setOnInsert": {"value": "first"}}, upsert=True)
        db.checkpoint_writes.update_one(f, {"$setOnInsert": {"value": "second"}}, upsert=True)
        nxt = gets(c) - g1
        print("save FIRST (trips, GETs):", first, "NEXT GETs:", nxt)
        assert first[0] <= 1, f"the first save of a customer took {first[0]} trips"
        assert nxt == 0, f"the next saves sent {nxt} GETs"
        # the reason the read before an upsert stays (fix 4): the second upsert must find the first document
        assert db.checkpoint_writes.find_one(f)["value"] == "first"
        assert db.checkpoint_writes.count_documents(f) == 1
    finally:
        s.term()


def test_no_tie_scan_on_a_unique_index(loaded):
    bucket, _ = loaded
    s = Server(bucket, "bigspeed", idle=600).start()
    try:
        c = s.client()
        db = c.app
        n0 = fx(c)["customerBox"]["indexTieScans"]
        for i in range(THREADS):
            list(db.checkpoints.find({"thread_id": tid(i), "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1))
        n1 = fx(c)["customerBox"]["indexTieScans"]
        assert n1 == n0, f"{n1 - n0} tie scans on the unique index"
        # control: the same reads on a non-unique index do complete ties
        db.plain.insert_many([{"k": i % 4, "v": i} for i in range(200)])
        db.plain.create_index([("k", 1)])
        for k in range(4):
            list(db.plain.find({"k": k}, sort=[("k", -1)], limit=2))
        assert fx(c)["customerBox"]["indexTieScans"] > n1, "control: no tie scan on a non-unique index"
    finally:
        s.term()


def test_memory_budget_from_the_limit():
    s = Server(make_bucket("membudget"), "membudget", idle=600, extra=["--memory-limit-mb", "1024"]).start()
    try:
        c = s.client()
        b = fx(c)["memoryBudget"]
        assert b["source"] == "flag" and b["limitBytes"] == 1 << 30
        assert b["cachesBytes"] == 256 << 20, b
        assert b["blockCacheBytes"] + b["metaCacheBytes"] + b["sealedCacheBytes"] == b["cachesBytes"]
        assert b["memtableBytes"] == 8 << 20
        assert "memory budget: limit 1024 MiB (flag), caches 256 MiB" in s.log_text()
    finally:
        s.term()
