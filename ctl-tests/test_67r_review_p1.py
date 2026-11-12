"""From the review of fix-query-defects 62b8619 (sessions/2026-11-12-review-fix-query-defects/evidence/test_r_p1.py). Review of fix-query-defects, P1: a change stream must deliver a gapless prefix of its events, then either all of them
# (P-durable-changelog: these tests cover the in-memory log, so their servers run with --change-history off; the
# durable history behind the same scenarios is test_75_durable_change_history.py.)
or ChangeStreamHistoryLost (286); never a later event after a gap. Scenarios the coordinator asked for:
- several streams on one collection reading at different speeds while the log trims;
- a customer erasure between the writes and the reads (customer-keyed collection);
- startAtOperationTime held, trimmed, and from before the process started (after a restart);
- a takeover: the stream's process is killed and another process serves the database (resume token, start time).
Run from ctl-tests (it uses lib.py): FQ_BINARY=<mongo-server> python -m pytest -q test_r_p1.py
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.environ.get("CTL_TESTS", "/home/user/fqd/ctl-tests"))
import pytest
from bson import SON
from pymongo.errors import OperationFailure

from lib import Server, make_bucket

BIN = os.environ.get("FQ_BINARY")
LOG_MB = 4


PORTS = iter(range(27281, 27400))


def rs(port):
    # as the judge and real deployments run it: a replica set member, so writes return operationTime
    return ["--replica-set", "rs0", "--advertise", f"localhost:{port}"]


def server(tag, extra=(), holder=None):
    b = make_bucket(tag)
    p = next(PORTS)
    return Server(b, tag, idle=600, port=p, extra=["--change-history", "off", "--change-log-max-mb", str(LOG_MB)] + rs(p) + list(extra), binary=BIN, holder=holder).start()


def open_stream(d, coll, **opts):
    r = d.command(SON([("aggregate", coll), ("pipeline", [{"$changeStream": opts}]), ("cursor", {})]))
    return r["cursor"]["id"], [e["documentKey"]["_id"] for e in r["cursor"]["firstBatch"]]


def drain(d, coll, cid, want, secs=20, pause=0.0):
    got, t0 = [], time.time()
    while time.time() - t0 < secs:
        try:
            g = d.command(SON([("getMore", cid), ("collection", coll), ("maxTimeMS", 300)]))
        except OperationFailure as e:
            return got, e.code
        got += [e["documentKey"]["_id"] for e in g["cursor"]["nextBatch"]]
        if g["cursor"]["id"] == 0 or len(got) >= want:
            break
        if pause:
            time.sleep(pause)
    return got, None


def gapless(got, expected):
    """got must be a prefix of expected (no gap, no reorder, no duplicate)."""
    return got == expected[: len(got)]


def test_streams_at_different_speeds():
    s = server("rp1speed")
    try:
        d = s.client().sp
        d.create_collection("cs")
        streams = {name: open_stream(d, "cs")[0] for name in ("fast", "slow", "idle")}
        n = 60
        expected = list(range(n))
        res = {}

        def fast():
            res["fast"] = drain(d, "cs", streams["fast"], n, secs=60)

        th = threading.Thread(target=fast)
        th.start()
        for i in range(n):
            d.cs.insert_one({"_id": i, "p": "x" * (400 << 10)})  # about 1.2 MB of the 4 MB log each
            time.sleep(0.02)
        res["slow"] = drain(d, "cs", streams["slow"], n, secs=60, pause=1.0)
        th.join()
        res["idle"] = drain(d, "cs", streams["idle"], n, secs=20)
        print(res)
        for name, (got, err) in res.items():
            assert gapless(got, expected), f"{name}: skipped or reordered events: {got}"
            assert len(got) == n or err == 286, f"{name}: {len(got)} of {n} events and error {err}"
        assert res["idle"][1] == 286, "the idle stream should have lost its position"
    finally:
        s.term()


@pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS", "off") in ("off", "0", ""),
                    reason="customer erasure exists only with sealed runs (fx/erase.rs); with them off delete_many is plain deletes "
                           "and the stream delivers them (integration-4)")
def test_a_customer_erasure_between_writes_and_reads():
    s = server("rp1erase", extra=["--sealed-erasure-batch-ms", "0"] if os.environ.get("FQ_ERASE_FLAG") else [])
    try:
        d = s.client().er
        d.create_collection("cs", customerKey="cust")
        cid, _ = open_stream(d, "cs")
        for i in range(30):
            d.cs.insert_one({"_id": i, "cust": "a" if i % 3 == 0 else "b", "v": i})
        n_del = d.cs.delete_many({"cust": "a"}).deleted_count  # a whole customer: an erasure
        d.cs.insert_one({"_id": 100, "cust": "b", "v": 100})
        got, err = [], None
        t0 = time.time()
        evs = []
        while time.time() - t0 < 10:
            try:
                g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 300)]))
            except OperationFailure as e:
                err = e.code
                break
            evs += [(e["operationType"], e["documentKey"]["_id"]) for e in g["cursor"]["nextBatch"]]
            if any(k == 100 for _, k in evs):
                break
        print("erasure: deleted", n_del, "events", evs, "error", err)
        inserts_b = [k for op, k in evs if op == "insert"]
        assert err is None, f"error {err}"
        assert inserts_b == [i for i in range(30) if i % 3 != 0] + [100], f"customer b's events: {inserts_b}"
        assert not any(k % 3 == 0 and k < 30 for op, k in evs if op == "insert"), f"the erased customer's events were delivered: {evs}"
    finally:
        s.term()


def test_start_at_operation_time():
    s = server("rp1time")
    try:
        c = s.client()
        d = c.tm
        d.create_collection("cs")
        with c.start_session() as ses:
            d.cs.insert_one({"_id": "first", "p": "x"}, session=ses)
            t_first = ses.operation_time
        assert t_first is not None
        for i in range(12):
            d.cs.insert_one({"_id": i, "p": "x" * (1 << 20)})  # pushes "first" and the early inserts out of the 4 MB log
        with c.start_session() as ses:
            d.cs.insert_one({"_id": "late", "p": "y"}, session=ses)
            t_late = ses.operation_time
        # a start time no longer held: 286 at the first getMore (or at the aggregate)
        try:
            cid, first = open_stream(d, "cs", startAtOperationTime=t_first)
            got, err = drain(d, "cs", cid, 20, secs=5)
        except OperationFailure as e:
            got, err = [], e.code
        print("trimmed start:", got, err)
        assert err == 286, f"a start time no longer held: events {got}, error {err}"
        # a start time held: the late event, nothing before it
        cid, first = open_stream(d, "cs", startAtOperationTime=t_late)
        got, err = drain(d, "cs", cid, 1, secs=5)
        assert first + got == ["late"] and err is None, (first, got, err)
    finally:
        s.term()


def test_start_at_operation_time_from_before_a_restart():
    """Events written before a restart are not in the new process's log: a stream started at a time before the restart
    must fail (286), not deliver only the events written after the restart."""
    s = server("rp1restart")
    try:
        c = s.client()
        d = c.rs
        d.create_collection("cs")
        with c.start_session() as ses:
            d.cs.insert_one({"_id": 0}, session=ses)
            t0 = ses.operation_time
        assert t0 is not None
        for i in range(1, 5):
            d.cs.insert_one({"_id": i})
        s.term()
        s.start()
        c = s.client()
        d = c.rs
        d.cs.insert_one({"_id": 5})
        try:
            cid, first = open_stream(d, "cs", startAtOperationTime=t0)
            got, err = drain(d, "cs", cid, 6, secs=5)
            got = first + got
        except OperationFailure as e:
            got, err = [], e.code
        print("start before restart:", got, err)
        assert gapless(got, list(range(6))) and (len(got) == 6 or err == 286), f"events {got}, error {err}: events 0-4 were skipped with no error"
    finally:
        s.term()


def test_takeover():
    """The stream's process is killed; another process takes the database over. Its resume token and its start time
    must fail with 286 there (the events are not in that process's log), never deliver the later events alone."""
    bucket = make_bucket("rp1take")
    pa, pb = next(PORTS), next(PORTS)
    a = Server(bucket, "rp1takeA", idle=600, port=pa, extra=["--change-history", "off", "--change-log-max-mb", str(LOG_MB)] + rs(pa), binary=BIN, holder="A").start()
    b = None
    try:
        c = a.client()
        d = c.tk
        d.create_collection("cs")
        with c.start_session() as ses:
            d.cs.insert_one({"_id": 0}, session=ses)
            t0 = ses.operation_time
        cid, _ = open_stream(d, "cs")
        d.cs.insert_one({"_id": 1})
        g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 500)]))
        token = g["cursor"]["postBatchResumeToken"]
        got_a = [e["documentKey"]["_id"] for e in g["cursor"]["nextBatch"]]
        for i in range(2, 6):
            d.cs.insert_one({"_id": i})
        a.kill9()
        b = Server(bucket, "rp1takeB", idle=600, port=pb, extra=["--change-history", "off", "--change-log-max-mb", str(LOG_MB)] + rs(pb), binary=BIN, holder="B").start()
        cb = b.client()
        db = cb.tk
        t_wait = time.time()
        while True:
            try:
                db.cs.insert_one({"_id": 6})
                break
            except OperationFailure:
                if time.time() - t_wait > 60:
                    raise
                time.sleep(1)
        out = {}
        for name, opts in (("resumeAfter", {"resumeAfter": token}), ("startAtOperationTime", {"startAtOperationTime": t0})):
            try:
                cid2, first = open_stream(db, "cs", **opts)
                got, err = drain(db, "cs", cid2, 7, secs=5)
                out[name] = (first + got, err)
            except OperationFailure as e:
                out[name] = ([], e.code)
        print("A delivered", got_a, "B:", out)
        assert out["resumeAfter"][1] == 286 or gapless(out["resumeAfter"][0], [2, 3, 4, 5, 6]), out
        assert out["startAtOperationTime"][1] == 286 or gapless(out["startAtOperationTime"][0], list(range(7))), out
    finally:
        if b:
            b.term()
        try:
            a.term()
        except Exception:
            pass
