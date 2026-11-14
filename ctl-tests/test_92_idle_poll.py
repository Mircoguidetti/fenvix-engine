"""fix-idle-poll (2026-11-13, PLAN Track B): an idle open database must not poll the store.

MEASURED on integration-4 c41559f (research/fix-idle-poll): an open writer database with no request at all sends about
375 GETs per second in sealed mode (SlateDB's compactor and worker polling every 20 and 10 ms), and still 1.65 GETs and
0.33 LISTs per second with fix-box-first-trip's gate alone (the writer's manifest poll every 2 s, the floor observation
and the live GC). With the holds of `fx::writer_gate`, `drop::SlateGate`, `DropCtl::maybe_refold` and
`GcState::idle_skip`, each poller reads at most about once a minute while the database is idle.

  1. test_idle_open_database_polls_rarely: one open writer database, compactor running, idle for 60 s after its work
     settled: at most 24 GETs (0.4 per second; the 20 ms polls alone were about 22,000 in 60 s). The writer's poll is
     held (serverStatus sealedRuns.writerPollsHeld). Control (`FENVIX_SLATE_IDLE_POLL_MS=0`, every hold off, the code
     of fix-box-first-trip's base): more than 60. On integration-4 the first assertion fails.
  2. test_fenced_idle_writer_acknowledges_nothing: P1 holds the database, idle long enough that its writer's manifest
     poll is held; P1 is frozen past its lease, P2 takes the database over and writes; P1 resumed is asked to write at
     once: the write is refused, and the database holds exactly what was acknowledged. Also with P1's ownership checks
     off (`--unsafe-skip-ownership-checks`): then only SlateDB's own fencing (its conditional writes) stands between P1
     and the data, and it must still refuse. Polling never was what refused a fenced writer's write: the old 2 s poll
     left a window just as large.
  3. test_idle_close_is_not_held: a database idle with every poll held closes (SIGTERM) as fast as before (the close
     does not wait for a held poll: the stop of 49edb1cd for SlateDB's compactor, `writer_gate::stopping` for the
     writer).
"""
import os
import signal
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, OperationFailure

from lib import Server, make_bucket

# sealed runs on with a compactor of the server's defaults (not the judge's small knobs), a first publish at once
ENV = {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
       "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1", "FENVIX_SEALED_COMPACTOR": "1"}
TTL = 4


def status(c):
    return c.admin.command("serverStatus")["fenvix"]


def gets(c):
    s = status(c)["s3"]
    return s["get"] + s.get("getRange", 0)


def wave(db, k, n=20):
    for i in range(n):
        db.checkpoints.update_one({"thread_id": f"t{i}", "checkpoint_ns": "", "checkpoint_id": f"x{k}"}, {"$set": {"v": i, "k": k}}, upsert=True)


def idle_gets(tag, extra_env, idle=60):
    b = make_bucket(tag)
    s = Server(b, tag, idle=600, env=dict(ENV, **extra_env)).start()
    try:
        c = s.client()
        db = c.app
        wave(db, 0)
        time.sleep(5)
        wave(db, 1)
        # the writes' own work (flush, absorption, fold, publish, the floor after the lease) settles
        time.sleep(40)
        st0, g0 = status(c)["sealedRuns"], gets(c)
        time.sleep(idle)
        st1, g1 = status(c)["sealedRuns"], gets(c)
        n = sum(1 for _ in db.checkpoints.find())
        held = st1.get("writerPollsHeld", 0) - st0.get("writerPollsHeld", 0)
        c.close()
        return g1 - g0, held, n
    finally:
        s.term()


def test_idle_open_database_polls_rarely():
    got, held, n = idle_gets("idle92", {})
    print(f"idle 60 s: {got} GETs, writer polls held {held}")
    assert n == 40
    assert got <= 24, f"an idle open database sent {got} GETs in 60 s"
    assert held >= 1, "the writer's manifest poll was never held"


def test_control_without_the_holds_polls_often():
    got, held, n = idle_gets("idle92c", {"FENVIX_SLATE_IDLE_POLL_MS": "0"})
    print(f"control, idle 60 s: {got} GETs, writer polls held {held}")
    assert n == 40
    assert got > 60, f"control: {got} GETs in 60 s"
    assert held == 0


def client(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000, retryWrites=False, retryReads=False)


def fenced(unsafe):
    tag = "idle92fu" if unsafe else "idle92f"
    b = make_bucket(tag)
    flags = ["--lease-ttl-secs", str(TTL)]
    p1 = Server(b, tag + "1", idle=600, env=ENV, extra=flags + (["--unsafe-skip-ownership-checks"] if unsafe else [])).start()
    p2 = Server(b, tag + "2", idle=600, env=ENV, extra=flags).start()
    try:
        c1, c2 = client(p1), client(p2)
        acked = set()
        for i in range(20):
            c1.d.t.insert_one({"_id": f"p1-{i}"})
            acked.add(f"p1-{i}")
        # idle: past the writer's active window, its manifest poll is held
        h0 = status(c1)["sealedRuns"].get("writerPollsHeld", 0)
        time.sleep(15)
        held = status(c1)["sealedRuns"].get("writerPollsHeld", 0) - h0
        os.kill(p1.proc.pid, signal.SIGSTOP)
        try:
            deadline = time.time() + TTL * 8
            took = False
            while time.time() < deadline:
                try:
                    c2.d.t.insert_one({"_id": "p2-a"})
                    acked.add("p2-a")
                    took = True
                    break
                except OperationFailure as e:
                    assert e.code == 134, e
                    time.sleep(0.5)
            assert took, "P2 never took the database over"
        finally:
            os.kill(p1.proc.pid, signal.SIGCONT)
        # P1 resumed, its manifest poll still held: a write at once
        try:
            c1.d.t.insert_one({"_id": "p1-late"})
            late = "accepted"
            acked.add("p1-late")
        except (OperationFailure, AutoReconnect) as e:
            late = f"refused {getattr(e, 'code', e)}"
        time.sleep(1)
        p2.restart()
        docs = {d["_id"] for d in client(p2).d.t.find()}
        return held, late, docs, acked
    finally:
        p1.term(); p2.term()


@pytest.mark.parametrize("unsafe", [False, True])
def test_fenced_idle_writer_acknowledges_nothing(unsafe):
    held, late, docs, acked = fenced(unsafe)
    print(f"unsafe={unsafe}: writer polls held while idle {held}; late write {late}; {len(docs)} documents")
    assert held >= 1, "the writer's manifest poll was not held: the case is not tested"
    assert late != "accepted", "a fenced writer acknowledged a write"
    assert acked <= docs, f"acknowledged writes lost: {acked - docs}"
    assert docs == acked, f"unacknowledged documents present: {docs - acked}"


def test_idle_close_is_not_held():
    b = make_bucket("idle92s")
    s = Server(b, "idle92s", idle=600, env=ENV).start()
    c = s.client()
    for d in range(3):
        wave(c[f"app{d}"], 0)
    time.sleep(25)
    st = status(c)["sealedRuns"]
    c.close()
    t0 = time.time()
    s.term()
    took = time.time() - t0
    print(f"SIGTERM of 3 idle databases: {took:.2f} s; writer polls held {st.get('writerPollsHeld')}, SlateDB polls held {st.get('slatePollsHeld')}")
    assert st.get("writerPollsHeld", 0) >= 1
    assert took < 8, f"the close of idle databases took {took:.1f} s"
    assert "did not finish" not in s.log_text()
