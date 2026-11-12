"""P-durable-changelog: change streams on a durable history (`mongo-server/src/chglog.rs`).

Every acknowledged write has its change event exactly once, in commit order, after restarts, takeovers and kill -9 at
every step of the history (the batch, the segment PUT, the tail delete, the trim); resume tokens and
startAtOperationTime work for any position kept, and fail with ChangeStreamHistoryLost (286) outside it, as mongod;
an erased customer's events are gone. Event documents are compared with mongod 7.0 (the judge's replica set on port
27020) where the scenario exists there.

Run from ctl-tests: python -m pytest -q test_75_durable_change_history.py
Long run (the prompt's 10 minutes of writes): CHG_LONG_SECS=600 python -m pytest -q test_75_durable_change_history.py -k ten_minutes
"""
import os
import threading
import time

import pytest
from bson import SON
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, make_bucket, list_prefix

REAL = os.environ.get("CHG_REAL", "mongodb://127.0.0.1:27020/?directConnection=true")


def rs(port):
    # as the judge and real deployments run it: a replica set member, so writes return operationTime
    return ["--replica-set", "rs0", "--advertise", f"localhost:{port}"]


def server(bucket, tag, extra=(), holder=None, env=None, port=None):
    from lib import free_port
    p = port or free_port()
    return Server(bucket, tag, idle=600, port=p, extra=rs(p) + list(extra), holder=holder, env=env)


def open_stream(d, coll, **opts):
    r = d.command(SON([("aggregate", coll), ("pipeline", [{"$changeStream": opts}]), ("cursor", {})]))
    return r["cursor"]["id"], r["cursor"]["firstBatch"], r["cursor"]["postBatchResumeToken"]


def drain(d, coll, cid, want=None, secs=30, until=None, batch=None):
    """Events of a stream until `want` events, `until(events)`, an error or `secs` without reaching them."""
    got, t0 = [], time.time()
    while time.time() - t0 < secs:
        cmd = SON([("getMore", cid), ("collection", coll), ("maxTimeMS", 300)])
        if batch:
            cmd["batchSize"] = batch
        try:
            g = d.command(cmd)
        except OperationFailure as e:
            return got, e.code
        got += g["cursor"]["nextBatch"]
        if g["cursor"]["id"] == 0 or (want is not None and len(got) >= want) or (until and until(got)):
            break
    return got, None


def ids(evs, op="insert"):
    return [e["documentKey"]["_id"] for e in evs if e["operationType"] == op]


def shape(e):
    """The parts of an event that must match mongod (not the token, times or UUIDs)."""
    out = {"op": e["operationType"], "ns": e.get("ns")}
    for k in ("documentKey", "fullDocument", "updateDescription", "to"):
        if k in e:
            out[k] = e[k]
    return out


def tenant_stats(c):
    return c.admin.command("tenantStats")["maintenance"]["changeHistory"]


def write_scenario(d):
    """The same operations on both servers: every kind of event a collection stream returns."""
    d.cs.insert_one({"_id": 1, "a": 1})
    d.cs.insert_many([{"_id": i, "a": i} for i in range(2, 6)])
    d.cs.update_one({"_id": 2}, {"$set": {"b": 2}, "$unset": {"a": 1}})
    d.cs.replace_one({"_id": 3}, {"z": 3})
    d.cs.delete_one({"_id": 4})
    d.cs.update_many({"_id": {"$gte": 5}}, {"$inc": {"a": 10}})


def test_events_after_a_restart_match_mongod():
    """A stream resumed after a restart from a token taken before the writes returns the events mongod returns."""
    real = MongoClient(REAL, serverSelectionTimeoutMS=3000)
    try:
        real.admin.command("ping")
    except PyMongoError:
        pytest.skip("the reference mongod (judge/setup.sh, port 27020) is not running")
    real.drop_database("chg_ref")
    rd = real.chg_ref
    rd.create_collection("cs")
    rcid, _, rtok = open_stream(rd, "cs")
    write_scenario(rd)
    rcid2, rfirst, _ = open_stream(rd, "cs", resumeAfter=rtok)
    ref, err = drain(rd, "cs", rcid2, want=9 - len(rfirst), secs=10) if len(rfirst) < 9 else ([], None)
    ref = rfirst + ref
    assert err is None and len(ref) == 9, (ref, err)

    b = make_bucket("chg75ref")
    s = server(b, "chg75ref").start()
    try:
        c = s.client()
        d = c.chg_ref
        d.create_collection("cs")
        _, _, tok = open_stream(d, "cs")
        write_scenario(d)
        s.term()
        s.start()
        d = s.client().chg_ref
        cid, first, _ = open_stream(d, "cs", resumeAfter=tok)
        ours, err = drain(d, "cs", cid, want=9 - len(first), secs=10)
        ours = first + ours
        assert err is None, err
        assert [shape(e) for e in ours] == [shape(e) for e in ref], f"ours {[shape(e) for e in ours]}\nmongod {[shape(e) for e in ref]}"
        # the tokens of the resumed stream resume it again, in a new process too
        mid = ours[3]["_id"]
        s.kill9()
        s.start()
        d = s.client().chg_ref
        cid, first, _ = open_stream(d, "cs", resumeAfter=mid)
        rest, err = drain(d, "cs", cid, want=5 - len(first), secs=10)
        assert [shape(e) for e in first + rest] == [shape(e) for e in ref[4:]], (first + rest, err)
    finally:
        s.term()
        real.drop_database("chg_ref")


def writers(d, coll, secs, threads=4, batch=50, tag="w"):
    """Insert as fast as this process can for `secs` seconds; returns each writer's acknowledged ids in order."""
    acked = {t: [] for t in range(threads)}
    stop = time.time() + secs

    def run(t):
        n = 0
        while time.time() < stop:
            docs = [{"_id": f"{tag}{t}-{n + i:09d}", "t": t, "n": n + i, "p": os.urandom(100).hex()} for i in range(batch)]
            try:
                d[coll].insert_many(docs, ordered=True)
            except PyMongoError:
                return
            acked[t] += [x["_id"] for x in docs]
            n += batch

    ths = [threading.Thread(target=run, args=(t,)) for t in range(threads)]
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    return acked


def check_exactly_once(delivered, acked, maybe=()):
    """Every acknowledged id exactly once; each writer's ids in its own order; nothing else except ids whose write was
    in flight when a process died (`maybe`, durable but never acknowledged)."""
    seen = set()
    dup = [x for x in delivered if x in seen or seen.add(x)]
    assert not dup, f"duplicate events: {dup[:10]}"
    all_acked = [x for v in acked.values() for x in v]
    missing = [x for x in all_acked if x not in seen]
    assert not missing, f"{len(missing)} acknowledged writes without an event (RULE 8 STOP): {missing[:10]}"
    extra = seen - set(all_acked) - set(maybe)
    assert not extra, f"events of writes never made: {sorted(extra)[:10]}"
    for t, v in acked.items():
        sv = set(v)
        mine = [x for x in delivered if x in sv]
        assert mine == v, f"writer {t}: events out of commit order"


def test_resume_after_ten_minutes_of_writes():
    """A token taken before CHG_LONG_SECS (default 60 here; 600 for the prompt's run) of writes at the fastest local rate
    resumes and returns every event once, in order, from segments and the tail."""
    secs = int(os.environ.get("CHG_LONG_SECS", "60"))
    b = make_bucket("chg75long")
    s = server(b, "chg75long", extra=["--change-segment-mb", "4"]).start()
    try:
        c = s.client()
        d = c.lg
        d.create_collection("cs")
        _, _, tok = open_stream(d, "cs")
        t0 = time.time()
        acked = writers(d, "cs", secs)
        wrote = time.time() - t0
        n = sum(len(v) for v in acked.values())
        st = tenant_stats(c)
        print(f"wrote {n} events in {wrote:.0f} s ({n / wrote:.0f}/s); history {st}")
        cid, first, _ = open_stream(d, "cs", resumeAfter=tok)
        t1 = time.time()
        # (ids only: millions of whole events do not fit in the test's memory)
        got, err = ids(first), None
        while len(got) < n and time.time() - t1 < max(300, secs * 2):
            try:
                g = d.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 300)]))
            except OperationFailure as e:
                err = e.code
                break
            got += [e["documentKey"]["_id"] for e in g["cursor"]["nextBatch"] if e["operationType"] == "insert"]
        read = time.time() - t1
        st2 = tenant_stats(c)
        print(f"read {len(got)} events in {read:.1f} s; history {st2}")
        assert err is None, err
        assert st["segmentsPut"] > 0, "the run never wrote a segment: it did not test the segments"
        check_exactly_once(got, acked)
        print(f"exactly once: {len(got)} events, {sum(len(v) for v in acked.values())} acknowledged writes")
    finally:
        s.term()


def test_streams_at_different_speeds_get_everything():
    """With a small in-memory log, a fast, a slow and an idle stream all get every event (P1's 286 is gone)."""
    b = make_bucket("chg75speed")
    s = server(b, "chg75speed", extra=["--change-log-max-mb", "4", "--change-segment-mb", "2"]).start()
    try:
        d = s.client().sp
        d.create_collection("cs")
        streams = {name: open_stream(d, "cs")[0] for name in ("fast", "slow", "idle")}
        n = 60
        res = {}

        def fast():
            got, err = drain(d, "cs", streams["fast"], want=n, secs=90)
            res["fast"] = (ids(got), err)

        th = threading.Thread(target=fast)
        th.start()
        for i in range(n):
            d.cs.insert_one({"_id": i, "p": "x" * (400 << 10)})
            time.sleep(0.02)
        got, err = drain(d, "cs", streams["slow"], want=n, secs=90, batch=3)
        res["slow"] = (ids(got), err)
        th.join()
        got, err = drain(d, "cs", streams["idle"], want=n, secs=60)
        res["idle"] = (ids(got), err)
        for name, (got, err) in res.items():
            assert err is None and got == list(range(n)), f"{name}: {len(got)} events, error {err}"
    finally:
        s.term()


def test_takeover_resumes_every_event_once():
    """Process A is killed with kill -9 while writing; process B takes the database over. A token taken on A before the
    writes resumes on B: every acknowledged write's event once, in order; B's own writes follow."""
    b = make_bucket("chg75take")
    a = server(b, "chg75takeA", holder="A", extra=["--change-segment-mb", "1"]).start()
    bsrv = None
    try:
        d = a.client().tk
        d.create_collection("cs")
        _, _, tok = open_stream(d, "cs")
        acked = {}
        stop = threading.Event()

        def run(t):
            acked[t] = []
            n = 0
            while not stop.is_set():
                docs = [{"_id": f"a{t}-{n + i:07d}", "p": "x" * 300} for i in range(20)]
                try:
                    d.cs.insert_many(docs)
                except PyMongoError:
                    return
                acked[t] += [x["_id"] for x in docs]
                n += 20

        ths = [threading.Thread(target=run, args=(t,)) for t in range(3)]
        for th in ths:
            th.start()
        time.sleep(8)
        a.kill9()
        stop.set()
        for th in ths:
            th.join()
        bsrv = server(b, "chg75takeB", holder="B").start()
        db = bsrv.client().tk
        t_wait = time.time()
        while True:
            try:
                db.cs.insert_one({"_id": "b-0"})
                break
            except OperationFailure:
                assert time.time() - t_wait < 60
                time.sleep(1)
        cid, first, _ = open_stream(db, "cs", resumeAfter=tok)
        got, err = drain(db, "cs", cid, secs=60, until=lambda g: any(e["documentKey"]["_id"] == "b-0" for e in g))
        got = ids(first + got)
        assert err is None, err
        assert got[-1] == "b-0", got[-5:]
        # writes in flight at the kill may be durable without an acknowledgement: allowed once, after the acknowledged
        # ones of their writer
        maybe = {x for x in got if x.startswith("a")} - {x for v in acked.values() for x in v}
        check_exactly_once([x for x in got if x != "b-0"], acked, maybe)
    finally:
        if bsrv:
            bsrv.term()
        a.term()


def test_start_at_operation_time_inside_and_outside_retention():
    b = make_bucket("chg75ret")
    s = server(b, "chg75ret", extra=["--change-retention-secs", "6", "--change-segment-secs", "1", "--change-sweep-secs", "2"]).start()
    try:
        c = s.client()
        d = c.rt
        d.create_collection("cs")
        with c.start_session() as ses:
            d.cs.insert_one({"_id": "old"}, session=ses)
            t_old = ses.operation_time
        for i in range(5):
            d.cs.insert_one({"_id": f"o{i}"})
        # inside retention, after a restart: everything from t_old
        s.term()
        s.start()
        c = s.client()
        d = c.rt
        cid, first, _ = open_stream(d, "cs", startAtOperationTime=t_old)
        got, err = drain(d, "cs", cid, want=6, secs=10)
        assert err is None and ids(first + got) == ["old"] + [f"o{i}" for i in range(5)], (ids(first + got), err)
        # past retention: the old events are archived, then trimmed; a start time before them fails (286), as mongod
        deadline = time.time() + 40
        while time.time() < deadline:
            d.cs.insert_one({"_id": f"n{int(time.time() * 1000)}"})
            time.sleep(0.5)
            st = tenant_stats(c)
            if st["segmentsDeleted"] > 0:
                break
        assert tenant_stats(c)["segmentsDeleted"] > 0, tenant_stats(c)
        with pytest.raises(OperationFailure) as e:
            cid, first, _ = open_stream(d, "cs", startAtOperationTime=t_old)
            drain(d, "cs", cid, want=1, secs=5)
            raise OperationFailure("no error", 0)
        assert e.value.code == 286, e.value
        # the tokens of trimmed positions fail the same way; a recent start time works
        with c.start_session() as ses:
            d.cs.insert_one({"_id": "recent"}, session=ses)
            t_recent = ses.operation_time
        cid, first, _ = open_stream(d, "cs", startAtOperationTime=t_recent)
        got, err = drain(d, "cs", cid, want=1, secs=5)
        assert ids(first + got) == ["recent"], (first + got, err)
    finally:
        s.term()


@pytest.mark.parametrize("step", ["archive.before_put", "archive.after_put", "archive.after_tail_delete", "trim.after_first_delete"])
def test_kill_at_every_step_of_the_history(step):
    """The process dies (abort, as kill -9) at one step of the archive or the trim; after a restart a token from before
    the writes returns every acknowledged event once, in order (no duplicate from a segment written twice, no gap)."""
    b = make_bucket("chg75kill")
    extra = ["--change-segment-mb", "1", "--change-segment-secs", "1"]
    if step.startswith("trim"):
        # (the test documents compress to almost nothing: trim by age; a token older than what is kept then fails 286)
        extra += ["--change-retention-secs", "3"]
    s = server(b, "chg75kill", extra=extra, env={"FENVIX_CHG_CRASH": step}).start()
    try:
        d = s.client().kl
        d.create_collection("cs")
        _, _, tok = open_stream(d, "cs")
        acked = {0: []}
        n = 0
        while s.proc.poll() is None and n < 200000:
            docs = [{"_id": n + i, "p": "x" * 2000} for i in range(100)]
            try:
                d.cs.insert_many(docs)
            except PyMongoError:
                break
            acked[0] += [x["_id"] for x in docs]
            n += 100
        s.proc.wait(30)
        assert "CHG CRASH at " + step in s.log_text(), "the crash point was not reached"
        s.env = {}
        s.start()
        d = s.client().kl
        if step.startswith("trim"):
            # positions past retention are gone: the old token fails with 286, never a later event alone
            with pytest.raises(OperationFailure) as e:
                cid, first, _ = open_stream(d, "cs", resumeAfter=tok)
                drain(d, "cs", cid, want=1, secs=5)
                raise OperationFailure("no error", 0)
            assert e.value.code == 286, e.value
            # what is still kept reads in order with no gap, from the oldest segment left
            segs = sorted(k for k, _ in list_prefix(b, "changes/"))
            assert segs, "no segment left"
            cid, first, _ = open_stream(d, "cs", startAtOperationTime=__import__("bson").Timestamp(int(segs[0].split("/")[-1].split("-")[3][:8], 16), int(segs[0].split("/")[-1].split("-")[3][8:], 16)))
            got, err = drain(d, "cs", cid, secs=60, until=lambda g: g and g[-1]["documentKey"]["_id"] == acked[0][-1])
            got = ids(first + got)
            assert err is None and got == sorted(got) and got == list(range(got[0], got[0] + len(got))), (err, got[:5], got[-5:])
            assert got[-1] >= acked[0][-1], (got[-1], acked[0][-1])
            return
        cid, first, _ = open_stream(d, "cs", resumeAfter=tok)
        total = len(acked[0])
        got, err = drain(d, "cs", cid, secs=60, until=lambda g: len(g) >= total)
        got = ids(first + got)
        assert err is None, err
        maybe = set(got) - set(acked[0])
        check_exactly_once(got, acked, maybe)
        # and the history goes on: a write after the restart follows
        d.cs.insert_one({"_id": "after"})
        more, err = drain(d, "cs", cid, secs=10, until=lambda g: any(e["documentKey"]["_id"] == "after" for e in g))
        assert ids(more)[-1] == "after", ids(more)
    finally:
        s.term()


def test_no_event_of_a_write_that_was_not_durable():
    """Events read while writes run, then kill -9: every event a stream saw before the kill is in the durable history
    after the restart (a stream never sees the event of a write a crash can still lose), and every acknowledged write
    has its event."""
    b = make_bucket("chg75dur")
    s = server(b, "chg75dur").start()
    try:
        d = s.client().du
        d.create_collection("cs")
        cid, _, tok = open_stream(d, "cs")
        seen = []
        acked = {}
        stop = threading.Event()

        def reader():
            rd = s.client().du
            while not stop.is_set():
                try:
                    g = rd.command(SON([("getMore", cid), ("collection", "cs"), ("maxTimeMS", 100)]))
                except PyMongoError:
                    return
                seen.extend(ids(g["cursor"]["nextBatch"]))

        def run(t):
            acked[t] = []
            n = 0
            while not stop.is_set():
                docs = [{"_id": f"{t}-{n + i:07d}"} for i in range(10)]
                try:
                    d.cs.insert_many(docs)
                except PyMongoError:
                    return
                acked[t] += [x["_id"] for x in docs]
                n += 10

        ths = [threading.Thread(target=reader)] + [threading.Thread(target=run, args=(t,)) for t in range(3)]
        for th in ths:
            th.start()
        time.sleep(5)
        s.kill9()
        stop.set()
        for th in ths:
            th.join()
        s.start()
        d = s.client().du
        cid, first, _ = open_stream(d, "cs", resumeAfter=tok)
        d.cs.insert_one({"_id": "z-after"})
        got, err = drain(d, "cs", cid, secs=60, until=lambda g: any(e["documentKey"]["_id"] == "z-after" for e in g))
        got = [x for x in ids(first + got) if x != "z-after"]
        assert err is None, err
        lost_seen = [x for x in seen if x not in set(got)]
        assert not lost_seen, f"a stream saw events of writes that are not durable: {lost_seen[:10]}"
        assert seen == got[: len(seen)], "the stream before the kill saw another order"
        check_exactly_once(got, acked, set(got) - {x for v in acked.values() for x in v})
    finally:
        s.term()


@pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS", "off") in ("off", "0", ""),
                    reason="customer erasure exists only with sealed runs (fx/erase.rs)")
def test_an_erased_customers_events_are_gone():
    """After a customer erasure no stream returns the customer's events: from memory, after a restart (the tail), from
    segments; after the sweep the segments that held them are deleted from the store."""
    b = make_bucket("chg75erase")
    s = server(b, "chg75erase", extra=["--change-segment-secs", "1", "--change-retention-secs", "8", "--change-sweep-secs", "2"]).start()
    try:
        d = s.client().er
        d.create_collection("cs", customerKey="cust")
        _, _, tok = open_stream(d, "cs")
        for i in range(30):
            d.cs.insert_one({"_id": i, "cust": "a" if i % 3 == 0 else "b", "v": i})
        time.sleep(3)  # the first events go to a segment
        d.cs.delete_many({"cust": "a"})  # a whole customer: an erasure
        d.cs.insert_one({"_id": 100, "cust": "b", "v": 100})

        def check(dd):
            cid, first, _ = open_stream(dd, "cs", resumeAfter=tok)
            got, err = drain(dd, "cs", cid, secs=15, until=lambda g: any(e["documentKey"]["_id"] == 100 for e in g))
            evs = first + got
            assert err is None, err
            assert not [e for e in evs if e["documentKey"].get("cust") == "a" or (e.get("fullDocument") or {}).get("cust") == "a"], "erased customer's events delivered"
            assert ids(evs) == [i for i in range(30) if i % 3] + [100], ids(evs)

        check(d)
        s.term()
        s.start()
        d = s.client().er
        check(d)
        segs_before = [k for k, _ in list_prefix(b, "changes/")]
        assert segs_before, "no segment was written"
        # the segments of the erased customer's time leave the store by age
        deadline = time.time() + 40
        while time.time() < deadline and any(k in [x for x, _ in list_prefix(b, "changes/")] for k in segs_before):
            time.sleep(1)
        left = [k for k in segs_before if k in [x for x, _ in list_prefix(b, "changes/")]]
        assert not left, f"segments holding the erased customer's events are still in the store: {left}"
    finally:
        s.term()
