"""C12: the memory of a process about sessions, when a database moves from one process to another and back.

A process remembers each session in memory (its latest transaction number, the statements of its latest retryable write,
its open transaction). That memory is only right for a database while the process has held the database without a break:
once another process owned it, the other process may have written there. Before C12 a process marked a session `loaded`
once, whatever database it was loaded from, and never read it again (found by the review of C10, test_41f, section 5): a
retryable write retried on P1 after P2 ran it was applied twice. The same family, found here: an open transaction that
survives the loss of its database (P1 loses its lease, P2 writes, P1 takes the database back) committed over P2's write
(a lost update), because the conflict check only knows the writes of this process.

Two processes on one bucket, idle close after 2 s, lease ttl 3 s. Everything goes straight to the test S3, except the cost
test, which goes through the counting proxy of test_41f.
"""
import json
import os
import tempfile
import time

import pytest
from bson import Int64
from pymongo.errors import OperationFailure

from lib import Server, free_port, make_bucket
import test_41_commit_retry as T


def _srv(tag, bucket, holder, idle=2, env=None, endpoint=None, ttl=3):
    port = free_port()
    return Server(bucket, tag, port=port, holder=holder, idle=idle, env=env, endpoint=endpoint,
                  extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}", "--lease-ttl-secs", str(ttl)]).start()


def _on(f, timeout=40):
    """Run f until the database is ours (134: another process holds it, retryable)."""
    deadline = time.time() + timeout
    while True:
        try:
            return f()
        except OperationFailure as e:
            if e.code != 134 or time.time() > deadline:
                raise
            time.sleep(0.3)


def _rw(c, lsid, n, db="ct", _id="rw"):
    return c[db].command({"update": "c", "updates": [{"q": {"_id": _id}, "u": {"$inc": {"v": 1}}, "upsert": True}], "lsid": lsid, "txnNumber": Int64(n)})


def _fam(c, lsid, n, db="ct", _id="fam"):
    return c[db].command({"findAndModify": "c", "query": {"_id": _id}, "update": {"$inc": {"v": 1}}, "upsert": True, "new": True, "lsid": lsid, "txnNumber": Int64(n)})


def _val(srv, db, _id):
    d = _on(lambda: T._Pm(srv)[db].c.find_one({"_id": _id}))
    return (d or {}).get("v")


def _move(r_other, db="ct"):
    """Make the other process take the database (a plain insert), then let it go idle and close."""
    _on(lambda: r_other[db].command({"insert": "other", "documents": [{"t": time.time()}]}))


# ----------------------------------------------------------------------------------------------- retryable writes

def test_retryable_write_in_a_second_database_that_moves_and_comes_back():
    """The session is loaded from database a on P1 (write 2) while database b went to P2 (write 3 there). Retry of 3 in b
    on P1: before C12 P1 trusted its memory (number 2, loaded from a) and ran write 3 again (rw of b = 3)."""
    bucket = make_bucket("c12two")
    P1, P2 = _srv("c12two1", bucket, "p1"), _srv("c12two2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        lsid = T._lsid()
        assert _rw(r1, lsid, 1, db="b")["ok"] == 1.0
        assert _rw(r1, lsid, 2, db="a")["ok"] == 1.0
        assert _on(lambda: _rw(r2, lsid, 3, db="b"))["ok"] == 1.0
        retried = _on(lambda: _rw(r1, lsid, 3, db="b"))
        got = {"retry_3_on_P1": retried.get("retriedStmtIds"), "rw_b": _val(P1, "b", "rw"), "rw_a": _val(P1, "a", "rw")}
        print(json.dumps(got))
        assert got == {"retry_3_on_P1": [0], "rw_b": 2, "rw_a": 1}, got
    finally:
        P1.kill9()
        P2.kill9()


def test_find_and_modify_retried_after_the_database_comes_back():
    """findAndModify 1 on P1, findAndModify 2 on P2, findAndModify 2 retried on P1: answered from the record (the stored
    document, v = 2), not run again (v = 3)."""
    bucket = make_bucket("c12fam")
    P1, P2 = _srv("c12fam1", bucket, "p1"), _srv("c12fam2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        lsid = T._lsid()
        assert _fam(r1, lsid, 1)["value"]["v"] == 1
        assert _on(lambda: _fam(r2, lsid, 2))["value"]["v"] == 2
        again = _on(lambda: _fam(r1, lsid, 2))
        got = {"value": again["value"]["v"], "retriedStmtId": again.get("retriedStmtId"), "stored": _val(P1, "ct", "fam")}
        print(json.dumps(got))
        assert got["stored"] == 2 and got["value"] == 2, got
    finally:
        P1.kill9()
        P2.kill9()


def test_older_write_retried_after_the_database_comes_back_is_too_old():
    """Write 1 on P1, write 2 on P2, write 1 retried on P1: like mongod (one session record) it is refused with 225
    (a newer write exists), and nothing runs again."""
    bucket = make_bucket("c12old")
    P1, P2 = _srv("c12old1", bucket, "p1"), _srv("c12old2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        lsid = T._lsid()
        assert _rw(r1, lsid, 1)["ok"] == 1.0
        assert _on(lambda: _rw(r2, lsid, 2))["ok"] == 1.0
        try:
            got = _on(lambda: _rw(r1, lsid, 1)).get("retriedStmtIds")
        except OperationFailure as e:
            got = e.code
        print(json.dumps({"retry_1_on_P1": got, "rw": _val(P1, "ct", "rw")}))
        assert got == 225, got
        assert _val(P1, "ct", "rw") == 2
    finally:
        P1.kill9()
        P2.kill9()


def test_session_used_while_the_database_never_left_reads_nothing_more():
    """The reload happens only when the database was opened again: 50 retryable writes of one session on a database that
    stays open on P1 keep answering retries from memory (a retry of the last one is answered, v counts 50)."""
    bucket = make_bucket("c12stay")
    P1 = _srv("c12stay1", bucket, "p1", idle=60)
    try:
        r1 = T.Raw(P1)
        lsid = T._lsid()
        for n in range(1, 51):
            assert _rw(r1, lsid, n)["ok"] == 1.0
        assert _rw(r1, lsid, 50).get("retriedStmtIds") == [0]
        assert _val(P1, "ct", "rw") == 50
    finally:
        P1.kill9()


# ------------------------------------------------------------------------------------------ open transactions

def _faults():
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    return f.name


def test_open_transaction_does_not_commit_over_the_writes_of_another_owner():
    """Txn 1 on P1 adds 1 to the counter (snapshot v = 0) and stays open. P1 loses its lease (its process lease renewals
    hang), P2 takes the database and adds 1 (v = 1), then closes it. P1 commits: before C12 the commit went through (its
    conflict check knows only the writes of P1) and wrote v = 1 over P2's write: a lost update. Now the commit is refused
    with a TransientTransactionError (as mongod aborts open transactions when the primary changes) and the driver's rerun
    makes v = 2."""
    bucket = make_bucket("c12txn")
    ff = _faults()
    P1 = _srv("c12txn1", bucket, "p1", env={"MONGO_TEST_FAULTS_FILE": ff})
    P2 = _srv("c12txn2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        r1["ct"].command({"insert": "c", "documents": [{"_id": "counter", "v": 0}]})
        lsid = T._lsid()
        r1["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}], "lsid": lsid,
                          "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        with open(ff, "w") as f:
            f.write("hang_put processes/p1/_lease\n")
        _on(lambda: r2["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}]}), timeout=60)
        with open(ff, "w") as f:
            f.write("")
        time.sleep(6)                     # P2 closes the idle database (2 s) and lets it go; P1's renewals go on
        try:
            T._commit(r1, lsid, 1)
            commit = "ok"
        except OperationFailure as e:
            commit = (e.code, "TransientTransactionError" in (e.details or {}).get("errorLabels", []))
        after_commit = _val(P1, "ct", "counter")
        if commit != "ok":
            # the driver's withTransaction runs the transaction again under a new number
            _on(lambda: r1["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}], "lsid": lsid,
                                          "txnNumber": Int64(2), "startTransaction": True, "autocommit": False}))
            T._commit(r1, lsid, 2)
        got = {"commit": commit, "after_commit": after_commit, "final": _val(P1, "ct", "counter")}
        print(json.dumps(got))
        assert got["final"] == 2, got
        assert commit == "ok" or commit[1], got
    finally:
        P1.kill9()
        P2.kill9()
        os.unlink(ff)


def test_statement_of_an_open_transaction_after_the_database_moved_is_refused():
    """Same loss of the database as above, but the next statement of the open transaction arrives (an insert) before the
    commit: it is refused with NoSuchTransaction (TransientTransactionError), and the transaction is gone."""
    bucket = make_bucket("c12stm")
    ff = _faults()
    P1 = _srv("c12stm1", bucket, "p1", env={"MONGO_TEST_FAULTS_FILE": ff})
    P2 = _srv("c12stm2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        r1["ct"].command({"insert": "c", "documents": [{"_id": "counter", "v": 0}]})
        lsid = T._lsid()
        r1["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}], "lsid": lsid,
                          "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        with open(ff, "w") as f:
            f.write("hang_put processes/p1/_lease\n")
        _on(lambda: r2["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}]}), timeout=60)
        with open(ff, "w") as f:
            f.write("")
        time.sleep(6)
        try:
            _on(lambda: r1["ct"].command({"insert": "c", "documents": [{"_id": "x"}], "lsid": lsid, "txnNumber": Int64(1), "autocommit": False}))
            stmt = "ok"
        except OperationFailure as e:
            stmt = (e.code, "TransientTransactionError" in (e.details or {}).get("errorLabels", []))
        try:
            T._commit(r1, lsid, 1)
            commit = "ok"
        except OperationFailure as e:
            commit = e.code
        got = {"statement": stmt, "commit": commit, "counter": _val(P1, "ct", "counter"), "x": T._Pm(P1).ct.c.find_one({"_id": "x"}) is not None}
        print(json.dumps(got))
        assert stmt != "ok" and stmt[1], got
        assert commit != "ok", got
        assert got["counter"] == 1 and not got["x"], got
    finally:
        P1.kill9()
        P2.kill9()
        os.unlink(ff)


# ---------------------------------------------------------------------- what was checked and is already right

def test_committed_transaction_remembered_across_a_move_still_answers_ok():
    """Txn 1 committed on P1 (remembered as committed), the database goes to P2 and back: the commit repeated on P1
    answers ok, the counter stays 1 (the memory of a committed and durable transaction stays true wherever the database
    went). A guard: passes before and after C12."""
    bucket = make_bucket("c12cmt")
    P1, P2 = _srv("c12cmt1", bucket, "p1"), _srv("c12cmt2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        r1["ct"].command({"insert": "c", "documents": [{"_id": "counter", "v": 0}]})
        lsid = T._lsid()
        r1["ct"].command({"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}], "lsid": lsid,
                          "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        assert T._commit(r1, lsid, 1)["ok"] == 1.0
        _move(r2)
        got = _on(lambda: T._commit(r1, lsid, 1))
        assert got["ok"] == 1.0
        assert _val(P1, "ct", "counter") == 1
    finally:
        P1.kill9()
        P2.kill9()


@pytest.mark.xfail(strict=True, reason="pre-existing, design: change streams live in the memory of one process (changes.rs); "
                                       "events written by another owner are not seen. Needs a stored change log, not a C12 fix.")
def test_change_stream_misses_the_events_written_by_the_other_owner():
    bucket = make_bucket("c12cs")
    P1, P2 = _srv("c12cs1", bucket, "p1"), _srv("c12cs2", bucket, "p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        r1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        cur = r1["ct"].command({"aggregate": "c", "pipeline": [{"$changeStream": {}}], "cursor": {}})["cursor"]["id"]
        _on(lambda: r2["ct"].command({"insert": "c", "documents": [{"_id": 1}]}))
        _on(lambda: r1["ct"].command({"insert": "c", "documents": [{"_id": 2}]}))
        seen = []
        deadline = time.time() + 10
        while time.time() < deadline and len(seen) < 2:
            b = r1["ct"].command({"getMore": cur, "collection": "c", "maxTimeMS": 500})["cursor"]["nextBatch"]
            seen += [e["documentKey"]["_id"] for e in b]
        assert seen == [1, 2], seen
    finally:
        P1.kill9()
        P2.kill9()


# ------------------------------------------------------------------------------------------------------ the cost

def test_cost_of_the_reload_when_the_database_comes_back():
    """N sessions with a retryable write on P1; the database goes to P2 and comes back. Requests to the store counted by
    the proxy of test_41f: (1) the opening of the database on P1 (a find), which must not grow with N; (2) the first
    write of each session after the return, which now reads the session's two records back. Printed for the report."""
    import test_41f_review_c10 as R
    n = int(os.environ.get("C12_SESSIONS", "200"))
    bucket = make_bucket("c12cost")
    P1 = _srv("c12cost1", bucket, "p1", endpoint=R.EP)
    P2 = _srv("c12cost2", bucket, "p2", endpoint=R.EP)
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        r1["ct"].command({"insert": "c", "documents": [{"_id": "seed"}]})
        sessions = [T._lsid() for _ in range(n)]
        for s in sessions:
            _rw(r1, s, 1, _id=str(s["id"]))
        _move(r2)
        time.sleep(4)                                    # P2 closes it
        R.PX["count"].clear()
        _on(lambda: r1["ct"].command({"find": "c", "filter": {"_id": "seed"}}))
        opening = dict(R.PX["count"])
        R.PX["count"].clear()
        t0 = time.time()
        for s in sessions:
            assert _rw(r1, s, 2, _id=str(s["id"]))["ok"] == 1.0
        dt = time.time() - t0
        writes = dict(R.PX["count"])
        R.PX["count"].clear()
        retried = [_rw(r1, s, 2, _id=str(s["id"])).get("retriedStmtIds") for s in sessions[:20]]
        again = dict(R.PX["count"])
        out = {"sessions": n, "opening": opening, "opening_total": sum(opening.values()), "first_writes": writes,
               "first_writes_gets": sum(v for k, v in writes.items() if k.startswith("GET") or k == "LIST"),
               "first_writes_s": round(dt, 2), "retries_20": again}
        print(json.dumps(out))
        assert all(x == [0] for x in retried), retried
    finally:
        P1.kill9()
        P2.kill9()
