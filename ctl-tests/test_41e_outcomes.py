"""C10: the two errors of the repeated commit found by the review of PR #34 (`test_41c`), from more sides.

1. The hint `txn-hints/{session}/{db}` must stay findable as long as the session can still ask for the outcome it points
   to: a hint is refreshed only every TTL/3, so it lives TTL + 2 * TTL/3 (lookup and cleanup), while the outcome itself
   still expires with the session (TTL after its last use), as on mongod after the session is reaped.
2. The outcome of the session's latest transaction (`meta/rw/{session}/txn`) and the record of its latest retryable write
   (`meta/rw/{session}`) are two keys: a retryable write after a committed transaction does not erase the outcome.

Session TTL 9 s through the test flag where timing matters (hint refresh and cleanup every 3 s).
"""
import time

import pytest
from bson import Int64
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket
import test_41_commit_retry as T

TTL_ENV = {"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_SESSION_TTL_MS": "9000"}


def _srv(tag, env=None):
    port = free_port()
    return Server(make_bucket(tag), tag, port=port, env=env, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()


def _restart(srv):
    srv.kill9()
    srv.proc = None
    srv.start()


def _code(f):
    try:
        return f().get("ok")
    except OperationFailure as e:
        return e.code


def _rw_insert(c, lsid, n, docs, db="ct"):
    return c[db].command({"insert": "c", "documents": docs, "lsid": lsid, "txnNumber": Int64(n)})


def test_hint_outlives_the_cleanup_while_the_session_lives_and_goes_after():
    """Txn 1 at 0 writes the hint, txn 2 at ~2.7 s does not rewrite it. The server keeps running past 9 s (the cleanup
    passes at 3, 6, 9 s): the hint must still be there; a commit of 2 repeated after a restart at ~11 s (session alive
    until ~11.7 s) answers ok. After the session expired the repeat answers 251 (as mongod once the session is reaped),
    and the cleanup removes the hint."""
    srv = _srv("c10h", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        c["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        hint = f"txn-hints/{lsid['id'].hex()}/ct"
        t0 = time.time()
        assert T._txn_insert(c, lsid, 1, {"_id": 1})["ok"] == 1.0
        time.sleep(max(0, 2.7 - (time.time() - t0)))
        assert T._txn_insert(c, lsid, 2, {"_id": 2})["ok"] == 1.0
        t2 = time.time() - t0
        time.sleep(max(0, 10.6 - (time.time() - t0)))
        still = [k for k, _ in list_prefix(srv.bucket, "txn-hints/")]
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        got = _code(lambda: T._commit(c, lsid, 2))
        at = time.time() - t0
        print("live session:", {"txn2_at": round(t2, 2), "repeat_at": round(at, 2), "alive_until": round(t2 + 9, 2), "repeat": got, "hints": still})
        assert t2 < 3.0 and at < t2 + 9.0, (t2, at)
        assert still == [hint], still
        assert got == 1.0, got
        # the repeat above is a use of the session only in memory; after the expiry of the stored outcome: 251
        c.close()
        time.sleep(max(0, t2 + 9.0 + 1.0 - (time.time() - t0)))
        _restart(srv)
        c = T.Raw(srv)
        late = _code(lambda: T._commit(c, lsid, 2))
        assert late == 251, late
        deadline = time.time() + 15
        while time.time() < deadline and list_prefix(srv.bucket, "txn-hints/"):
            time.sleep(0.5)
        gone_at = time.time() - t0
        print("expired:", {"repeat": late, "hint_gone_at": round(gone_at, 2)})
        assert list_prefix(srv.bucket, "txn-hints/") == []
        assert sorted(d["_id"] for d in c.ct.c.find({})) == [0, 1, 2]
    finally:
        srv.kill9()


def test_outcome_and_retryable_write_live_together():
    """Txn 1 committed, then retryable write 2 in the same database, restart: the commit of 1 repeated answers ok (the
    outcome is stored; before C10 the write had erased it: 251); the write 2 retried answers from its record
    (retriedStmtIds [0]) and inserts nothing; a write with the old number 1 is TransactionTooOld."""
    srv = _srv("c10o")
    try:
        c = T.Raw(srv)
        lsid = T._lsid()
        assert T._txn_insert(c, lsid, 1, {"_id": 1, "amount": 100})["ok"] == 1.0
        assert _rw_insert(c, lsid, 2, [{"_id": 2}])["ok"] == 1.0
        keys = sorted(k for k, _ in list_prefix(srv.bucket, "txn-hints/"))
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        got = _code(lambda: T._commit(c, lsid, 1))
        r = _rw_insert(c, lsid, 2, [{"_id": 2}])
        old = _code(lambda: _rw_insert(c, lsid, 1, [{"_id": 3}]))
        print("outcome and write:", {"repeat_commit_1": got, "retried": r.get("retriedStmtIds"), "old_write": old, "hints": keys})
        assert got == 1.0, got
        assert r.get("retriedStmtIds") == [0] and r.get("n") == 1, r
        assert old == 225, old
        assert sorted(d["_id"] for d in c.ct.c.find({})) == [1, 2]
    finally:
        srv.kill9()


def test_retryable_write_then_transaction_number_space_after_restart():
    """Retryable write 1, then txn 2 committed in the same database, restart: the write 1 retried is TransactionTooOld,
    a write at 2 is refused as the number of a transaction (217), the commit of 2 repeated answers ok, the commit of 1
    is TransactionTooOld (225), never 251."""
    srv = _srv("c10n")
    try:
        c = T.Raw(srv)
        lsid = T._lsid()
        assert _rw_insert(c, lsid, 1, [{"_id": "w"}])["ok"] == 1.0
        assert T._txn_insert(c, lsid, 2, {"_id": "t"})["ok"] == 1.0
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res = {
            "commit_2": _code(lambda: T._commit(c, lsid, 2)),
            "commit_1": _code(lambda: T._commit(c, lsid, 1)),
        }
        c.close()
        _restart(srv)                        # the writes below load the session from the database, not from a commit
        c = T.Raw(srv)
        res["write_1"] = _code(lambda: _rw_insert(c, lsid, 1, [{"_id": "w"}]))
        res["write_2"] = _code(lambda: _rw_insert(c, lsid, 2, [{"_id": "x"}]))
        print("number space:", res)
        assert res == {"commit_2": 1.0, "commit_1": 225, "write_1": 225, "write_2": 217}, res
        assert sorted(d["_id"] for d in c.ct.c.find({})) == ["t", "w"]
    finally:
        srv.kill9()


def test_two_transactions_and_writes_in_two_databases():
    """Txn 1 in a, retryable write 2 in a, txn 3 in b, retryable write 4 in b, restart: commit 3 ok, commit 1 ok (its
    outcome in a is still stored), abort 3 refused as committed (256), commit 2 (a write's number) 225, never 251."""
    srv = _srv("c10d")
    try:
        c = T.Raw(srv)
        lsid = T._lsid()
        c["a"].command({"insert": "c", "documents": [{"_id": "seed"}]})
        c["b"].command({"insert": "c", "documents": [{"_id": "seed"}]})
        c["a"].command({"insert": "c", "documents": [{"_id": 1}], "lsid": lsid, "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        assert T._commit(c, lsid, 1)["ok"] == 1.0
        assert _rw_insert(c, lsid, 2, [{"_id": 2}], db="a")["ok"] == 1.0
        c["b"].command({"insert": "c", "documents": [{"_id": 3}], "lsid": lsid, "txnNumber": Int64(3), "startTransaction": True, "autocommit": False})
        assert T._commit(c, lsid, 3)["ok"] == 1.0
        assert _rw_insert(c, lsid, 4, [{"_id": 4}], db="b")["ok"] == 1.0
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res = {
            "commit_3": _code(lambda: T._commit(c, lsid, 3)),
            "commit_1": _code(lambda: T._commit(c, lsid, 1)),
            "commit_2": _code(lambda: T._commit(c, lsid, 2)),
        }
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["abort_3"] = _code(lambda: c.admin.command({"abortTransaction": 1, "lsid": lsid, "txnNumber": Int64(3), "autocommit": False}))
        print("two databases:", res)
        assert res == {"commit_3": 1.0, "commit_1": 1.0, "commit_2": 225, "abort_3": 256}, res
    finally:
        srv.kill9()
