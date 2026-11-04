"""Review of C12 (PR #52): tries to break the reload of a session's memory when a database changes process.

Run against any binary with MONGO_SERVER_BIN; every test prints what it saw (JSON) for the report.
"""
import json
import os
import random
import threading
import time

import pytest
from bson import Int64
from pymongo import MongoClient, monitoring
from pymongo.errors import OperationFailure

from lib import make_bucket
import test_41_commit_retry as T
from test_42_sessions_move import _srv, _on, _rw, _val


def _try(f):
    try:
        r = f()
        return {"ok": 1, "retried": r.get("retriedStmtIds")}
    except OperationFailure as e:
        return {"code": e.code}


class _KeepOpen:
    """Writes (plain inserts) to a database every 0.4 s so the idle closer leaves it open."""

    def __init__(self, srv, db):
        self.c, self.db, self.stop = MongoClient(srv.uri, serverSelectionTimeoutMS=30000), db, False
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()

    def _run(self):
        while not self.stop:
            try:
                self.c[self.db].k.insert_one({"t": time.time()})
            except Exception:
                pass
            time.sleep(0.4)

    def close(self):
        self.stop = True
        self.t.join()


def test_old_retry_after_aborted_transaction_and_reopening_runs_a_write_twice():
    """One process. Session S: retryable write 3 in a, retryable write 5 in b, transaction 6 in a (aborted). a goes idle and
    is opened again (b stays open). Retry of 3 in a, then retry of 5 in b. mongod: both 225 (6 started). C12: the reload of
    a takes a's record as the authority ("the number came from a"), the memory drops from 6 to 3, and the retry of 5 in b
    (b never reopened, so not reloaded) is taken as a new write: b's counter goes from 1 to 2."""
    bucket = make_bucket("r12abort")
    P1 = _srv("r12abort1", bucket, "p1")
    keep = None
    try:
        r1 = T.Raw(P1)
        lsid = T._lsid()
        assert _rw(r1, lsid, 3, db="a")["ok"] == 1.0
        r1["b"].command({"insert": "k", "documents": [{"_id": "made"}]})   # b exists: the next write sees it open
        assert _rw(r1, lsid, 5, db="b")["ok"] == 1.0
        keep = _KeepOpen(P1, "b")
        r1["a"].command({"update": "c", "updates": [{"q": {"_id": "t"}, "u": {"$inc": {"v": 1}}, "upsert": True}], "lsid": lsid,
                         "txnNumber": Int64(6), "startTransaction": True, "autocommit": False})
        r1.admin.command({"abortTransaction": 1, "lsid": lsid, "txnNumber": Int64(6), "autocommit": False})
        time.sleep(5)                       # a closes when idle (2 s); b is kept open
        got = {"retry_3_in_a": _try(lambda: _rw(r1, lsid, 3, db="a")), "retry_5_in_b": _try(lambda: _rw(r1, lsid, 5, db="b"))}
        keep.close()
        keep = None
        got.update({"rw_a": _val(P1, "a", "rw"), "rw_b": _val(P1, "b", "rw")})
        print(json.dumps(got))
        assert got["rw_b"] == 1, got
        assert got["retry_5_in_b"].get("code") == 225, got
    finally:
        if keep:
            keep.close()
        P1.kill9()


def test_old_retry_after_aborted_transaction_same_database():
    """Same as above with one database: write 3, transaction 6 (aborted), reopening, retry of 3. mongod: 225. Nothing runs
    twice either way (the record of 3 is in a); printed to see the difference."""
    bucket = make_bucket("r12abort1db")
    P1 = _srv("r12abort1db1", bucket, "p1")
    try:
        r1 = T.Raw(P1)
        lsid = T._lsid()
        assert _rw(r1, lsid, 3)["ok"] == 1.0
        r1["ct"].command({"update": "c", "updates": [{"q": {"_id": "t"}, "u": {"$inc": {"v": 1}}, "upsert": True}], "lsid": lsid,
                          "txnNumber": Int64(6), "startTransaction": True, "autocommit": False})
        r1.admin.command({"abortTransaction": 1, "lsid": lsid, "txnNumber": Int64(6), "autocommit": False})
        time.sleep(5)
        got = {"retry_3": _try(lambda: _rw(r1, lsid, 3)), "rw": _val(P1, "ct", "rw")}
        print(json.dumps(got))
        assert got["rw"] == 1, got
    finally:
        P1.kill9()


def test_ttl_wake_of_a_sleeping_database_after_a_retryable_write():
    """--ttl-wake-sleeping: a document expires while the database sleeps; the maintenance pass wakes it and removes the
    document. The last command before the close is a retryable write, which leaves `require_opening` set on the entry; the
    TTL wake takes the entry without `Dbs::lock` (which is what clears it), opens the database in a new opening, and its
    batch is refused with 134."""
    bucket = make_bucket("r12ttl")
    from lib import Server, free_port
    port = free_port()
    import os, stat, tempfile
    from lib import BIN
    wrap = tempfile.NamedTemporaryFile("w", delete=False, suffix=".sh")
    wrap.write('#!/bin/bash\nargs=()\nfor a in "$@"; do [ "$a" != "--no-ttl" ] && args+=("$a"); done\nexec %s "${args[@]}"\n' % BIN)
    wrap.close()
    os.chmod(wrap.name, stat.S_IRWXU)
    P1 = Server(bucket, "r12ttl1b", port=port, holder="p1", idle=2, binary=wrap.name,
                extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}", "--lease-ttl-secs", "3",
                       "--ttl-wake-sleeping", "--ttl-maintenance-secs", "1", "--ttl-min-gap-secs", "1", "--ttl-sweep-secs", "600"]).start()
    try:
        r1 = T.Raw(P1)
        r1["ct"].command({"createIndexes": "e", "indexes": [{"key": {"at": 1}, "name": "at_1", "expireAfterSeconds": 0}]})
        lsid = T._lsid()
        import datetime
        soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=4)
        r1["ct"].command({"insert": "e", "documents": [{"_id": i, "at": soon} for i in range(5)], "lsid": lsid, "txnNumber": Int64(1)})
        r1["ct"].command({"find": "e", "filter": {}})   # a shared read: marks the tenant as having TTL documents, takes no lock()
        time.sleep(20)                       # close at ~2 s, expiry at 4 s, passes every 1 s
        log = P1.log_text()
        failed = [l for l in log.splitlines() if "ttl wake of ct failed" in l]
        r1.close()
        # how many documents are still stored (reads hide expired ones): a plain write sweeps first, so count via the log
        wakes = log.count("LEASE acquired tenant=ct")
        left = T.Raw(P1)["ct"].command({"aggregate": "e", "pipeline": [{"$collStats": {"count": {}}}], "cursor": {}})["cursor"]["firstBatch"] if False else None
        got = {"ttl_wake_failures": len(failed), "lease_acquisitions": wakes, "first_failure": failed[0][:300] if failed else None}
        print(json.dumps(got))
        assert not failed, got
    finally:
        P1.kill9()


def test_three_processes_random_moves_exactly_once():
    """Three processes on one bucket, two databases, three sessions. 18 rounds: a new retryable write on a random process,
    then (half the time) the same write retried on another random process (must be answered from the record), and
    sometimes an older write retried (must be 225). At the end each counter equals the number of new writes."""
    seed = int(os.environ.get("R12_SEED", time.time()))
    print("seed", seed)
    rnd = random.Random(seed)
    bucket = make_bucket("r12three")
    P = [_srv(f"r12three{i}", bucket, f"p{i}") for i in range(3)]
    try:

        sessions = [T._lsid() for _ in range(3)]
        num = [0, 0, 0]
        expect = {}
        bad = []
        log = []
        for rnd_i in range(18):
            s = rnd.randrange(3)
            db = rnd.choice(["a", "b"])
            p = rnd.randrange(3)
            num[s] += 1
            _id = f"s{s}"
            r = _on(lambda: _rw(T.Raw(P[p]), sessions[s], num[s], db=db, _id=_id), timeout=60)
            expect[(db, _id)] = expect.get((db, _id), 0) + 1
            if r.get("retriedStmtIds"):
                bad.append(("new write answered as retry", rnd_i))
            entry = {"r": rnd_i, "s": s, "n": num[s], "db": db, "p": p}
            if rnd.random() < 0.5:
                q = rnd.choice([x for x in range(3) if x != p])
                rr = _on(lambda: _rw(T.Raw(P[q]), sessions[s], num[s], db=db, _id=_id), timeout=60)
                entry["retry_on"] = q
                if rr.get("retriedStmtIds") != [0]:
                    bad.append(("retry not answered from record", rnd_i, q, rr.get("retriedStmtIds")))
            if num[s] > 1 and rnd.random() < 0.3:
                q = rnd.randrange(3)
                try:
                    _on(lambda: _rw(T.Raw(P[q]), sessions[s], num[s] - 1, db=db, _id=_id), timeout=60)
                    bad.append(("older retry accepted", rnd_i, q))
                except OperationFailure as e:
                    if e.code != 225:
                        bad.append(("older retry code", rnd_i, q, e.code))
                entry["old_on"] = q
            log.append(entry)
        final = {f"{db}.{i}": _val(P[0], db, i) for (db, i) in expect}
        want = {f"{db}.{i}": v for (db, i), v in expect.items()}
        print(json.dumps({"log": log, "final": final, "want": want, "bad": bad}))
        assert final == want and not bad, (final, want, bad)
    finally:
        for p in P:
            p.kill9()


def test_plain_write_and_driver_retry_of_134():
    """Question 5 of the prompt. (a) Plain writes (no txnNumber) right after the database came back never get 134 from the new
    check. (b) The driver (pymongo, retryWrites) retries a 134 by itself: P2 holds the database, an insert on P1 gets 134
    (TenantLeaseHeld, label RetryableWriteError, the same error the new check returns); the command monitor counts the
    attempts."""
    bucket = make_bucket("r12plain")
    P1, P2 = _srv("r12plain1", bucket, "p1"), _srv("r12plain2", bucket, "p2")

    class L(monitoring.CommandListener):
        def __init__(self):
            self.s, self.f = [], []

        def started(self, e):
            if e.command_name == "insert":
                self.s.append(e.command.get("txnNumber"))

        def succeeded(self, e):
            pass

        def failed(self, e):
            if e.command_name == "insert":
                self.f.append(getattr(e.failure, "get", lambda k: None)("code") if isinstance(e.failure, dict) else str(e.failure)[:80])

    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        plain_errors = []
        for i in range(4):
            _on(lambda: r2["ct"].command({"insert": "o", "documents": [{"i": i}]}))
            time.sleep(3.5)                     # P2 closes it
            try:
                r1["ct"].command({"insert": "c", "documents": [{"i": i}]})
            except OperationFailure as e:
                plain_errors.append(e.code)
            time.sleep(3.5)                     # P1 closes it
        # (b) P2 holds the database (kept busy), the driver on P1 tries an insert
        keep = _KeepOpen(P2, "ct")
        _on(lambda: r2["ct"].command({"insert": "o", "documents": [{"hold": 1}]}))
        lis = L()
        c = MongoClient(P1.uri, serverSelectionTimeoutMS=30000, retryWrites=True, event_listeners=[lis])
        err = None
        try:
            c["ct"].c.insert_one({"x": 1})
        except Exception as e:
            err = (getattr(e, "code", None), str(e)[:120], getattr(e, "_error_labels", None))
        keep.close()
        got = {"plain_errors": plain_errors, "insert_attempts": len(lis.s), "same_txnNumber": len(set(map(str, lis.s))) == 1,
               "final_error": err}
        print(json.dumps(got, default=str))
        assert plain_errors == [], got
        assert len(lis.s) == 2, got
    finally:
        P1.kill9()
        P2.kill9()


def test_node_driver_retries_134_once():
    """Same as (b) above with the Node driver (mongodb 6.10, NODE_PATH pointing at its node_modules)."""
    import shutil
    import subprocess
    if not shutil.which("node") or not os.environ.get("NODE_PATH"):
        pytest.skip("node or NODE_PATH missing")
    bucket = make_bucket("r12node")
    P1, P2 = _srv("r12node1", bucket, "p1"), _srv("r12node2", bucket, "p2")
    try:
        keep = _KeepOpen(P2, "ct")
        _on(lambda: T.Raw(P2)["ct"].command({"insert": "o", "documents": [{"hold": 1}]}))
        out = subprocess.run(["node", os.path.join(os.path.dirname(__file__), "r12_node_insert.js"), P1.uri], capture_output=True, text=True, timeout=120)
        keep.close()
        print(out.stdout.strip(), out.stderr.strip()[:300])
        got = json.loads(out.stdout.strip().splitlines()[-1])
        assert got["attempts"] == 2 and len(set(got["txnNumbers"])) == 1, got
    finally:
        P1.kill9()
        P2.kill9()
