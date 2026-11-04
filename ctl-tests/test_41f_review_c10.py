"""Review of PR #50 (C10): the hint that lives 50 minutes, and the outcome of a transaction in a key of its own.

Everything goes through a small proxy in front of the test S3 that answers HEAD with the real Content-Length, counts the
requests by kind, and can shift the clock of the store (the LastModified of a LIST) to try a store whose clock is behind
or ahead of ours. Session TTL 9 s through the test flag where timing matters: hint refresh every 3 s, hint TTL 15 s,
cleanup every 3 s. 1 minute of the real rule is 0.3 s here.

The binary is MONGO_SERVER_BIN as for the other tests; the migration test also wants REV_OLD_BIN (the code before C10).
"""
import datetime as dt
import http.client
import http.server
import json
import os
import re
import socketserver
import threading
import time

import pytest
from bson import Int64
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket, MINIO, BIN
import test_41_commit_retry as T

TTL_ENV = {"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_SESSION_TTL_MS": "9000"}
MIN = 0.3        # one minute of the real rule, in seconds of the test rule (TTL 30 min -> 9 s)

PX = {"skew": 0.0, "count": {}}
_HOST, _PORT = MINIO.split("//", 1)[1].split(":")
_LM = re.compile(rb"<LastModified>([^<]+)</LastModified>")


def _kind(method, path):
    p = path.split("?", 1)[0]
    if method == "GET" and ("list-type" in path or "prefix=" in path) and p.count("/") <= 2:
        return "LIST"
    for k in ("/wal/", "/txn-hints/", "/compacted/", "/manifest/"):
        if k in p:
            return f"{method} {k.strip('/')}"
    return f"{method} other"


def _shift(m):
    t = dt.datetime.strptime(m.group(1).decode()[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    frac = m.group(1).decode()[19:23] if len(m.group(1)) > 19 else ".000"
    t = t - dt.timedelta(seconds=PX["skew"])
    return b"<LastModified>" + (t.strftime("%Y-%m-%dT%H:%M:%S") + (frac if frac.startswith(".") else ".000") + "Z").encode() + b"</LastModified>"


class _Px(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _go(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        k = _kind(self.command, self.path)
        PX["count"][k] = PX["count"].get(k, 0) + 1
        c = http.client.HTTPConnection(_HOST, int(_PORT), timeout=120)
        c.request(self.command, self.path, body=body, headers={k: v for k, v in self.headers.items() if k.lower() != "connection"})
        r = c.getresponse()
        data = r.read()
        if k == "LIST" and PX["skew"]:
            data = _LM.sub(_shift, data)
        self.send_response(r.status)
        for h, v in r.getheaders():
            if h.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(h, v)
        self.send_header("Content-Length", r.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(data)
            except OSError:
                pass

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _go

    def log_message(self, *a):
        pass


class _TS(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


_pport = free_port()
threading.Thread(target=_TS(("127.0.0.1", _pport), _Px).serve_forever, daemon=True).start()
EP = f"http://127.0.0.1:{_pport}"


def _srv(tag, bucket=None, env=None, holder=None, binary=None, idle=60):
    port = free_port()
    kw = {"holder": holder} if holder else {}
    return Server(bucket or make_bucket(tag), tag, port=port, endpoint=EP, env=env, binary=binary, idle=idle,
                  extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"], **kw).start()


def _restart(srv, binary=None):
    srv.kill9()
    srv.proc = None
    if binary:
        srv.binary = binary
    srv.start()


def _code(f):
    try:
        r = f()
        return r.get("ok")
    except OperationFailure as e:
        return e.code


def _begin(c, lsid, n, cmd, db="ct"):
    c[db].command(dict(cmd, lsid=lsid, txnNumber=Int64(n), startTransaction=True, autocommit=False))


def _inc_txn(c, lsid, n, db="ct"):
    """A transaction that adds 1 to the counter and commits."""
    _begin(c, lsid, n, {"update": "c", "updates": [{"q": {"_id": "counter"}, "u": {"$inc": {"v": 1}}}]}, db=db)
    return T._commit(c, lsid, n)


def _rw(c, lsid, n, db="ct", docs=None):
    """A retryable write: an insert of `docs` (two statements) or a $inc of the counter."""
    if docs is not None:
        return c[db].command({"insert": "c", "documents": docs, "lsid": lsid, "txnNumber": Int64(n)})
    return c[db].command({"update": "c", "updates": [{"q": {"_id": "rw"}, "u": {"$inc": {"v": 1}}, "upsert": True}], "lsid": lsid, "txnNumber": Int64(n)})


def _driver_commit_retry(c, lsid, n, db="ct", rerun=None):
    """What a driver's withTransaction does with the answer of a repeated commit: ok or an error without the label ends
    it; 251 (TransientTransactionError) runs the whole transaction again, under a new number."""
    try:
        T._commit(c, lsid, n)
        return "ok"
    except OperationFailure as e:
        if "TransientTransactionError" in (e.details or {}).get("errorLabels", []):
            if rerun:
                rerun()
            return f"{e.code} rerun"
        return e.code


def _val(srv, db, _id):
    d = T._Pm(srv)[db].c.find_one({"_id": _id})
    return (d or {}).get("v")


def _seed(c, db="ct"):
    c[db].command({"insert": "c", "documents": [{"_id": "counter", "v": 0}]})


# ------------------------------------------------------------------------- error 1: the time of the hint (accelerated)

@pytest.mark.parametrize("idle_min", [25, 35, 45])
def test_session_idle_25_35_45_minutes(idle_min):
    """Txn 1 at 0 writes the hint; txn 2 at ~9 min (2.7 s) does not rewrite it (fresh in memory): the worst case for the
    hint. The session then sits idle `idle_min` minutes while the server runs (the cleanup passes every 10 minutes), and
    after a restart the commit of 2 is repeated. Alive (25): ok, and the hint is still there. Expired (35, 45): 251 is
    the right answer (the session is gone, as on mongod after the reaper), and the transaction is never run twice."""
    srv = _srv(f"r10i{idle_min}", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        lsid = T._lsid()
        t0 = time.time()
        assert _inc_txn(c, lsid, 1)["ok"] == 1.0
        time.sleep(max(0, 9 * MIN - (time.time() - t0)))
        assert _inc_txn(c, lsid, 2)["ok"] == 1.0
        t2 = time.time() - t0
        mem = None
        time.sleep(max(0, t2 + idle_min * MIN - (time.time() - t0)))
        hints = [k for k, _ in list_prefix(srv.bucket, "txn-hints/")]
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        at = time.time() - t0
        rerun = lambda: _inc_txn(c, lsid, 3)
        got = _driver_commit_retry(c, lsid, 2, rerun=rerun)
        v = _val(srv, "ct", "counter")
        print(json.dumps({"idle_min": idle_min, "txn2_at_s": round(t2, 2), "repeat_at_s": round(at, 2), "hint_age_min": round(at / MIN, 1),
                          "session_idle_min": round((at - t2) / MIN, 1), "hints_before_restart": len(hints), "repeat": got, "counter": v}))
        if idle_min == 25:
            assert hints, "the cleanup deleted the hint of a live session"
            assert got == "ok", got
            assert v == 2
        else:
            assert got in ("ok", "251 rerun"), got
    finally:
        srv.kill9()


@pytest.mark.parametrize("idle_min", [25, 35, 45])
def test_session_idle_in_memory(idle_min):
    """Same as above without the restart: the process remembers the transaction."""
    srv = _srv(f"r10m{idle_min}", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        lsid = T._lsid()
        t0 = time.time()
        assert _inc_txn(c, lsid, 1)["ok"] == 1.0
        time.sleep(max(0, 9 * MIN - (time.time() - t0)))
        assert _inc_txn(c, lsid, 2)["ok"] == 1.0
        t2 = time.time() - t0
        time.sleep(max(0, t2 + idle_min * MIN - (time.time() - t0)))
        got = _driver_commit_retry(c, lsid, 2, rerun=lambda: _inc_txn(c, lsid, 3))
        print(json.dumps({"idle_min": idle_min, "memory_repeat": got, "counter": _val(srv, "ct", "counter")}))
        if idle_min == 25:
            assert got == "ok"
    finally:
        srv.kill9()


@pytest.mark.parametrize("skew_min", [-20, 0, 5, 9, 15])
def test_store_clock_behind_or_ahead(skew_min):
    """The age of a hint is decided by the clock of S3 (LastModified), the expiry of the outcome by ours. The proxy
    moves the store's clock `skew_min` minutes behind ours (negative: ahead). Worst case for the hint: written at 0,
    txn 2 at ~9.7 min (not rewritten), the commit of 2 repeated after a restart at ~39 min (the outcome lives until
    ~39.7). The server keeps running (the cleanup also reads the store's clock). The margin of the rule is 10 minutes:
    behind by 5 or 9 must still answer ok; 15 shows where the rule ends."""
    PX["skew"] = skew_min * MIN
    srv = _srv(f"r10k{abs(skew_min)}", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        lsid = T._lsid()
        t0 = time.time()
        assert _inc_txn(c, lsid, 1)["ok"] == 1.0
        time.sleep(max(0, 9.7 * MIN - (time.time() - t0)))
        assert _inc_txn(c, lsid, 2)["ok"] == 1.0
        t2 = time.time() - t0
        time.sleep(max(0, t2 + 28.7 * MIN - (time.time() - t0)))
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        at = time.time() - t0
        got = _driver_commit_retry(c, lsid, 2, rerun=lambda: _inc_txn(c, lsid, 3))
        print(json.dumps({"skew_min": skew_min, "txn2_at_min": round(t2 / MIN, 1), "repeat_at_min": round(at / MIN, 1),
                          "outcome_alive_until_min": round((t2 + 9) / MIN, 1), "repeat": got, "counter": _val(srv, "ct", "counter")}))
        assert at < t2 + 9.0, "too slow: the outcome itself expired"
        if skew_min <= 9:
            assert got == "ok", got
    finally:
        PX["skew"] = 0.0
        srv.kill9()


def test_cleanup_never_deletes_a_useful_hint():
    """Many sessions, each committing at a random moment of its hint's life (0..9.9 min after the hint), checked all along
    while the server runs for 40 minutes: whenever a session's outcome is alive, its hint must be there."""
    import random
    rnd = random.Random(7)
    srv = _srv("r10c", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        sess = []
        t0 = time.time()
        plan = sorted((rnd.uniform(0, 12) * MIN, i) for i in range(12))
        for at, i in plan:
            time.sleep(max(0, at - (time.time() - t0)))
            lsid = T._lsid()
            assert _inc_txn(c, lsid, 1)["ok"] == 1.0
            first = time.time() - t0
            time.sleep(rnd.uniform(0, 9.9) * MIN if i % 2 else 0)
            assert _inc_txn(c, lsid, 2)["ok"] == 1.0
            sess.append((lsid["id"].hex(), first, time.time() - t0))
        bad = []
        while time.time() - t0 < 40 * MIN + 12 * MIN:
            now = time.time() - t0
            have = {k.split("/")[1] for k, _ in list_prefix(srv.bucket, "txn-hints/")}
            for key, first, last in sess:
                if now < last + 9.0 - 0.3 and key not in have:      # outcome alive (minus the time of a listing)
                    bad.append((key[:8], round(first / MIN, 1), round(last / MIN, 1), round(now / MIN, 1)))
            time.sleep(0.25)
        print(json.dumps({"sessions": len(sess), "missing_while_alive": bad[:5], "n_bad": len(bad)}))
        assert not bad, bad
    finally:
        srv.kill9()


# -------------------------------------------------------------- error 2: outcome and retryable writes in separate keys

def test_commit_repeated_after_several_retryable_writes():
    """Txn 1 ($inc), then retryable writes 2, 3, 4 in the same database and 5 in another: the commit of 1 repeated in
    memory, after a restart, and after a second restart at a process that loads the session from the other database:
    never 251, the counter incremented once, the last write retried answers from its record."""
    srv = _srv("r10w")
    try:
        c = T.Raw(srv)
        _seed(c); _seed(c, "o")
        lsid = T._lsid()
        assert _inc_txn(c, lsid, 1)["ok"] == 1.0
        for n in (2, 3, 4):
            assert _rw(c, lsid, n)["ok"] == 1.0
        assert _rw(c, lsid, 5, db="o")["ok"] == 1.0
        res = {"memory": _driver_commit_retry(c, lsid, 1, rerun=lambda: _inc_txn(c, lsid, 6))}
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["restart"] = _driver_commit_retry(c, lsid, 1, rerun=lambda: _inc_txn(c, lsid, 6))
        r4 = _rw(c, lsid, 4)                         # loads the session from ct: number 4 is the latest there
        res["retry_4"] = r4.get("retriedStmtIds")
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["retry_5_other_db"] = _rw(c, lsid, 5, db="o").get("retriedStmtIds")
        res["restart2"] = _driver_commit_retry(c, lsid, 1, rerun=lambda: _inc_txn(c, lsid, 6))
        res["counter"] = _val(srv, "ct", "counter")
        res["rw_ct"], res["rw_o"] = _val(srv, "ct", "rw"), _val(srv, "o", "rw")
        print(json.dumps(res))
        assert res["memory"] != "251 rerun" and res["restart"] == "ok" and res["restart2"] in ("ok", 225), res
        assert res["retry_4"] == [0] and res["retry_5_other_db"] == [0], res
        assert res["counter"] == 1 and res["rw_ct"] == 3 and res["rw_o"] == 1, res
    finally:
        srv.kill9()


def test_commit_repeated_after_retryable_write_and_cleanup_of_the_outcome():
    """Txn 1 at 0, retryable write 2 at ~20 min keeps the session alive; another session's write at ~35 min runs the
    cleanup inside its batch, which removes the outcome of txn 1 (30 minutes old). The commit of 1 repeated after a
    restart at ~38 min (session alive until ~50): must not be 251 (225, as mongod: a newer number exists)."""
    srv = _srv("r10s", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        lsid, other = T._lsid(), T._lsid()
        t0 = time.time()
        assert _inc_txn(c, lsid, 1)["ok"] == 1.0
        time.sleep(max(0, 20 * MIN - (time.time() - t0)))
        assert _rw(c, lsid, 2)["ok"] == 1.0
        time.sleep(max(0, 35 * MIN - (time.time() - t0)))
        assert _rw(c, other, 1)["ok"] == 1.0
        time.sleep(max(0, 38 * MIN - (time.time() - t0)))
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        got = _driver_commit_retry(c, lsid, 1, rerun=lambda: _inc_txn(c, lsid, 3))
        print(json.dumps({"repeat_commit_1_at_min": round((time.time() - t0) / MIN, 1), "repeat": got, "counter": _val(srv, "ct", "counter")}))
        assert got != "251 rerun", got
        assert _val(srv, "ct", "counter") == 1
    finally:
        srv.kill9()


def test_two_processes_outcome_and_writes():
    """One session on two processes over one bucket: txn 1 in a on P1, retryable write 2 in b on P2, retryable write 3
    in a on P1, txn 4 in b on P2, retryable write 5 in a on P1. The commits of 1 and 4 repeated on both processes, before
    and after both restart, and after P1 dies for good (P2 takes a over): never 251, counters incremented once, the
    writes retried never applied twice."""
    bucket = make_bucket("r10two")
    P1 = _srv("r10two1", bucket=bucket, holder="p1")
    P2 = _srv("r10two2", bucket=bucket, holder="p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        _seed(r1, "a"); _seed(r2, "b")
        lsid = T._lsid()
        assert _inc_txn(r1, lsid, 1, db="a")["ok"] == 1.0
        assert _rw(r2, lsid, 2, db="b")["ok"] == 1.0
        assert _rw(r1, lsid, 3, db="a")["ok"] == 1.0
        assert _inc_txn(r2, lsid, 4, db="b")["ok"] == 1.0
        assert _rw(r1, lsid, 5, db="a")["ok"] == 1.0
        seen = {}

        def ask(tag, r):
            for n in (1, 4):
                v = _driver_commit_retry(r, lsid, n)
                deadline = time.time() + 20
                while v == 134 and time.time() < deadline:
                    time.sleep(0.5)
                    v = _driver_commit_retry(r, lsid, n)
                seen[f"{tag}_commit{n}"] = v
        ask("P1", r1); ask("P2", r2)
        r1.close(); r2.close()
        _restart(P1); _restart(P2)
        r1, r2 = T.Raw(P1), T.Raw(P2)
        ask("P1r", r1); ask("P2r", r2)
        P1.kill9()
        time.sleep(8)                               # the lease of P1 (6 s) runs out
        ask("P2only", r2)
        try:
            seen["retry5_on_P2"] = _rw(r2, lsid, 5, db="a").get("retriedStmtIds")
        except OperationFailure as e:
            seen["retry5_on_P2"] = e.code
        seen["counter_a"], seen["counter_b"] = _val(P2, "a", "counter"), _val(P2, "b", "counter")
        seen["rw_a"], seen["rw_b"] = _val(P2, "a", "rw"), _val(P2, "b", "rw")
        print(json.dumps(seen))
        assert not any(v == "251 rerun" for v in seen.values()), seen
        assert seen["P2only_commit1"] == "ok" and seen["P2only_commit4"] == "ok", seen
        assert seen["retry5_on_P2"] == [0], seen
        assert (seen["counter_a"], seen["counter_b"], seen["rw_a"], seen["rw_b"]) == (1, 1, 2, 1), seen
    finally:
        P1.kill9()
        P2.kill9()


def test_database_moves_between_processes_and_back():
    """A database closed for idleness on P1 is opened by P2 and then again by P1, while P1 still remembers the session:
    retryable write 1 on P1, write 2 on P2, then write 2 retried on P1. mongod (one config.transactions) answers it from
    the record. Pre-existing: the session memory of a process is not tied to its hold on the database."""
    bucket = make_bucket("r10mv")
    P1 = _srv("r10mv1", bucket=bucket, holder="p1", idle=2)
    P2 = _srv("r10mv2", bucket=bucket, holder="p2", idle=2)
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        _seed(r1)
        lsid = T._lsid()
        assert _rw(r1, lsid, 1)["ok"] == 1.0

        def on(r, f):
            deadline = time.time() + 40
            while True:
                try:
                    return f()
                except OperationFailure as e:
                    if e.code != 134 or time.time() > deadline:
                        raise
                    time.sleep(0.5)
        on(r2, lambda: _rw(r2, lsid, 2))
        retried = on(r1, lambda: _rw(r1, lsid, 2))
        v = _val(P1, "ct", "rw") if P1.proc.poll() is None else None
        print(json.dumps({"retry_2_on_P1": retried.get("retriedStmtIds"), "rw": v}))
        assert retried.get("retriedStmtIds") == [0], retried
    finally:
        P1.kill9()
        P2.kill9()


# ------------------------------------------------------------------------------- migration: data of the code before

OLD = os.environ.get("REV_OLD_BIN")


@pytest.mark.skipif(not OLD, reason="REV_OLD_BIN not set")
def test_migration_from_records_of_the_code_before():
    """The code before C10 writes: txn 1 in a (its outcome in meta/rw/{s}), retryable write 2 in b, txn 3 in b, then
    a second session with retryable write 1 and txn 2 in a. The new code then: repeated commits, aborts, retries, and new
    writes on the old records. Never 251 for a committed transaction, nothing applied twice."""
    srv = _srv("r10mig", binary=OLD)
    try:
        c = T.Raw(srv)
        _seed(c, "a"); _seed(c, "b")
        s, u = T._lsid(), T._lsid()
        assert _inc_txn(c, s, 1, db="a")["ok"] == 1.0
        assert _rw(c, s, 2, db="b")["ok"] == 1.0
        assert _inc_txn(c, s, 3, db="b")["ok"] == 1.0
        assert _rw(c, u, 1, db="a")["ok"] == 1.0
        assert _inc_txn(c, u, 2, db="a")["ok"] == 1.0
        c.close()
        _restart(srv, binary=BIN)
        c = T.Raw(srv)
        res = {}
        res["s_commit1"] = _driver_commit_retry(c, s, 1)
        res["s_commit3"] = _driver_commit_retry(c, s, 3)
        res["s_abort3"] = _code(lambda: c.admin.command({"abortTransaction": 1, "lsid": s, "txnNumber": Int64(3), "autocommit": False}))
        res["u_commit2"] = _driver_commit_retry(c, u, 2)
        res["u_commit1"] = _driver_commit_retry(c, u, 1)
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["u_write1"] = _code(lambda: _rw(c, u, 1, db="a"))       # loads u from a: an old txn record, number 2
        res["u_write2"] = _code(lambda: _rw(c, u, 2, db="a"))
        res["s_write2_b"] = _code(lambda: _rw(c, s, 2, db="b"))
        # new code on old records: a new write of s in a overwrites the old outcome there
        res["s_write4_a"] = _code(lambda: _rw(c, s, 4, db="a"))
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["s_commit1_after_write4"] = _driver_commit_retry(c, s, 1)
        res["s_commit3_after_write4"] = _driver_commit_retry(c, s, 3)
        assert _inc_txn(c, s, 5, db="a")["ok"] == 1.0
        assert _rw(c, s, 6, db="a")["ok"] == 1.0
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        res["s_commit5_new"] = _driver_commit_retry(c, s, 5)
        res["counters"] = [_val(srv, "a", "counter"), _val(srv, "b", "counter")]
        res["rw"] = [_val(srv, "a", "rw"), _val(srv, "b", "rw")]
        print(json.dumps(res))
        assert not any(v == "251 rerun" for v in res.values()), res
        assert res["s_commit1"] == "ok" and res["s_commit3"] == "ok" and res["u_commit2"] == "ok", res
        assert res["s_abort3"] == 256 and res["u_commit1"] == 225, res
        assert res["u_write1"] == 225 and res["u_write2"] == 217, res
        assert res["counters"] == [3, 1] and res["rw"] == [3, 1], res
    finally:
        srv.kill9()


@pytest.mark.skipif(not OLD, reason="REV_OLD_BIN not set")
def test_rollback_to_the_code_before():
    """A rollback after the merge: the new code writes txn 1 in a (outcome in meta/rw/{s}/txn), then the old binary
    starts on the same bucket. Informational: what the old code answers to the repeated commit."""
    srv = _srv("r10rb")
    try:
        c = T.Raw(srv)
        _seed(c)
        s = T._lsid()
        assert _inc_txn(c, s, 1)["ok"] == 1.0
        c.close()
        _restart(srv, binary=OLD)
        c = T.Raw(srv)
        got = _driver_commit_retry(c, s, 1)
        w1 = _code(lambda: _rw(c, s, 1))
        print(json.dumps({"old_binary_repeat_commit1": got, "old_binary_write1": w1}))
    finally:
        srv.kill9()


# ------------------------------------------------------------------------------------------------ cost in requests

def _count(f):
    PX["count"] = {}
    f()
    return dict(sorted(PX["count"].items()))


def test_requests_per_commit_and_per_retryable_write():
    """S3 requests counted at the proxy: 50 commits (hint fresh), 50 retryable writes, and one repeated commit on a
    process that does not remember a session that wrote in 8 databases. Printed, compared between binaries by hand."""
    srv = _srv("r10cost")
    try:
        c = T.Raw(srv)
        dbs = [f"d{i}" for i in range(8)]
        for d in dbs + ["ct"]:
            _seed(c, d)
        s = T._lsid()
        assert _inc_txn(c, s, 1)["ok"] == 1.0
        time.sleep(1)
        commits = _count(lambda: [_inc_txn(c, s, n) for n in range(2, 52)])
        rws = _count(lambda: [_rw(c, s, n) for n in range(52, 102)])
        # many sessions, each new: the first use of a session reads its records (one key before C10, two after)
        for i in range(300):
            _rw(c, T._lsid(), 1, docs=[{"_id": f"warm{i}"}])
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        c["ct"].command({"insert": "c", "documents": [{"_id": "open"}]})
        time.sleep(1)
        new_sessions = _count(lambda: [_rw(c, T._lsid(), 1) for _ in range(50)])
        m = T._lsid()
        for i, d in enumerate(dbs):
            _inc_txn(c, m, i + 1, db=d)
        _rw(c, m, 20, db="d0")
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        time.sleep(2)
        rare = _count(lambda: _driver_commit_retry(c, m, 8))
        rare_ans = _driver_commit_retry(c, m, 3)
        print(json.dumps({"binary": BIN, "per_50_commits": commits, "per_50_retryable_writes": rws, "per_50_new_sessions_after_restart": new_sessions, "rare_repeat_8dbs": rare, "rare_answer_txn3": rare_ans}))
    finally:
        srv.kill9()


def test_expired_outcome_alone_after_restart():
    """Small difference found reading `session_load`: a session with only an outcome in a database (no retryable write
    record there) takes its number from the outcome also when it expired, because `last` starts at now. Txn 1 committed,
    the session idle past its TTL (12 s with TTL 9 s), restart, a retryable write with number 1: mongod (session reaped)
    runs it; informational."""
    srv = _srv("r10x", env=TTL_ENV)
    try:
        c = T.Raw(srv)
        _seed(c)
        s = T._lsid()
        assert _inc_txn(c, s, 1)["ok"] == 1.0
        time.sleep(12)
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        got = _code(lambda: _rw(c, s, 1))
        print(json.dumps({"write_1_after_expiry": got}))
    finally:
        srv.kill9()
