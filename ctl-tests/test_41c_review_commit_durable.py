"""Review of PR #34 (C8 fix): cases C8 did not try. Every ok answered for a commit or a retried write must survive a SIGKILL;
no repeated command may hang; no committed transaction may be answered 251 NoSuchTransaction.

S3 goes through a small proxy in front of the test S3 that answers HEAD with the real Content-Length (C7's proxy answered
0 and made durable data look lost). The proxy can slow or fail (503) the PUTs of the WAL and of the hints.
"""
import http.client
import http.server
import socketserver
import threading
import os
import time

from bson import Int64
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket, MINIO
import test_41_commit_retry as T

CTL = {"wal_delay": 0.0, "hint_delay": 0.0, "wal_fail_until": 0.0, "wal_puts": 0, "wal_fails": 0}
OUTAGE = float(os.environ.get("RV_OUTAGE", "8"))
_HOST, _PORT = MINIO.split("//", 1)[1].split(":")


class _Px(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _go(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        wal_put = self.command == "PUT" and "/wal/" in self.path
        if wal_put:
            CTL["wal_puts"] += 1
            if time.time() < CTL["wal_fail_until"]:
                CTL["wal_fails"] += 1
                msg = b"<Error><Code>ServiceUnavailable</Code><Message>slow down</Message></Error>"
                self.send_response(503)
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
                return
            if CTL["wal_delay"]:
                time.sleep(CTL["wal_delay"])
        if self.command == "PUT" and "/txn-hints/" in self.path and CTL["hint_delay"]:
            time.sleep(CTL["hint_delay"])
        c = http.client.HTTPConnection(_HOST, int(_PORT), timeout=120)
        c.request(self.command, self.path, body=body, headers={k: v for k, v in self.headers.items() if k.lower() != "connection"})
        r = c.getresponse()
        data = r.read()
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
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


def _reset():
    CTL.update(wal_delay=0.0, hint_delay=0.0, wal_fail_until=0.0)


def _srv(tag, bucket=None, holder=None, env=None):
    port = free_port()
    kw = {"holder": holder} if holder else {}
    return Server(bucket or make_bucket(tag), tag, port=port, endpoint=EP, env=env, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"], **kw).start()


def _res(f):
    try:
        r = f()
        return r.get("ok") if isinstance(r, dict) else r
    except OperationFailure as e:
        return e.code
    except (OSError, TypeError) as e:
        return f"no reply ({type(e).__name__})"


def _bg(out, key, f, t0):
    def go():
        out[key] = _res(f)
        out[key + "_at"] = round(time.time() - t0, 2)
    th = threading.Thread(target=go, daemon=True)
    th.start()
    return th


def _restart(srv):
    srv.kill9()
    srv.proc = None
    srv.start()


def _begin(c, lsid, n, doc, db="ct"):
    c[db].command({"insert": "c", "documents": [doc], "lsid": lsid, "txnNumber": Int64(n), "startTransaction": True, "autocommit": False})


def _abort(c, lsid, n):
    return c.admin.command({"abortTransaction": 1, "lsid": lsid, "txnNumber": Int64(n), "autocommit": False})


# ------------------------------------------------------------------------------------------------ 1. after an abort

def test_commit_after_abort_is_251_before_and_after_restart():
    srv = _srv("rv41a")
    try:
        c = T.Raw(srv)
        c["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        # txn 1 committed, txn 2 aborted
        _begin(c, lsid, 1, {"_id": 1})
        assert T._commit(c, lsid, 1)["ok"] == 1.0
        _begin(c, lsid, 2, {"_id": 2})
        assert _abort(c, lsid, 2)["ok"] == 1.0
        got = _res(lambda: T._commit(c, lsid, 2))
        assert got == 251, got
        assert _res(lambda: T._commit(c, lsid, 1)) == 225     # older than the current one
        _restart(srv)
        c = T.Raw(srv)
        got = _res(lambda: T._commit(c, lsid, 2))
        assert got == 251, got                                # the stored record is txn 1: txn 2 never committed
        assert sorted(d["_id"] for d in c.ct.c.find({})) == [0, 1]
        # a retried abort of the aborted txn after the restart, then a commit: still nothing applied
        _res(lambda: _abort(c, lsid, 2))
        assert _res(lambda: T._commit(c, lsid, 2)) == 251
        assert c.ct.c.count_documents({"_id": 2}) == 0
    finally:
        _reset()
        srv.kill9()


def test_abort_during_slow_commit_is_refused_and_repeat_is_ok():
    """An abortTransaction sent while the commit waits for S3 must not abort it (117), the repeated commit then ok, the
    document once, also after SIGKILL + restart."""
    srv = _srv("rv41b")
    try:
        c1, c2, c3 = T.Raw(srv), T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        _begin(c1, lsid, 1, {"_id": 1})
        CTL["wal_delay"] = 3.0
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: T._commit(c1, lsid, 1), t0)
        time.sleep(0.8)
        out["abort"] = _res(lambda: _abort(c2, lsid, 1))
        out["repeat"] = _res(lambda: T._commit(c3, lsid, 1))
        out["repeat_at"] = round(time.time() - t0, 2)
        th.join(30)
        _reset()
        print("abort during commit:", out)
        assert out["abort"] == 117, out
        assert out["first"] == 1.0 and out["repeat"] == 1.0, out
        assert out["repeat_at"] >= 2.5, out
        _restart(srv)
        c = T.Raw(srv)
        assert c.ct.c.count_documents({"_id": 1}) == 1
        assert T._commit(c, lsid, 1)["ok"] == 1.0
    finally:
        _reset()
        srv.kill9()


# ------------------------------------------------------------------------------------- 2. S3 fails during the wait

def test_s3_fails_during_wait_and_comes_back():
    """WAL PUTs answer 503 for 8 s. A commit and its repeats: none may hang past the end of the outage plus a margin;
    whatever answered ok must be there after SIGKILL + restart; a later repeat answers the same as the truth."""
    srv = _srv("rv41s")
    try:
        c1, c2 = T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        _begin(c1, lsid, 1, {"_id": 1})
        CTL["wal_fail_until"] = time.time() + OUTAGE
        CTL["wal_fails"] = 0
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: T._commit(c1, lsid, 1), t0)
        time.sleep(1.0)
        th2 = _bg(out, "repeat1", lambda: T._commit(c2, lsid, 1), t0)
        th.join(OUTAGE + 90); th2.join(OUTAGE + 90)
        out["hung"] = th.is_alive() or th2.is_alive()
        out["fails"] = CTL["wal_fails"]
        time.sleep(1)
        c3 = T.Raw(srv)
        out["repeat2"] = _res(lambda: T._commit(c3, lsid, 1))
        out["repeat2_at"] = round(time.time() - t0, 2)
        # a plain write after the outage
        out["plain"] = _res(lambda: c3["ct"].command({"insert": "c", "documents": [{"_id": 9}]}))
        _restart(srv)
        c = T.Raw(srv)
        out["docs_after_restart"] = c.ct.c.count_documents({"_id": 1})
        out["repeat_after_restart"] = _res(lambda: T._commit(c, lsid, 1))
        print("s3 outage:", out)
        assert not out["hung"], out
        acked = any(out.get(k) == 1.0 for k in ("first", "repeat1", "repeat2"))
        assert not (acked and out["docs_after_restart"] == 0), out   # an ok must be durable
        if out["docs_after_restart"] == 1:
            assert out["repeat_after_restart"] == 1.0, out
        else:
            assert out["repeat_after_restart"] == 251, out
    finally:
        _reset()
        srv.kill9()


def test_s3_fails_during_wait_and_comes_back_retried_write():
    srv = _srv("rv41sw")
    try:
        c1, c2 = T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        cmd = {"insert": "c", "documents": [{"_id": 1}, {"_id": 2}], "lsid": lsid, "txnNumber": Int64(3)}
        CTL["wal_fail_until"] = time.time() + OUTAGE
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: c1["ct"].command(cmd), t0)
        time.sleep(1.0)
        th2 = _bg(out, "retry1", lambda: c2["ct"].command(cmd), t0)
        th.join(OUTAGE + 90); th2.join(OUTAGE + 90)
        out["hung"] = th.is_alive() or th2.is_alive()
        c3 = T.Raw(srv)
        out["retry2"] = _res(lambda: c3["ct"].command(cmd))
        _restart(srv)
        c = T.Raw(srv)
        out["docs_after_restart"] = c.ct.c.count_documents({"_id": {"$in": [1, 2]}})
        out["retry_after_restart"] = _res(lambda: c["ct"].command(cmd))
        out["docs_end"] = c.ct.c.count_documents({"_id": {"$in": [1, 2]}})
        print("s3 outage, retried write:", out)
        assert not out["hung"], out
        acked = any(out.get(k) == 1.0 for k in ("first", "retry1", "retry2"))
        assert not (acked and out["docs_after_restart"] < 2), out
        assert out["docs_end"] == 2, out
    finally:
        _reset()
        srv.kill9()


# -------------------------------------------------------------------------------- 3. many sessions in parallel

def _parallel_round(srv, sessions, delay, kill_at=None):
    CTL["wal_delay"] = delay
    out = {}
    lsids = [T._lsid() for _ in range(sessions)]
    conns = [(T.Raw(srv), T.Raw(srv)) for _ in range(sessions)]
    base = int(time.time() * 1000) % 1_000_000 * 1000
    for i, (a, _) in enumerate(conns):
        _begin(a, lsids[i], 1, {"_id": base + i})
    t0 = time.time()
    ths = []
    for i, (a, b) in enumerate(conns):
        ths.append(_bg(out, f"{i}a", (lambda a=a, i=i: T._commit(a, lsids[i], 1)), t0))
        ths.append(_bg(out, f"{i}b", (lambda b=b, i=i: T._commit(b, lsids[i], 1)), t0))
    if kill_at is not None:
        time.sleep(kill_at)
        srv.kill9()
    for th in ths:
        th.join(60)
    hung = sum(th.is_alive() for th in ths)
    _reset()
    for a, b in conns:
        a.close(); b.close()
    return out, base, hung, time.time() - t0


def test_many_sessions_with_repeated_commits_in_parallel():
    srv = _srv("rv41p")
    try:
        c = T.Raw(srv)
        c["ct"].command({"insert": "c", "documents": [{"_id": -1}]})
        out, base, hung, took = _parallel_round(srv, 40, 0.3)
        answers = [out[f"{i}{s}"] for i in range(40) for s in "ab"]
        print("parallel:", {"hung": hung, "took": round(took, 2), "answers": {str(a): answers.count(a) for a in set(answers)}})
        assert hung == 0
        assert all(a == 1.0 for a in answers), out
        assert c.ct.c.count_documents({"_id": {"$gte": base, "$lt": base + 40}}) == 40
        # again, with a SIGKILL while the commits wait for S3 (0.8 s PUTs, kill at 1.2 s)
        out, base, hung, took = _parallel_round(srv, 40, 1.5, kill_at=float(os.environ.get("RV_KILL_AT", "1.0")))
        srv.proc = None
        srv.start()
        c = T.Raw(srv)
        acked = sorted({i for i in range(40) if out.get(f"{i}a") == 1.0 or out.get(f"{i}b") == 1.0})
        present = {d["_id"] - base for d in c.ct.c.find({"_id": {"$gte": base, "$lt": base + 40}})}
        lost = [i for i in acked if i not in present]
        print("parallel with SIGKILL:", {"acked": len(acked), "present": len(present), "lost": lost, "hung": hung})
        assert hung == 0
        assert lost == [], (acked, present)
    finally:
        _reset()
        srv.kill9()


# ---------------------------------------------------------------------------- 4. retryable writes, several stmtIds

def test_retried_multi_statement_write_waits_for_durability():
    srv = _srv("rv41m")
    try:
        c1, c2 = T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0, "n": 0}]})
        lsid = T._lsid()
        ins = {"insert": "c", "documents": [{"_id": 1}, {"_id": 2}, {"_id": 3}], "lsid": lsid, "txnNumber": Int64(1), "stmtIds": [10, 11, 12]}
        # (a) SIGKILL while the original waits for S3: the retry must not say ok
        CTL["wal_delay"] = 4.0
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: c1["ct"].command(ins), t0)
        time.sleep(0.8)
        th2 = _bg(out, "retry", lambda: c2["ct"].command(ins), t0)
        time.sleep(0.7)
        srv.kill9()
        th.join(20); th2.join(20)
        _reset()
        srv.proc = None
        srv.start()
        c = T.Raw(srv)
        docs = c.ct.c.count_documents({"_id": {"$in": [1, 2, 3]}})
        print("multi-stmt kill:", out, docs)
        assert out["retry"] != 1.0 and out["first"] != 1.0, out
        assert docs in (0, 3), docs                          # atomic batch
        # (b) no SIGKILL: update with two statements ($inc) retried while slow: once, retriedStmtIds both
        lsid2 = T._lsid()
        upd = {"update": "c", "updates": [{"q": {"_id": 0}, "u": {"$inc": {"n": 1}}}, {"q": {"_id": 0}, "u": {"$inc": {"n": 10}}}],
               "lsid": lsid2, "txnNumber": Int64(1), "stmtIds": [0, 1]}
        c1, c2 = T.Raw(srv), T.Raw(srv)
        CTL["wal_delay"] = 2.0
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: c1["ct"].command(upd), t0)
        time.sleep(0.6)
        r = c2["ct"].command(upd)
        at = time.time() - t0
        th.join(20)
        _reset()
        print("multi-stmt update retry:", r, round(at, 2), out)
        assert r["ok"] == 1.0 and sorted(r.get("retriedStmtIds", [])) == [0, 1], r
        assert r["nModified"] == 2 and at >= 1.5, (r, at)
        assert c2.ct.c.find_one({"_id": 0})["n"] == 11
        # (c) a retry that adds a new statement to the same txnNumber: the old ones from the record, the new one runs
        ins2 = {"insert": "c", "documents": [{"_id": 20}, {"_id": 21}], "lsid": lsid2, "txnNumber": Int64(2), "stmtIds": [0, 1]}
        assert c1["ct"].command(ins2)["n"] == 2
        ins3 = dict(ins2, documents=[{"_id": 20}, {"_id": 21}, {"_id": 22}], stmtIds=[0, 1, 2])
        r = c2["ct"].command(ins3)
        assert r["n"] == 3 and sorted(r.get("retriedStmtIds", [])) == [0, 1], r
        _restart(srv)
        c = T.Raw(srv)
        assert c.ct.c.count_documents({"_id": {"$in": [20, 21, 22]}}) == 3
        assert c.ct.c.find_one({"_id": 0})["n"] == 11
        r = c["ct"].command(ins3)                                    # retried after the restart: from the record
        assert r["n"] == 3 and sorted(r.get("retriedStmtIds", [])) == [0, 1, 2], r
        assert c.ct.c.count_documents({"_id": {"$in": [20, 21, 22]}}) == 3
        assert _res(lambda: c["ct"].command(upd)) == 225                 # older txnNumber
        assert c.ct.c.find_one({"_id": 0})["n"] == 11
    finally:
        _reset()
        srv.kill9()


# ------------------------------------------------------------------------- 5. two processes, three databases, LIST

def test_two_processes_three_databases_never_251_for_committed():
    bucket = make_bucket("rv41two")
    P1 = _srv("rv41two1", bucket=bucket, holder="p1")
    P2 = _srv("rv41two2", bucket=bucket, holder="p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        lsid = T._lsid()
        plan = [(r1, "a"), (r2, "b"), (r1, "c"), (r2, "b"), (r1, "a"), (r2, "d")]   # txn 1..6
        for n, (r, db) in enumerate(plan, start=1):
            _begin(r, lsid, n, {"_id": n}, db=db)
            assert T._commit(r, lsid, n)["ok"] == 1.0
        sess = lsid["id"].hex()
        hints = sorted(k for k, _ in list_prefix(bucket, f"txn-hints/{sess}/"))
        assert hints == [f"txn-hints/{sess}/{d}" for d in "abcd"], hints
        seen = {}
        for name, r in (("P1", r1), ("P2", r2)):
            for n in range(1, 7):
                seen[(name, n)] = _res(lambda: T._commit(r, lsid, n))
        print("two processes, memory:", seen)
        # both processes remember the session (number 6 is the last on P2; P1 saw up to 5)
        assert all(v != 251 for v in seen.values()), seen
        r1.close(); r2.close()
        _restart(P1)
        _restart(P2)
        r1, r2 = T.Raw(P1), T.Raw(P2)
        after = {}
        for name, r in (("P1", r1), ("P2", r2)):
            for n in range(1, 7):
                v = _res(lambda: T._commit(r, lsid, n))
                deadline = time.time() + 15
                while v == 134 and time.time() < deadline:
                    time.sleep(0.5)
                    v = _res(lambda: T._commit(r, lsid, n))
                after[(name, n)] = v
        print("two processes, after restart:", after)
        assert all(v != 251 for v in after.values()), after
        for name in ("P1", "P2"):
            assert after[(name, 6)] in (1.0, 134), after
            for n in range(1, 6):
                assert after[(name, n)] in (225, 134, 1.0), after   # ok is harmless: txn n was committed
    finally:
        P1.kill9()
        P2.kill9()


# -------------------------------------------------------------- 6. the hint expires while the session is still alive

def test_hint_cleanup_vs_live_session_record():
    """TTL 9 s (test flag), hint refresh every 3 s. Txn 1 at t=0 writes the hint; txn 2 at ~2.7 s does not rewrite it
    (fresh in this process's memory). The session's last use is ~2.7 s, so it is alive until ~11.7 s; the hint (written
    at 0) counts as expired from 9 s. A commit of txn 2 repeated after a restart at ~10 s, while the session is alive:
    mongod answers ok (the outcome is in config.transactions while the session lives)."""
    env = {"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_SESSION_TTL_MS": "9000"}
    srv = _srv("rv41h", env=env)
    try:
        c = T.Raw(srv)
        c["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        t0 = time.time()
        _begin(c, lsid, 1, {"_id": 1})
        assert T._commit(c, lsid, 1)["ok"] == 1.0
        time.sleep(max(0, 2.7 - (time.time() - t0)))
        _begin(c, lsid, 2, {"_id": 2})
        assert T._commit(c, lsid, 2)["ok"] == 1.0
        t2 = time.time() - t0
        time.sleep(max(0, 9.6 - (time.time() - t0)))
        _restart(srv)
        c = T.Raw(srv)
        got = _res(lambda: T._commit(c, lsid, 2))
        at = time.time() - t0
        print("hint vs session:", {"txn2_at": round(t2, 2), "repeat_at": round(at, 2), "session_alive_until": round(t2 + 9, 2), "repeat": got})
        assert t2 < 3.0 and at < t2 + 9.0, (t2, at)            # the session is alive at the repeat
        assert got == 1.0, (got, t2, at)
    finally:
        srv.kill9()


# ------------------------------------------------------------------- 7. the commit interrupted between staging and ack

def test_sigterm_during_slow_commit():
    """The risk C8 wrote down: the command stops between the staging of the batch and the publication of its ticket.
    killOp does not reach commitTransaction (not in the killable list), so the only interruption is a shutdown: SIGTERM
    while the commit waits for a 4 s WAL PUT. Whatever answered ok must be there after the restart, and a repeated
    commit must say ok if the transaction is there and 251 only if it is not."""
    for rnd in range(3):
        srv = _srv(f"rv41t{rnd}")
        try:
            c1, c2 = T.Raw(srv), T.Raw(srv)
            c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
            lsid = T._lsid()
            _begin(c1, lsid, 1, {"_id": 1})
            CTL["wal_delay"] = 4.0
            out, t0 = {}, time.time()
            th = _bg(out, "first", lambda: T._commit(c1, lsid, 1), t0)
            time.sleep(0.8)
            th2 = _bg(out, "repeat", lambda: T._commit(c2, lsid, 1), t0)
            time.sleep(0.5)
            srv.term(timeout=90)
            th.join(60); th2.join(60)
            _reset()
            srv.proc = None
            srv.start()
            c = T.Raw(srv)
            docs = c.ct.c.count_documents({"_id": 1})
            again = _res(lambda: T._commit(c, lsid, 1))
            print(f"sigterm {rnd}:", dict(out, docs=docs, repeat_after_restart=again))
            acked = out.get("first") == 1.0 or out.get("repeat") == 1.0
            assert not (acked and docs == 0), (out, docs)
            if docs == 1:
                assert again == 1.0, (out, docs, again)      # committed: never 251 (the driver would redo it)
            else:
                assert again == 251, (out, docs, again)
        finally:
            _reset()
            srv.kill9()


def test_killop_does_not_interrupt_a_commit():
    srv = _srv("rv41k")
    try:
        c1, c2 = T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})
        lsid = T._lsid()
        _begin(c1, lsid, 1, {"_id": 1})
        CTL["wal_delay"] = 3.0
        out, t0 = {}, time.time()
        th = _bg(out, "first", lambda: T._commit(c1, lsid, 1), t0)
        time.sleep(0.8)
        ops = c2.admin.command({"currentOp": 1})["inprog"]
        mine = [o for o in ops if o.get("command", {}).get("commitTransaction")]
        for o in mine:
            c2.admin.command({"killOp": 1, "op": o["opid"]})
        th.join(60)
        _reset()
        print("killop:", {"ops": len(mine), "first": out.get("first"), "at": out.get("first_at")})
        assert out["first"] == 1.0, out
        assert c2.ct.c.count_documents({"_id": 1}) == 1
        assert T._commit(c2, lsid, 1)["ok"] == 1.0
    finally:
        _reset()
        srv.kill9()


# ----------------------------------------------- 8. the outcome of a transaction overwritten by a retryable write

def test_outcome_survives_a_later_retryable_write_of_the_same_session():
    """One record per session and database (`meta/rw/{session}`) holds either the outcome of the latest transaction
    (txn: true) or the statements of the latest retryable write (txn: false). A retryable write after a committed
    transaction overwrites it, and the lookup skips records with txn: false. mongod keeps one `config.transactions`
    entry per session too, but it answers TransactionTooOld (225, not retryable, not transient) for the older
    transaction; 251 NoSuchTransaction carries TransientTransactionError and makes the driver run the whole
    transaction again."""
    srv = _srv("rv41o")
    try:
        c = T.Raw(srv)
        lsid = T._lsid()
        _begin(c, lsid, 1, {"_id": 1, "amount": 100})
        assert T._commit(c, lsid, 1)["ok"] == 1.0
        # a retryable write of the same session in the same database, with the next number
        assert c["ct"].command({"insert": "c", "documents": [{"_id": 2}], "lsid": lsid, "txnNumber": Int64(2)})["ok"] == 1.0
        c.close()
        _restart(srv)
        c = T.Raw(srv)
        got = _res(lambda: T._commit(c, lsid, 1))
        print("outcome after a retryable write:", {"repeat_commit_txn1": got})
        assert got != 251, got                    # 251 + TransientTransactionError: the driver redoes the transaction
    finally:
        srv.kill9()
