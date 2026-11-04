"""C8: a repeated commit (or a retried write) never answers ok before the data it answers for is durable, and the hints
of a session stay right with two processes.

Errors found by C7 on PR #34, both MEASURED there, each reproduced here:

1. A `commitTransaction` repeated while the first commit of the transaction is still waiting for S3 answered ok at once:
   the process marked the transaction committed as soon as its batch was staged, before the WAL PUT. A SIGKILL before the
   PUT ended lost a transaction that the driver had been told was committed (3 out of 3 in C7). A commit repeated while
   the first one was still staging answered 117 ConflictingOperationInProgress. The same holds for a retryable write
   answered from the session record (`retriedStmtIds`). Now the repeated command waits for the durability of the first
   command's batch, as mongod waits for the write concern of a retried write or commit.
2. One hint per session (`txn-hints/{session}` = `{db}`): with two processes and one session used on both, a process
   whose memory said the hint was fresh did not rewrite it, the hint named the other database and the repeated commit
   answered 251 NoSuchTransaction (the driver then runs the transaction again). Now one hint per session and database
   (`txn-hints/{session}/{db}`), never rewritten with another database; the lookup lists the session's prefix and takes
   the highest number.

S3 is slowed by a small proxy in front of the test S3 (only the PUTs of the WAL, or of the hints): no test-only flag in
the server.
"""
import http.client
import http.server
import socketserver
import threading
import time

import bson
from bson import Int64
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket, MINIO, s3
import test_41_commit_retry as T

DELAY = {"wal": 0.0, "hint": 0.0}
_HOST, _PORT = MINIO.split("//", 1)[1].split(":")


class _Slow(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _go(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        if self.command == "PUT" and "/wal/" in self.path and DELAY["wal"]:
            time.sleep(DELAY["wal"])
        if self.command == "PUT" and "/txn-hints/" in self.path and DELAY["hint"]:
            time.sleep(DELAY["hint"])
        c = http.client.HTTPConnection(_HOST, int(_PORT), timeout=120)
        c.request(self.command, self.path, body=body, headers={k: v for k, v in self.headers.items() if k.lower() != "connection"})
        r = c.getresponse()
        data = r.read()
        self.send_response(r.status)
        for k, v in r.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        # a HEAD answers the object's size, not the empty body's (with 0 the server took the WAL objects for empty)
        self.send_header("Content-Length", r.getheader("Content-Length", "0") if self.command == "HEAD" else str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(data)
            except OSError:                                  # the server was killed meanwhile
                pass

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _go

    def log_message(self, *a):
        pass


class _TS(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


_pport = free_port()
threading.Thread(target=_TS(("127.0.0.1", _pport), _Slow).serve_forever, daemon=True).start()
EP = f"http://127.0.0.1:{_pport}"


def _srv(tag, bucket=None, holder=None):
    port = free_port()
    kw = {"holder": holder} if holder else {}
    return Server(bucket or make_bucket(tag), tag, port=port, endpoint=EP, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"], **kw).start()


def _res(f):
    try:
        return f().get("ok")
    except OperationFailure as e:
        return e.code
    except (OSError, TypeError) as e:                        # the connection closed (SIGKILL) before a reply
        return f"no reply ({type(e).__name__})"


def _restart(srv):
    srv.kill9()
    srv.proc = None
    srv.start()


def _in_thread(out, key, f, t0):
    def go():
        out[key] = _res(f)
        out[key + "_at"] = time.time() - t0
    th = threading.Thread(target=go)
    th.start()
    return th


# --------------------------------------------------------------------------------------- 1. durability of the repeat

def test_repeated_commit_waits_for_the_first_commits_durability():
    """C7's crash: slow WAL PUT (4 s), the same commit again after 0.8 s, SIGKILL at 1.5 s. Before: the repeat answers ok
    at once and the transaction is gone after the restart. Now: no ok before the SIGKILL, so nothing acknowledged is lost."""
    for rnd in range(2):
        srv = _srv(f"cr41bk{rnd}")
        try:
            c1 = T.Raw(srv)
            lsid = T._lsid()
            c1["ct"].command({"insert": "c", "documents": [{"_id": 1}], "lsid": lsid, "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
            DELAY["wal"] = 4.0
            out, t0 = {}, time.time()
            th = _in_thread(out, "first", lambda: T._commit(c1, lsid, 1), t0)
            time.sleep(0.8)
            c2 = T.Raw(srv)
            th2 = _in_thread(out, "second", lambda: T._commit(c2, lsid, 1), t0)
            time.sleep(0.7)
            srv.kill9()
            th.join(); th2.join()
            DELAY["wal"] = 0
            srv.proc = None
            srv.start()
            c3 = T.Raw(srv)
            docs = c3.ct.c.count_documents({})
            # an ok means durable: if any commit answered ok, the document survives the SIGKILL
            assert not (out["second"] == 1.0 and docs == 0), (rnd, out, docs)
            assert out["second"] != 1.0, (rnd, out, docs)       # it cannot be durable yet: the PUT takes 4 s
        finally:
            DELAY["wal"] = 0
            srv.kill9()


def test_repeated_commit_while_first_runs_answers_ok_after_it():
    """No SIGKILL: the repeat waits for the first commit and answers ok (not at once, not 117), the document once."""
    srv = _srv("cr41bw")
    try:
        c1 = T.Raw(srv)
        lsid = T._lsid()
        c1["ct"].command({"insert": "c", "documents": [{"_id": 1}], "lsid": lsid, "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        DELAY["wal"] = 3.0
        out, t0 = {}, time.time()
        th = _in_thread(out, "first", lambda: T._commit(c1, lsid, 1), t0)
        time.sleep(0.8)
        c2 = T.Raw(srv)
        out["second"] = _res(lambda: T._commit(c2, lsid, 1))
        out["second_at"] = time.time() - t0
        th.join()
        DELAY["wal"] = 0
        assert out["first"] == 1.0 and out["second"] == 1.0, out
        assert out["second_at"] >= 2.5, out                  # it waited for the WAL PUT
        assert c2.ct.c.count_documents({}) == 1
        _restart(srv)
        c3 = T.Raw(srv)
        assert c3.ct.c.count_documents({}) == 1
        assert T._commit(c3, lsid, 1)["ok"] == 1.0
    finally:
        DELAY["wal"] = 0
        srv.kill9()


def test_repeated_commit_while_first_is_staging_waits_instead_of_117():
    """The first commit is still before its batch (its hint PUT is slow): the repeat waits for it and answers ok."""
    srv = _srv("cr41bs")
    try:
        c1 = T.Raw(srv)
        lsid = T._lsid()
        c1["ct"].command({"insert": "c", "documents": [{"_id": 1}], "lsid": lsid, "txnNumber": Int64(1), "startTransaction": True, "autocommit": False})
        DELAY["hint"] = 2.5
        out, t0 = {}, time.time()
        th = _in_thread(out, "first", lambda: T._commit(c1, lsid, 1), t0)
        time.sleep(0.8)
        c2 = T.Raw(srv)
        out["second"] = _res(lambda: T._commit(c2, lsid, 1))
        th.join()
        DELAY["hint"] = 0
        assert out["first"] == 1.0 and out["second"] == 1.0, out   # before: second 117 ConflictingOperationInProgress
        assert c2.ct.c.count_documents({}) == 1
    finally:
        DELAY["hint"] = 0
        srv.kill9()


def test_retried_write_waits_for_the_original_writes_durability():
    """The same schema for retryable writes: an insert with lsid and txnNumber is slow (WAL PUT 4 s), the same insert is
    retried after 0.8 s, SIGKILL at 1.5 s. Before: the retry answers ok from the session record at once and the document
    is gone after the restart. Now: no ok before the SIGKILL. Without SIGKILL the retry answers ok after the PUT, with
    retriedStmtIds, and the document is there once."""
    srv = _srv("cr41br")
    try:
        lsid = T._lsid()
        cmd = {"insert": "c", "documents": [{"_id": 1}], "lsid": lsid, "txnNumber": Int64(5)}
        c1, c2 = T.Raw(srv), T.Raw(srv)
        c1["ct"].command({"insert": "c", "documents": [{"_id": 0}]})   # the database exists: its creation is not slowed
        DELAY["wal"] = 4.0
        out, t0 = {}, time.time()
        th = _in_thread(out, "first", lambda: c1["ct"].command(cmd), t0)
        time.sleep(0.8)
        th2 = _in_thread(out, "retry", lambda: c2["ct"].command(cmd), t0)
        time.sleep(0.7)
        srv.kill9()
        th.join(); th2.join()
        DELAY["wal"] = 0
        srv.proc = None
        srv.start()
        c3 = T.Raw(srv)
        docs = c3.ct.c.count_documents({"_id": 1})
        assert not (out["retry"] == 1.0 and docs == 0), (out, docs)
        assert out["retry"] != 1.0, (out, docs)
        # no SIGKILL: the retry waits and answers from the record
        lsid2 = T._lsid()
        cmd2 = {"insert": "c", "documents": [{"_id": 2}], "lsid": lsid2, "txnNumber": Int64(1)}
        c1, c2 = T.Raw(srv), T.Raw(srv)
        DELAY["wal"] = 3.0
        out, t0 = {}, time.time()
        th = _in_thread(out, "first", lambda: c1["ct"].command(cmd2), t0)
        time.sleep(0.8)
        r = c2["ct"].command(cmd2)
        at = time.time() - t0
        th.join()
        DELAY["wal"] = 0
        assert r["ok"] == 1.0 and r.get("retriedStmtIds") == [0], r
        assert at >= 2.5, at
        assert c2.ct.c.count_documents({"_id": 2}) == 1
    finally:
        DELAY["wal"] = 0
        srv.kill9()


# --------------------------------------------------------------------------------------- 2. hints with two processes

def test_hints_per_session_and_database_with_two_processes():
    """C7's case: P1 owns db a, P2 owns db b, one session on both: txn 1 on P1 (a), txn 2 on P2 (b), txn 3 on P1 (a).
    Before: the single hint said b after txn 3 and the commit of txn 3 repeated on P2 answered 251 NoSuchTransaction with
    TransientTransactionError (the driver runs it again: a double transfer). Now: one hint per database, never 251 for a
    committed transaction; P2 cannot read a (P1 owns it) and answers a retryable error instead; P1 after its restart ok."""
    bucket = make_bucket("cr41bp")
    P1 = _srv("cr41bp1", bucket=bucket, holder="p1")
    P2 = _srv("cr41bp2", bucket=bucket, holder="p2")
    try:
        r1, r2 = T.Raw(P1), T.Raw(P2)
        lsid = T._lsid()

        def txn(c, db, n, d):
            c[db].command({"insert": "c", "documents": [d], "lsid": lsid, "txnNumber": Int64(n), "startTransaction": True, "autocommit": False})
            return T._commit(c, lsid, n)["ok"]

        assert txn(r1, "a", 1, {"_id": 1}) == 1.0
        assert txn(r2, "b", 2, {"_id": 2}) == 1.0
        assert txn(r1, "a", 3, {"_id": 3}) == 1.0
        hints = sorted(k for k, _ in list_prefix(bucket, "txn-hints/"))
        sess = lsid["id"].hex()
        assert hints == [f"txn-hints/{sess}/a", f"txn-hints/{sess}/b"], hints
        assert bson.decode(s3().get_object(Bucket=bucket, Key=f"txn-hints/{sess}/a")["Body"].read())["db"] == "a"
        on_p2 = _res(lambda: T._commit(r2, lsid, 3))
        assert on_p2 != 251, on_p2                             # before: 251 NoSuchTransaction (TransientTransactionError)
        r1.close()
        _restart(P1)
        r1 = T.Raw(P1)
        # 134 TenantLeaseHeld is retryable (the drivers retry a commit on it): retried here as a driver would, and before
        # the fix it stayed 134 (P1 looked in b, which P2 owns)
        deadline = time.time() + 15
        while (got := _res(lambda: T._commit(r1, lsid, 3))) == 134 and time.time() < deadline:
            time.sleep(0.5)
        assert got == 1.0, got
        with_old = _res(lambda: T._commit(r1, lsid, 2))
        assert with_old == 225, with_old                        # the highest number wins: 2 is older than 3
        assert r1["a"].command({"count": "c"})["n"] == 2
    finally:
        P1.kill9()
        P2.kill9()
