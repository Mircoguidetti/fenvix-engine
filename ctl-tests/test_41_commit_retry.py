"""C6: a repeated `commitTransaction` of a committed transaction answers ok, also on a process that does not remember it.

Before the fix the outcome of a transaction lived only in the memory of the process: a commit repeated after a restart
(or sent to another process) answered 251 NoSuchTransaction with the label TransientTransactionError, and the drivers'
`withTransaction` then ran the whole transaction again: a transaction applied twice (a double `$inc`). mongod answers
ok, because the outcome is in `config.transactions`. Now the commit stores `{number, committed}` in the session record,
in the transaction's own atomic batch, and a small hint object `txn-hints/{session}` tells a process the database of the
session (a commit names no database).

1. commit, restart, the same commit again -> ok, the document once;
2. a transaction never committed, restart, commit -> 251 NoSuchTransaction with TransientTransactionError, as before;
3. commit, restart, abort -> 256 TransactionCommitted (mongod's answer);
4. commit, restart, a commit with an older txnNumber -> 225 TransactionTooOld;
5. the driver: `with_transaction` doing `$inc`, the connection cut after the server committed and before the reply
   arrives, the process restarted in between: the counter is incremented once (before the fix: twice);
6. one record and one hint per session (not per transaction); both expire with the logical sessions (30 minutes; a
   few seconds here through the test-only MONGO_TEST_SESSION_TTL_MS) and their cleanup removes only expired ones.
"""
import os
import socket
import struct
import threading
import time
import uuid

import bson
import pytest
from bson import Binary, Int64
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, free_port, list_prefix, make_bucket


def _srv(tag):
    port = free_port()
    return Server(make_bucket(tag), tag, port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()


def _lsid():
    return {"id": Binary(uuid.uuid4().bytes, 4)}


class Raw:
    """Commands sent as they are, over one socket: a driver would put its own (pooled) session in `lsid`."""

    def __init__(self, srv):
        self.srv, self.rid = srv, 0
        self.s = socket.create_connection(("127.0.0.1", srv.port), timeout=60)

    def run(self, db, cmd):
        self.rid += 1
        body = struct.pack("<I", 0) + b"\x00" + bson.encode(dict(cmd, **{"$db": db}))
        self.s.sendall(struct.pack("<iiii", 16 + len(body), self.rid, 0, OP_MSG) + body)
        m = CutAfterCommit._read(self.s)
        d = bson.decode(m[21:])
        if d.get("ok") != 1.0:
            raise OperationFailure(d.get("errmsg", ""), d.get("code"), d)
        return d

    @property
    def admin(self):
        return _Db(self, "admin")

    def __getitem__(self, name):
        return _Db(self, name)

    @property
    def ct(self):
        return _Pm(self.srv).ct

    def close(self):
        self.s.close()


class _Db:
    def __init__(self, raw, name):
        self.raw, self.name = raw, name

    def command(self, cmd):
        return self.raw.run(self.name, cmd)


def _Pm(srv):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=30000)


def _txn_insert(c, lsid, n, doc, commit=True):
    c["ct"].command({"insert": "c", "documents": [doc], "lsid": lsid, "txnNumber": Int64(n), "startTransaction": True, "autocommit": False})
    if commit:
        return _commit(c, lsid, n)


def _commit(c, lsid, n):
    return c.admin.command({"commitTransaction": 1, "lsid": lsid, "txnNumber": Int64(n), "autocommit": False})


def _client(srv):
    return Raw(srv)


def test_repeated_commit_after_restart_is_ok():
    srv = _srv("cr41a")
    try:
        c = _client(srv)
        lsid = _lsid()
        assert _txn_insert(c, lsid, 1, {"_id": 1, "v": "a"})["ok"] == 1.0
        assert _commit(c, lsid, 1)["ok"] == 1.0          # same process: ok before and after the fix
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        r = _commit(c, lsid, 1)
        assert r["ok"] == 1.0, r
        assert list(c.ct.c.find({})) == [{"_id": 1, "v": "a"}]
        # a session of the hint prefix exists for it
        assert any(k.endswith(lsid["id"].hex()) for k, _ in list_prefix(srv.bucket, "txn-hints/"))
        c.close()
    finally:
        srv.kill9()


def test_never_committed_still_no_such_transaction():
    srv = _srv("cr41b")
    try:
        c = _client(srv)
        lsid = _lsid()
        _txn_insert(c, lsid, 1, {"_id": 1}, commit=False)
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        with pytest.raises(OperationFailure) as e:
            _commit(c, lsid, 1)
        assert e.value.code == 251 and "TransientTransactionError" in e.value.details.get("errorLabels", [])
        assert c.ct.c.count_documents({}) == 0
        # a session with an earlier committed transaction and a later one never committed: still 251 for the later one
        lsid2 = _lsid()
        assert _txn_insert(c, lsid2, 1, {"_id": 10})["ok"] == 1.0
        _txn_insert(c, lsid2, 2, {"_id": 11}, commit=False)
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        with pytest.raises(OperationFailure) as e:
            _commit(c, lsid2, 2)
        assert e.value.code == 251 and "TransientTransactionError" in e.value.details.get("errorLabels", [])
        assert sorted(d["_id"] for d in c.ct.c.find({})) == [10]
    finally:
        srv.kill9()


def test_abort_after_restart_of_committed_is_refused():
    srv = _srv("cr41c")
    try:
        c = _client(srv)
        lsid = _lsid()
        _txn_insert(c, lsid, 5, {"_id": 1})
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        with pytest.raises(OperationFailure) as e:
            c.admin.command({"abortTransaction": 1, "lsid": lsid, "txnNumber": Int64(5), "autocommit": False})
        assert e.value.code == 256, e.value.details
        with pytest.raises(OperationFailure) as e:
            _commit(c, lsid, 4)
        assert e.value.code == 225, e.value.details
        assert _commit(c, lsid, 5)["ok"] == 1.0
        assert c.ct.c.count_documents({}) == 1
    finally:
        srv.kill9()


# ------------------------------------------------------------------------------------------------ 5. the driver

OP_MSG = 2013


class CutAfterCommit:
    """A TCP proxy in front of the server. The first time it sees a `commitTransaction` go to the server, it keeps the
    server's reply, tells the test (`held`), waits until the test says go on (`resume`), and then closes the client's
    connection without the reply: the driver sees a network error after the server committed."""

    def __init__(self, target_port):
        self.target = target_port
        self.port = free_port()
        self.held, self.resume = threading.Event(), threading.Event()
        self.armed = True
        self.ls = socket.socket()
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", self.port))
        self.ls.listen(64)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                cs, _ = self.ls.accept()
            except OSError:
                return
            threading.Thread(target=self._conn, args=(cs,), daemon=True).start()

    @staticmethod
    def _read(sock):
        head = b""
        while len(head) < 16:
            ch = sock.recv(16 - len(head))
            if not ch:
                return None
            head += ch
        n = struct.unpack("<i", head[:4])[0]
        rest = b""
        while len(rest) < n - 16:
            ch = sock.recv(n - 16 - len(rest))
            if not ch:
                return None
            rest += ch
        return head + rest

    def _conn(self, cs):
        try:
            ss = socket.create_connection(("127.0.0.1", self.target))
        except OSError:
            cs.close()
            return
        watch = {}

        def up():
            try:
                while True:
                    m = self._read(cs)
                    if m is None:
                        break
                    rid, op = struct.unpack("<i", m[4:8])[0], struct.unpack("<i", m[12:16])[0]
                    if op == OP_MSG and self.armed:
                        try:
                            d = bson.decode(m[21:])
                            if next(iter(d)) == "commitTransaction":
                                self.armed = False
                                watch["rid"] = rid
                        except Exception:
                            pass
                    ss.sendall(m)
            except OSError:
                pass
            finally:
                for s in (ss, cs):
                    try:
                        s.close()
                    except OSError:
                        pass

        threading.Thread(target=up, daemon=True).start()
        try:
            while True:
                m = self._read(ss)
                if m is None:
                    break
                if watch.get("rid") is not None and struct.unpack("<i", m[8:12])[0] == watch["rid"]:
                    self.held.set()
                    self.resume.wait(120)
                    break                       # the reply is dropped; the client's connection closes below
                cs.sendall(m)
        except OSError:
            pass
        finally:
            for s in (ss, cs):
                try:
                    s.close()
                except OSError:
                    pass

    def close(self):
        self.ls.close()


def test_with_transaction_inc_once_across_a_cut_and_a_restart():
    srv = _srv("cr41d")
    px = CutAfterCommit(srv.port)
    try:
        setup = _Pm(srv)
        setup.ct.k.insert_one({"_id": "n", "v": 0})
        setup.close()
        c = MongoClient(f"mongodb://127.0.0.1:{px.port}/?directConnection=true", serverSelectionTimeoutMS=60000, retryWrites=True)
        runs = []

        def body(s):
            runs.append(1)
            c.ct.k.update_one({"_id": "n"}, {"$inc": {"v": 1}}, session=s)

        def restart_when_held():
            assert px.held.wait(60), "the commit never reached the server"
            srv.kill9()                  # the process forgets the transaction (its commit is durable: the reply existed)
            srv.proc = None
            srv.start()
            px.resume.set()

        t = threading.Thread(target=restart_when_held)
        t.start()
        with c.start_session() as s:
            s.with_transaction(body)
        t.join(120)
        c.close()
        check = _Pm(srv)
        v = check.ct.k.find_one({"_id": "n"})["v"]
        print("callback runs:", len(runs), "counter:", v)
        assert v == 1, f"the transaction was applied {v} times (callback ran {len(runs)} times)"
        check.close()
    finally:
        px.close()
        srv.kill9()


# ------------------------------------------------------------------------------------------------ 6. cleanup

def test_hint_and_record_are_per_session_and_overwritten():
    """One record and one hint per session, not one per transaction: 20 transactions of one session leave one of each."""
    srv = _srv("cr41e")
    try:
        c = _client(srv)
        lsid = _lsid()
        for n in range(1, 21):
            assert _txn_insert(c, lsid, n, {"_id": n})["ok"] == 1.0
        hints = [k for k, _ in list_prefix(srv.bucket, "txn-hints/")]
        assert len(hints) == 1, hints
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        assert _commit(c, lsid, 20)["ok"] == 1.0
        with pytest.raises(OperationFailure) as e:
            _commit(c, lsid, 19)
        assert e.value.code == 225
        assert c.ct.c.count_documents({}) == 20
    finally:
        srv.kill9()


def test_expired_session_record_and_hint_are_removed():
    port = free_port()
    srv = Server(make_bucket("cr41f"), "cr41f", port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"],
                 env={"MONGO_ALLOW_UNSAFE_TEST_FLAGS": "1", "MONGO_TEST_SESSION_TTL_MS": "8000"}).start()
    try:
        c = _client(srv)
        a, b = _lsid(), _lsid()
        assert _txn_insert(c, a, 1, {"_id": "a"})["ok"] == 1.0
        time.sleep(9)                                    # a is now older than the TTL
        assert _txn_insert(c, b, 1, {"_id": "b"})["ok"] == 1.0   # its batch deletes a's expired record
        deadline = time.time() + 15
        while time.time() < deadline:                   # the hint cleanup runs every TTL/3
            hints = sorted(k.rsplit("/", 1)[1] for k, _ in list_prefix(srv.bucket, "txn-hints/"))
            if hints == [b["id"].hex()]:
                break
            time.sleep(0.5)
        assert hints == [b["id"].hex()], hints
        assert _commit(c, b, 1)["ok"] == 1.0            # the live session is untouched
        c.close()
        srv.kill9()
        srv.proc = None
        srv.start()
        c = _client(srv)
        with pytest.raises(OperationFailure) as e:      # expired: as mongod after the session is reaped
            _commit(c, a, 1)
        assert e.value.code == 251
        assert sorted(d["_id"] for d in c.ct.c.find({})) == ["a", "b"]
    finally:
        srv.kill9()
