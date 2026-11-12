"""Reviewer's test for fix-store-errors: after SIGTERM's forced exit (--shutdown-timeout-secs, default 25), every
acknowledged write is read back by a fresh process (the store reached directly), including the writes acknowledged during
the 25 s of the bounded close.

- test_forced_exit_keeps_every_acknowledged_write[hold]: 3 databases, 2 writers each (insert_many of 20, update $inc),
  then every WAL PUT of database w0 is held (no answer): its writes hang, w1 and w2 go on; SIGTERM while that lasts.
- [slow]: every PUT of the store is answered 2 s late (a slow store, not a refusing one), then SIGTERM.
"""
import os
import signal
import subprocess
import sys
import threading
import time

import pytest
from pymongo import MongoClient

from lib import Server, make_bucket, MINIO

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402


@pytest.mark.parametrize("mode", ["hold", "slow"])
def test_forced_exit_keeps_every_acknowledged_write(mode):
    host, port = MINIO.split("//")[1].split(":")
    px = PathProxy((host, int(port)))
    bucket = make_bucket("r66" + mode)
    srv = Server(bucket, "r66" + mode, idle=600, endpoint=px.endpoint, extra=["--lease-ttl-secs", "10"]).start()
    dbs = ["w0", "w1", "w2"]
    acked = {db: set() for db in dbs}
    upd = {db: 0 for db in dbs}
    after_term = [0]
    lock = threading.Lock()
    stop = threading.Event()
    t_term = [None]

    def writer(db, k):
        cl = MongoClient(srv.uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=120000, retryWrites=False)
        i = 0
        while not stop.is_set():
            base = (k * 10_000_000) + i * 20
            try:
                cl[db].t.insert_many([{"_id": base + j} for j in range(20)])
                with lock:
                    acked[db].update(base + j for j in range(20))
                    if t_term[0] is not None:
                        after_term[0] += 1
                cl[db].c.update_one({"_id": 0}, {"$inc": {"n": 1}}, upsert=True)
                with lock:
                    upd[db] += 1
            except Exception:
                if srv.proc.poll() is not None:
                    return
                time.sleep(0.05)
            i += 1
    ws = [threading.Thread(target=writer, args=(db, k), daemon=True) for db in dbs for k in range(2)]
    try:
        for w in ws:
            w.start()
        time.sleep(5)
        if mode == "hold":
            px.hold("PUT", "/tenants/w0/wal/")
        else:
            px.slow(("PUT",), "/", 2.0)
        time.sleep(4)
        t_term[0] = time.time()
        srv.proc.send_signal(signal.SIGTERM)
        try:
            srv.proc.wait(120)
            took = time.time() - t_term[0]
        except subprocess.TimeoutExpired:
            took = None
        stop.set()
        log = srv.log_text()
        forced = "exiting without it" in log
    finally:
        stop.set()
        px.release()
        srv.kill9()
        px.close()
    srv2 = Server(bucket, "r66b" + mode, idle=600, extra=["--lease-ttl-secs", "10"]).start()
    problems, lines = [], []
    try:
        c2 = MongoClient(srv2.uri, socketTimeoutMS=300000)
        for db in dbs:
            deadline = time.time() + 120
            while True:
                try:
                    have = {d["_id"] for d in c2[db].t.find({}, {"_id": 1})}
                    n = (c2[db].c.find_one({"_id": 0}) or {}).get("n", 0)
                    break
                except Exception as e:
                    if time.time() > deadline:
                        raise
                    time.sleep(1)
            miss = acked[db] - have
            lines.append(f"{db}: {len(acked[db])} acknowledged inserts, {len(miss)} missing; {upd[db]} acknowledged updates, counter {n}")
            if miss or n < upd[db]:
                problems.append(lines[-1])
    finally:
        srv2.term()
    print(f"{mode}: SIGTERM {'stopped in %.1f s' % took if took else 'NOT stopped'}; forced exit: {forced}; inserts acknowledged after SIGTERM: {after_term[0]}; " + "; ".join(lines))
    assert not problems, problems
    assert took is not None and took <= 40, took


# ---- a WAL PUT that the store applied but whose answer was lost ------------------------------------------------------

import http.client as _hc
from http.server import BaseHTTPRequestHandler as _BH, ThreadingHTTPServer as _TS
from bson import SON


class LoseAnswer:
    """A forwarder that forwards every request; while armed, the first PUT to each WAL object (path containing
    `part`) is forwarded and stored, and then its answer is dropped (the connection is closed): the client never
    learns that it worked, and retries."""

    def __init__(self, dst, part):
        self.dst, self.part, self.armed, self.seen, self.lost = dst, part, False, set(), 0
        px = self

        class H(_BH):
            def log_message(self, *a):
                pass

            def _any(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                path = self.path.split("?")[0]
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ("connection", "keep-alive", "transfer-encoding", "expect")}
                headers["Content-Length"] = str(len(body))
                c = _hc.HTTPConnection(*px.dst, timeout=120)
                try:
                    c.request(self.command, self.path, body=body, headers=headers)
                    resp = c.getresponse()
                    data = resp.read()
                finally:
                    c.close()
                if px.armed and self.command == "PUT" and px.part in path and path not in px.seen and resp.status == 200:
                    px.seen.add(path)
                    px.lost += 1
                    self.close_connection = True
                    try:
                        self.connection.shutdown(2)
                    except OSError:
                        pass
                    return
                self.send_response_only(resp.status, resp.reason)
                for k, v in resp.getheaders():
                    if k.lower() not in ("connection", "keep-alive", "transfer-encoding", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", resp.getheader("Content-Length") if self.command == "HEAD" and resp.getheader("Content-Length") else str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _any

        class S(_TS):
            request_queue_size = 256
        self.srv = S(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.endpoint = f"http://127.0.0.1:{self.srv.server_address[1]}"


def test_a_wal_put_whose_answer_is_lost():
    host, port = MINIO.split("//")[1].split(":")
    px = LoseAnswer((host, int(port)), "/tenants/lw/wal/")
    bucket = make_bucket("r66lost")
    srv = Server(bucket, "r66lost", idle=600, endpoint=px.endpoint)
    srv.extra += ["--replica-set", "rs0", "--advertise", f"localhost:{srv.port}"]
    srv.start()
    out = []
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=120000)
        c.lw.t.insert_one({"_id": "seed"})
        c.lw.c.insert_one({"_id": 0, "n": 0})
        # 1. a transaction (the driver runs it again on TransientTransactionError)
        px.armed = True
        tries = [0]
        with c.start_session() as s:
            def cb(sess):
                tries[0] += 1
                c.lw.c.update_one({"_id": 0}, {"$inc": {"n": 1}}, session=sess)
            try:
                s.with_transaction(cb)
                out.append(f"transaction: committed, callback ran {tries[0]} times")
            except Exception as e:
                out.append(f"transaction: error {str(e)[:150]}")
        px.armed = False
        n = None
        for _ in range(60):
            try:
                n = c.lw.c.find_one({"_id": 0})["n"]
                break
            except Exception:
                time.sleep(0.5)
        out.append(f"counter after one $inc transaction: {n}")
        # 2. a retryable insert (the driver retries it once with the same txnNumber)
        px.seen.clear()
        px.armed = True
        try:
            c.lw.t.insert_one({"_id": "R"})
            out.append("retryable insert: ok")
        except Exception as e:
            out.append(f"retryable insert: error {str(e)[:150]}")
        px.armed = False
        # 3. a write without retries: the client is told what happened
        px.seen.clear()
        px.armed = True
        c3 = MongoClient(srv.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=120000, retryWrites=False)
        try:
            c3.lw.t.insert_one({"_id": "N"})
            out.append("plain insert: ok")
        except Exception as e:
            out.append(f"plain insert: error {getattr(e, 'code', None)} {str(e)[:150]}")
        px.armed = False
        for _ in range(60):
            try:
                ids = sorted(d["_id"] for d in c.lw.t.find({}) if isinstance(d["_id"], str))
                break
            except Exception:
                time.sleep(0.5)
        out.append(f"documents: {ids}; answers dropped: {px.lost}")
    finally:
        srv.term()
    print(" | ".join(out))
    assert px.lost > 0, "no answer was dropped: the test did not reach the case"
    assert n == 1, f"one $inc transaction left the counter at {n}"
