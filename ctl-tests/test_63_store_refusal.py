"""fix-store-errors (2026-11-12): a store that refuses writes (MinIO 507 XMinioStorageFull on a full disk, S3 503
SlowDown) or does not answer a write, then recovers.

Found by the integration-4 ballast proof (research/integration-4/results/ballast-10gb/stuck-b03/): with the store
refusing writes, SIGTERM did not stop the server in 240 s ("shutting down: closing tenants cleanly"); the clean close of
the databases waited for the store. Rule 8 is checked in every test: every acknowledged write is read back by a fresh
process that talks to the store directly.

- test_sigterm_stops_while_a_wal_put_hangs / ..._while_the_store_refuses_writes: one insert is stuck on its WAL PUT (held
  by the forwarder, or answered 507), then SIGTERM. Before the fix: not stopped in 180 s (2 of 2). Now the clean close
  gets at most --shutdown-timeout-secs (25 s by default), then the process exits; nothing acknowledged is lost (it is in
  the WAL; the next owner replays it).
- test_databases_answer_after_the_store_refuses_writes[507|503]: writers on two databases while every PUT and POST is
  refused for 30 s (the process lease and the databases' leases lapse, the databases are abandoned and opened again).
  After the store takes writes again, each database answers a find within 15 s, and every acknowledged insert and
  update is there, live and after a restart.

The forwarder is tools/lease-stall/pathproxy.py (its HEAD answers carry the object's length since this fix round: with
a length of 0, SlateDB took WAL objects for empty fences on replay and acknowledged writes looked lost).
"""
import os
import signal
import subprocess
import sys
import threading
import time

from pymongo import MongoClient

from lib import Server, make_bucket, MINIO

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from pathproxy import PathProxy  # noqa: E402

SEALED = {"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
          "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}
STOP_BOUND_S = 40   # --shutdown-timeout-secs 25 by default, plus the stop of what follows


def proxy():
    host, port = MINIO.split("//")[1].split(":")
    return PathProxy((host, int(port)))


def _sigterm_with_a_stuck_write(rule):
    px = proxy()
    bucket = make_bucket("srterm")
    srv = Server(bucket, "srterm", idle=600, endpoint=px.endpoint, extra=["--lease-ttl-secs", "30"]).start()
    try:
        c = srv.client()
        c.d.t.insert_one({"_id": 0})
        rule(px)
        res = {}

        def stuck():
            try:
                MongoClient(srv.uri, socketTimeoutMS=600000, retryWrites=False).d.t.insert_one({"_id": 1})
                res["ok"] = True
            except Exception as e:
                res["err"] = repr(e)[:200]

        th = threading.Thread(target=stuck, daemon=True)
        th.start()
        time.sleep(5)
        t0 = time.time()
        srv.proc.send_signal(signal.SIGTERM)
        try:
            srv.proc.wait(STOP_BOUND_S + 140)
            took = time.time() - t0
        except subprocess.TimeoutExpired:
            took = None
        print(f"SIGTERM: {'stopped in %.1f s' % took if took is not None else 'NOT stopped in 180 s'}; stuck insert: {res}")
    finally:
        px.release()
        srv.kill9()
        px.close()
    srv2 = Server(bucket, "srterm2", idle=600).start()
    try:
        ids = {d["_id"] for d in srv2.client().d.t.find({})}
        assert 0 in ids, ids
        if res.get("ok"):
            assert 1 in ids, f"the insert was acknowledged but is missing: {ids}"
    finally:
        srv2.term()
    assert took is not None and took <= STOP_BOUND_S, f"SIGTERM took {took} s (bound {STOP_BOUND_S} s)"


def test_sigterm_stops_while_a_wal_put_hangs():
    _sigterm_with_a_stuck_write(lambda px: px.hold("PUT", "/tenants/d/wal/"))


def test_sigterm_stops_while_the_store_refuses_writes():
    _sigterm_with_a_stuck_write(lambda px: px.refuse(("PUT", "POST"), "/", 507))


def _refusal(status, secs=30):
    px = proxy()
    bucket = make_bucket(f"srref{status}")
    srv = Server(bucket, "srref", idle=600, endpoint=px.endpoint, env=SEALED).start()
    dbs = ["r0", "r1"]
    acked = {db: set() for db in dbs}
    upd = {db: 0 for db in dbs}
    stop = threading.Event()
    lock = threading.Lock()

    def writer(db):
        cl = MongoClient(srv.uri, serverSelectionTimeoutMS=600000, socketTimeoutMS=600000, retryWrites=False)
        i = 0
        while not stop.is_set():
            try:
                cl[db].t.insert_many([{"_id": i * 50 + k, "v": "x" * 300} for k in range(50)])
                with lock:
                    acked[db].update(i * 50 + k for k in range(50))
                cl[db].c.update_one({"_id": 0}, {"$inc": {"n": 1}}, upsert=True)
                with lock:
                    upd[db] += 1
            except Exception:
                pass  # refused while the store refuses: not acknowledged
            i += 1

    ws = [threading.Thread(target=writer, args=(db,), daemon=True) for db in dbs]
    try:
        for w in ws:
            w.start()
        time.sleep(10)
        px.refuse(("PUT", "POST"), "/", status)
        time.sleep(secs)
        px.release()
        t_rec = time.time()
        late = {}
        for db in dbs:
            while True:
                cl = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=10000)
                try:
                    cl[db].t.find_one({"_id": 1})
                    late[db] = round(time.time() - t_rec, 1)
                    break
                except Exception:
                    if time.time() - t_rec > 120:
                        late[db] = None
                        break
                    time.sleep(0.5)
                finally:
                    cl.close()
        print(f"{status}: refused {px.refused} requests; answered after the store recovered (s): {late}")
        time.sleep(5)
        stop.set()
        for w in ws:
            w.join(120)
        live = MongoClient(srv.uri, socketTimeoutMS=300000)
        problems = []
        for db in dbs:
            have = {d["_id"] for d in live[db].t.find({}, {"_id": 1})}
            n = (live[db].c.find_one({"_id": 0}) or {}).get("n", 0)
            if acked[db] - have or n < upd[db]:
                problems.append(f"live {db}: {len(acked[db] - have)} of {len(acked[db])} acknowledged inserts missing; counter {n} < {upd[db]} acknowledged")
        t0 = time.time()
        srv.term(timeout=STOP_BOUND_S)
        print(f"SIGTERM stopped the server in {time.time() - t0:.1f} s")
    finally:
        stop.set()
        px.release()
        srv.kill9()
        px.close()
    srv2 = Server(bucket, "srref2", idle=600, env=SEALED).start()
    try:
        c2 = MongoClient(srv2.uri, socketTimeoutMS=300000)
        for db in dbs:
            have = {d["_id"] for d in c2[db].t.find({}, {"_id": 1})}
            n = (c2[db].c.find_one({"_id": 0}) or {}).get("n", 0)
            print(f"after a restart {db}: {len(acked[db])} acknowledged inserts, {len(acked[db] - have)} missing; {upd[db]} acknowledged updates, counter {n}")
            if acked[db] - have or n < upd[db]:
                problems.append(f"restart {db}: {len(acked[db] - have)} acknowledged inserts missing; counter {n} < {upd[db]}")
    finally:
        srv2.term()
    assert not problems, "\n".join(problems)
    slow = {db: s for db, s in late.items() if s is None or s > 15}
    assert not slow, f"databases that did not answer within 15 s of the store's recovery: {slow}"


def test_databases_answer_after_the_store_refuses_writes_507():
    _refusal(507)


def test_databases_answer_after_the_store_refuses_writes_503():
    _refusal(503)
