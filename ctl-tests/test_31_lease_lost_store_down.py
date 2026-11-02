"""The engine recovers after losing its lease to an object store that stopped answering (fix-lease-stall branch,
docs/fix-lease-stall.md).

Found by the review of S7 (store down 40 s, lease 30 s): the process lost its lease, then never answered again on that
database, and SIGTERM did not stop it, even after the store was back. Two waits without an end, both reached only when the
store hangs:
 1. `Tenant::abandon` (lease lost) closed slatedb with its default final memtable flush. With L0 full that flush waits for a
    compaction, but the writer is already out of the L0 watch and nobody compacts it: the wait never ends, and it holds the
    database's lock, so every later command on the database and the SIGTERM close of all databases wait behind it.
 2. A compaction pass stopped its slatedb compactor while `Compactor::run` was still starting (its first reads hang while the
    store is down); `run` then started its tasks and waited for them forever, so the maintenance worker never came back and
    the maintenance shutdown (after SIGTERM) waited forever.

test_engine_recovers_after_lease_lost_to_store_outage: 16 threads insert batches of 500 documents of ~1 KB into one database
through a proxy in front of MinIO, with 1 MiB memtables so L0 fills quickly; the proxy then stops forwarding (the store hangs)
for FREEZE_S, longer than the lease (TTL_S), and comes back. Fails when, RECOVER_S after the store is back, a new insert on the
database is not acknowledged, tenantStats does not answer, SIGTERM does not stop the server within SHUTDOWN_S, or a document
whose insert was acknowledged is missing after a restart. Probabilistic before the fix (it needs L0 full or a compaction
starting when the store stops): measured on main 3 of 4 runs with the 40 s outage in tools/lease-stall/repro.py.
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
from blockproxy import BlockProxy  # noqa: E402

TTL_S = 6
FREEZE_S = 40            # past the lease and past object_store's 30 s request timeout (the renewal fails, the lease lapses)
RECOVER_S = 20
SHUTDOWN_S = 30
THREADS = 16
BATCH = 500


def test_engine_recovers_after_lease_lost_to_store_outage():
    host, port = MINIO.split("//")[1].split(":")
    px = BlockProxy((host, int(port)))
    bucket = make_bucket("lstall")
    srv = Server(bucket, "lstall", idle=600, endpoint=px.endpoint, extra=["--lease-ttl-secs", str(TTL_S), "--memtable-max-mb", "1"]).start()
    stop = threading.Event()
    acked, lock, nxt = [], threading.Lock(), [0]

    def loader():
        cl = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False, maxPoolSize=1)
        while not stop.is_set():
            with lock:
                b = nxt[0]
                nxt[0] += 1
            ids = [b * BATCH + i for i in range(BATCH)]
            try:
                cl.lst.t.insert_many([{"_id": i, "f": os.urandom(500).hex()} for i in ids], ordered=False)
                with lock:
                    acked.extend(ids)
            except Exception:
                time.sleep(0.5)   # refused while the lease is lost or the store is down: expected, not acknowledged
        cl.close()

    try:
        srv.client().lst.t.insert_one({"_id": -1})
        acked.append(-1)
        threads = [threading.Thread(target=loader, daemon=True) for _ in range(THREADS)]
        for t in threads:
            t.start()
        time.sleep(4)
        px.freeze()
        time.sleep(FREEZE_S)
        px.thaw()
        time.sleep(RECOVER_S)
        stop.set()
        for t in threads:
            t.join(70)

        probe = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=False)
        t0 = time.time()
        try:
            probe.lst.t.insert_one({"_id": "after"})
            acked.append("after")
        except Exception as e:
            raise AssertionError(f"insert after the store came back failed after {time.time() - t0:.1f} s: {e!r}"[:400])
        st = probe.admin.command("tenantStats")
        probe.close()
        print(f"acknowledged {len(acked)} documents; abandonCloseTimeouts {st['maintenance'].get('abandonCloseTimeouts')}")

        srv.proc.send_signal(signal.SIGTERM)
        t0 = time.time()
        try:
            srv.proc.wait(SHUTDOWN_S)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"SIGTERM did not stop the server in {SHUTDOWN_S} s (see {srv.log_path})")
        print(f"SIGTERM stopped the server in {time.time() - t0:.1f} s")
    finally:
        srv.kill9()

    srv2 = Server(bucket, "lstall2", idle=600).start()
    try:
        c = srv2.client()
        present = set(d["_id"] for d in c.lst.t.find({}, {"_id": 1}))
        missing = [i for i in acked if i not in present]
        assert not missing, f"{len(missing)} acknowledged documents missing, e.g. {missing[:5]}"
    finally:
        srv2.term()
