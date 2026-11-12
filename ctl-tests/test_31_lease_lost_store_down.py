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

from pymongo import MongoClient, WriteConcern
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, list_prefix, wait_until, MINIO

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools", "lease-stall"))
from blockproxy import BlockProxy  # noqa: E402
from pathproxy import PathProxy  # noqa: E402

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

        # integration-4 (coordinator, rule 8: we must know): a failed probe insert or a slow SIGTERM is recorded, and
        # the restart count below always runs; the failures are raised after it
        late = []
        probe = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=False)
        t0 = time.time()
        try:
            probe.lst.t.insert_one({"_id": "after"})
            acked.append("after")
        except Exception as e:
            late.append(f"insert after the store came back failed after {time.time() - t0:.1f} s: {e!r}"[:400])
        try:
            st = probe.admin.command("tenantStats")
            print(f"acknowledged {len(acked)} documents; abandonCloseTimeouts {st['maintenance'].get('abandonCloseTimeouts')}")
        except Exception as e:
            late.append(f"tenantStats after the outage failed: {e!r}"[:200])
        probe.close()

        srv.proc.send_signal(signal.SIGTERM)
        t0 = time.time()
        try:
            srv.proc.wait(SHUTDOWN_S)
            print(f"SIGTERM stopped the server in {time.time() - t0:.1f} s")
        except subprocess.TimeoutExpired:
            late.append(f"SIGTERM did not stop the server in {SHUTDOWN_S} s (see {srv.log_path})")
    finally:
        srv.kill9()

    srv2 = Server(bucket, "lstall2", idle=600).start()
    try:
        c = srv2.client()
        # the count always runs (rule 8): a database still leased by the stopped process (its release did not land, e.g.
        # a store that refused the PUT) is refused with "retry" until that lease runs out, so the read is retried for 60 s
        deadline = time.time() + 60
        while True:
            try:
                present = set(d["_id"] for d in c.lst.t.find({}, {"_id": 1}))
                break
            except OperationFailure as e:
                if "retry" not in str(e) or time.time() > deadline:
                    raise
                time.sleep(1)
        missing = [i for i in acked if i not in present]
        print(f"after a restart: {len(acked)} acknowledged, {len(missing)} missing")
        assert not missing, f"{len(missing)} acknowledged documents missing, e.g. {missing[:5]}"
    finally:
        srv2.term()
    assert not late, f"every acknowledged insert there ({len(acked)}), but: " + "; ".join(late)


def test_erase_does_not_delete_while_the_close_is_unfinished():
    """Erase abandons the database's tenant, then deletes every object of the database. With the store not answering the
    tenant's writes, the abandon leaves the close behind (ABANDON_CLOSE_TIMEOUT, 10 s) and slatedb's tasks of the tenant stay
    alive: had the erase deleted the objects, the WAL write held by the store would land after the delete (an orphan under
    tenants/<db>/, replayed into a database created again with the same name). The erase must stop before deleting anything
    with a retryable error (503 close_pending), and succeed, leaving nothing, once the store answers and the close ended.

    The store is frozen only for the WAL writes of the database, PUTs under tenants/<db>/wal/ (path-aware proxy), so the erase's own reads and its intent
    record go through; relaxed durability keeps acknowledged writes in the WAL buffer, so the abandon has a WAL write to make."""
    host, port = MINIO.split("//")[1].split(":")
    px = PathProxy((host, int(port)))
    bucket = make_bucket("erasefrz")
    db = "frz"
    srv = Server(bucket, "erasefrz", idle=600, endpoint=px.endpoint).start()
    try:
        c = srv.client()
        c[db].t.insert_one({"_id": 0})
        st, r = srv.admin.put(f"/v1/databases/{db}/durability", {"relaxed_flush_ms": 10000})
        assert st == 200, r
        relaxed = c[db].get_collection("t", write_concern=WriteConcern(w=1, j=False))
        relaxed.insert_many([{"_id": i, "f": "x" * 100} for i in range(1, 200)])
        px.hold("PUT", f"/tenants/{db}/wal/")
        relaxed.insert_many([{"_id": i, "f": "y" * 100} for i in range(200, 400)])   # acknowledged from memory
        t0 = time.time()
        st, r = srv.admin.delete(f"/v1/databases/{db}", timeout=120)
        took = time.time() - t0
        held = px.held
        assert held > 0, "no PUT of the tenant was held: the test did not reach the case"
        assert st == 503 and r.get("error", {}).get("code") == "close_pending", (st, r, f"{took:.1f} s")
        print(f"erase with the store frozen: {st} close_pending in {took:.1f} s ({held} PUTs held)")
        assert list_prefix(bucket, f"tenants/{db}/"), "objects deleted although the close did not finish"
        assert list_prefix(bucket, f"control/ops/{db}.json") == [], "the stopped erase left its intent record"
        assert srv.admin.get(f"/v1/databases/{db}")[0] == 200
        assert c.admin.command("tenantStats")["maintenance"]["abandonClosesPending"] >= 1

        px.release()
        wait_until(lambda: c.admin.command("tenantStats")["maintenance"]["abandonClosesPending"] == 0, timeout=60,
                   what="the close left behind ends once the store answers")
        st, r = srv.admin.delete(f"/v1/databases/{db}", timeout=120)
        assert st == 200 and r["erased"], (st, r)
        time.sleep(5)   # a late write of the old tenant would show up here
        left = list_prefix(bucket, f"tenants/{db}/")
        assert left == [], f"objects under tenants/{db}/ after the erase: {left[:5]}"
    finally:
        px.release()
        srv.kill9()
        px.close()
