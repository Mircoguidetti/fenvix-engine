"""A database abandoned (lease lost) and opened again while one write command runs (c2-fix-outage-loss).

Found by the review of c2-fix-clone-uploads (ctl test_31 with sealed runs on: acknowledged inserts missing after a store
outage past the lease). The inserts were not lost from storage: the documents were stored, but missing from collection
scans. An insert takes a natural-order number for each document (`natural.rs`, kept in memory per opening). When one of the
command's reads finds the lease invalid, the database is abandoned and opened again inside the same command; the reopen
forgets the numbers, so the rest of the batch takes the same numbers again, and each later document's `nat/` entry
overwrites an earlier one's. The fix refuses the write of a command whose database was reopened after the command had
begun to use it (`dbs::Reopened`, retryable, nothing applied).

test_insert_spanning_a_lease_loss: fault injection (`MONGO_TEST_FAULTS_FILE`, test-only) delays the renewals of the
process lease for a few seconds while large insert commands run, so the lease is lost and taken back during a command.
Fails when a document whose insert was acknowledged is missing from a collection scan (now, and after a restart), when two
documents share a natural-order number, or when no command spanned a lease loss (the test did not reach the case).
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

TTL = 2
BATCH = 5000
ROUNDS = int(os.environ.get("T52_ROUNDS", "6"))


def test_insert_spanning_a_lease_loss():
    bucket = make_bucket("reopen")
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    ff = f.name
    srv = Server(bucket, "reopen", idle=600, holder="R1", extra=["--lease-ttl-secs", str(TTL)], env={"MONGO_TEST_FAULTS_FILE": ff}).start()
    acked, refused, losses = [], [], 0
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=False)
        col = c.d.t
        col.insert_one({"_id": -1})
        acked.append(-1)
        stop, nxt, lock = threading.Event(), [0], threading.Lock()

        def loader():
            cl = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=False)
            while not stop.is_set():
                with lock:
                    b = nxt[0]
                    nxt[0] += 1
                ids = list(range(b * BATCH, (b + 1) * BATCH))
                try:
                    cl.d.t.insert_many([{"_id": i, "f": "x" * 50} for i in ids], ordered=False)
                    with lock:
                        acked.extend(ids)
                except PyMongoError as e:
                    with lock:
                        refused.append(str(e)[:200])
            cl.close()
        th = threading.Thread(target=loader)
        th.start()
        try:
            for r in range(ROUNDS):
                time.sleep(1.5)
                with open(ff, "w") as fh:
                    fh.write(f"delay_put processes/R1/_lease {TTL * 1000 + 1500}\n")
                time.sleep(TTL + 1.0)
                with open(ff, "w") as fh:
                    fh.write("")
                time.sleep(TTL + 2)
        finally:
            stop.set()
            th.join(150)
        losses = srv.log_text().count("LEASE lost")
        print(f"batches {nxt[0]}, acknowledged {len(acked)} documents, refused {len(refused)} batches {refused[:2]}; lease losses {losses}")
        st = c.admin.command("tenantStats")["maintenance"]
        print(f"reopenedWritesRefused {st.get('reopenedWritesRefused')}")
        scanned = [d["_id"] for d in col.find({}, {"_id": 1}).hint([("$natural", 1)])]
        dup = len(scanned) - len(set(scanned))
        missing = sorted(set(acked) - set(scanned))
        print(f"acknowledged {len(acked)}, scanned {len(scanned)}, missing from the scan {len(missing)}")
        assert losses > 0, "no lease loss happened while an insert ran: the test did not reach the case"
        assert dup == 0, f"{dup} documents scanned twice"
        assert not missing, f"{len(missing)} acknowledged documents missing from a collection scan, e.g. {missing[:5]}"
        extra = set(scanned) - set(acked)
        assert not extra, f"{len(extra)} documents of refused inserts were applied, e.g. {sorted(extra)[:5]}"
    finally:
        srv.term()
    srv2 = Server(bucket, "reopen2", idle=600).start()
    try:
        scanned = set(d["_id"] for d in srv2.client().d.t.find({}, {"_id": 1}).hint([("$natural", 1)]))
        missing = sorted(set(acked) - scanned)
        assert not missing, f"after a restart: {len(missing)} acknowledged documents missing from a collection scan, e.g. {missing[:5]}"
    finally:
        srv2.term()
