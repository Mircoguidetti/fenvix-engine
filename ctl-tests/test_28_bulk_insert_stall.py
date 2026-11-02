"""Bulk inserts do not stop the writes of a database for good (fix-stall branch, docs/fix-stall.md).

Before the fix, the compaction of an open writer was asked for only by a write that had finished, at most once every 10 s per
database. Under bulk inserts L0 went from the compaction threshold (4 SSTs) to slatedb's `l0_max_ssts` (8) within those 10 s;
the memtable flushes then stopped, every write blocked in slatedb's backpressure (`max_unflushed_bytes`) while holding the
database lock, no write finished, so no compaction was asked for again: the database never took another write, its reads
queued behind the lock, and a SIGTERM shutdown hung on it. Measured with the bench/mongod bulk load (200 000 documents of
~1 KB, insertMany of 500, 16 in parallel): 3 stalls in 3 loads on 1 core and the store on the local disk (docs/fix-stall.md).

test_bulk_insert_does_not_stall: the same kind of load on MinIO, scaled down to run in about a minute: 16 threads insert
batches of 500 documents of ~1 KB into one database, with 1 MiB memtables (`--memtable-max-mb 1`, so L0 fills after a few MB)
and slatedb's default `l0_max_ssts` (8). Fails when a batch is not acknowledged within BATCH_TIMEOUT_S (the stall is permanent;
an acknowledged batch takes well under a second), when not every document is there afterwards, when SIGTERM does not stop
the server within SHUTDOWN_S, or when the close of the database gave up its final flush after its 20 s timeout (the same
missing compaction, in `Tenant::close`: measured 2 of 3 runs before that part of the fix). With the fix the L0 watch of the maintenance loop asks for the compactions
(tenantStats maintenance.l0WatchRequests > 0) and the writes keep going.
"""
import os
import threading
import time

from pymongo import MongoClient

from lib import Server, make_bucket

THREADS = 16
BATCH = 500
BATCHES = 200            # 100 000 documents, ~100 MB
BATCH_TIMEOUT_S = 60
SHUTDOWN_S = 45


def test_bulk_insert_does_not_stall():
    bucket = make_bucket("stall")
    srv = Server(bucket, "stall", idle=300, extra=["--memtable-max-mb", "1"]).start()
    try:
        c = srv.client()
        c.bulk.t.insert_one({"_id": -1})          # the database exists before the parallel load
        next_batch = [0]
        lock = threading.Lock()
        errors, lat = [], []

        def loader():
            cl = MongoClient(srv.uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=BATCH_TIMEOUT_S * 1000, maxPoolSize=1)
            coll = cl.bulk.t
            while not errors:
                with lock:
                    b = next_batch[0]
                    next_batch[0] += 1
                if b >= BATCHES:
                    break
                docs = [{"_id": b * BATCH + i, "f": os.urandom(500).hex()} for i in range(BATCH)]
                t = time.time()
                try:
                    coll.insert_many(docs, ordered=False)
                except Exception as e:      # recorded, the test fails below
                    errors.append(f"batch {b} after {time.time() - t:.1f} s: {repr(e)[:300]}")
                    break
                lat.append(time.time() - t)
            cl.close()

        t0 = time.time()
        threads = [threading.Thread(target=loader, daemon=True) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(BATCHES * BATCH_TIMEOUT_S)
        secs = time.time() - t0
        assert len(lat) + len(errors) > 0, "no batch was attempted"
        st = c.admin.command("tenantStats")["maintenance"]
        report = (f"{len(lat)} batches of {BATCH} in {secs:.1f} s, max batch {max(lat, default=0):.2f} s, "
                  f"compactions {st.get('compactRuns')}, L0 watch requests {st.get('l0WatchRequests')}")
        print(report)
        with open(os.path.join(os.path.dirname(srv.log_path), "test_28_bulk_insert.txt"), "a") as f:
            f.write(f"{time.strftime('%F %T')} {report} errors={errors[:2]}\n")
        assert not errors, f"writes stalled: {errors[:3]} ({report})"
        assert c.bulk.t.count_documents({}) == BATCHES * BATCH + 1
        srv.proc.terminate()
        t = time.time()
        try:
            code = srv.proc.wait(SHUTDOWN_S)
        except Exception:
            code = None
        assert code == 0, f"SIGTERM did not stop the server in {SHUTDOWN_S} s (exit {code}, {time.time() - t:.0f} s)"
        # The close flushed the memtable: it did not give up after its timeout (the final flush waiting for room in L0).
        assert "final flush did not finish" not in srv.log_text(), "the close of the database fell back to an unclean close"
    finally:
        srv.term()
