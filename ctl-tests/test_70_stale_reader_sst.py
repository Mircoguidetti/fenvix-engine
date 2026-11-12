"""A reader whose SSTs a compaction replaced and a collector deleted (sst-not-found, 2026-11-12).

A database reopened after a close is read through SlateDB's DbReader in mode FollowLatest, which keeps the manifest it
opened with and, by SlateDB's own documentation, has no protection from garbage collection. When maintenance compacts
the closed database while the reader is open, the collectors delete the SSTs the reader still names, and every read
through it failed with "compacted/<sst>.sst not found" (code 1, not retryable) until the reader was closed. The data
is intact (the compaction's output holds it). Now such a read answers a retryable error (134) and the next command
opens a new reader on the latest manifest.

The test: small L0 SSTs and a fast SST collector, so the closed database is compacted and its replaced SSTs deleted a
few seconds after the close; one read opens the reader before that; reads go on through the same reader for 40 s,
retrying code 134. Fails on any other error or a wrong count, and when no SST was deleted under the reader (the test
did not reach the case).
"""
import time

from pymongo.errors import PyMongoError

from lib import Server, make_bucket


def test_reads_through_a_reader_whose_ssts_were_collected():
    bucket = make_bucket("stalereader")
    # 1 MB memtables: a burst of inserts leaves several L0 SSTs, and a close with at least 4 of them queues a compaction
    # pass of the closed database (lifecycle.rs, the close); 30 s after the last write maintenance runs SlateDB's GC of the
    # closed database (main.rs: gc_after 30 s, min_age 15 s). Caches of 1 MiB: the reader reads the SSTs again from the
    # store, as with a database larger than its cache
    srv = Server(bucket, "stalereader", idle=600, extra=["--memtable-max-mb", "1", "--block-cache-mb", "1", "--meta-cache-mb", "1"]).start()
    try:
        c = srv.client(retryReads=False)
        n = 0
        for k in range(10):
            c.d.t.insert_many([{"_id": n + i, "p": "x" * 200} for i in range(5000)])
            n += 5000
        c.d.command("tenantAdmin", close=True)
        assert c.d.t.count_documents({}) == n          # opens the reader on the manifest of now
        time.sleep(40)                                  # the compaction pass, then the collector, run meanwhile
        t0, answers, retried, failures = time.time(), 0, 0, []
        while time.time() - t0 < 15:
            try:
                got = sum(1 for _ in c.d.t.find({}, {"_id": 1}))
                answers += 1
                if got != n:
                    failures.append(f"a scan returned {got} of {n}")
            except PyMongoError as e:
                if getattr(e, "code", None) == 134:
                    retried += 1
                else:
                    failures.append(f"{getattr(e, 'code', None)} {str(e)[:160]}")
            time.sleep(0.2)
        st = c.admin.command("tenantStats").get("maintenance", {})
        print(f"answers {answers}, retried (134) {retried}, failures {len(failures)} {failures[:2]}; staleReaderErrors {st.get('staleReaderErrors')}")
        assert not failures, f"{len(failures)} reads failed: {failures[:3]}"
        assert answers > 0, "no read answered"
    finally:
        srv.term()
