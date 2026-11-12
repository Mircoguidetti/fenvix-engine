"""Reviewer's fix check of fix-long-commands round 1 (9a52571).

- test_sorted_idle_cursors_still_hold_and_free_memory: test_27's old heap check, on cursors that are not streamed (a
  sort): they still hold their documents, and their expiry frees them. Shows where cursor memory is now, and keeps the
  check test_27 no longer makes.
- test_streamed_cursors_hold_no_documents: 40 idle streamed cursors after a getMore each (so a position and a buffer, if
  any, exist): heap before, open, after expiry.
- test_non_streamed_find_longer_than_the_window_is_answered: the finds that round 1 no longer streams (collation, let,
  snapshot read concern, and a sort) must still be answered when one command is longer than the serving window (the F7
  deadline is read from the live lease at each step): lease ttl 3 s, every GET of the database slowed by 60 ms (as
  test_55).
"""
import os
import tempfile
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket
from test_27_memory_per_write import PAD, heap_mb


def idle_cursors(sort):
    srv = Server(make_bucket("r60c"), "r60c", idle=300, extra=["--cursor-timeout-secs", "6"]).start()
    try:
        c = srv.client()
        c.cdb.t.insert_many([{"_id": i, "pad": PAD} for i in range(500)])
        heap0 = heap_mb(c)
        ids = []
        for _ in range(40):
            kw = {"batchSize": 1}
            if sort:
                kw["sort"] = {"_id": 1}
            cid = c.cdb.command("find", "t", **kw)["cursor"]["id"]
            c.cdb.command("getMore", cid, collection="t", batchSize=1)
            ids.append(cid)
        heap1 = heap_mb(c)
        time.sleep(14)
        heap2 = heap_mb(c)
        st = c.admin.command("serverStatus")["metrics"]["cursor"]
        return heap0, heap1, heap2, st.get("timedOut")
    finally:
        srv.term()


def test_sorted_idle_cursors_still_hold_and_free_memory():
    h0, h1, h2, to = idle_cursors(sort=True)
    print(f"40 sorted (not streamed) idle cursors: heap MB {h0:.1f} -> {h1:.1f} open -> {h2:.1f} after expiry; timed out {to}")
    assert to == 40
    assert h1 > h0 + 10, "the sorted cursors hold their documents: expected about 20 MB"
    assert h2 < h1 - 10, "their expiry must free them"


def test_streamed_cursors_hold_no_documents():
    h0, h1, h2, to = idle_cursors(sort=False)
    print(f"40 streamed idle cursors: heap MB {h0:.1f} -> {h1:.1f} open -> {h2:.1f} after expiry; timed out {to}")
    assert to == 40
    assert h1 < h0 + 10 and h2 < h1 + 10


def test_non_streamed_find_longer_than_the_window_is_answered():
    n = 100_000
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    f.close()
    srv = Server(make_bucket("r60w"), "r60w", idle=3, extra=["--lease-ttl-secs", "3"],
                 env={"MONGO_TEST_FAULTS_FILE": f.name, "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"})
    srv.extra += ["--replica-set", "rs0", "--advertise", f"localhost:{srv.port}"]
    srv.start()
    try:
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=300000, retryReads=False)
        for s in range(0, n, 5000):
            c.d.t.insert_many([{"_id": i, "name": "abc", "pad": "x" * 200} for i in range(s, s + 5000)])
        lines, bad = [], []
        cases = [("collation", {"filter": {"name": "ABC"}, "collation": {"locale": "en", "strength": 2}}),
                 ("let", {"filter": {"$expr": {"$eq": ["$name", "$$v"]}}, "let": {"v": "abc"}}),
                 ("snapshot", {"filter": {}, "readConcern": {"level": "snapshot"}}),
                 ("sort", {"filter": {}, "sort": {"pad": 1}})]
        for name, opts in cases:
            with open(f.name, "w") as fh:
                fh.write("")
            deadline = time.time() + 60
            closes = srv.log_text().count("LEASE writer_close tenant=d ")
            while time.time() < deadline and srv.log_text().count("LEASE writer_close tenant=d ") == closes and name == "collation":
                time.sleep(0.5)     # the first case waits for the writer's idle close (the others: the reader closes when idle)
            time.sleep(4)
            with open(f.name, "w") as fh:
                fh.write("delay_get tenants/d/ 60\n")
            c.d.t.find_one({"_id": 0})
            t0 = time.time()
            try:
                cmd = {"find": "t", "batchSize": 200_000, "projection": {"_id": 1}}
                cmd.update(opts)
                from bson import SON
                r = c.d.command(SON(list(cmd.items())))
                got = len(r["cursor"]["firstBatch"])
                out = f"{name}: {got} documents in one command, {time.time() - t0:.1f} s"
                if got != n:
                    bad.append(out)
            except PyMongoError as e:
                out = f"{name}: refused after {time.time() - t0:.1f} s: {getattr(e, 'code', None)} {str(e)[:100]}"
                bad.append(out)
            lines.append(out)
        with open(f.name, "w") as fh:
            fh.write("")
        print("; ".join(lines) + f"; F7 refusals in the log {srv.log_text().count('it begins no new read (F7)')}")
        assert not bad, bad
        assert all(float(x.split(", ")[1].split(" s")[0]) > 3.0 for x in lines), "a find was not longer than the window"
    finally:
        srv.term()
        os.unlink(f.name)
