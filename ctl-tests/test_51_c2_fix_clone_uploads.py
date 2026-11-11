"""c2-fix-clone-uploads: the old compaction pass on databases with a compactor, compactor jobs after a close, and multipart
uploads left unfinished.

1. With compactors on, the old compaction pass ran on a database while it was closed (queued by the close when L0 held 4
   SSTs). Its scheduler is not gated on absorption: it folded the close's memtable flush (never absorbed: the compactor
   stops before that flush) into a sorted run. After the next open the absorptions moved X_d past those rows and the
   merged reader, which ignores SlateDB rows at or below X_d, answered with their older values. MEASURED before the fix
   with this test's shape: 60,000 stale answers in 10 rounds. The server's builder publish at the first write after an
   open hid it when it ran (every 2nd write batch in the judge, so at the first write after an open about half the time);
   here every 7th, as in the measurement.
2. A compactor stopped by a close kept its running jobs: an absorption read every L0 SST it had planned, seconds after
   the close, and threw the result away (MEASURED: 4.5-6 MB at 100,000 documents, read during ctl test_08's clone).
3. Uploads: a server killed while a run is being uploaded leaves the upload open (S3 bills its parts until someone aborts
   it); the next start aborts those older than `--abandoned-upload-secs`.
"""
import threading
import time

import pytest
from pymongo import UpdateOne

from lib import Server, make_bucket, s3, wait_until
from proxy import CountingProxy

# compactors on with the judge's small knobs; the builder publishes rarely (see 1.)
ENV = {"FENVIX_SEALED_RUNS": "7", "FENVIX_SEALED_COMPACTOR": "small", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0",
       "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}
SMALL_L0 = ["--l0-sst-bytes", "262144"]


def asleep(srv, db):
    srv.client()[db].command("tenantAdmin", close=True)
    wait_until(lambda: srv.admin.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 15, what="asleep")


def write_round(col, rnd, n, expect, batches=8):
    """Every document of this round is written several times in this round only: its last value is in the round's last
    batches, which the close flushes."""
    for b in range(batches):
        ops = []
        for i in range(rnd * n, rnd * n + n):
            if (i + b) % 3 == 0:
                v = rnd * 100 + b
                ops.append(UpdateOne({"_id": i}, {"$set": {"v": v, "pad": "x" * 200}}, upsert=True))
                expect[i] = v
        col.bulk_write(ops)


def wrong(col, expect):
    got = {d["_id"]: d.get("v") for d in col.find({}, {"v": 1})}
    return [(i, v, got.get(i)) for i, v in expect.items() if got.get(i) != v]


def test_no_stale_answer_after_a_close():
    bucket = make_bucket("c2fix-stale")
    srv = Server(bucket, "c2fix-stale", idle=600, extra=SMALL_L0, env=ENV).start()
    try:
        c = srv.client()
        col = c["g"]["c"]
        expect, bad, rounds_with_pass = {}, [], 0
        for rnd in range(8):
            write_round(col, rnd, 2000, expect)
            st = c.admin.command("serverStatus")["fenvix"]
            assert st["compactor"]["enabled"]
            p0 = st["compaction"]["passes"]
            asleep(srv, "g")
            time.sleep(3)
            if c.admin.command("serverStatus")["fenvix"]["compaction"]["passes"] > p0:
                rounds_with_pass += 1
            w = wrong(col, expect)
            print(f"round {rnd}: wrong {len(w)} {w[:3]}")
            bad += w
        st = c.admin.command("serverStatus")["fenvix"]["compactor"]
        print("compactor", st, "rounds with a pass on the closed database", rounds_with_pass)
        assert st["absorbs"] > 0, "no absorption ran: the test did not exercise the compactor"
        assert not bad, f"{len(bad)} stale or missing answers, e.g. {bad[:5]}"
    finally:
        srv.term()


def test_a_closed_database_reads_nothing():
    bucket = make_bucket("c2fix-idle")
    proxy = CountingProxy()
    srv = Server(bucket, "c2fix-idle", idle=600, endpoint=proxy.endpoint, extra=SMALL_L0, env=ENV).start()
    try:
        c = srv.client()
        col = c["d"]["c"]
        expect = {}
        # one round (about 5 L0 SSTs): enough for absorptions, not a full L0 at the close (an absorption of 256 KiB L0 SSTs
        # with 4 KiB blocks reads one block per GET and took up to 35 s here, MEASURED: with L0 full the close's flush
        # waits for it; that is the absorption's speed, a separate item of the report)
        write_round(col, 0, 2000, expect)
        assert c.admin.command("serverStatus")["fenvix"]["compactor"]["started"] > 0, "no compactor ran: the test did not exercise it"
        # close while the writes are fresh: the compactor has L0 SSTs to absorb, maybe an absorption running
        asleep(srv, "d")
        time.sleep(0.5)
        proxy.reset()
        time.sleep(6)
        snap = proxy.snapshot()
        reads = {k: v for k, v in snap["by_prefix"].items() if k.startswith("tenants/d") and any(x in v for x in ("GET", "GET_RANGE", "HEAD"))}
        data = {k: v for k, v in snap["bytes_by_prefix"].items() if k.startswith("tenants/d")}
        got = sum(v.get("GET:out", 0) + v.get("GET_RANGE:out", 0) for v in data.values())
        print("reads of the closed database in 6 s:", reads, "bytes", got)
        # the compaction pass may still compact the closed database over what its compactor absorbed (manifests, and
        # SSTs when there is L0 to fold); what must not happen is a stopped compactor's job reading its L0 SSTs block by
        # block (integration-3: 1.9 MB in GET_RANGEs here)
        assert not any("GET_RANGE" in v for v in reads.values()) and got < 100_000, f"a closed database was read: {reads}, {got} bytes"
    finally:
        srv.term()
        proxy.stop()


def unfinished(bucket):
    r = s3().list_multipart_uploads(Bucket=bucket)
    return [(u["Key"], u["UploadId"]) for u in r.get("Uploads", [])]


def test_kill_mid_upload_then_none_left():
    bucket = make_bucket("c2fix-up")
    srv = Server(bucket, "c2fix-up", idle=600, extra=SMALL_L0 + ["--abandoned-upload-secs", "2"], env=ENV).start()
    seen, killed = [], threading.Event()
    expect = {}

    def watch():
        # kill -9 the moment an upload is open (a run being written)
        while not killed.is_set():
            u = unfinished(bucket)
            if u:
                seen.extend(u)
                srv.kill9()
                killed.set()
                return
            time.sleep(0.005)

    try:
        col = srv.client(retryWrites=False)["k"]["c"]
        th = threading.Thread(target=watch, daemon=True)
        th.start()
        t0, rnd = time.time(), 0
        while not killed.is_set() and time.time() - t0 < 300:
            try:
                write_round(col, rnd, 3000, expect, batches=4)
            except Exception:
                # the kill cut this round: its writes may or may not be acknowledged, drop them from the check
                for i in range(rnd * 3000, rnd * 3000 + 3000):
                    expect.pop(i, None)
                break
            rnd += 1
        killed.set()
        th.join()
        assert seen, "no upload was ever seen open: the test did not exercise the window"
        left = unfinished(bucket)
        print(f"open uploads after kill -9: {len(left)} (seen {seen[:2]}), rounds {rnd}")
        assert left, "the kill left no open upload: the test did not exercise the window"
        time.sleep(3)
        srv.proc = None
        srv.start()
        wait_until(lambda: not unfinished(bucket), 30, what="no unfinished upload after the restart")
        assert "older than" in srv.log_text()
        # nothing acknowledged before the cut round was lost by the kill
        w = wrong(srv.client()["k"]["c"], expect)
        assert not w, f"{len(w)} acknowledged writes lost or stale after kill -9: {w[:5]}"
    finally:
        killed.set()
        srv.term()


def test_a_young_upload_is_left_alone():
    """The sweep must not abort an upload a live process may still be writing: only those older than the bound."""
    bucket = make_bucket("c2fix-young")
    up = s3().create_multipart_upload(Bucket=bucket, Key="tenants/x/fx/runs/young")
    srv = Server(bucket, "c2fix-young", idle=600, extra=["--abandoned-upload-secs", "3600"], env=ENV).start()
    try:
        wait_until(lambda: "unfinished multipart uploads" in srv.log_text(), 20, what="the sweep's log line")
        assert unfinished(bucket) == [("tenants/x/fx/runs/young", up["UploadId"])]
    finally:
        srv.term()
        s3().abort_multipart_upload(Bucket=bucket, Key="tenants/x/fx/runs/young", UploadId=up["UploadId"])
