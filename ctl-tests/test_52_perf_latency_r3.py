"""perf-latency round 3: the review of d9dbc7c (sessions/2026-11-11-review-perf-latency).

D1: `Compactor::stop` aborts the compactor's jobs at their next await (perf-latency); a job cut while it uploads a run left
the run's multipart upload incomplete: the database's rows, invisible to LIST and so to the erase's proof (the reviewer
found 789 MB after `remaining_objects: 0`). Now a run writer dropped mid-upload aborts its upload, and the erase lists
and aborts every incomplete upload under the database's prefix (that also covers a process that exited mid-upload).
D2: this is the test of the `stop` change: a job paused mid-run (the background's CPU share at 1%), an erase, then
nothing of the database is left, objects or incomplete uploads.
"""
import os
import time

import pytest

from lib import Server, make_bucket, s3, list_prefix

DB = "mpvictim"


def incomplete_uploads(bucket, prefix):
    """Incomplete multipart uploads under `prefix`, listed directly from MinIO (MinIO lists them for the whole bucket
    or an exact key only, not for a partial prefix: the whole bucket is listed and filtered)."""
    out = []
    c = s3()
    kw = {"Bucket": bucket}
    while True:
        r = c.list_multipart_uploads(**kw)
        out += [(u["Key"], u["UploadId"]) for u in r.get("Uploads", []) if u["Key"].startswith(prefix)]
        if not r.get("IsTruncated"):
            return out
        kw["KeyMarker"], kw["UploadIdMarker"] = r["NextKeyMarker"], r["NextUploadIdMarker"]


def write_mb(client, mb, start):
    coll = client[DB]["c"]
    n = mb * 10        # 100 KB documents that do not compress
    for i in range(start, start + n, 50):
        coll.insert_many([{"_id": j, "pad": os.urandom(100_000)} for j in range(i, min(i + 50, start + n))])
    return start + n


def test_erase_leaves_no_incomplete_upload():
    bucket = make_bucket("plr3mp")
    prefix = f"tenants/{DB}/"
    # phase 1, default settings: data and compaction work behind it
    srv = Server(bucket, "plr3mp1", idle=600).start()
    nxt = write_mb(srv.client(), 220, 0)
    srv.term()
    # phase 2: the background's CPU share at 1%: its jobs (reseals, merges) take minutes, so the erase catches one uploading
    srv = Server(bucket, "plr3mp2", idle=600, extra=["--compaction-cpu-pct", "1"]).start()
    t0 = time.time()
    # incomplete uploads phase 1's exit left (jobs in flight at a clean shutdown): not the case checked here
    before = set(incomplete_uploads(bucket, prefix))
    nxt = write_mb(srv.client(), 60, nxt)
    caught = []
    deadline = time.time() + 240
    while time.time() < deadline and not caught:
        # an upload this process started (a run of writer epoch 2, or an SST of its compaction)
        caught = [k for k, i in incomplete_uploads(bucket, prefix) if (k, i) not in before]
        if not caught:
            time.sleep(1)
    assert caught, "no job was caught mid-upload: the test did not reach the case it checks"
    st, r = srv.admin.delete(f"/v1/databases/{DB}")
    assert st == 200, r
    assert r["erased"] is True and r["proof"]["remaining_objects"] == 0 and r["proof"]["remaining_incomplete_uploads"] == 0, r
    # right after the reply, and again after any in-flight upload had time to land: nothing of the database, objects or parts
    for wait in (0, 15):
        time.sleep(wait)
        assert list_prefix(bucket, prefix) == []
        left = incomplete_uploads(bucket, prefix)
        assert left == [], f"incomplete uploads left after the erase: {left}"
    log = srv.log_text()
    srv.term()
    print(f"caught mid-upload: {caught}; aborted by the erase: {r.get('incomplete_uploads_aborted')}; "
          f"phase 2 lasted {time.time() - t0:.0f} s")
    assert "abort after the writer was dropped failed" not in log
    print(f"incomplete uploads left by phase 1's clean shutdown: {len(before)}")


def test_sweep_finds_a_planted_upload():
    """A process that exited mid-upload leaves an incomplete upload no writer will abort: the erase finds it (a planted
    one, the case that is older than the branch)."""
    bucket = make_bucket("plr3pl")
    srv = Server(bucket, "plr3pl", idle=600).start()
    srv.client()["planted"]["c"].insert_one({"_id": 1})
    c = s3()
    key = "tenants/planted/fx/run/g0/left-by-a-dead-process.run"
    u = c.create_multipart_upload(Bucket=bucket, Key=key)
    c.upload_part(Bucket=bucket, Key=key, UploadId=u["UploadId"], PartNumber=1, Body=b"r" * (5 << 20))
    # another database's upload under a name with the same start is not touched
    other = "tenants/planted2/fx/run/g0/live.run"
    u2 = c.create_multipart_upload(Bucket=bucket, Key=other)
    st, r = srv.admin.delete("/v1/databases/planted")
    assert st == 200, r
    assert r["incomplete_uploads_aborted"] == 1, r
    assert incomplete_uploads(bucket, "tenants/planted/") == []
    assert [k for k, _ in incomplete_uploads(bucket, "tenants/planted2/")] == [other]
    c.abort_multipart_upload(Bucket=bucket, Key=other, UploadId=u2["UploadId"])
    srv.term()
