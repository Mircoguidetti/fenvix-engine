"""Reviewer's test for fix-write-stalls cause 1 (the first build off the database's lock).

The cut of the first build is a WAL flush, then a snapshot at the last committed seq; its X_d is that seq. Under the lock
nothing was staged between the two, so X_d was durable. Off the lock, writes go on: a write staged while the build's WAL
flush is in flight is in the snapshot (seq at or below X_d) but not yet durable. If the version lands and the process then
dies before that write's own WAL object lands, the next owner replays the WAL up to a seq below X_d and gives its new
writes seqs at or below X_d: reads take rows at or below X_d from our runs only, and those acknowledged writes would not
be found.

Deterministic: every WAL PUT of the database answered 6 s late (fault file) from the start; 4 writers; the first build's
WAL flush waits, writes are staged meanwhile; the version lands (fx/manifest); SIGKILL before the next WAL object lands;
a second process takes over and writes; every write acknowledged by either process must read back.
"""
import os
import tempfile
import threading
import time

from pymongo import MongoClient

from lib import Server, make_bucket, list_prefix, wait_until

DB = "cut"
SEALED = ["--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--lease-ttl-secs", "6"]


def test_first_build_cut_and_a_crash_before_the_staged_writes_are_durable():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("r68")
    with open(ff.name, "w") as fh:
        fh.write(f"delay_put tenants/{DB}/wal/ 6000\n")
    p1 = Server(bucket, "r68a", idle=600, extra=SEALED, env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    acked, lock, stop = {}, threading.Lock(), threading.Event()

    def writer(k):
        cl = MongoClient(p1.uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=60000, retryWrites=False)
        i = 0
        while not stop.is_set():
            _id = f"p1-{k}-{i}"
            try:
                cl[DB].c.insert_one({"_id": _id, "v": 1})
                with lock:
                    acked[_id] = 1
            except Exception:
                if p1.proc.poll() is not None:
                    return
            i += 1
    ws = [threading.Thread(target=writer, args=(k,), daemon=True) for k in range(int(os.environ.get("R68_WRITERS", "32")))]
    for w in ws:
        w.start()
    t0 = time.time()
    try:
        wait_until(lambda: any("/fx/manifest/" in k for k, _ in list_prefix(bucket, f"tenants/{DB}/fx/")), 60, every=0.05, what="the first version")
        t_version = time.time() - t0
        p1.kill9()
        stop.set()
    finally:
        stop.set()
        p1.kill9()
    log1 = p1.log_text()
    first = [l for l in log1.splitlines() if "FIRST_PUBLISH" in l]
    with open(ff.name, "w") as fh:
        fh.write("")
    p2 = Server(bucket, "r68b", idle=600, extra=SEALED).start()
    try:
        c2 = p2.client()
        wait_until(lambda: c2[DB].c.find_one({}) is not None or True, 60, what="open")
        deadline = time.time() + 60
        p2_acked = []
        while len(p2_acked) < 300 and time.time() < deadline:
            _id = f"p2-{len(p2_acked)}"
            try:
                c2[DB].c.insert_one({"_id": _id, "v": 2})
                p2_acked.append(_id)
            except Exception:
                time.sleep(0.3)
        ids = {d["_id"] for d in c2[DB].c.find({}, {"_id": 1})}
        miss1 = [i for i in acked if i not in ids]
        miss2 = [i for i in p2_acked if i not in ids]
        found = [i for i in p2_acked if c2[DB].c.find_one({"_id": i}) is not None]
        p2.restart()
        ids_r = {d["_id"] for d in p2.client()[DB].c.find({}, {"_id": 1})}
        miss2r = [i for i in p2_acked if i not in ids_r]
    finally:
        p2.term()
        os.unlink(ff.name)
    import json, boto3
    s3c = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    keys = sorted(k for k, _ in list_prefix(bucket, f"tenants/{DB}/fx/manifest/"))
    v1 = json.loads(s3c.get_object(Bucket=bucket, Key=keys[0])["Body"].read()) if keys else {}
    print(f"first version x_d={v1.get('x_d')} writer_epoch={v1.get('writer_epoch')}; P1 acknowledged writes before the kill: {len(acked)}")
    print(f"version landed after {t_version:.1f} s; P1 log: {first[:2]}; P1 acknowledged {len(acked)}, missing {len(miss1)}; "
          f"P2 acknowledged {len(p2_acked)}, missing from a scan {len(miss2)} (first: {miss2[:5]}), found by _id {len(found)}; after a restart missing {len(miss2r)}")
    assert not miss1, f"P1's acknowledged writes missing: {miss1[:5]}"
    assert not miss2 and not miss2r, f"RULE 8: P2's acknowledged writes missing: {miss2[:5]}"
