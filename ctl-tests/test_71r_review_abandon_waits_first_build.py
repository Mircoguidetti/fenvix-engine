"""Review of fix-compactor-start a60f4fe (2026-11-13): an abandon now lets a first build running off the lock end by itself
(up to 10 s) before cutting it. The risky case for rule 8 and F7: the lease is lost while the first build is still
uploading its runs, so the old owner's build goes on for seconds while another process takes the database over and
writes. Every write acknowledged by either process must be there after the takeover and after a restart; none twice.

P1's first build is slowed (every PUT of its runs delayed, fault file) and its process lease renewal is delayed past the
lease (ttl 3 s), so the lease lapses with the build in flight. P2 writes meanwhile; P1's clients keep writing too
(refused or acknowledged, as the engine decides).
"""
import os
import tempfile
import threading
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

# the judge's and CI's "off" mode sets FENVIX_SEALED_RUNS=off: there is no first build to slow
pytestmark = pytest.mark.skipif(os.environ.get("FENVIX_SEALED_RUNS") == "off", reason="needs sealed runs (FENVIX_SEALED_RUNS=off)")

DB = "lb"
SEALED = ["--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--lease-ttl-secs", "3",
          "--sealed-version-retention-secs", "3600"]


def versions(bucket):
    """Every version of our manifest kept (retention 1 h): number, writer epoch, X_d, kind, groups, delta runs, time."""
    import json
    from lib import s3
    c = s3()
    out = []
    for o in c.list_objects_v2(Bucket=bucket, Prefix=f"tenants/{DB}/fx/manifest/").get("Contents", []):
        v = json.loads(c.get_object(Bucket=bucket, Key=o["Key"])["Body"].read())
        out.append((v["version"], v.get("writer_epoch"), v.get("x_d"), v.get("kind"), len(v.get("groups", [])), len(v.get("delta", [])), o["LastModified"].strftime("%H:%M:%S.%f")[:-3]))
    return out


def writer(uri, tag, acked, stop, errors):
    c = MongoClient(uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False)
    k = 0
    while not stop.is_set():
        try:
            c[DB]["c"].insert_many([{"_id": f"{tag}-{k}-{i}", "p": "x" * 300} for i in range(20)], ordered=True)
            acked.extend(f"{tag}-{k}-{i}" for i in range(20))
        except PyMongoError as e:
            errors.append(str(e)[:80])
            time.sleep(0.2)
        k += 1


def test_lease_lost_while_the_first_build_uploads_then_takeover():
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    bucket = make_bucket("abfb")
    p1 = Server(bucket, "abfb1", idle=600, holder="P71a", extra=SEALED, env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
    p2 = Server(bucket, "abfb2", idle=600, holder="P71b", extra=SEALED).start()
    try:
        with open(ff.name, "w") as fh:
            fh.write(f"delay_put tenants/{DB}/fx/run/ 9000\n")
        a1, a2, e1, e2, stop = [], [], [], [], threading.Event()
        t1 = threading.Thread(target=writer, args=(p1.uri, "p1", a1, stop, e1), daemon=True)
        t1.start()
        # the first build starts at a write after E1's announcement and then waits on its run PUTs
        t0 = time.time()
        while "delay_put" not in p1.log_text() or "FAULT delay_put tenants/" not in p1.log_text():
            assert time.time() - t0 < 60, "the first build never uploaded a run"
            time.sleep(0.2)
        # now the process lease lapses (renewal delayed past ttl 3 s) with the build in flight
        with open(ff.name, "a") as fh:
            fh.write("delay_put processes/P71a/_lease 6000\n")
        t2 = threading.Thread(target=writer, args=(p2.uri, "p2", a2, stop, e2), daemon=True)
        t2.start()
        time.sleep(30)
        stop.set()
        t1.join(timeout=90)
        t2.join(timeout=90)
        with open(ff.name, "w") as fh:
            fh.write("")
        log1 = p1.log_text()
        p1.kill9()
        p2.term()
        p2.proc = None
        p2.start()
        c = MongoClient(p2.uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=120000)
        ids = []
        for _ in range(60):
            try:
                ids = [d["_id"] for d in c[DB]["c"].find({}, {"_id": 1})]
                break
            except PyMongoError:
                time.sleep(1)
        have = set(ids)
        dup = len(ids) - len(have)
        miss1 = [i for i in a1 if i not in have]
        miss2 = [i for i in a2 if i not in have]
        lost_lines = log1.count("LEASE lost") + log1.count("process_lease_lapsed")
        cut = log1.count("first build") + 0
        print(f"P1 acknowledged {len(a1)} ({len(e1)} errors), P2 acknowledged {len(a2)} ({len(e2)} errors); after the takeover and a "
              f"restart {len(have)} documents, {dup} twice, missing P1 {len(miss1)} P2 {len(miss2)}; P1 lease-loss lines {lost_lines}; "
              f"P1 FIRST_PUBLISH lines {log1.count('FIRST_PUBLISH')}, TIMING abandon lines {log1.count('TIMING abandon')}")
        vs = versions(bucket)
        print("versions (n, writer epoch, X_d, kind, groups, delta, time):", vs[:12], "...", vs[-3:])
        assert a2, "P2 never took the database over: the test did not reach its case"
        assert not miss1 and not miss2 and dup == 0, f"RULE 8: missing P1 {miss1[:5]} P2 {miss2[:5]}, {dup} twice"
    finally:
        p1.term()
        p2.term()
        os.unlink(ff.name)
