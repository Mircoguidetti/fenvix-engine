"""Local log prototype (`--local-log-dir`): the four defects of review B3, each with a test that fails without its fix.

a. Sequence numbers must never go back across runs: run 1 uploads records up to seq ~5000 (the database's marker in object
   storage says 5000), run 2 writes nothing, run 3 writes and is killed: run 3's records must be applied at the next start.
   Before the fix every run numbered from 1 again, and the restart skipped run 3's records as "already uploaded".
b. A failed write (disk full in the middle of a record, test-only `MONGO_TEST_LOCALLOG_FAIL_AT`) must stop the process:
   nothing acknowledged after it, and every acknowledged write present after the restart. Before the fix the writer kept
   appending after the half-written record; the restart stops reading at the bad bytes, so later acknowledged writes were lost.
c. Records must not be applied over a newer writer: P1 (local log) acknowledges a write and is killed before uploading it;
   P2 takes the database over and writes the same document; P1 restarts. Before the fix P1's replay overwrote the document
   and left the secondary index pointing at the newer value.
d. Ownership is checked again after the fsync: P1 is frozen between the fsync and the reply (test-only `stall_local_ack`
   plus SIGSTOP) past its lease, P2 takes over. Before the fix P1 acknowledged a write that no one has.
"""
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time

from pymongo import MongoClient
from pymongo.errors import OperationFailure, AutoReconnect, NetworkTimeout, ConnectionFailure, PyMongoError

from lib import Server, make_bucket

TTL = 4


def ll_server(bucket, tag, holder, d, env=None, extra=()):
    return Server(bucket, tag, idle=600, holder=holder, extra=["--local-log-dir", d, "--local-log-upload-secs", "60", "--lease-ttl-secs", str(TTL)] + list(extra), env=env or {})


def client(srv, timeout_ms=5000):
    return MongoClient(srv.uri, serverSelectionTimeoutMS=timeout_ms, socketTimeoutMS=30000, retryWrites=False, retryReads=False)


def test_a_sequence_numbers_never_restart_below_a_stored_marker():
    bucket = make_bucket("lla")
    d = tempfile.mkdtemp(prefix="lla-")
    try:
        s = ll_server(bucket, "lla1", "A1", d).start()
        c = client(s)
        coll = c["a"]["c"]

        def w(lo, hi):
            for i in range(lo, hi):
                coll.insert_one({"_id": i})
        ts = [threading.Thread(target=w, args=(k * 1250, (k + 1) * 1250)) for k in range(4)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        s.term()                       # clean: the close uploads everything, the marker says ~5000
        s = ll_server(bucket, "lla2", "A1", d).start()
        s.term()                       # run 2: nothing written
        s = ll_server(bucket, "lla3", "A1", d).start()
        c = client(s)
        for i in range(10):
            c["a"]["c"].insert_one({"_id": f"run3-{i}"})
        s.kill9()                      # run 3's records are only in the local log
        s = ll_server(bucket, "lla4", "A1", d).start()
        c = client(s)
        got = [x["_id"] for x in c["a"]["c"].find({"_id": {"$regex": "^run3-"}})]
        print("run 3 writes after restart:", len(got), "of 10")
        assert len(got) == 10, f"run 3's acknowledged writes lost: {10 - len(got)} of 10"
        s.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_b_a_failed_write_stops_the_process_and_loses_no_acknowledged_write():
    bucket = make_bucket("llb")
    d = tempfile.mkdtemp(prefix="llb-")
    try:
        s = ll_server(bucket, "llb1", "B1", d, env={"MONGO_TEST_LOCALLOG_FAIL_AT": "300"}).start()
        c = client(s, timeout_ms=500)
        acked, failed_at = [], None
        t_fail = None
        for i in range(600):
            if t_fail and time.time() - t_fail > 5:
                break                      # 5 s of attempts after the failure are enough
            try:
                c["b"]["c"].insert_one({"_id": i})
                acked.append(i)
            except PyMongoError:
                if failed_at is None:
                    failed_at, t_fail = i, time.time()
        after = [i for i in acked if failed_at is not None and i > failed_at]
        print(f"first failure at write {failed_at}; acknowledged after it: {len(after)}")
        s.kill9()
        s = ll_server(bucket, "llb2", "B1", d).start()
        c = client(s)
        got = {x["_id"] for x in c["b"]["c"].find({}, {"_id": 1})}
        missing = [i for i in acked if i not in got]
        print(f"acknowledged {len(acked)}, missing after restart {len(missing)}")
        assert failed_at is not None, "the injected failure did not happen"
        assert not missing, f"{len(missing)} acknowledged writes lost (first {missing[:5]})"
        assert failed_at not in got, "the write that failed is visible after the restart"
        s.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_c_replay_is_refused_over_a_newer_writer():
    bucket = make_bucket("llc")
    d = tempfile.mkdtemp(prefix="llc-")
    try:
        p1 = ll_server(bucket, "llc1", "C1", d).start()
        c1 = client(p1)
        c1["c"]["c"].create_index("v")
        c1["c"]["c"].insert_one({"_id": 0, "v": 0})
        p1.term()                      # index and first document uploaded
        p1 = ll_server(bucket, "llc2", "C1", d).start()
        c1 = client(p1)
        c1["c"]["c"].insert_one({"_id": 1, "v": 1})      # acknowledged, only in P1's local log
        p1.kill9()
        p2 = Server(bucket, "llc3", idle=600, holder="C2", extra=["--lease-ttl-secs", str(TTL)]).start()
        c2 = client(p2)
        deadline = time.time() + 30
        while True:
            try:
                c2["c"]["c"].insert_one({"_id": 1, "v": 2})
                break
            except OperationFailure as e:
                if e.code != 134 or time.time() > deadline:
                    raise
                time.sleep(0.3)
        p2.term()
        p1 = ll_server(bucket, "llc4", "C1", d).start()
        c1 = client(p1)
        doc = c1["c"]["c"].find_one({"_id": 1})
        by_v1 = list(c1["c"]["c"].find({"v": 1}).hint("v_1"))
        by_v2 = list(c1["c"]["c"].find({"v": 2}).hint("v_1"))
        print("doc:", doc, "index v=1:", by_v1, "index v=2:", by_v2)
        assert doc == {"_id": 1, "v": 2}, f"P1's replay overwrote the newer write: {doc}"
        assert by_v1 == [] and by_v2 == [{"_id": 1, "v": 2}], f"secondary index inconsistent: v=1 {by_v1}, v=2 {by_v2}"
        p1.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_d_no_acknowledgement_after_ownership_was_lost_during_the_fsync():
    bucket = make_bucket("lld")
    d = tempfile.mkdtemp(prefix="lld-")
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    try:
        p1 = ll_server(bucket, "lld1", "D1", d, env={"MONGO_TEST_FAULTS_FILE": ff.name}).start()
        c1 = client(p1)
        c1["d"]["c"].insert_one({"_id": 0})
        with open(ff.name, "w") as f:
            f.write("stall_local_ack 3000\n")
        out = {}

        def write():
            try:
                c1["d"]["c"].insert_one({"_id": "x"})
                out["r"] = "acknowledged"
            except PyMongoError as e:
                out["r"] = f"error: {e}"[:160]
        t = threading.Thread(target=write)
        t.start()
        time.sleep(0.8)                          # the record is on disk; the reply is held by the stall
        p1.proc.send_signal(signal.SIGSTOP)
        time.sleep(TTL + 3)
        p2 = Server(bucket, "lld2", idle=600, holder="D2", extra=["--lease-ttl-secs", str(TTL)]).start()
        c2 = client(p2)
        deadline = time.time() + 30
        while True:
            try:
                c2["d"]["c"].insert_one({"_id": "y"})
                break
            except OperationFailure as e:
                if e.code != 134 or time.time() > deadline:
                    raise
                time.sleep(0.3)
        p1.proc.send_signal(signal.SIGCONT)
        t.join(60)
        present = c2["d"]["c"].find_one({"_id": "x"}) is not None
        print("write through P1:", out.get("r"), "| present in the database:", present)
        assert out.get("r") != "acknowledged" or present, "P1 acknowledged a write after losing the database, and the write is not there"
        p2.term()
        p1.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)
        os.unlink(ff.name)
