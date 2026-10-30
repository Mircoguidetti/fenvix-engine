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


def _pending(bucket, d, holder, dbs, ff=None):
    """Writes acknowledged only in the local log of a killed process."""
    s = ll_server(bucket, f"llp-{holder}", holder, d).start()
    c = client(s)
    for db in dbs:
        for i in range(5):
            c[db]["c"].insert_one({"_id": i})
    s.kill9()


def test_e_the_replay_runs_before_the_admin_interface():
    """Should-fix: the local log is applied before the control layer starts, so no admin operation (erase, restore, fork)
    can run on a database whose writes are still only in the log. The replay is held (its WAL PUT hangs, fault injection);
    while it is held the admin port must not answer. Before the fix the control layer started first."""
    import urllib.request
    bucket = make_bucket("lle")
    d = tempfile.mkdtemp(prefix="lle-")
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    try:
        _pending(bucket, d, "E1", ["e"])
        with open(ff.name, "w") as f:
            f.write("hang_put tenants/e/wal/\n")
        s = ll_server(bucket, "lle2", "E1", d, env={"MONGO_TEST_FAULTS_FILE": ff.name})
        s.start(wait=False)
        deadline = time.time() + 30
        while "FAULT hang_put tenants/e/wal/" not in s.log_text() and time.time() < deadline:
            time.sleep(0.2)
        assert "FAULT hang_put tenants/e/wal/" in s.log_text(), "the replay did not reach its WAL write"
        import urllib.error
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{s.admin_port}/v1/health", timeout=2)
            admin_up = True
        except urllib.error.HTTPError:
            admin_up = True                 # it answered (401 without the token): the admin interface is up
        except Exception:
            admin_up = False
        print("admin interface answering during the replay:", admin_up)
        open(ff.name, "w").close()      # let the replay finish
        s.wait_ready(60)
        assert client(s)["e"]["c"].count_documents({}) == 5
        s.term()
        assert not admin_up, "the admin interface (erase, restore, fork) was up while the local log was still being applied"
    finally:
        shutil.rmtree(d, ignore_errors=True)
        os.unlink(ff.name)


def test_f_one_failing_database_does_not_block_the_restart():
    """Should-fix: the replay of one database fails (reading its manifest fails, fault injection); the restart must still finish,
    the other database must be replayed, and the failing one's records must be kept and applied by the next restart.
    Before the fix the first error stopped the whole restart (the process exited)."""
    bucket = make_bucket("llf")
    d = tempfile.mkdtemp(prefix="llf-")
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    try:
        _pending(bucket, d, "F1", ["f1", "f2"])
        with open(ff.name, "w") as f:
            f.write("fail_get tenants/f1/manifest/\n")
        s = ll_server(bucket, "llf2", "F1", d, env={"MONGO_TEST_FAULTS_FILE": ff.name}, extra=["--local-log-replay-timeout-secs", "5"])
        try:
            s.start()
            started = True
        except RuntimeError:
            started = False
        print("restart with one failing database finished:", started)
        assert started, "one failing database stopped the whole restart"
        c = client(s)
        assert c["f2"]["c"].count_documents({}) == 5, "the healthy database was not replayed"
        s.kill9()
        s = ll_server(bucket, "llf3", "F1", d).start()     # no fault now
        n = client(s)["f1"]["c"].count_documents({})
        print("failing database's writes after the next restart:", n, "of 5")
        assert n == 5, "the failing database's records were not kept for the next restart"
        s.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)
        os.unlink(ff.name)


def test_g_a_database_kept_for_a_later_replay_loses_no_acknowledged_write():
    """Review defect 1 (after test f): the replay of database g1 fails at the restart (reading its manifest fails, fault
    injection), so its 5 records are kept in the log; then the fault goes away (a transient S3 error). Between this restart
    and the next one a client writes to g1. Either the write is refused while g1 is in recovery, or g1's kept records are
    applied first: after a clean shutdown and the next restart, every acknowledged write of g1 (the 5 kept ones and the
    client's) must be there. Before the fix the client's write opened g1 as a new writer at once: the 5 kept records were
    missing from its reads, and the next restart skipped them (they were older than the newer writer's marker): lost."""
    bucket = make_bucket("llg")
    d = tempfile.mkdtemp(prefix="llg-")
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    try:
        _pending(bucket, d, "G1", ["g1"])
        with open(ff.name, "w") as f:
            f.write("fail_get tenants/g1/manifest/\n")
        s = ll_server(bucket, "llg2", "G1", d, env={"MONGO_TEST_FAULTS_FILE": ff.name}, extra=["--local-log-replay-timeout-secs", "5"]).start()
        assert "replay of database g1 failed" in s.log_text(), "the fault did not make g1's replay fail (the test needs it)"
        with open(ff.name, "w") as f:
            f.write("")                                   # the S3 error was transient
        c = client(s)
        refused, acked, t0 = 0, None, time.time()
        while acked is None and time.time() - t0 < 60:
            try:
                c["g1"]["c"].insert_one({"_id": "between"})
                acked = round(time.time() - t0, 1)
            except OperationFailure as e:
                refused += 1
                last = str(e)
                time.sleep(0.5)
        seen = c["g1"]["c"].count_documents({}) if acked is not None else None
        print(f"write between the restarts: refused {refused} times, acknowledged after {acked} s; documents then visible: {seen}")
        if refused:
            print("refusal:", last[:200])
        assert acked is not None, "g1 stayed refused for 60 s after the fault went away (the kept records were never applied)"
        s.term()
        s = ll_server(bucket, "llg3", "G1", d).start()
        got = {x["_id"] for x in client(s)["g1"]["c"].find({}, {"_id": 1})}
        missing = [i for i in [0, 1, 2, 3, 4, "between"] if i not in got]
        print("acknowledged writes of g1 missing after the next restart:", missing)
        assert not missing, f"acknowledged writes lost: {missing}"
        assert seen == 6, f"while g1 was open for writes its reads showed {seen} of 6 acknowledged documents"
        s.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)
        os.unlink(ff.name)


def test_h_kept_records_survive_a_clean_shutdown_and_a_replay_that_opened_the_database():
    """Review defect 1, the other paths: the replay of h1 fails AFTER it opened the database as a writer (its WAL writes
    fail, fault injection: the stored writer epoch is now one above the records'), and the fault stays for the whole run,
    so no retry succeeds. The process then shuts down cleanly (the close of the database and the trim of the log must not
    count the kept records as uploaded). At the next restart, without the fault, the 5 records must be applied, not refused
    as written by another writer. Before the fix they were refused (the epoch the replay itself opened looked like a newer
    writer) and lost."""
    bucket = make_bucket("llh")
    d = tempfile.mkdtemp(prefix="llh-")
    ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults")
    ff.close()
    try:
        _pending(bucket, d, "H1", ["h1"])
        with open(ff.name, "w") as f:
            f.write("fail_put tenants/h1/wal/\n")
        s = ll_server(bucket, "llh2", "H1", d, env={"MONGO_TEST_FAULTS_FILE": ff.name}, extra=["--local-log-replay-timeout-secs", "5"]).start()
        assert "replay of database h1 failed" in s.log_text(), "the fault did not make h1's replay fail (the test needs it)"
        c = client(s)
        try:
            c["h1"]["c"].insert_one({"_id": "between"})
            between = "acknowledged"
        except OperationFailure as e:
            between = f"refused ({e.code}: {str(e)[:160]})"
        print("write while h1's records are kept:", between)
        try:
            seen = c["h1"]["c"].count_documents({})
        except OperationFailure as e:
            seen = f"refused ({e.code})"
        print("read while h1's records are kept:", seen)
        assert seen == 5 or str(seen).startswith("refused"), f"a read served h1 without its kept writes ({seen} of 5 documents)"
        time.sleep(3)
        s.term()                                            # clean: closes, uploads what it can, trims the log
        s = ll_server(bucket, "llh3", "H1", d).start()     # no fault now
        got = {x["_id"] for x in client(s)["h1"]["c"].find({}, {"_id": 1})}
        missing = [i for i in range(5) if i not in got]
        print("kept writes of h1 missing after the next restart:", missing, "| refused as another writer:", "opened by another writer" in s.log_text())
        assert not missing, f"acknowledged writes lost: {missing}"
        if between == "acknowledged":
            assert "between" in got, "a write acknowledged while the records were kept was lost"
        s.term()
    finally:
        shutil.rmtree(d, ignore_errors=True)
        os.unlink(ff.name)
