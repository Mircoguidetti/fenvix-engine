"""Correction 6: with `--store local` (a directory instead of S3) the control operations and the wake of a sleeping database used to fail
with "PutMode::Update not yet implemented": object_store's LocalFileSystem has no conditional overwrite (If-Match). The engine now
wraps the local store with a compare-and-swap on the entity tag (valid for ONE process on the directory; S3 does it server-side with
If-Match). These tests run every operation that needs it on the local store, leases on."""
import os
import shutil
import subprocess
import tempfile
import time

import pytest
from pymongo import MongoClient

from lib import Admin, TOKEN, BIN, OUT, free_port, wait_until


class Local:
    def __init__(self, idle=60, extra=()):
        self.dir = tempfile.mkdtemp(prefix="localstore")
        self.port, self.aport = free_port(), free_port()
        self.log = os.path.join(OUT, f"local-{self.port}.log")
        self.args = [BIN, "--listen", f"127.0.0.1:{self.port}", "--admin-listen", f"127.0.0.1:{self.aport}", "--store", "local", "--local-dir", self.dir,
                     "--idle-secs", str(idle), "--holder", f"loc{self.port}", "--no-ttl", "--lease-ttl-secs", "6", "--retention-interval-secs", "2", *extra]
        self.p = None
        self.admin = Admin(f"http://127.0.0.1:{self.aport}")

    def start(self):
        self.p = subprocess.Popen(self.args, stdout=open(self.log, "ab"), stderr=subprocess.STDOUT, env=dict(os.environ, MONGO_ADMIN_TOKEN=TOKEN))
        wait_until(lambda: self.admin.get("/v1/health")[0] == 200, 30, what="admin port")
        return self

    def client(self):
        return MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=20000)

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(30)
            except subprocess.TimeoutExpired:
                self.p.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


@pytest.fixture()
def srv():
    s = Local().start()
    yield s
    s.stop()


def test_create_fork_keepawake_retention_export_erase_on_the_local_store(srv):
    a, c = srv.admin, srv.client()
    st, r = a.post("/v1/databases", {"name": "lc1"})
    assert st == 201, r
    c.lc1.t.insert_many([{"_id": i, "v": i} for i in range(200)])
    for mode in ("clone", "copy"):
        st, r = a.post("/v1/databases/lc1/fork", {"target": f"lcf_{mode}", "mode": mode})
        assert st == 201, (mode, r)
        assert c[f"lcf_{mode}"].t.count_documents({}) == 200
        c[f"lcf_{mode}"].t.insert_one({"_id": "only-fork"})
        assert c.lc1.t.count_documents({}) == 200
    st, r = a.put("/v1/databases/lc1/keep-awake")
    assert st == 200 and r["keep_awake"], r
    st, r = a.delete("/v1/databases/lc1/keep-awake")
    assert st == 200, r
    st, r = a.put("/v1/databases/lc1/retention", {"window_secs": 600, "interval_secs": 2})
    assert st == 200, r
    st, r = a.post("/v1/databases/lc1/exports")
    assert st == 201, r
    st, r = a.delete("/v1/databases/lcf_clone")
    assert st == 200 and r["erased"], r
    st, r = a.delete("/v1/databases/lcf_copy")
    assert st == 200 and r["erased"], r


def test_restore_on_the_local_store(srv):
    a, c = srv.admin, srv.client()
    a.post("/v1/databases", {"name": "lr1"})
    assert a.put("/v1/databases/lr1/retention", {"window_secs": 600, "interval_secs": 2})[0] == 200
    c.lr1.t.insert_many([{"_id": i} for i in range(50)])
    time.sleep(2.6)
    t = int(time.time() * 1000)
    time.sleep(2.6)
    c.lr1.t.insert_many([{"_id": 100 + i} for i in range(20)])
    time.sleep(2.6)
    st, r = a.post("/v1/databases/lr1/restore", {"to": t})
    assert st == 200, r
    assert c.lr1.t.count_documents({}) == 50


def test_a_sleeping_database_wakes_up_with_leases_on():
    s = Local(idle=2).start()
    try:
        c = s.client()
        c.sl.t.insert_many([{"_id": i} for i in range(30)])
        time.sleep(5)                                     # idle close: the database goes to sleep (its lease is released)
        assert c.sl.t.count_documents({}) == 30           # wake: takes the lease again
        c.sl.t.insert_one({"_id": "after-wake"})
        time.sleep(5)
        assert c.sl.t.count_documents({}) == 31
        assert "PutMode::Update" not in open(s.log).read()
    finally:
        s.stop()


def test_the_compare_and_swap_refuses_a_stale_writer():
    """The local store wrapper must behave like If-Match: a write with an old entity tag fails, with the current one succeeds. Seen
    through the lease of a database: two engines on one directory cannot both own it (that is a property of S3's If-Match on a bucket, and
    of this wrapper inside ONE process; two processes on one directory are not protected, see results-final-engine.md)."""
    s = Local().start()
    try:
        c = s.client()
        c.cas.t.insert_one({"a": 1})
        a = s.admin
        # create twice: the second must be refused as already existing (PutMode::Create)
        assert a.post("/v1/databases", {"name": "casdb"})[0] == 201
        assert a.post("/v1/databases", {"name": "casdb"})[0] == 409
    finally:
        s.stop()
