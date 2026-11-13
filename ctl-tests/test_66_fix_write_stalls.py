"""fix-write-stalls (2026-11-12): the first build of a database runs off its lock, and the compactor reads its unlisted
L0 SSTs off the lock. These are the new steps; each is held open with a test pause (FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS,
FENVIX_TEST_BUILD_PAUSE_CREATE_MS, FENVIX_TEST_RECOGNISE_PAUSE_MS) so that the process can be killed with SIGKILL inside
it, the database erased or closed in the middle of it, or another process take the database over after it.

Every test counts acknowledged writes (insert and update replies) and requires every one of them back after the event:
rule 8, zero acknowledged writes lost. The writes made while the first build runs must not wait for it (under 1 s each:
before this branch they waited for the whole build, 2.8-8.6 s on the benchmark).
"""
import os
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until

DB = "ws"
SEALED = ["--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]


def taken_over(client):
    """The new process serves the database once the killed one's lease ran out (6 s in these tests): until then every
    command is refused with TenantLeaseHeld (retryable)."""
    wait_until(lambda: client[DB]["c"].find_one({}, {"_id": 1}) is not None, timeout=60, what="the takeover")


def stats(client):
    return client.admin.command("serverStatus")["fenvix"]["compactor"]


class Acked:
    """Every acknowledged write: the value each _id must read back."""

    def __init__(self, coll):
        self.coll, self.want, self.longest, self.n = coll, {}, 0.0, 0

    def insert(self, i, v, pad=200):
        t0 = time.time()
        self.coll.insert_one({"_id": i, "v": v, "thread_id": f"t{i % 7}", "pad": os.urandom(pad // 2).hex()})
        self._done(t0)
        self.want[i] = v

    def update(self, i, v):
        t0 = time.time()
        r = self.coll.update_one({"_id": i}, {"$set": {"v": v}})
        self._done(t0)
        if r.matched_count:
            self.want[i] = v

    def _done(self, t0):
        self.longest = max(self.longest, time.time() - t0)
        self.n += 1

    def check(self, coll, drop_threads=()):
        got = {d["_id"]: d["v"] for d in coll.find({}, {"v": 1})}
        exp = {i: v for i, v in self.want.items() if f"t{i % 7}" not in drop_threads}
        missing = [i for i in exp if i not in got]
        wrong = [i for i in exp if i in got and got[i] != exp[i]]
        extra = [i for i in got if f"t{i % 7}" in drop_threads]
        assert not missing and not wrong and not extra, f"{len(missing)} missing, {len(wrong)} wrong, {len(extra)} erased but readable (of {len(exp)})"
        return len(exp)


def write_until_build(c, a, start, what="firstBuilds"):
    i = start
    t0 = time.time()
    while stats(c)[what] < 1:
        a.insert(i, f"pre-{i}")
        i += 1
        assert time.time() - t0 < 60, f"{what} did not start"
    return i


def write_during(a, i, secs):
    """Writes for `secs` while the first build is held: new documents and updates of documents it seals."""
    end = time.time() + secs
    a.longest = 0.0
    while time.time() < end:
        a.insert(i, f"during-{i}")
        if i % 3 == 0:
            a.update(i // 2, f"upd-{i}")
        i += 1
    return i


@pytest.mark.parametrize("where", ["snapshot", "create"])
def test_kill9_during_the_first_build_then_takeover(where):
    """SIGKILL while the first build is held after its snapshot (runs not written) or before its version's create (runs
    written, no version): another process takes the database over; every acknowledged write reads back, it builds its
    own first version and starts its compactor, and everything still reads back."""
    bucket = make_bucket(f"ws9{where[:2]}")
    pause = {"snapshot": "FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS", "create": "FENVIX_TEST_BUILD_PAUSE_CREATE_MS"}[where]
    srv = Server(bucket, f"ws9{where[:2]}a", idle=600, extra=SEALED, env={pause: "20000"}).start()
    c = srv.client()
    a = Acked(c[DB]["c"])
    i = write_until_build(c, a, 0)
    i = write_during(a, i, 3.0)
    s = stats(c)
    assert s["firstBuilds"] == 1 and s["firstInstalls"] == 0, s
    longest = a.longest
    srv.kill9()
    srv2 = Server(bucket, f"ws9{where[:2]}b", idle=600, extra=SEALED).start()
    c2 = srv2.client()
    taken_over(c2)
    n1 = a.check(c2[DB]["c"])
    a.coll = c2[DB]["c"]
    i = write_until_build(c2, a, i, "started")
    for _ in range(200):
        a.insert(i, f"after-{i}")
        i += 1
    n2 = a.check(c2[DB]["c"])
    s2 = stats(c2)
    srv2.term()
    print(f"kill at {where}: {a.n} writes acknowledged, longest while the build ran {longest:.3f} s; read back {n1} after the takeover, {n2} at the end; second process {s2['firstInstalls']} first installs, {s2['started']} compactors")
    assert longest < 1.0, f"a write waited for the first build: {longest:.2f} s"
    assert s2["started"] >= 1


def test_kill9_after_the_version_landed_before_its_install():
    """The first build's version is in the store but no write installed it yet (the install happens at a later write):
    SIGKILL, takeover, every acknowledged write reads back."""
    bucket = make_bucket("ws9land")
    srv = Server(bucket, "ws9landa", idle=600, extra=SEALED, env={"FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS": "1500"}).start()
    c = srv.client()
    a = Acked(c[DB]["c"])
    i = write_until_build(c, a, 0)
    i = write_during(a, i, 0.5)
    # no more writes: the version lands, nothing installs it
    wait_until(lambda: list_prefix(bucket, f"tenants/{DB}/fx/manifest/"), timeout=30, what="the first version in the store")
    time.sleep(1.0)
    assert stats(c)["firstInstalls"] == 0
    srv.kill9()
    srv2 = Server(bucket, "ws9landb", idle=600, extra=SEALED).start()
    c2 = srv2.client()
    taken_over(c2)
    n = a.check(c2[DB]["c"])
    a.coll = c2[DB]["c"]
    i = write_until_build(c2, a, i, "started")
    for _ in range(100):
        a.insert(i, f"after-{i}")
        i += 1
    a.check(c2[DB]["c"])
    srv2.term()
    print(f"version landed, not installed: {n} acknowledged writes read back after the takeover")


def test_kill9_during_the_compactors_recognition():
    """A restart whose compactor finds L0 SSTs its version does not list reads them in a job off the lock; SIGKILL while
    that job is held; a third process reads every acknowledged write back. Small memtables give many L0 SSTs."""
    bucket = make_bucket("ws9rec")
    small = SEALED + ["--memtable-max-mb", "1"]
    srv = Server(bucket, "ws9reca", idle=600, extra=small, env={"FENVIX_TEST_ABSORB_MIN_L0": "100"}).start()
    c = srv.client()
    a = Acked(c[DB]["c"])
    i = write_until_build(c, a, 0, "started")
    for _ in range(1500):
        a.insert(i, f"one-{i}", pad=2000)       # about 3 MB: L0 SSTs of 1 MB that the first process never absorbs
        i += 1
    srv.kill9()
    srv2 = Server(bucket, "ws9recb", idle=600, extra=small, env={"FENVIX_TEST_RECOGNISE_PAUSE_MS": "30000"}).start()
    c2 = srv2.client()
    taken_over(c2)
    a.coll = c2[DB]["c"]
    # the writes after the opening start the compactor (a few writes after a takeover, as on the base): none waits for
    # the L0 read, which its job does
    a.longest = 0.0
    t0 = time.time()
    while stats(c2)["started"] < 1:
        a.insert(i, f"opening-{i}")
        i += 1
        assert time.time() - t0 < 60, "the compactor did not start"
    first_write = a.longest
    wait_until(lambda: stats(c2)["recogniseJobs"] >= 1, timeout=30, what="the recognition job")
    s2_jobs = stats(c2)["recogniseJobs"]
    i = write_during(a, i, 2.0)
    n1 = a.check(c2[DB]["c"])
    srv2.kill9()
    srv3 = Server(bucket, "ws9recc", idle=600, extra=small).start()
    c3 = srv3.client()
    taken_over(c3)
    n2 = a.check(c3[DB]["c"])
    s3 = stats(c3)
    srv3.term()
    assert s2_jobs >= 1
    print(f"recognition: {s2_jobs} job(s), longest write after the opening {first_write:.3f} s; {n1} read back while the job was held, {n2} after the kill and restart; third process read {s3['l0RecognisedByRead']} SSTs")
    assert first_write < 1.0, f"a write after the opening waited {first_write:.2f} s"


def test_erasures_in_the_middle_of_the_first_build():
    """A customer's erasure (deleteMany on its key) while the first build is held waits for the build, then is recorded;
    the erased thread reads back empty and the others whole. Then a whole-database erase while a second database's first
    build is held: nothing of the database is left, at once and after the build's pause would have ended."""
    bucket = make_bucket("wser")
    srv = Server(bucket, "wser", idle=600, extra=SEALED + ["--sealed-erasure-poll-ms", "200", "--sealed-erasure-batch-ms", "0"], env={"FENVIX_TEST_BUILD_PAUSE_SNAPSHOT_MS": "4000"}).start()
    c = srv.client()
    c[DB].create_collection("c", customerKey="thread_id")
    a = Acked(c[DB]["c"])
    i = write_until_build(c, a, 0)
    i = write_during(a, i, 1.0)
    t0 = time.time()
    cut = i
    n = c[DB]["c"].delete_many({"thread_id": "t3"}).deleted_count
    took = time.time() - t0
    st, r = srv.admin.get(f"/v1/databases/{DB}/erasures")
    assert st == 200 and len(r["erasures"]) == 1, r
    for _ in range(100):
        a.insert(i, f"after-{i}")
        i += 1
    a.want = {k: v for k, v in a.want.items() if k % 7 != 3 or k >= cut}
    kept = a.check(c[DB]["c"], drop_threads=())
    e = wait_until(lambda: (x := srv.admin.get(f"/v1/databases/{DB}/erasures")[1]["erasures"][0])["state"] == "scrubbed" and x, timeout=60, what="the scrub")
    assert c[DB]["c"].count_documents({"thread_id": "t3", "v": {"$regex": "^(pre|during)"}}) == 0
    # whole-database erase during another database's first build
    other = "wsgone"
    b = Acked(c[other]["c"])
    j = write_until_build(c, b, 0) if stats(c)["firstBuilds"] < 2 else 0
    while stats(c)["firstBuilds"] < 2:
        b.insert(j, "x")
        j += 1
    st, r = srv.admin.delete(f"/v1/databases/{other}")
    assert st == 200 and r["erased"] and r["proof"]["remaining_objects"] == 0, r
    for wait in (0, 6):
        time.sleep(wait)
        assert list_prefix(bucket, f"tenants/{other}/") == [], "objects of the erased database left (a late run or version of the first build)"
    srv.term()
    print(f"erasure during the first build: deleteMany of {n} took {took:.2f} s (it waits for the build); {kept} documents kept and read back; scrub {e['rows_erased']} rows")


def test_kill9_mid_merge_with_an_l0_job_beside_it_then_takeover():
    """Cause 3: an L0-only SlateDB job runs beside a merge of sorted runs. A merge is held at its first row
    (FENVIX_TEST_HOLD_MERGE) while writes go on and L0 jobs run beside it; SIGKILL; another process takes the database
    over (no hold): every acknowledged write reads back, the new owner's compactor runs, and everything still reads back."""
    bucket = make_bucket("ws9merge")
    extra = SEALED + ["--memtable-max-mb", "1"]
    srv = Server(bucket, "ws9mergea", idle=600, extra=extra, env={"FENVIX_TEST_HOLD_MERGE": "1"}).start()
    c = srv.client()
    a = Acked(c[DB]["c"])
    i, t0 = 0, time.time()
    while not stats(c).get("mergeHeld"):
        for _ in range(50):
            a.insert(i, f"pre-{i}", pad=2000)
            i += 1
        assert time.time() - t0 < 180, "no merge of sorted runs was proposed"
    t1 = time.time()
    a.longest = 0.0
    while stats(c)["l0JobsBesideMerge"] < 2:
        for _ in range(50):
            a.insert(i, f"during-{i}", pad=2000)
            if i % 4 == 0:
                a.update(i // 2, f"upd-{i}")
            i += 1
        assert time.time() - t1 < 120, f"no L0 job beside the held merge: {stats(c)}"
    longest, s = a.longest, stats(c)
    assert s["mergeHeld"] == 1, s
    srv.kill9()
    srv2 = Server(bucket, "ws9mergeb", idle=600, extra=extra).start()
    c2 = srv2.client()
    taken_over(c2)
    n1 = a.check(c2[DB]["c"])
    a.coll = c2[DB]["c"]
    i = write_until_build(c2, a, i, "started")
    for _ in range(300):
        a.insert(i, f"after-{i}", pad=2000)
        i += 1
    n2 = a.check(c2[DB]["c"])
    srv2.term()
    print(f"kill mid-merge: {a.n} writes acknowledged, {s['l0JobsBesideMerge']} L0 jobs beside the held merge, longest write while it was held {longest:.3f} s; read back {n1} after the takeover, {n2} at the end")
