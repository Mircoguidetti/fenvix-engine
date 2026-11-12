"""erase-cost-1tb (2026-11-12): the physical erasure rides the compactor's ordinary reseals and merges ("one reseal serves
many"); a deadline scheduler forces only what they did not do in time.

- Ordinary path: with small compactor knobs and a write load, the groups of the erased customer are resealed by theta and
  the delta runs merged; the erasure becomes "scrubbed" with `cleaned_by` "ordinary", nothing forced (serverStatus
  `forcedReseals` and `cleanses` 0), and ends with no object of ours holding the customer (the proof tool).
- Forced path, ordinary passes stopped (default knobs: a tiny database never reaches theta): the erasure is forced at
  three quarters of a short deadline and is clean in the live runs before the deadline (`overdue` false), with forced
  reseals counted; the proof at 0 objects after GC.
- kill -9 at every new step of the pass (after the prepare, after a forced reseal, after the groups-clear record is
  saved, after a cleanse, before the erasure is marked clean): after the restart no acknowledged document is missing, no
  erased document is readable, and the erasure finishes.
- Forks: a fork taken while the erasure is pending, and one taken after its physical removal, never read the customer,
  before and after a restart.
- An erasure racing writes of the same customer: every document acknowledged after the delete returned stays, before and
  after the physical removal; no document the delete removed comes back.
"""
import os
import threading
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until

BASE = ["--sealed-runs", "2", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
        "--sealed-erasure-poll-ms", "200", "--sealed-hold-life-ms", "1500", "--sealed-put-timeout-ms", "500", "--sealed-gc-grace-ms", "3100",
        "--sealed-remote-lease-ms", "3000", "--sealed-version-retention-secs", "0", "--sealed-erasure-check-ms", "500"]
PROOF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target", "release", "fx_erase_proof")


def proof(srv, db, ids):
    import json
    import subprocess
    import tempfile
    from lib import MINIO
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("\n".join(ids) + "\n")
    env = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1")
    r = subprocess.run([PROOF, MINIO, srv.bucket_name, db, f.name], capture_output=True, text=True, env=env, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]
    return json.loads(r.stdout)


def erasures(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/erasures")
    assert st == 200, r
    return r["erasures"]


def compactor(c):
    return c.admin.command("serverStatus")["fenvix"]["compactor"]


def fill(c, db, users=("u1", "u2", "u3", "u4", "u5"), rounds=3, n=20, pad=0):
    if "msgs" not in c[db].list_collection_names():
        c[db].create_collection("msgs", customerKey="userId")
        c[db]["msgs"].create_index([("email", 1)])  # outside the customer's range: its entries are targets of their own
    for r in range(rounds):
        c[db]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 100 + i, "email": f"{u}-{r}-{i}@m", "body": "ERASEDMARK" if u == "u1" else "keep", "pad": "p" * pad}
                                   for u in users for i in range(n)])


def unreadable(c, db, u, ids=(), allow=()):
    assert {d["_id"] for d in c[db]["msgs"].find({"userId": u}, {"_id": 1})} <= set(allow), f"erased documents of {u} readable"
    assert c[db]["msgs"].find_one({"_id": f"{u}-1-3"}) is None
    assert c[db]["msgs"].find_one({"email": f"{u}-2-4@m"}) is None
    assert all(d["userId"] != u or d["_id"] in allow for d in c[db]["msgs"].find({}))
    for i in ids:
        assert c[db]["msgs"].find_one({"_id": i}) is None, f"erased document {i} readable"


def others_intact(c, db, users=("u2", "u3", "u4", "u5"), rounds=3, n=20):
    got = {d["_id"] for d in c[db]["msgs"].find({"userId": {"$in": list(users)}}, {"_id": 1})}
    exp = {f"{u}-{r}-{i}" for u in users for r in range(rounds) for i in range(n)}
    assert exp <= got, f"{len(exp - got)} acknowledged documents missing, e.g. {sorted(exp - got)[:5]}"


def wait_done(srv, c, db, timeout=240):
    def done():
        c[db]["other"].insert_one({"t": time.time()})  # keeps the compactor (and its GC) running
        st, r = srv.admin.post(f"/v1/databases/{db}/erasures/run", {})
        assert st == 200, r
        return all(e["done_ms"] is not None for e in r["erasures"]) and r
    return wait_until(done, timeout=timeout, every=2, what="every erasure done (GC removed what the compactor replaced)")


def start(tag, extra=(), env=None):
    bucket = make_bucket(tag)
    s = Server(bucket, tag, idle=300, extra=BASE + list(extra), env=env).start()
    s.bucket_name = bucket
    return s


def test_ordinary_reseals_and_merges_serve_the_erasure_with_nothing_forced():
    s = start("ecost1", env={"FENVIX_SEALED_COMPACTOR": "small"})
    try:
        c = s.client()
        db = "ec1"
        fill(c, db)
        f0, c0 = compactor(c)["forcedReseals"], compactor(c)["cleanses"]
        assert c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 60
        unreadable(c, db, "u1")
        es = erasures(s, db)
        assert [e["state"] for e in es] == ["pending"], es
        # the customer writes again right after its erasure: new data, kept
        c[db]["msgs"].insert_one({"_id": "u1-new", "userId": "u1", "t": 1, "email": "new@m", "body": "after"})
        # ordinary writes of the other customers (and of u1 again, new data): theta reseals and tier merges follow
        k = 0
        def ridden():
            nonlocal k
            k += 1
            c[db]["msgs"].insert_many([{"_id": f"w{k}-{i}", "userId": f"u{2 + i % 4}", "t": k, "email": f"w{k}-{i}@m", "body": "x" * 400} for i in range(200)])
            unreadable(c, db, "u1", ["u1-0-0", "u1-2-19"], allow=("u1-new",))
            x = erasures(s, db)
            return x[0]["state"] == "scrubbed" and x
        es = wait_until(ridden, timeout=180, every=0.5, what="the erasure scrubbed by ordinary reseals and merges")
        assert es[0]["cleaned_by"] == "ordinary", es
        st = compactor(c)
        assert st["forcedReseals"] == f0 and st["cleanses"] == c0, f"nothing should have been forced: {st}"
        assert c[db]["msgs"].find_one({"_id": "u1-new"})["body"] == "after"
        assert c[db]["msgs"].count_documents({"userId": "u1"}) == 1
        others_intact(c, db)
        wait_done(s, c, db)
        p = proof(s, db, [f"u1-{r}-" for r in range(3)])
        assert p["our_objects_holding"] == 0, p
    finally:
        s.term()


def test_the_deadline_is_met_with_ordinary_passes_stopped():
    """Default knobs: a tiny database never reaches theta, so no ordinary reseal ever runs. Deadline 40 s: the force time
    is at 30 s (three quarters), the batch window 5 s (an eighth): forced from about 25 s."""
    s = start("ecost2", extra=["--sealed-erasure-deadline-ms", "40000"])
    try:
        c = s.client()
        db = "ec2"
        fill(c, db)
        f0 = compactor(c)["forcedReseals"]
        t0 = time.time()
        assert c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 60
        while time.time() - t0 < 15:
            unreadable(c, db, "u1")
            time.sleep(1)
        es = erasures(s, db)
        assert es[0]["state"] == "pending" and compactor(c)["forcedReseals"] == f0, "nothing is forced before the force time"
        def forced():
            unreadable(c, db, "u1")
            x = erasures(s, db)
            return x[0]["state"] == "scrubbed" and x[0]["live_done_ms"] is not None and x
        es = wait_until(forced, timeout=60, every=0.5, what="the forced erasure")
        e = es[0]
        assert e["cleaned_by"] == "forced", e
        assert e["live_done_ms"] <= e["deadline_ms"], f"live runs clean {e['live_done_ms'] - e['deadline_ms']} ms after the deadline"
        assert not e["overdue"], e
        assert compactor(c)["forcedReseals"] > f0
        others_intact(c, db)
        unreadable(c, db, "u1")
        wait_done(s, c, db)
        p = proof(s, db, [f"u1-{r}-" for r in range(3)])
        assert p["our_objects_holding"] == 0, p
    finally:
        s.term()


STEPS = ["erase.after_prepare", "erase.after_forced_reseal", "erase.after_clear_saved", "erase.after_cleanse", "erase.before_mark_clean"]


@pytest.mark.parametrize("step", STEPS)
def test_kill9_at_every_step(step):
    """The pass forced at once (`--sealed-erasure-batch-ms 0`) stops at `step`; kill -9; a restart finishes it."""
    tag = "ecost3" + step.split(".")[1][:6].replace("_", "")
    bucket = make_bucket(tag)
    extra = BASE + ["--sealed-erasure-batch-ms", "0"]
    # a cleanse needs a delta run that outlives the forced reseals (a group they do not touch keeps it listed): small
    # compactor knobs (groups of 4 MiB) and larger documents give several groups
    many = step == "erase.after_cleanse"
    env = {"CTL_FAILPOINT": step}
    if many:
        env["FENVIX_SEALED_COMPACTOR"] = "small"
    s = Server(bucket, tag, idle=300, extra=extra, env=env).start()
    s.bucket_name = bucket
    try:
        c = s.client()
        db = "ec3"
        fill(c, db, pad=40000 if many else 0)
        if many:
            c.admin.command({"fenvixReseal": db})  # every group resealed now: the 4 MiB split gives several groups
        # u1 also written after the seal (live rows in a delta run, so the path has a delta run to cleanse)
        c[db]["msgs"].insert_many([{"_id": f"u1-late-{i}", "userId": "u1", "t": 900 + i, "email": f"u1-late-{i}@m", "body": "ERASEDMARK"} for i in range(30)])
        assert c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 90
        unreadable(c, db, "u1", ["u1-late-3"])
        wait_until(lambda: f"FAILPOINT {step} reached" in s.log_text(), 60, what=f"the pass at {step}")
        unreadable(c, db, "u1", ["u1-late-3"])
        s.kill9()
        s.env = {"FENVIX_SEALED_COMPACTOR": "small"} if many else {}
        s.start()
        c = s.client()
        others_intact(c, db)
        unreadable(c, db, "u1", ["u1-late-3"])
        es = wait_until(lambda: (x := erasures(s, db)) and x[0]["state"] == "scrubbed" and x, timeout=90, what="the erasure after the restart")
        unreadable(c, db, "u1", ["u1-late-3"])
        others_intact(c, db)
        wait_done(s, c, db)
        p = proof(s, db, [f"u1-{r}-" for r in range(3)] + ["u1-late-"])
        assert p["our_objects_holding"] == 0, p
    finally:
        s.term()


def fork(srv, src, dst, mode):
    st, r = srv.admin.post(f"/v1/databases/{src}/fork", {"target": dst, "mode": mode})
    assert st in (200, 201), r
    return r


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_forks_never_read_the_erased_customer(mode):
    s = start("ecost4" + mode, extra=["--sealed-erasure-batch-ms", "3600000"])
    try:
        c = s.client()
        db = "ec4"
        fill(c, db)
        assert c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 60
        fork(s, db, "ec4f1", mode)  # while the erasure is pending
        unreadable(c, "ec4f1", "u1")
        others_intact(c, "ec4f1")
        st, r = s.admin.post(f"/v1/databases/{db}/erasures/run", {})  # forced now
        assert st == 200 and r["erasures"][0]["state"] == "scrubbed", r
        unreadable(c, db, "u1")
        unreadable(c, "ec4f1", "u1")
        fork(s, db, "ec4f2", mode)  # after the physical removal from the live runs
        unreadable(c, "ec4f2", "u1")
        others_intact(c, "ec4f2")
        s.restart()
        c = s.client()
        for d in (db, "ec4f1", "ec4f2"):
            unreadable(c, d, "u1")
            others_intact(c, d)
    finally:
        s.term()


def test_an_erasure_racing_writes_of_the_same_customer():
    s = start("ecost5", extra=["--sealed-erasure-batch-ms", "3600000"])
    try:
        c = s.client()
        db = "ec5"
        fill(c, db)
        acked_after = []
        stop = threading.Event()
        returned = threading.Event()
        def writer():
            cw = s.client()
            k = 0
            while not stop.is_set():
                k += 1
                cw[db]["msgs"].insert_one({"_id": f"u1-race-{k}", "userId": "u1", "t": 5000 + k, "email": f"u1-race-{k}@m", "body": "race"})
                if returned.is_set():
                    acked_after.append(f"u1-race-{k}")
        th = threading.Thread(target=writer)
        th.start()
        time.sleep(0.3)
        c[db]["msgs"].delete_many({"userId": "u1"})
        returned.set()
        time.sleep(0.5)
        stop.set()
        th.join()
        assert acked_after, "the writer acknowledged nothing after the delete returned"
        # the old documents are gone; the race documents present now are the delete's survivors (written after it)
        survivors = {d["_id"] for d in c[db]["msgs"].find({"userId": "u1"}, {"_id": 1})}
        assert set(acked_after) <= survivors, f"acknowledged after the delete and missing: {sorted(set(acked_after) - survivors)[:5]}"
        assert not any(not x.startswith("u1-race-") for x in survivors), "an old document of u1 survived the delete"
        st, r = s.admin.post(f"/v1/databases/{db}/erasures/run", {})
        assert st == 200 and r["erasures"][0]["state"] == "scrubbed", r
        after = {d["_id"] for d in c[db]["msgs"].find({"userId": "u1"}, {"_id": 1})}
        assert after == survivors, f"lost {sorted(survivors - after)[:5]}, came back {sorted(after - survivors)[:5]}"
        s.restart()
        c = s.client()
        after = {d["_id"] for d in c[db]["msgs"].find({"userId": "u1"}, {"_id": 1})}
        assert after == survivors
        others_intact(c, db)
    finally:
        s.term()
