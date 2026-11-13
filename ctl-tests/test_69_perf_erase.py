"""perf-erase (2026-11-11): a whole-customer delete returns at once and is unreadable at once; the physical erasure runs
later, in batches (one scrub serves many erasures), and still ends with no object of ours holding the customer.

- With the product's batch window (1 hour), the erasure stays "pending" after the delete: no pass ran for it, and the
  customer is not readable by any path (filter, `_id`, a secondary index, a count). `POST .../erasures/run` (operators)
  runs the batch now: every pending erasure is scrubbed in one batch.
- With `--sealed-erasure-batch-max 3`, the third erasure makes the batch due at once: the three are scrubbed together.
- The erasure records never show the customer's keys (the targets travel in the record until the scrub).
"""
import os
import time

import pytest

from lib import Server, make_bucket, list_prefix, wait_until

BASE = ["--sealed-runs", "2", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
        "--sealed-erasure-poll-ms", "200", "--sealed-hold-life-ms", "1500", "--sealed-put-timeout-ms", "500", "--sealed-gc-grace-ms", "3100",  # integration-4: check_config needs grace > T_pin + hold + put (3000)
        "--sealed-remote-lease-ms", "3000",
        # restore points (old versions and their runs) kept 5 s instead of 7 days, so "done" is reached within the test
        "--sealed-version-retention-secs", "5"]
# erase-cost-1tb: with the ride (default), small compactor knobs (`FENVIX_SEALED_COMPACTOR=small`, the CI's sealed mode)
# reseal these tiny databases within seconds and the erasures are cleaned by ordinary reseals before the run these tests
# check. Their premise is "nothing physical before the batch": the default knobs (a tiny database never reaches theta).
KNOBS = {"FENVIX_SEALED_COMPACTOR": "1"}
PROOF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target", "release", "fx_erase_proof")


def proof(srv, db, ids):
    """Every object of the database, runs decoded (`src/bin/fx_erase_proof.rs`): how many of ours hold an erased id."""
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


def fill(c, db, users):
    c[db].create_collection("msgs", customerKey="userId")
    c[db]["msgs"].create_index([("email", 1)])  # outside the customer's range
    for r in range(3):
        c[db]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 100 + i, "email": f"{u}-{r}-{i}@m", "body": "ERASEDMARK" if u in users else "keep"}
                                   for u in ("u1", "u2", "u3", "u4", "u5") for i in range(20)])


def unreadable(c, db, u):
    assert c[db]["msgs"].count_documents({"userId": u}) == 0
    assert c[db]["msgs"].find_one({"_id": f"{u}-1-3"}) is None
    assert c[db]["msgs"].find_one({"email": f"{u}-2-4@m"}) is None
    assert all(d["userId"] != u for d in c[db]["msgs"].find({}))


@pytest.fixture()
def srv():
    bucket = make_bucket("perfer")
    s = Server(bucket, "perfer", idle=300, extra=BASE, env=dict(KNOBS)).start()
    s.bucket_name = bucket
    yield s
    s.term()


def test_the_delete_returns_before_the_physical_erasure_and_a_batch_serves_them_all(srv):
    c = srv.client()
    db = "pe1"
    fill(c, db, ("u1", "u2"))
    for u in ("u1", "u2"):
        assert c[db]["msgs"].delete_many({"userId": u}).deleted_count == 60
        unreadable(c, db, u)
    time.sleep(3)  # 15 polls of the agent: with a 1-hour window none runs the pass
    es = erasures(srv, db)
    assert [e["state"] for e in es] == ["pending", "pending"], es
    assert all("targets" not in e for e in es), "an erasure record must never show the customer's keys"
    st, r = srv.admin.post(f"/v1/databases/{db}/erasures/run", {})
    assert st == 200, r
    es = r["erasures"]
    assert [e["state"] for e in es] == ["scrubbed", "scrubbed"], es
    assert len({e["batch"] for e in es}) == 1 and es[0]["batch_erasures"] == 2, f"one batch for both: {es}"
    assert all("targets" not in e for e in es)
    for u in ("u1", "u2"):
        unreadable(c, db, u)
    assert c[db]["msgs"].count_documents({}) == 180
    # after the GC grace the replaced runs go and the erasures are done
    def done():
        st, r = srv.admin.post(f"/v1/databases/{db}/erasures/run", {})
        return all(e["done_ms"] is not None for e in r["erasures"]) and r
    # before the restore window ends the erasures are not done: older versions may still list the customer's rows
    st, r0 = srv.admin.post(f"/v1/databases/{db}/erasures/run", {})
    assert all(e["done_ms"] is None for e in r0["erasures"]), r0
    # GC deletes the old versions and their runs once the window (5 s here) and the grace end; a write starts nothing
    # new, it only keeps the compactor running
    def done_after_writes():
        c[db]["other"].insert_one({"t": time.time()})
        return done()
    r = wait_until(done_after_writes, timeout=240, every=3, what="the retire step after the grace and the restore window (GC runs every 60 s)")
    assert all(not e["overdue"] for e in r["erasures"])
    assert list_prefix(srv.bucket_name, f"control/erasures/{db}/") == []
    for u in ("u1", "u2"):
        unreadable(c, db, u)
    # the proof: none of our objects (runs decoded, manifests, records) holds an erased customer's id
    p = proof(srv, db, [f"{u}-{r}-" for u in ("u1", "u2") for r in range(3)])
    assert p["our_objects_holding"] == 0, p


def test_a_full_batch_runs_without_waiting_for_the_window():
    bucket = make_bucket("perfer2")
    s = Server(bucket, "perfer2", idle=300, extra=BASE + ["--sealed-erasure-batch-max", "3"], env=dict(KNOBS)).start()
    s.bucket_name = bucket
    try:
        c = s.client()
        db = "pe2"
        fill(c, db, ("u1", "u2", "u3"))
        for u in ("u1", "u2"):
            c[db]["msgs"].delete_many({"userId": u})
        time.sleep(2)
        assert [e["state"] for e in erasures(s, db)] == ["pending", "pending"]
        c[db]["msgs"].delete_many({"userId": "u3"})
        es = wait_until(lambda: (x := erasures(s, db)) and all(e["state"] == "scrubbed" for e in x) and x, timeout=30, what="the full batch")
        assert len({e["batch"] for e in es}) == 1 and es[0]["batch_erasures"] == 3, es
        for u in ("u1", "u2", "u3"):
            unreadable(c, db, u)
        assert c[db]["msgs"].count_documents({}) == 120
    finally:
        s.term()


def test_an_erasure_recorded_while_a_pass_runs_is_not_forgotten():
    """The pass pauses after it listed the records; a second erasure is recorded meanwhile. The agent must still run
    it, without waiting for an unrelated pass (here the retire pass, one GC grace of 60 s later). Control:
    FENVIX_CONTROL_FORGET_NOTED=1 (the agent's count reset to the pass's listing) leaves it pending for the grace."""
    control = os.environ.get("FENVIX_CONTROL_FORGET_NOTED") == "1"
    bucket = make_bucket("perfer3")
    env = {"CTL_FAILPOINT": "erase.after_prepare", "CTL_FAILPOINT_PAUSE_MS": "4000", **KNOBS}
    if control:
        env["FENVIX_CONTROL_FORGET_NOTED"] = "1"
    extra = [x for x in BASE]
    k = extra.index("--sealed-gc-grace-ms")
    extra[k + 1] = "60000"
    s = Server(bucket, "perfer3", idle=300, extra=extra + ["--sealed-erasure-batch-ms", "0"], env=env).start()
    try:
        c = s.client()
        db = "pe3"
        fill(c, db, ("u1", "u2"))
        c[db]["msgs"].delete_many({"userId": "u1"})
        wait_until(lambda: "FAILPOINT erase.after_prepare reached" in s.log_text(), 20, what="the pass of u1's erasure")
        c[db]["msgs"].delete_many({"userId": "u2"})
        unreadable(c, db, "u2")
        try:
            wait_until(lambda: (x := erasures(s, db)) and len(x) == 2 and all(e["state"] == "scrubbed" for e in x) and x, timeout=40, what="both erasures scrubbed")
            ok = True
        except AssertionError:
            ok = False
        if control:
            assert not ok, "control: the erasure recorded during the pass should have stayed pending until the retire pass"
        else:
            assert ok, "the erasure recorded while a pass ran stayed pending"
    finally:
        s.term()
