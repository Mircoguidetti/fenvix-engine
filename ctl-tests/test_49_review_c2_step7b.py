"""Review of compaction 2 step 7b (fenvix-brain sessions/2026-11-09-review-c2-step7b/REPORT.md).

1. FIX 1: an erasure inside a `clone` fork. The fork's first version lists its source's runs by reference. The fork's
   scrub replaces them and its retire step deletes the replaced runs, which live under the source's prefix and are still
   listed by the source's newest version. The source must keep every acknowledged row.
2. FIX 2: a kill after fork_paired() made its pairing checkpoint and pair, before it deletes them (probe failpoint
   `fork.after_pairing`, review build only): nothing may be left in the source.
3. FIX 3: an erased customer stays readable through a change stream resumed from before the erasure.
4. Note: the `_lease` leftover of test_02b_fork_crash::test_random_kill_loop (probe failpoint `fork.after_lease`).

The failpoints `fork.after_pairing` and `fork.after_lease` and the env-gated remedy `REVIEW_FIX1` in fx/erase.rs exist
only on the review branch; they are probes, not for merge.
"""
import time

import pytest

from lib import Server, make_bucket, dump_db, list_prefix, wait_until, s3

# short hold life and GC grace so the retire step runs within the test (check_config: grace > hold + put, remote > hold + put)
SEALED = ["--sealed-runs", "2", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
          # perf-erase: these tests check what the scrub does, so a batch is due at the agent's next poll
          "--sealed-erasure-batch-ms", "0",
          "--sealed-erasure-poll-ms", "200", "--sealed-hold-life-ms", "1500", "--sealed-put-timeout-ms", "500", "--sealed-gc-grace-ms", "2500",
          "--sealed-remote-lease-ms", "3000"]


@pytest.fixture()
def srv():
    bucket = make_bucket("r7b")
    s = Server(bucket, "r7b", idle=300, extra=SEALED, env={"FENVIX_TEST_DROP": "1"}).start()
    s.bucket_name = bucket
    yield s
    s.term()


def erasures(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/erasures")
    assert st == 200, r
    return r["erasures"]


def fill(c, db):
    c[db].create_collection("msgs", customerKey="userId")
    for r in range(6):
        c[db]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 100 + i, "body": ("ERASEDMARK" if u == "u1" else "keep") + "-" * 40}
                                   for u in ("u1", "u2", "u3") for i in range(30)])


def runs(srv, db):
    return {k for k, _ in list_prefix(srv.bucket_name, f"tenants/{db}/fx/run/")}


def test_an_erasure_in_a_clone_fork_keeps_every_run_of_its_source(srv):
    c = srv.client()
    src, dst = "esrc", "efork"
    fill(c, src)
    want = dump_db(c, src)
    before = runs(srv, src)
    assert before, "the source must have runs of ours (sealed runs on)"
    st, f = srv.admin.post(f"/v1/databases/{src}/fork", {"target": dst, "mode": "clone"})
    assert st == 201, f
    # the customer is erased in the fork only
    assert c[dst]["msgs"].delete_many({"userId": "u1"}).deleted_count == 180
    wait_until(lambda: (e := erasures(srv, dst)) and e[0]["state"] == "scrubbed", timeout=30, what="the fork's scrub")
    time.sleep(5)  # past the GC grace: the retire step runs at the next polls
    e = erasures(srv, dst)[0]
    gone = sorted(before - runs(srv, src))
    # the source must still answer every row, after a restart (no cached block)
    srv.restart()
    c = srv.client()
    try:
        got = dump_db(c, src)
        lost = sum(len(set(want[k]) - set(got.get(k, {}))) for k in want)
        err = None
    except Exception as x:
        got, lost, err = None, None, repr(x)[:300]
    assert err is None and got == want and gone == [], (
        f"the fork's erasure deleted {len(gone)} runs of its source {gone}; the source's reads: error {err}, rows lost {lost} of {sum(len(v) for v in want.values())}")


def test_a_kill_between_the_fork_lease_and_its_intent_leaves_nothing():
    """Cause of the `_lease` flake of test_02b_fork_crash::test_random_kill_loop: fork() takes the branch's lease
    (`tenants/{target}/_lease`) before fork_leased() writes the intent record that the recovery at the next start reads.
    A kill in between leaves the lease object of a database that does not exist, and nothing removes it. Needs the review
    probe failpoint `fork.after_lease` (a build with it; without it the failpoint is never reached and the test skips)."""
    from test_02b_fork_crash import assert_fork_absent
    from lib import fill as fill_docs
    import threading
    bucket = make_bucket("r7blease")
    srv = Server(bucket, "r7bl", idle=300, env={"CTL_FAILPOINT": "fork.after_lease"}).start()
    try:
        c = srv.client()
        fill_docs(c, "src", "c", 300)
        c["src"].command("tenantAdmin", close=True)
        ref = dump_db(c, "src")
        th = threading.Thread(target=lambda: srv.admin.post("/v1/databases/src/fork", {"target": "kid"}, timeout=10), daemon=True)
        th.start()
        try:
            wait_until(lambda: "FAILPOINT fork.after_lease reached" in srv.log_text(), 10, what="failpoint")
        except Exception:
            pytest.skip("this build has no fork.after_lease failpoint")
        srv.kill9()
        srv.env = {}
        srv.proc = None
        srv.start()
        assert dump_db(srv.client(), "src") == ref
        assert_fork_absent(srv, bucket, "kid", ancestors=("src",))
    finally:
        srv.term()


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_a_kill_after_the_fork_pairing_leaves_no_checkpoint_or_pair(mode):
    """Sealed runs on: fork_paired() makes a checkpoint of the source and its pair before the clone or copy, and deletes
    both after it. A kill in between must not leave either in the source: a permanent checkpoint pins SlateDB's objects
    forever, and the pair keeps its version's runs (an erasure of the source could then never be done). Needs the review
    probe failpoint `fork.after_pairing`."""
    from test_02b_fork_crash import assert_fork_absent, cp_ids
    from lib import fill as fill_docs
    import threading
    bucket = make_bucket("r7bpair")
    srv = Server(bucket, "r7bp", idle=300, extra=SEALED, env={"CTL_FAILPOINT": "fork.after_pairing"}).start()
    try:
        c = srv.client()
        for r in range(4):
            fill_docs(c, "src", "c", 200, start=r * 200)
        c["src"].command("tenantAdmin", close=True)
        ref = dump_db(c, "src")
        pins_before = cp_ids(srv, "src")
        th = threading.Thread(target=lambda: srv.admin.post("/v1/databases/src/fork", {"target": "kid", "mode": mode}, timeout=10), daemon=True)
        th.start()
        try:
            wait_until(lambda: "FAILPOINT fork.after_pairing reached" in srv.log_text(), 15, what="failpoint")
        except Exception:
            pytest.skip("this build has no fork.after_pairing failpoint")
        srv.kill9()
        srv.env = {"FENVIX_TEST_DROP": "1"}
        srv.proc = None
        srv.start()
        c = srv.client()
        assert dump_db(c, "src") == ref
        assert_fork_absent(srv, bucket, "kid", ancestors=("src",))
        pairs = list_prefix(bucket, "tenants/src/fx/pairs/")
        assert cp_ids(srv, "src") == pins_before and pairs == [], f"left in the source: checkpoints {sorted(set(cp_ids(srv, 'src')) - set(pins_before))}, pairs {pairs}"
    finally:
        srv.term()


def test_an_erased_customer_is_not_readable_through_a_change_stream(srv):
    """The change log (`changes.rs`) keeps inserted documents whole, in memory, bounded by count and size only. After an
    erasure is scrubbed, a change stream resumed from a token taken before it must not return the erased customer's
    documents (the erasure's rows leave within 24 hours; nothing here bounds the log's age)."""
    c = srv.client()
    db = "cs"
    c[db].create_collection("msgs", customerKey="userId")
    with c[db]["msgs"].watch() as st:
        c[db]["msgs"].insert_one({"_id": "first", "userId": "u2", "body": "keep"})
        st.next()
        token = st.resume_token
    c[db]["msgs"].insert_many([{"_id": f"u1-{i}", "userId": "u1", "body": "ERASEDMARK"} for i in range(20)])
    assert c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count == 20
    wait_until(lambda: (e := erasures(srv, db)) and e[0]["state"] == "scrubbed", timeout=30, what="the scrub")
    seen = []
    with c[db]["msgs"].watch(resume_after=token, max_await_time_ms=500) as st:
        for _ in range(60):
            ev = st.try_next()
            if ev is None:
                break
            if "ERASEDMARK" in str(ev.get("fullDocument")):
                seen.append(ev["documentKey"]["_id"])
    assert seen == [], f"{len(seen)} documents of the erased customer still readable through a change stream after the scrub: {seen[:3]}"
