"""Reviewer tests for compaction step 1 (commit 91414a6): customer-first keys.

Each test states the guarantee it checks. The `srv` fixture runs with `--customer-key cc.m=u` (collection cc.m stores
its documents customer first; the locator `loc/m/{_id}` is its unique `_id` index); `plain` is the control without a key.
Expected on 91414a6: the [m] cases and the format test FAIL (defects of this commit), the [plain] cases PASS, and the
refused-fork test FAILS on 91414a6 and on integration alike (pre-existing, not counted against this commit).
"""
import time

import pytest
from pymongo.errors import PyMongoError

from lib import Server, make_bucket

FLAGS = ["--customer-key", "cc.m=u", "--replica-set", "rs0"]


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("revcomp"), "revcomp", idle=600, extra=FLAGS).start()
    yield s
    s.term()


def _fresh(c, name="m"):
    c.cc.drop_collection(name)
    return c.cc[name]


@pytest.mark.parametrize("coll", ["m", "plain"])  # m: customer keyed; plain: the control, no customer key (passes)
def test_two_transactions_cannot_insert_the_same_id_for_two_customers(srv, coll):
    """Guarantees 4 and 7: `_id` is unique in a collection. Two transactions that insert the same `_id` under two
    customers lock two different document keys; the locator (the unique `_id` index) is checked against the committed
    state only and never locked. mongod lets at most one of them commit (WriteConflict / DuplicateKey)."""
    c = srv.client()
    m = _fresh(c, coll)
    m.insert_one({"_id": "seed", "u": 0})  # the collection exists and is customer keyed
    s1, s2 = c.start_session(), c.start_session()
    s1.start_transaction()
    s2.start_transaction()
    committed = 0
    try:
        m.insert_one({"_id": 7, "u": "alice"}, session=s1)
        try:
            m.insert_one({"_id": 7, "u": "bob"}, session=s2)
        except PyMongoError:
            s2.abort_transaction()
            s2 = None
        for s in (s1, s2):
            if s is None:
                continue
            try:
                s.commit_transaction()
                committed += 1
            except PyMongoError:
                pass
    finally:
        s1.end_session()
    docs = list(m.find({"_id": 7}))
    scan = [d for d in m.find({}) if d["_id"] == 7]
    if coll == "m":
        # compaction 2 step 2 (founder, 2026-11-07): `_id` is unique per customer in a customer-keyed collection, like a
        # sharded collection whose shard key is not `_id`, so both commit; every read agrees on what was committed. The
        # same `_id` twice under ONE customer is refused: test_44_layout_v2.py.
        assert committed == 2 and len(scan) == 2 and len(docs) == 2, f"{committed} commits, scan {scan}, find by _id {docs}"
        return
    assert committed <= 1 and len(scan) <= 1, f"{committed} transactions committed _id 7; a collection scan finds {scan}, find by _id finds {docs}"


@pytest.mark.parametrize("coll", ["m", "plain"])
def test_insert_outside_a_transaction_cannot_duplicate_an_id_a_transaction_holds(srv, coll):
    """Same guarantee, one side outside a transaction: a plain insert of `_id` 8 for customer bob while an open
    transaction has inserted `_id` 8 for alice. In mongod one of the two fails; here both can succeed."""
    c = srv.client()
    m = _fresh(c, coll)
    m.insert_one({"_id": "seed", "u": 0})
    s1 = c.start_session()
    s1.start_transaction()
    ok = 0
    try:
        m.insert_one({"_id": 8, "u": "alice"}, session=s1)
        try:
            m.insert_one({"_id": 8, "u": "bob"})
            ok += 1
        except PyMongoError:
            pass
        try:
            s1.commit_transaction()
            ok += 1
        except PyMongoError:
            pass
    finally:
        s1.end_session()
    scan = [d for d in m.find({}) if d["_id"] == 8]
    if coll == "m":
        # `_id` unique per customer (founder, 2026-11-07): alice's and bob's `_id` 8 are two documents
        assert ok == 2 and len(scan) == 2 and len(list(m.find({"_id": 8}))) == 2, f"{ok} acknowledged, scan {scan}"
        return
    assert ok <= 1 and len(scan) <= 1, f"both writes of _id 8 were acknowledged; a collection scan finds {scan}"


@pytest.mark.parametrize("flags,want", [([], ["lz4"]), (["--sst-compression", "none"], ["none"])])
def test_with_customer_keys_off_the_stored_format_does_not_change(flags, want):
    """Contract of compaction-1's review: "with the option off nothing changes". A server started without any compaction or
    customer-key flag wrote its SSTs lz4-compressed, which the engine of that day could not read: MEASURED on 91414a6, "Data
    error: invalid composite filter block". So the default was kept uncompressed, for a rollback (guarantee 10).
    P-perf-bytes (2026-11-11): before launch no older release reads these databases, so lz4 is the default again (SlateDB's
    copy was 56% of the stored bytes of the local benchmark, uncompressed); `--sst-compression none` still writes the old
    format, for a rollback when one exists. Set INTEGRATION_BIN to a binary built from integration to also run the
    rollback read itself (the `none` case)."""
    import os
    b = make_bucket("revformat")
    s = Server(b, "revformat", idle=600, extra=["--memtable-max-mb", "1"] + flags).start()
    try:
        c = s.client()
        c.app.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(5000)])
        c.app.command("tenantAdmin", close=True)
        info = c.admin.command("fenvixSstInfo", "app")
        codecs = sorted({x["codec"] for x in info["l0"]} | {x["codec"] for r in info["runs"] for x in r["ssts"]})
    finally:
        s.term()
    old = os.environ.get("INTEGRATION_BIN")
    if old and want == ["none"]:
        s2 = Server(b, "revformat-old", idle=600, binary=old).start()
        try:
            assert s2.client().app.t.count_documents({}) == 5000, "the integration binary cannot read what this commit wrote"
        finally:
            s2.term()
    assert codecs == want, f"flags {flags}: the SSTs are written {codecs}, expected {want}"


def test_a_refused_fork_leaves_no_object_of_its_target():
    """Guarantee 6, and the cause of the flaky test_05_erase.py::test_concurrent_erase_and_fork_are_serialised. A fork
    whose source does not exist (in test_05: erased by the erase that won the race) takes the target's lease, then
    returns 404 and deletes the lease; but those requests were charged to the target's usage counters, and the usage
    flusher writes `control/usage/<target>/<day>/...` for a database that never existed, which nothing deletes. test_05
    only sees it when a flush (every 1 s there) lands between the fork's answer and its listing. Same on integration
    (this is not caused by compaction-1; see REVIEW.md)."""
    from lib import list_prefix
    b = make_bucket("revfork")
    s = Server(b, "revfork", idle=300, extra=["--usage-flush-secs", "1"]).start()
    try:
        st, _ = s.admin.post("/v1/databases/never_was/fork", {"target": "nf_k"})
        assert st == 404, st
        time.sleep(3)  # three usage flushes
        left = [k for k, _ in list_prefix(b, "") if "nf_k" in k or "never_was" in k]
        assert left == [], f"a fork refused with 404 left {left}"
    finally:
        s.term()
