"""Compaction 2, step 7b: forks read through a pair, and whole-customer deletes are erasures.

Forks (rule 20; review of step 7a, note 6): after a drop (the test hook `POST /v1/test/databases/{db}/drop`, a stand-in
for step 4's drop job), the source's SlateDB alone lacks rows that our runs hold. A fork must still have every row: it is
cloned from a checkpoint paired with a version, and its first version of our manifest is that version.

Erasure (DECISIONS 2026-11-07): `deleteMany({customerField: v})` on a customer-keyed collection is an erasure, recorded
with a 24-hour deadline (here the server's own short poll) and processed by the erasure agent; other deletes are ordinary.
Every object of the database is scanned for the erased customer's marker in `fx/tests_7b.rs` (runs are lz4 and decoded
there); here the server's answers and the erasure records are checked end to end.

Control: `FENVIX_CONTROL_RAW_PATHS=1` makes forks clone the latest state with no pair (the code before step 7b); the fork
tests must fail with it.
"""
import os
import time

import pytest

from lib import Server, make_bucket, dump_db, list_prefix, wait_until

SEALED = ["--sealed-runs", "2", "--sealed-reseal-every", "1000000", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
          # perf-erase: these tests check what the scrub does, so a batch is due at the agent's next poll
          "--sealed-erasure-batch-ms", "0",
          "--sealed-erasure-poll-ms", "200", "--sealed-gc-grace-ms", "150000"]
CONTROL = os.environ.get("FENVIX_CONTROL_RAW_PATHS") == "1"


@pytest.fixture()
def srv():
    bucket = make_bucket("c2s7b")
    env = {"FENVIX_TEST_DROP": "1"}
    if CONTROL:
        env["FENVIX_CONTROL_RAW_PATHS"] = "1"
    s = Server(bucket, "c2s7b", idle=300, extra=SEALED, env=env).start()
    s.bucket_name = bucket
    yield s
    s.term()


def drop(srv, db):
    st, r = srv.admin.post(f"/v1/test/databases/{db}/drop", {})
    assert st == 200, r
    assert r["dropped"] > 0, f"the drop removed nothing: {r}"
    return r


def write_rounds(c, db, base, rounds=6):
    for r in range(rounds):
        c[db]["a"].insert_many([{"_id": base + r * 100 + i, "v": r, "s": "x" * (i % 13)} for i in range(100)])
        c[db]["b"].insert_one({"_id": f"{base}-{r}", "r": r})
        c[db]["a"].update_many({"_id": {"$gte": base + r * 100, "$lt": base + r * 100 + 30}}, {"$set": {"v": -r}})
        c[db]["a"].delete_many({"_id": {"$gte": base + r * 100 + 90, "$lt": base + r * 100 + 100}})


def diff(got, want):
    out = []
    for coll in sorted(set(got) | set(want)):
        g, w = got.get(coll, {}), want.get(coll, {})
        missing = set(w) - set(g)
        extra = set(g) - set(w)
        wrong = [k for k in w if k in g and g[k] != w[k]]
        if missing or extra or wrong:
            out.append(f"{coll}: {len(missing)} missing, {len(extra)} extra, {len(wrong)} wrong of {len(w)}")
    return out


@pytest.mark.parametrize("mode", ["clone", "copy"])
def test_a_fork_after_a_drop_has_every_row(srv, mode):
    c = srv.client()
    src, dst = f"fsrc{mode}", f"fdst{mode}"
    write_rounds(c, src, 0)
    want = dump_db(c, src)
    r = drop(srv, src)
    assert dump_db(c, src) == want, "the source's own reads changed after the drop"
    st, cps0 = srv.admin.get(f"/v1/databases/{src}/checkpoints")
    st, f = srv.admin.post(f"/v1/databases/{src}/fork", {"target": dst, "mode": mode})
    assert st == 201, f
    bad = diff(dump_db(c, dst), want)
    assert not bad, f"fork ({mode}) after a drop at {r['floor']}: {bad}"
    # the fork's own write is read (its seq is above the paired X_d) and stays out of the source
    c[dst]["a"].insert_one({"_id": "fork-only"})
    assert c[dst]["a"].find_one({"_id": "fork-only"}) is not None
    assert c[src]["a"].find_one({"_id": "fork-only"}) is None
    refs = list_prefix(srv.bucket_name, f"tenants/{src}/fx/forks/")
    if mode == "clone":
        assert [k for k, _ in refs] == [f"tenants/{src}/fx/forks/{dst}"], refs
        # the source cannot be erased while the shallow fork reads its objects
        st, e = srv.admin.delete(f"/v1/databases/{src}")
        assert st == 409 and e["error"]["code"] == "has_forks", (st, e)
    else:
        assert refs == [], f"a copy shares nothing: {refs}"
        assert list_prefix(srv.bucket_name, f"tenants/{dst}/fx/run/fork/"), "the copy's runs are its own"
    # the fork goes: its reference goes, the source still answers as before
    st, e = srv.admin.delete(f"/v1/databases/{dst}")
    assert st == 200, e
    assert list_prefix(srv.bucket_name, f"tenants/{src}/fx/forks/") == []
    assert dump_db(c, src) == want
    # no pairing checkpoint is left in the source (the clone's own goes with the fork); restore points of the retention
    # agent ("ret:...") may come meanwhile
    st, cps = srv.admin.get(f"/v1/databases/{src}/checkpoints")
    before = {cp["id"] for cp in cps0["checkpoints"]}
    new = [cp for cp in cps["checkpoints"] if cp["id"] not in before and not (cp["name"] or "").startswith("ret:")]
    assert st == 200 and new == [], new
    assert list_prefix(srv.bucket_name, f"tenants/{src}/fx/pairs/") == []


def erasures(srv, db):
    st, r = srv.admin.get(f"/v1/databases/{db}/erasures")
    assert st == 200, r
    return r["erasures"]


def test_a_whole_customer_delete_is_an_erasure(srv):
    if CONTROL:
        pytest.skip("the control covers forks only")
    c = srv.client()
    db = "er"
    c[db].create_collection("msgs", customerKey="userId")
    c[db]["msgs"].create_index([("userId", 1), ("t", 1)])
    c[db]["msgs"].create_index([("email", 1)])  # not led by the customer: its entries are outside the customer's range
    for r in range(4):
        c[db]["msgs"].insert_many([{"_id": f"{u}-{r}-{i}", "userId": u, "t": r * 100 + i, "email": f"{u}-{i}@ERASEDMARK" if u == "u1" else f"{u}-{i}@x",
                                    "body": "ERASEDMARK" if u == "u1" else "keep"} for u in ("u1", "u2", "u3") for i in range(25)])
    c[db]["msgs"].update_many({"userId": "u1", "t": {"$lt": 10}}, {"$set": {"body": "ERASEDMARK v2"}})
    before = dump_db(c, db)
    # ordinary deletes: one document, and a filter that is not the customer alone
    c[db]["msgs"].delete_one({"_id": "u2-0-0"})
    c[db]["msgs"].delete_many({"userId": "u3", "t": {"$lt": 5}})
    assert erasures(srv, db) == [], "an ordinary delete is not an erasure"
    # the erasure
    n = c[db]["msgs"].delete_many({"userId": "u1"}).deleted_count
    assert n == 100
    es = erasures(srv, db)
    assert len(es) == 1, es
    e = es[0]
    assert e["kind"] == "customer" and e["collection"] == "msgs" and e["ranges"] == 1, e
    assert e["keys"] >= 100, f"the deletes' keys outside the customer's range (locators, the email index) must be targets: {e}"
    assert e["deadline_ms"] - e["requested_ms"] == 24 * 3600 * 1000, e
    # the customer writes again after the erasure: new data, kept
    c[db]["msgs"].insert_one({"_id": "u1-new", "userId": "u1", "t": 999, "email": "new@x", "body": "after"})
    # the record is saved "scrubbed" by the scrub and saved again with `live_done_ms` by the retire step of the same pass
    # (fx/erase.rs `record_scrub`, then `retire`): a read between the two saw it without (c2-fix-clone-uploads)
    e = wait_until(lambda: (x := erasures(srv, db)[0])["state"] == "scrubbed" and x["live_done_ms"] is not None and x, timeout=30, what="the erasure agent's scrub")
    assert not e["overdue"], e
    # c2-fix-clone-uploads: with a compactor the erasure is its job (every group the customer meets is resealed without
    # the rows, every delta run it meets rewritten, compactor.rs "Erasures"), so the agent's scrub finds no row of ours
    # left to rewrite and `rows_erased` stays 0. The erasure is checked by its effects: the documents gone (`check`
    # below, before and after the drop of SlateDB's rows) and, for the whole database, the proof at 0 objects.
    compactor = c.admin.command("serverStatus")["fenvix"]["compactor"]["started"] > 0
    if not compactor:
        assert e["rows_erased"] > 0, e
    assert list_prefix(srv.bucket_name, f"tenants/{db}/fx/erasures/{e['id']}.keys") == [], "the customer's keys must go with the scrub"
    # reads, before and after the drop of SlateDB's rows at or below X_d (>= S_e)
    def check():
        got = {d["_id"] for d in c[db]["msgs"].find({})}
        exp = {k for k in before["msgs"]}
        exp = {eval(k) for k in exp if not eval(k).startswith("u1-")} - {"u2-0-0"} - {f"u3-0-{i}" for i in range(5)} | {"u1-new"}
        assert got == exp, f"{len(exp - got)} missing, {len(got - exp)} extra"
        assert c[db]["msgs"].count_documents({"userId": "u1"}) == 1
        assert c[db]["msgs"].count_documents({"email": {"$regex": "ERASEDMARK"}}) == 0
        assert c[db]["msgs"].find_one({"email": "u1-3@ERASEDMARK"}) is None
    check()
    drop(srv, db)
    check()
    # whole-database erasure: every object goes at once, our runs and records included
    st, r = srv.admin.delete(f"/v1/databases/{db}")
    assert st == 200 and r["proof"]["remaining_objects"] == 0, r
    assert list_prefix(srv.bucket_name, f"tenants/{db}/") == []
    assert list_prefix(srv.bucket_name, f"control/erasures/{db}/") == []
