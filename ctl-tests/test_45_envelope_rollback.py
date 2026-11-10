"""Compaction 2, step 2: the two-release rule for envelopes (plan round 2, fix 4, rule I6; envelope.rs).

A database is written by release E1 (`--envelopes write` with E1's wait at 0, rule 26: from its first write on, every value is an envelope and every
delete a one-byte envelope), on a customer-keyed collection and a plain one, with inserts, updates, moves and deletes,
and flushed and compacted. Then:

1. the reading release E0 (`E0_BIN`, a build of this branch's E0 commit) reads it: every answer equals the model; it
   writes more (inserts, deletes) and E1 reads that back too (rollback inside the window, then forward again);
2. the release before E0 (`OLD_BIN`, a build of `compaction` a7890b3) is shown NOT to read it: on the plain collection
   (so layout v2 is not the reason) its answers differ from the model, or it fails. If it read it correctly, the
   two-release rule would not be needed and this test would be wrong: so this is asserted.

Without E0_BIN / OLD_BIN the corresponding part is skipped (the judge does not have the binaries).
"""
import os
import random

import pytest

from lib import Server, make_bucket

E0 = os.environ.get("E0_BIN")
OLD = os.environ.get("OLD_BIN")
CK = ["--customer-key", "rb.keyed=u", "--memtable-max-mb", "1"]


def _write(c, model, r, n):
    """Random writes on both collections, mirrored in `model` ({coll: {(u, _id): doc}})."""
    for coll in ("keyed", "plain"):
        col = c.rb[coll]
        m = model.setdefault(coll, {})
        for _ in range(n):
            op = r.random()
            if op < 0.55 or not m:
                i = r.randrange(10**9)
                d = {"_id": i, "u": r.randrange(25), "k": r.randrange(50), "pad": "p" * r.randrange(300)}
                if coll == "plain" and any(key[1] == i for key in m):
                    continue
                if coll == "keyed" and (d["u"], i) in m:
                    continue
                col.insert_one(d)
                m[(d["u"], i)] = d
            elif op < 0.7:
                key = r.choice(list(m))
                k = r.randrange(50)
                col.update_one({"_id": key[1], "u": key[0]}, {"$set": {"k": k}})
                m[key]["k"] = k
            elif op < 0.78:
                key = r.choice(list(m))
                nu = r.randrange(25)
                if (nu, key[1]) in m:
                    continue
                col.update_one({"_id": key[1], "u": key[0]}, {"$set": {"u": nu}})
                d = m.pop(key)
                d["u"] = nu
                m[(nu, key[1])] = d
            else:
                key = r.choice(list(m))
                col.delete_one({"_id": key[1], "u": key[0]})
                del m[key]


def _check(c, model):
    """Every read the model can answer; returns the differences (empty = equal)."""
    bad = []
    for coll in ("keyed", "plain"):
        col = c.rb[coll]
        m = model[coll]
        got = sorted((d["u"], d["_id"], d["k"]) for d in col.find({}))
        want = sorted((d["u"], d["_id"], d["k"]) for d in m.values())
        if got != want:
            bad.append(f"{coll}: scan {len(got)} documents, model {len(want)}")
        for u in range(0, 25, 3):
            g = sorted(d["_id"] for d in col.find({"u": u}))
            w = sorted(i for (uu, i) in m if uu == u)
            if g != w:
                bad.append(f"{coll}: customer {u}")
        for k in range(0, 50, 7):
            g = sorted(d["_id"] for d in col.find({"k": k}))
            w = sorted(d["_id"] for d in m.values() if d["k"] == k)
            if g != w:
                bad.append(f"{coll}: k {k}")
        if col.count_documents({}) != len(m):
            bad.append(f"{coll}: count")
    return bad


def _flush(c):
    c.rb.command("tenantAdmin", close=True)


@pytest.mark.skipif(not E0, reason="E0_BIN (a build of the reading release) not set")
def test_reading_release_reads_what_the_writing_release_wrote():
    bucket = make_bucket("envrb")
    r = random.Random(3)
    model = {}
    e1 = Server(bucket, "envrb-e1", idle=600, extra=CK + ["--envelopes", "write", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]).start()
    try:
        c = e1.client()
        c.rb.create_collection("keyed", customerKey="u")
        for col in ("keyed", "plain"):
            c.rb[col].create_index([("u", 1), ("k", 1)])
            c.rb[col].create_index([("k", 1)])
        for _ in range(4):
            _write(c, model, r, 700)
            _flush(c)
        assert _check(c, model) == [], "E1 reads its own writes"
        st = c.admin.command("serverStatus")["fenvix"]["envelopes"]
        assert st["start"] and st["liveRead"] > 0 and st["deadRead"] > 0, st
    finally:
        e1.term()
    e0 = Server(bucket, "envrb-e0", idle=600, extra=CK, binary=E0).start()
    try:
        c = e0.client()
        assert _check(c, model) == [], "the reading release E0 reads what E1 wrote"
        _write(c, model, r, 500)  # E0 writes too (envelopes above E_env, envelope.rs)
        _flush(c)
        assert _check(c, model) == []
    finally:
        e0.term()
    e1 = Server(bucket, "envrb-e1b", idle=600, extra=CK + ["--envelopes", "write", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]).start()
    try:
        assert _check(e1.client(), model) == [], "E1 again (forward after the rollback) reads what E0 wrote"
    finally:
        e1.term()
    if OLD:
        old = Server(bucket, "envrb-old", idle=600, extra=["--memtable-max-mb", "1"], binary=OLD).start()
        try:
            c = old.client()
            try:
                plain = sorted((d["u"], d["_id"], d["k"]) for d in c.rb.plain.find({}))
                want = sorted((d["u"], d["_id"], d["k"]) for d in model["plain"].values())
                wrong = plain != want
                detail = f"{len(plain)} documents read, model {len(want)}"
            except Exception as e:  # an error is also "cannot read"
                wrong, detail = True, f"error: {type(e).__name__}: {str(e)[:200]}"
            print("release before E0 on an E1 database:", detail)
            assert wrong, "the release before E0 read an E1 database correctly: the two-release rule would not be needed"
        finally:
            old.term()


def test_without_the_option_nothing_is_enveloped():
    """Release E0's promise: with `--envelopes` off it never starts envelopes, so the release before it still reads its
    databases (plain collections)."""
    bucket = make_bucket("envoff")
    s = Server(bucket, "envoff", idle=600, extra=["--memtable-max-mb", "1"]).start()
    try:
        c = s.client()
        c.rb.plain.insert_many([{"_id": i, "u": i % 5} for i in range(500)])
        c.rb.plain.delete_many({"u": 0})
        _flush(c)
        st = c.admin.command("serverStatus")["fenvix"]["envelopes"]
        assert not st["start"] and st["liveRead"] == 0 and st["deadRead"] == 0, st
        assert c.rb.plain.count_documents({}) == 400
    finally:
        s.term()
    if OLD:
        old = Server(bucket, "envoff-old", idle=600, binary=OLD).start()
        try:
            assert old.client().rb.plain.count_documents({}) == 400
        finally:
            old.term()


E0_BEFORE_FIX = os.environ.get("E0_BEFORE_FIX_BIN")


def _collated_check(c):
    m = c.rb.ci
    got = sorted((d["u"], d["_id"]) for d in m.find({"_id": "doc-1"}))
    assert got == [("alice", "doc-1"), ("bob", "DOC-1"), ("carol", "Doc-1")], got
    assert m.count_documents({}) == 3 + 40


@pytest.mark.skipif(not E0, reason="E0_BIN (a build of the reading release) not set")
def test_reading_release_reads_the_per_customer_collation_id_index():
    """Fix round (review FIX 3): the collation `_id` index of a customer-keyed collection is per customer
    (`custkey::idc_key_of`), a new key format. E1 writes it (inserts, a move, deletes, with envelopes); E0 must find
    every customer holding a collation-equal `_id`, refuse one under a customer that holds it and accept one under
    another; then E1 reads E0's writes. With `E0_BEFORE_FIX_BIN` (the E0 build before the fix round), that build is shown
    not to read it, so E0 had to take the fix."""
    bucket = make_bucket("envci")
    coll_opts = dict(customerKey="u", collation={"locale": "en", "strength": 2})
    e1 = Server(bucket, "envci-e1", idle=600, extra=["--memtable-max-mb", "1", "--envelopes", "write", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]).start()
    try:
        c = e1.client()
        c.rb.create_collection("ci", **coll_opts)
        m = c.rb.ci
        m.insert_one({"_id": "doc-1", "u": "alice"})
        m.insert_one({"_id": "DOC-1", "u": "bob"})
        m.insert_one({"_id": "doc-1", "u": "dave"})
        m.update_one({"_id": "doc-1", "u": "dave"}, {"$set": {"u": "carol"}})
        m.update_one({"_id": "doc-1", "u": "carol"}, {"$set": {"_x": 1}})
        m.replace_one({"_id": "doc-1", "u": "carol"}, {"u": "carol"})
        m.delete_one({"_id": "doc-1", "u": "carol"})
        m.insert_one({"_id": "Doc-1", "u": "carol"})
        m.insert_many([{"_id": f"s{i}", "u": f"c{i % 4}"} for i in range(40)])
        _flush(c)
        _collated_check(c)
    finally:
        e1.term()
    if E0_BEFORE_FIX:
        old = Server(bucket, "envci-e0old", idle=600, extra=["--memtable-max-mb", "1"], binary=E0_BEFORE_FIX).start()
        try:
            try:
                _collated_check(old.client())
                wrong, detail = False, "read correctly"
            except Exception as e:
                wrong, detail = True, f"{type(e).__name__}: {str(e)[:200]}"
            print("E0 before the fix round on a per-customer collation index:", detail)
            assert wrong, "the E0 build before the fix round read the new collation index: E0 would not need the fix"
        finally:
            old.term()
    e0 = Server(bucket, "envci-e0", idle=600, extra=["--memtable-max-mb", "1"], binary=E0).start()
    try:
        c = e0.client()
        _collated_check(c)
        from pymongo.errors import DuplicateKeyError
        with pytest.raises(DuplicateKeyError):
            c.rb.ci.insert_one({"_id": "dOc-1", "u": "bob"})
        c.rb.ci.insert_one({"_id": "dOc-1", "u": "erin"})
        c.rb.ci.delete_one({"_id": "s0", "u": "c0"})
        _flush(c)
    finally:
        e0.term()
    e1 = Server(bucket, "envci-e1b", idle=600, extra=["--memtable-max-mb", "1", "--envelopes", "write", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait"]).start()
    try:
        c = e1.client()
        got = sorted(d["u"] for d in c.rb.ci.find({"_id": "DOC-1"}))
        assert got == ["alice", "bob", "carol", "erin"], got
        assert c.rb.ci.count_documents({}) == 3 + 40
        assert c.rb.command("validate", "ci")["valid"]
    finally:
        e1.term()
