"""Review of compaction 2 step 2 (key layout v2, `_id` unique per customer, envelopes): tests that try to break it.

Each test states what it checks. A test that fails on the delivered commit is a defect (see REVIEW.md).
"""
import threading
import time

import pytest
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError

from lib import Server, make_bucket


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("revc2s2"), "revc2s2", idle=600, extra=["--replica-set", "rs0", "--memtable-max-mb", "1"]).start()
    yield s
    s.term()


def _keyed(c, name, key="u", db="rv"):
    c[db].drop_collection(name)
    c[db].create_collection(name, customerKey=key)
    return c[db][name]


def test_change_stream_update_lookup_returns_the_updated_customers_document(srv):
    """Two customers hold `_id` 1 (allowed by the founder's rule). An update of bob's document, watched with
    `fullDocument: updateLookup`, must carry bob's document, never alice's: the lookup by `_id` alone takes the first
    customer holding the `_id`, so another customer's data reaches the watcher."""
    c = srv.client()
    m = _keyed(c, "cs")
    m.insert_one({"_id": 1, "u": "alice", "secret": "alice-data"})
    m.insert_one({"_id": 1, "u": "bob", "secret": "bob-data"})
    with m.watch(full_document="updateLookup", max_await_time_ms=500) as st:
        m.update_one({"_id": 1, "u": "bob"}, {"$set": {"n": 1}})
        ev = None
        for _ in range(20):
            ev = st.try_next()
            if ev is not None:
                break
        assert ev is not None, "no change event"
        assert ev["operationType"] == "update"
        assert ev["fullDocument"] is not None
        assert ev["fullDocument"]["u"] == "bob", f"updateLookup returned another customer's document: {ev['fullDocument']}"



def test_change_stream_document_key_names_the_customer(srv):
    """With `_id` unique per customer only (founder: like a sharded collection whose shard key is not `_id`), a change
    event's `documentKey` must say which document changed: mongod puts the shard key in it for such a collection. With
    `_id` alone, a consumer that applies the delete of bob's document 1 (a cache, a search index, an agent's memory)
    cannot tell it from alice's document 1."""
    c = srv.client()
    m = _keyed(c, "csk")
    m.insert_one({"_id": 1, "u": "alice"})
    m.insert_one({"_id": 1, "u": "bob"})
    with m.watch(max_await_time_ms=500) as st:
        m.delete_one({"_id": 1, "u": "bob"})
        ev = None
        for _ in range(20):
            ev = st.try_next()
            if ev is not None:
                break
        assert ev is not None and ev["operationType"] == "delete"
        assert ev["documentKey"].get("u") == "bob", f"documentKey {ev['documentKey']} does not say which customer's document 1 was deleted"


def test_same_string_id_under_two_customers_in_a_collated_collection(srv):
    """The founder's rule (the same `_id` under two customers is accepted) holds in a collection with a default
    collation too: the collation `_id` index (`idc/`) is still one per collection, so a string `_id` is refused under a
    second customer and the application's write fails."""
    c = srv.client()
    c.rv.drop_collection("coll_ci")
    c.rv.create_collection("coll_ci", customerKey="u", collation={"locale": "en", "strength": 2})
    m = c.rv.coll_ci
    m.insert_one({"_id": "doc-1", "u": "alice"})
    with pytest.raises(DuplicateKeyError):
        m.insert_one({"_id": "DOC-1", "u": "alice"})  # equal under the collation, same customer: refused
    m.insert_one({"_id": "doc-1", "u": "bob"})  # same _id, another customer: accepted by the rule
    assert sorted(d["u"] for d in m.find({"_id": "doc-1"})) == ["alice", "bob"]

def _norm(docs):
    return [sorted(d.items(), key=lambda kv: kv[0]) for d in docs]


QUERIES = [
    # (filter, sort, limit, skip, hint)
    ({}, [("_id", 1)], 0, 0, None),
    ({}, [("_id", -1)], 7, 0, None),
    ({"_id": {"$gte": 50}}, [("_id", 1)], 5, 0, None),
    ({"_id": {"$lt": 50}}, [("_id", -1)], 5, 3, None),
    ({"_id": {"$gte": 20, "$lt": 120}}, None, 0, 0, None),
    ({"_id": {"$in": [3, 77, 150, 199]}}, [("_id", 1)], 0, 0, None),
    ({"u": "c3"}, [("_id", 1)], 0, 0, None),
    ({"u": "c3"}, [("ts", -1), ("_id", 1)], 3, 0, None),
    ({"u": {"$gte": "c2", "$lt": "c5"}}, [("u", -1), ("_id", -1)], 9, 2, None),
    ({"u": {"$in": ["c1", "c7", None]}}, [("_id", 1)], 0, 0, None),
    ({"u": None}, [("_id", 1)], 0, 0, None),
    ({"u": {"$exists": False}}, [("_id", 1)], 0, 0, None),
    ({"u": {"$type": "array"}}, [("_id", 1)], 0, 0, None),
    ({"u": "c4", "ts": {"$gte": 30}}, [("ts", 1), ("_id", 1)], 0, 0, None),
    ({"u": "c4", "ts": {"$gte": 30}}, [("ts", -1), ("_id", -1)], 2, 0, None),
    ({"x": {"$gt": 5}}, [("x", 1), ("_id", 1)], 6, 0, None),
    ({"x": {"$gt": 5}}, [("x", -1), ("_id", -1)], 6, 1, "x_1"),
    ({"u": {"$gt": "c5"}}, [("u", 1), ("x", 1), ("_id", 1)], 0, 0, "u_1_x_1"),
    ({"u": {"$lte": "c5"}}, [("u", -1), ("x", -1), ("_id", -1)], 4, 0, "u_1_x_1"),
    ({}, [("u", 1), ("x", 1), ("_id", 1)], 0, 0, "u_1_x_1"),
    ({}, [("u", -1), ("x", -1), ("_id", -1)], 11, 0, "u_1_x_1"),
    ({"u": "c2"}, [("_id", -1)], 0, 0, "_id_"),
    ({}, [("_id", 1)], 10, 5, "_id_"),
    ({}, [("_id", -1)], 10, 5, "_id_"),
    ({"ts": {"$gte": 10, "$lt": 60}}, [("ts", 1), ("_id", 1)], 0, 0, None),
]


def _fill_pair(c, name, key):
    import random
    rnd = random.Random(46)
    keyed = _keyed(c, name + "_k", key=key)
    c.rv.drop_collection(name + "_p")
    plain = c.rv[name + "_p"]
    docs = []
    for i in range(200):
        d = {"_id": i, "x": rnd.randint(0, 20)}
        r = rnd.random()
        if r < 0.08:
            pass  # customer missing
        elif r < 0.14:
            d["u"] = [f"c{rnd.randint(0, 8)}", f"c{rnd.randint(0, 8)}"]
        elif r < 0.18:
            d["u"] = None
        elif r < 0.22:
            d["u"] = rnd.randint(0, 3)
        else:
            d["u"] = f"c{rnd.randint(0, 8)}"
        t = rnd.random()
        if t < 0.1:
            pass
        elif t < 0.15:
            d["ts"] = None
        else:
            d["ts"] = rnd.randint(0, 99)
        docs.append(d)
    for coll in (keyed, plain):
        coll.create_index([("u", 1), ("x", 1)])
        coll.create_index([("x", 1)])
        coll.create_index([("u", 1), ("ts", -1)])
        coll.insert_many(docs)
    # moves between customers and in time
    for i in range(0, 200, 7):
        for coll in (keyed, plain):
            coll.update_one({"_id": i}, {"$set": {"u": f"c{i % 9}", "ts": (i * 13) % 100}})
    for i in range(3, 200, 11):
        for coll in (keyed, plain):
            coll.delete_one({"_id": i})
    return keyed, plain


@pytest.mark.parametrize("key", ["u", {"field": "u", "order": "ts"}], ids=["plain-key", "time-key"])
def test_queries_on_a_keyed_collection_equal_a_plain_one(srv, key):
    """Finds with sorts, limits, skips and hints (descending, `_id` ranges, a customer value that is an array, missing,
    null or a number, indexes led and not led by the customer field) give the same answer on a customer-keyed
    collection (with and without the time field) as on the same data in a plain collection."""
    c = srv.client()
    keyed, plain = _fill_pair(c, "q" + ("t" if isinstance(key, dict) else "p"), key)
    bad = []
    for f, s, lim, sk, h in QUERIES:
        def run(coll):
            cur = coll.find(f)
            if s:
                cur = cur.sort(s)
            if lim:
                cur = cur.limit(lim)
            if sk:
                cur = cur.skip(sk)
            if h:
                cur = cur.hint(h)
            out = list(cur)
            return out if s else sorted(out, key=lambda d: d["_id"])
        try:
            a, b = run(keyed), run(plain)
        except PyMongoError as e:
            bad.append((f, s, lim, sk, h, f"error {e}"))
            continue
        if _norm(a) != _norm(b):
            bad.append((f, s, lim, sk, h, [d["_id"] for d in a], [d["_id"] for d in b]))
    for f in [{}, {"u": "c3"}, {"_id": {"$gt": 100}}, {"u": {"$type": "array"}}]:
        if keyed.count_documents(f) != plain.count_documents(f):
            bad.append(("count", f, keyed.count_documents(f), plain.count_documents(f)))
    if sorted(map(str, keyed.distinct("u"))) != sorted(map(str, plain.distinct("u"))):
        bad.append(("distinct u",))
    v = c.rv.command("validate", keyed.name)
    if not v.get("valid"):
        bad.append(("validate", v))
    assert not bad, "\n".join(map(str, bad))


def _snapshot(coll):
    return {
        "all": sorted((str(d["_id"]), str(d.get("u")), d.get("x")) for d in coll.find()),
        "by_u": sorted((str(d["_id"]), d.get("x")) for d in coll.find({"u": "c1"}).hint("u_1_x_1")),
        "by_x": sorted(str(d["_id"]) for d in coll.find({"x": {"$gte": 10}}).hint("x_1")),
        "n": coll.count_documents({}),
    }


def test_collection_operations_keep_customer_keyed_data(srv):
    """Rename within a database and across databases, drop and re-create, index drop and re-create, `$out` and
    `cloneCollectionAsCapped` of a customer-keyed collection: every read after each one equals the plain control."""
    c = srv.client()
    keyed, plain = _fill_pair(c, "ops", "u")
    want = _snapshot(plain)
    assert _snapshot(keyed) == want
    # rename within the database
    c.rv.drop_collection("ops_k2")
    keyed.rename("ops_k2")
    k2 = c.rv["ops_k2"]
    assert _snapshot(k2) == want, "after a rename"
    assert c.rv["ops_k"].count_documents({}) == 0
    # rename across databases
    c.rv2.drop_collection("ops_k3")
    c.admin.command("renameCollection", "rv.ops_k2", to="rv2.ops_k3")
    k3 = c.rv2["ops_k3"]
    assert _snapshot(k3) == want, "after a rename across databases"
    assert c.rv["ops_k2"].count_documents({}) == 0
    # writes keep working after the moves, with `_id` per customer
    k3.insert_one({"_id": 1000, "u": "c1", "x": 1})
    k3.insert_one({"_id": 1000, "u": "c2", "x": 1})
    with pytest.raises(DuplicateKeyError):
        k3.insert_one({"_id": 1000, "u": "c1", "x": 2})
    k3.delete_many({"_id": 1000})
    assert _snapshot(k3) == want
    # index dropped and built again
    k3.drop_index("u_1_x_1")
    k3.create_index([("u", 1), ("x", 1)])
    assert _snapshot(k3) == want, "after an index rebuild"
    # $out into a new collection
    c.rv2.drop_collection("ops_out")
    list(k3.aggregate([{"$match": {}}, {"$out": "ops_out"}]))
    out = c.rv2["ops_out"]
    out.create_index([("u", 1), ("x", 1)])
    out.create_index([("x", 1)])
    assert _snapshot(out) == want, "after $out"
    # drop, then a new collection of the same name holds nothing of the old one
    c.rv2.drop_collection("ops_k3")
    c.rv2.create_collection("ops_k3", customerKey="u")
    k3 = c.rv2["ops_k3"]
    k3.create_index([("u", 1), ("x", 1)])
    assert k3.count_documents({}) == 0
    assert list(k3.find({"u": "c1"}).hint("u_1_x_1")) == []
    assert list(k3.find().sort("_id", -1).limit(3)) == []


def _model_docs(coll):
    return sorted((repr(d["_id"]), repr(d.get("u")), d.get("k")) for d in coll.find({}))


def _write_some(col, r, n, start):
    for i in range(n):
        col.insert_one({"_id": start + i, "u": f"c{r.randrange(6)}", "k": r.randrange(9)})
    ids = [d["_id"] for d in col.find({}, {"_id": 1})]
    for i in r.sample(ids, len(ids) // 4):
        col.update_one({"_id": i}, {"$set": {"u": f"c{r.randrange(6)}", "k": r.randrange(9)}})
    for i in r.sample(ids, len(ids) // 5):
        col.delete_many({"_id": i})


def test_export_and_restore_across_the_envelope_boundary():
    """A database written without envelopes (release E0), restore point P0; then written by release E1 (`--envelopes
    write`: the marker, envelopes and delete envelopes), restore point P1. With E1 running:
    - an export of the customer-keyed collection holds exactly its documents (no deleted one, no stray byte);
    - a restore to P0 (a past with no marker into a live database with one) gives P0's documents, and writes after it
      read back;
    - a restore to P1 gives P1's documents."""
    import random
    import tempfile
    import export_client as xc
    from lib import TOKEN
    import bson as _bson
    bucket = make_bucket("revenv")
    r = random.Random(7)
    ret = ["--retention-interval-secs", "2", "--retention-prune-secs", "3", "--memtable-max-mb", "1"]
    a = Server(bucket, "revenv-e0", idle=600, extra=ret).start()
    try:
        c = a.client()
        c.e.create_collection("k", customerKey="u")
        c.e.k.create_index([("u", 1), ("k", 1)])
        c.e.p.insert_one({"_id": -1})
        st, res = a.admin.put("/v1/databases/e/retention", {"window_secs": 3600, "interval_secs": 2})
        assert st == 200, res
        _write_some(c.e.k, r, 300, 0)
        _write_some(c.e.p, r, 300, 0)
        s0 = (_model_docs(c.e.k), _model_docs(c.e.p))
        time.sleep(0.2)
        t0 = int(time.time() * 1000)
        time.sleep(3.5)
        assert c.admin.command("serverStatus")["fenvix"]["envelopes"]["start"] is False
    finally:
        a.term()
    b = Server(bucket, "revenv-e1", idle=600, extra=ret + ["--envelopes", "write"]).start()
    try:
        c = b.client()
        assert (_model_docs(c.e.k), _model_docs(c.e.p)) == s0
        _write_some(c.e.k, r, 200, 1000)
        _write_some(c.e.p, r, 200, 1000)
        c.e.command("tenantAdmin", close=True)
        s1 = (_model_docs(c.e.k), _model_docs(c.e.p))
        env = c.admin.command("serverStatus")["fenvix"]["envelopes"]
        assert env["deadRead"] + env["liveRead"] > 0, env
        time.sleep(0.2)
        t1 = int(time.time() * 1000)
        time.sleep(3.5)
        # export: exactly the documents
        st, res = b.admin.post("/v1/databases/e/exports")
        assert st == 201, res
        dest = tempfile.mkdtemp(prefix="revdump-")
        ver = xc.download(f"http://127.0.0.1:{b.admin_port}", TOKEN, res["id"], dest)
        assert all(v == "ok" for v in ver.values()), ver
        for coll, want in (("k", s1[0]), ("p", s1[1])):
            got = sorted((repr(d["_id"]), repr(d.get("u")), d.get("k")) for d in _bson.decode_all(open(f"{dest}/e/{coll}.bson", "rb").read()))
            assert got == want, f"export of {coll}: {len(got)} documents, want {len(want)}"
        # restore to P0, across the boundary
        st, res = b.admin.post("/v1/databases/e/restore", {"to": t0})
        assert st == 200, res
        assert (_model_docs(c.e.k), _model_docs(c.e.p)) == s0, "restore to the point before the envelopes"
        assert c.e.k.find_one({"u": "c1"}, hint="u_1_k_1") is not None or not any(x[1] == "'c1'" for x in s0[0])
        c.e.k.insert_one({"_id": 5000, "u": "c1", "k": 1})
        c.e.k.delete_many({"_id": 0})
        c.e.command("tenantAdmin", close=True)
        assert c.e.k.find_one({"_id": 5000})["u"] == "c1"
        assert c.e.k.count_documents({"_id": 0}) == 0
        # restore to P1
        st, res = b.admin.post("/v1/databases/e/restore", {"to": t1})
        assert st == 200, res
        assert (_model_docs(c.e.k), _model_docs(c.e.p)) == s1, "restore to the point after the envelopes"
    finally:
        b.term()


def test_convert_to_capped_of_a_keyed_collection(srv):
    """`convertToCapped` and `cloneCollectionAsCapped` of a customer-keyed collection, then capped inserts that evict the
    oldest documents: every read equals the same steps on the plain control (a capped collection may not have a
    customer key, `custkey::allowed`)."""
    c = srv.client()
    keyed, plain = _fill_pair(c, "cap", "u")
    bad = []
    for src in (keyed, plain):
        c.rv.drop_collection(src.name + "_clone")
        c.rv.command("cloneCollectionAsCapped", src.name, toCollection=src.name + "_clone", size=1 << 20)
        c.rv.command("convertToCapped", src.name, size=1 << 20)
    for kc, pc in ((keyed, plain), (c.rv[keyed.name + "_clone"], c.rv[plain.name + "_clone"])):
        a = [d["_id"] for d in kc.find()]
        b = [d["_id"] for d in pc.find()]
        if a != b:
            bad.append((kc.name, "natural order after the conversion", a[:10], b[:10]))
    for kc, pc in ((keyed, plain), (c.rv[keyed.name + "_clone"], c.rv[plain.name + "_clone"])):
        for coll in (kc, pc):
            c.rv.command("collMod", coll.name, cappedMax=50)
            coll.insert_many([{"_id": 10000 + i, "u": f"c{i % 3}", "x": i} for i in range(30)])
        a = [(d["_id"], d.get("u")) for d in kc.find()]
        b = [(d["_id"], d.get("u")) for d in pc.find()]
        if a != b:
            bad.append((kc.name, "after capped inserts", len(a), len(b), a[:5], b[:5]))
        if kc.count_documents({"u": "c1"}) != pc.count_documents({"u": "c1"}):
            bad.append((kc.name, "count by customer"))
        try:
            kc.insert_one({"_id": 10001, "u": "c1"})
            dup_k = False
        except DuplicateKeyError:
            dup_k = True
        try:
            pc.insert_one({"_id": 10001, "u": "c1"})
            dup_p = False
        except DuplicateKeyError:
            dup_p = True
        if dup_k != dup_p:
            bad.append((kc.name, "duplicate _id in a capped collection", dup_k, dup_p))
        v = c.rv.command("validate", kc.name)
        if not v.get("valid"):
            bad.append((kc.name, "validate", v.get("errors")))
    assert not bad, "\n".join(map(str, bad))


@pytest.mark.parametrize("mode", ["copy", "clone"])
def test_fork_across_the_envelope_boundary(mode):
    """Round-2 note on E_env (forks and clones break sequence order): a database written without envelopes is forked
    by a release E1 process; the fork and the source are then both written with envelopes (deletes, moves). Each reads
    exactly its own model, after a close and reopen; and a fork of the enveloped source reads the source's model."""
    import random
    bucket = make_bucket("revfork")
    r = random.Random(11)
    a = Server(bucket, "revfork-e0", idle=600, extra=["--memtable-max-mb", "1"]).start()
    try:
        c = a.client()
        c.s.create_collection("k", customerKey="u")
        c.s.k.create_index([("u", 1), ("k", 1)])
        _write_some(c.s.k, r, 300, 0)
        _write_some(c.s.p, r, 300, 0)
        c.s.command("tenantAdmin", close=True)
    finally:
        a.term()
    b = Server(bucket, "revfork-e1", idle=600, extra=["--memtable-max-mb", "1", "--envelopes", "write"]).start()
    try:
        c = b.client()
        st, res = b.admin.post("/v1/databases/s/fork", {"target": "f", "mode": mode})
        assert st == 201, res
        _write_some(c.f.k, r, 100, 2000)
        _write_some(c.f.p, r, 100, 2000)
        _write_some(c.s.k, r, 100, 3000)
        _write_some(c.s.p, r, 100, 3000)
        want_f = (_model_docs(c.f.k), _model_docs(c.f.p))
        want_s = (_model_docs(c.s.k), _model_docs(c.s.p))
        for db in ("f", "s"):
            c[db].command("tenantAdmin", close=True)
        assert (_model_docs(c.f.k), _model_docs(c.f.p)) == want_f
        assert (_model_docs(c.s.k), _model_docs(c.s.p)) == want_s
        st, res = b.admin.post("/v1/databases/s/fork", {"target": "g", "mode": mode})
        assert st == 201, res
        assert (_model_docs(c.g.k), _model_docs(c.g.p)) == want_s
        c.g.k.delete_many({"u": "c1"})
        c.g.command("tenantAdmin", close=True)
        assert c.g.k.count_documents({"u": "c1"}) == 0
        assert (_model_docs(c.s.k), _model_docs(c.s.p)) == want_s
    finally:
        b.term()


def test_unique_index_builds_and_upserts_with_id_per_customer(srv):
    """`_id` per customer with index builds and upserts:
    - a unique index led by the customer field (co-located) refuses a build over a value held twice by one customer,
      accepts the same value under two customers, and refuses a later duplicate insert;
    - a unique index not led by the customer field stays unique across customers;
    - an upsert by `_id` alone when two customers hold it updates one and inserts nothing; an `updateMany` by `_id`
      reaches both; concurrent upserts of one (customer, `_id`) leave one document with every increment."""
    c = srv.client()
    m = _keyed(c, "ub")
    m.insert_many([{"_id": 1, "u": "a", "e": "x"}, {"_id": 2, "u": "b", "e": "x"}, {"_id": 3, "u": "a", "e": "y"}])
    m.create_index([("u", 1), ("e", 1)], unique=True)
    with pytest.raises(DuplicateKeyError):
        m.insert_one({"_id": 4, "u": "a", "e": "x"})
    m.insert_one({"_id": 4, "u": "c", "e": "x"})
    m.insert_one({"_id": 5, "u": "a", "e": "x2"})
    with pytest.raises(OperationFailure) as e:
        m.create_index([("e", 1)], unique=True)
    assert e.value.code == 11000
    # a build over a value held twice by one customer
    c.rv.drop_collection("ub2")
    c.rv.create_collection("ub2", customerKey="u")
    m2 = c.rv.ub2
    m2.insert_many([{"_id": 1, "u": "a", "e": "x"}, {"_id": 2, "u": "a", "e": "x"}, {"_id": 1, "u": "b", "e": "x"}])
    with pytest.raises(OperationFailure) as e:
        m2.create_index([("u", 1), ("e", 1)], unique=True)
    assert e.value.code == 11000
    m2.delete_one({"_id": 2, "u": "a"})
    m2.create_index([("u", 1), ("e", 1)], unique=True)
    assert sorted(d["u"] for d in m2.find({"e": "x"}).hint("u_1_e_1")) == ["a", "b"]
    # upserts by `_id` alone
    c.rv.drop_collection("ups")
    c.rv.create_collection("ups", customerKey="u")
    up = c.rv.ups
    up.insert_many([{"_id": 9, "u": "a", "n": 0}, {"_id": 9, "u": "b", "n": 0}])
    r = up.update_one({"_id": 9}, {"$inc": {"n": 1}}, upsert=True)
    assert r.matched_count == 1 and r.upserted_id is None
    assert up.count_documents({"_id": 9}) == 2
    r = up.update_many({"_id": 9}, {"$inc": {"n": 10}})
    assert r.matched_count == 2
    assert sorted(d["n"] for d in up.find({"_id": 9})) == [10, 11]
    # concurrent upserts of one (customer, _id)
    errs = []

    def worker():
        cc = srv.client()
        for _ in range(40):
            for _ in range(5):
                try:
                    cc.rv.ups.update_one({"_id": 77, "u": "z"}, {"$inc": {"n": 1}}, upsert=True)
                    break
                except DuplicateKeyError:
                    continue
            else:
                errs.append("gave up")
    ts = [threading.Thread(target=worker) for _ in range(6)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    docs = list(up.find({"_id": 77}))
    assert not errs
    assert len(docs) == 1 and docs[0]["n"] == 240, docs


def test_upserts_in_transactions_with_id_per_customer(srv):
    """Upserts of one `_id` in two transactions: under two customers both commit, under one customer at most one
    commits, and the collection then holds exactly one document per (customer, `_id`)."""
    c = srv.client()
    m = _keyed(c, "uptx")
    m.insert_one({"_id": "seed", "u": "s"})
    for same in (False, True):
        i = 50 + int(same)
        s1, s2 = c.start_session(), c.start_session()
        s1.start_transaction()
        s2.start_transaction()
        m.update_one({"_id": i, "u": "a"}, {"$inc": {"n": 1}}, upsert=True, session=s1)
        ok2 = True
        try:
            m.update_one({"_id": i, "u": "a" if same else "b"}, {"$inc": {"n": 1}}, upsert=True, session=s2)
        except PyMongoError:
            ok2 = False
        oks = []
        for s, can in ((s1, True), (s2, ok2)):
            if not can:
                s.abort_transaction()
                oks.append(False)
                continue
            try:
                s.commit_transaction()
                oks.append(True)
            except PyMongoError:
                oks.append(False)
        docs = list(m.find({"_id": i}))
        if same:
            assert sum(oks) <= 1 and len(docs) == 1, (oks, docs)
        else:
            assert oks == [True, True] and sorted(d["u"] for d in docs) == ["a", "b"], (oks, docs)
