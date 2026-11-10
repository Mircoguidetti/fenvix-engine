"""Compaction 2 step 2, fix round (FIX 3 of the review): the places that identified a document by `_id` alone in a
customer-keyed collection, where `_id` is unique per customer only (founder, 2026-11-07: like MongoDB sharded
collections). The reviewer's three tests are in test_46; these cover the shapes around them and the extra places
found in the same search ($merge, mapReduce reduce output).

The expected change-stream shapes were MEASURED on mongos 7.0.34 with a collection sharded on the customer key:
`documentKey` is the shard key fields in key order (dotted paths kept dotted, a missing field left out), then `_id`;
an update that moves the document to another customer carries the old customer; `updateLookup` looks up by the whole
`documentKey`, so it finds nothing once the document has moved.
"""
import pytest
from pymongo.errors import DuplicateKeyError, OperationFailure

from lib import Server, make_bucket


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("c2s2fix"), "c2s2fix", idle=600, extra=["--replica-set", "rs0", "--memtable-max-mb", "1"]).start()
    yield s
    s.term()


def _keyed(c, name, key="u", db="fx", **kw):
    c[db].drop_collection(name)
    c[db].create_collection(name, customerKey=key, **kw)
    return c[db][name]


def _events(st, n):
    out = []
    for _ in range(40):
        ev = st.try_next()
        if ev is not None:
            out.append(ev)
            if len(out) >= n:
                break
    return out


def test_document_key_shape_matches_a_sharded_collection(srv):
    """insert, update, replace and delete name the customer and the time field before `_id`, in key order; a document
    without the customer field has `{_id}` only (mongos leaves a missing shard key field out)."""
    c = srv.client()
    m = _keyed(c, "dk", key={"field": "u", "order": "ts"})
    with m.watch(max_await_time_ms=500) as st:
        m.insert_one({"x": 0, "_id": 1, "u": "bob", "ts": 5})
        m.update_one({"_id": 1, "u": "bob"}, {"$set": {"n": 1}})
        m.replace_one({"_id": 1, "u": "bob"}, {"u": "bob", "ts": 5, "r": 1})
        m.delete_one({"_id": 1, "u": "bob"})
        m.insert_one({"_id": 2, "u": "x"})
        m.insert_one({"_id": 3})
        m.delete_one({"_id": 3})
        evs = _events(st, 7)
    got = [(e["operationType"], list(e["documentKey"].items())) for e in evs]
    assert got == [
        ("insert", [("u", "bob"), ("ts", 5), ("_id", 1)]),
        ("update", [("u", "bob"), ("ts", 5), ("_id", 1)]),
        ("replace", [("u", "bob"), ("ts", 5), ("_id", 1)]),
        ("delete", [("u", "bob"), ("ts", 5), ("_id", 1)]),
        ("insert", [("u", "x"), ("_id", 2)]),
        ("insert", [("_id", 3)]),
        ("delete", [("_id", 3)]),
    ], got


def test_document_key_dotted_customer_field_and_plain_collection(srv):
    """A dotted customer field is named by its path (as mongos does for `{"a.b": 1}`); a collection without a customer
    key keeps `{_id}`."""
    c = srv.client()
    m = _keyed(c, "dkdot", key="a.b")
    c.fx.drop_collection("plain")
    p = c.fx.plain
    p.insert_one({"_id": 0})
    with c.fx.watch(max_await_time_ms=500) as st:
        m.insert_one({"_id": 1, "a": {"b": 7, "c": 1}})
        m.delete_one({"_id": 1})
        p.insert_one({"_id": 1, "u": "bob"})
        evs = _events(st, 3)
    assert [list(e["documentKey"].items()) for e in evs] == [[("a.b", 7), ("_id", 1)], [("a.b", 7), ("_id", 1)], [("_id", 1)]]


def test_update_lookup_by_customer_and_after_a_move(srv):
    """`updateLookup` returns the document of the event's customer for each of two customers holding `_id` 1; an update
    that moves alice's document to carol carries alice in `documentKey`, and the lookup then finds nothing (null), as
    mongos does; never bob's document."""
    c = srv.client()
    m = _keyed(c, "ul")
    m.insert_one({"_id": 1, "u": "alice", "s": "a"})
    m.insert_one({"_id": 1, "u": "bob", "s": "b"})
    with m.watch(full_document="updateLookup", max_await_time_ms=500) as st:
        m.update_one({"_id": 1, "u": "alice"}, {"$set": {"n": 1}})
        m.update_one({"_id": 1, "u": "bob"}, {"$set": {"n": 2}})
        evs = _events(st, 2)
    assert [(e["documentKey"]["u"], e["fullDocument"]["s"]) for e in evs] == [("alice", "a"), ("bob", "b")]
    with m.watch(full_document="updateLookup", max_await_time_ms=500) as st:
        m.update_one({"_id": 1, "u": "alice"}, {"$set": {"u": "carol"}})
        evs = _events(st, 1)
    assert evs[0]["documentKey"] == {"u": "alice", "_id": 1}
    assert evs[0]["fullDocument"] is None
    assert sorted(d["u"] for d in m.find({"_id": 1})) == ["bob", "carol"]


def test_update_lookup_in_a_transaction(srv):
    """Events of a committed transaction carry the customer too."""
    c = srv.client()
    m = _keyed(c, "ult")
    m.insert_one({"_id": 1, "u": "alice", "s": "a"})
    m.insert_one({"_id": 1, "u": "bob", "s": "b"})
    with m.watch(full_document="updateLookup", max_await_time_ms=500) as st:
        with c.start_session() as s:
            with s.start_transaction():
                m.update_one({"_id": 1, "u": "bob"}, {"$set": {"n": 1}}, session=s)
        evs = _events(st, 1)
    assert evs[0]["documentKey"] == {"u": "bob", "_id": 1}
    assert evs[0]["fullDocument"]["s"] == "b"


def test_collated_id_per_customer_moves_deletes_and_transactions(srv):
    """The collation `_id` index is per customer: a move to a customer that holds a collation-equal `_id` is refused, a
    move to a free one is accepted and the old customer's `_id` becomes free; a delete frees it; in a transaction the
    same rule holds; finds by a collation-equal `_id` see every customer."""
    c = srv.client()
    m = _keyed(c, "ci", collation={"locale": "en", "strength": 2})
    m.insert_one({"_id": "doc-1", "u": "alice"})
    m.insert_one({"_id": "DOC-1", "u": "bob"})
    with pytest.raises(DuplicateKeyError):
        m.insert_one({"_id": "Doc-1", "u": "bob"})
    assert sorted(d["u"] for d in m.find({"_id": "doc-1"})) == ["alice", "bob"]
    # a move onto a customer that holds an equal `_id`: refused, nothing changes
    with pytest.raises(DuplicateKeyError):
        m.update_one({"_id": "doc-1", "u": "alice"}, {"$set": {"u": "bob"}})
    assert sorted(d["u"] for d in m.find({"_id": "DOC-1"})) == ["alice", "bob"]
    # a move to a free customer: alice's `_id` is free afterwards
    m.update_one({"_id": "doc-1", "u": "alice"}, {"$set": {"u": "carol"}})
    m.insert_one({"_id": "dOc-1", "u": "alice"})
    assert sorted(d["u"] for d in m.find({"_id": "doc-1"})) == ["alice", "bob", "carol"]
    assert m.count_documents({"_id": "DOC-1", "u": "carol"}) == 1
    # a delete frees the `_id` under its customer only
    m.delete_one({"_id": "doc-1", "u": "bob"})
    m.insert_one({"_id": "doc-1", "u": "bob", "again": True})
    with pytest.raises(DuplicateKeyError):
        m.insert_one({"_id": "DOC-1", "u": "carol"})
    # transactions: an insert under a new customer commits, one under a customer that holds it fails
    with c.start_session() as s:
        with s.start_transaction():
            m.insert_one({"_id": "DOC-1", "u": "dave"}, session=s)
            assert sorted(d["u"] for d in m.find({"_id": "doc-1"}, session=s)) == ["alice", "bob", "carol", "dave"]
    with c.start_session() as s:
        with pytest.raises(OperationFailure) as e:
            with s.start_transaction():
                m.insert_one({"_id": "doC-1", "u": "dave"}, session=s)
        assert e.value.code == 11000
    assert sorted(d["u"] for d in m.find({"_id": "doc-1"})) == ["alice", "bob", "carol", "dave"]
    assert c.fx.command("validate", "ci")["valid"]


def test_merge_on_id_matches_the_source_documents_customer(srv):
    """`$merge` (default `on: _id`) into a customer-keyed target: a source document matches the target document of its
    own customer only. Before the fix the source `{_id: 1, u: B}` overwrote customer A's document 1 and moved it to B."""
    c = srv.client()
    t = _keyed(c, "tg")
    t.insert_one({"_id": 1, "u": "A", "x": 1})
    t.insert_one({"_id": 2, "u": "B", "x": 1})
    c.fx.drop_collection("src")
    c.fx.src.insert_many([{"_id": 1, "u": "B", "y": 2}, {"_id": 2, "u": "B", "y": 3}])
    list(c.fx.src.aggregate([{"$merge": {"into": "tg"}}]))
    got = sorted((d["_id"], d["u"], d.get("x"), d.get("y")) for d in t.find())
    assert got == [(1, "A", 1, None), (1, "B", None, 2), (2, "B", 1, 3)], got
    # whenMatched: fail sees only the same customer's document
    c.fx.src.delete_many({})
    c.fx.src.insert_one({"_id": 1, "u": "C"})
    list(c.fx.src.aggregate([{"$merge": {"into": "tg", "whenMatched": "fail"}}]))
    assert t.count_documents({"_id": 1}) == 3


def test_map_reduce_reduce_output_into_a_customer_keyed_target(srv):
    """mapReduce `out: {reduce}` writes `{_id, value}` documents (the null customer); they are reduced with that
    customer's documents only, never with another customer's document of the same `_id`."""
    c = srv.client()
    t = _keyed(c, "mr")
    t.insert_one({"_id": "k", "u": "A", "value": 100})
    c.fx.drop_collection("mrsrc")
    c.fx.mrsrc.insert_many([{"k": "k", "n": 1}, {"k": "k", "n": 2}])
    for _ in range(2):
        c.fx.command("mapReduce", "mrsrc", map="function() { emit(this.k, this.n); }", reduce="function(k, v) { return Array.sum(v); }", out={"reduce": "mr"})
    got = sorted(((d.get("u"), d["value"]) for d in t.find({"_id": "k"})), key=str)
    assert got == sorted([(None, 6), ("A", 100)], key=str), got
