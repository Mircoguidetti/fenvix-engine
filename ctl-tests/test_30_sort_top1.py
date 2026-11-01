"""Sort with a limit without keeping every candidate in memory (task N5, fix-sort-top1; found by the S11 reviewer).

Before: updateOne / replaceOne / findAndModify with `sort`, and find with sort + limit, read and kept in memory every
matching document and sorted them all when no index gave the order; a sort on `_id` did not use the `_id` order of the
stored documents; inside a transaction that had written to the collection, even an index that gives the order was read
to the end. After: the first `limit` documents are kept while the candidates stream by (top-k), a sort on `_id` alone
reads the documents in `_id` order and stops as soon as the first ones are certain, and a transaction's own writes no
longer prevent stopping early.

Every test compares the limited answer with the answer without a limit (which still sorts every candidate), so the
new paths must give exactly the same documents in the same order, ties included."""
import datetime

import pytest
from bson import Binary, ObjectId
from pymongo import ASCENDING, DESCENDING

from lib import Server, free_port, make_bucket


@pytest.fixture(scope="module")
def srv():
    port = free_port()
    s = Server(make_bucket("sorttop1"), "sorttop1", idle=600, port=port,
               extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    s.mc = s.client()
    yield s
    s.mc.close()
    s.term()


def _coll(srv, name, docs, **create):
    db = srv.mc["sorttop1"]
    db.drop_collection(name)
    db.create_collection(name, **create)
    c = db[name]
    if docs:
        c.insert_many(docs)
    return c


def _same_as_full(c, flt, sort, ks, collation=None, session=None, skip=0):
    full = list(c.find(flt, sort=sort, collation=collation, session=session))
    for k in ks:
        got = list(c.find(flt, sort=sort, limit=k, skip=skip, collation=collation, session=session))
        assert got == full[skip:skip + k], (sort, k, skip)
    return full


def _examined(c, flt, sort, limit):
    ex = c.find(flt, sort=sort, limit=limit).explain()
    return ex["executionStats"]["totalDocsExamined"]


MIXED_IDS = [5, -2.5, 7, "b", "B", "a", "ab", {"a": "b"}, {"a": "ab"}, {"a": 1, "b": 2}, Binary(b"\x02"), Binary(b"\x01\x01"),
             ObjectId("650000000000000000000001"), ObjectId("640000000000000000000009"), True, False,
             datetime.datetime(2020, 1, 1), datetime.datetime(2019, 6, 1), None]


def test_sort_id_reads_few_documents(srv):
    c = _coll(srv, "oid", [{"v": i, "pad": "x" * 50} for i in range(3000)])
    first, last = c.find_one(sort=[("_id", 1)]), c.find_one(sort=[("_id", -1)])
    all_ids = sorted(d["_id"] for d in c.find({}, {"_id": 1}))
    assert (first["_id"], last["_id"]) == (all_ids[0], all_ids[-1])
    # mongod reads one document through the _id index; here: at most two (the second proves the first is the smallest)
    assert _examined(c, {}, [("_id", 1)], 1) <= 2
    assert _examined(c, {}, [("_id", -1)], 1) <= 2
    assert _examined(c, {}, [("_id", 1)], 10) <= 11
    # updateOne / findOneAndUpdate with sort on _id change the first document in _id order
    r = c.update_one({}, {"$set": {"hit": 1}}, sort={"_id": -1})
    assert r.modified_count == 1 and c.find_one({"hit": 1})["_id"] == all_ids[-1]
    d = c.find_one_and_update({"v": {"$gte": 10}}, {"$set": {"hit": 2}}, sort=[("_id", 1)])
    assert d["_id"] == c.find_one({"v": {"$gte": 10}}, sort=[("_id", 1)])["_id"] == min(x["_id"] for x in c.find({"v": {"$gte": 10}}))


@pytest.mark.parametrize("direction", [1, -1])
def test_sort_id_mixed_types(srv, direction):
    # embedded documents and binary data: their key bytes are not in sort order, the scan must read the whole type
    c = _coll(srv, "mixed", [{"_id": i, "n": n} for n, i in enumerate(MIXED_IDS)])
    full = _same_as_full(c, {}, [("_id", direction)], range(1, len(MIXED_IDS) + 1))
    assert len(full) == len(MIXED_IDS)
    _same_as_full(c, {"n": {"$gte": 3}}, [("_id", direction)], range(1, 8))
    _same_as_full(c, {}, [("_id", direction)], [1, 2, 5], skip=3)
    only_docs = _coll(srv, "docids", [{"_id": {"a": "b"}}, {"_id": {"a": "ab"}}, {"_id": {"a": "abc", "z": 1}}])
    assert only_docs.find_one(sort=[("_id", 1)])["_id"] == {"a": "ab"}
    only_bin = _coll(srv, "binids", [{"_id": Binary(b"\x01\x01")}, {"_id": Binary(b"\x02")}])
    assert bytes(only_bin.find_one(sort=[("_id", 1)])["_id"]) == b"\x02"   # shorter binary first, as in MongoDB


def test_sort_id_strings_with_collation(srv):
    ids = ["b", "B", "a", "A", "c", "ab", "Ab"]
    c = _coll(srv, "strcoll", [{"_id": s} for s in ids], collation={"locale": "en"})
    for d in (1, -1):
        _same_as_full(c, {}, [("_id", d)], range(1, len(ids) + 1))
    assert c.find_one(sort=[("_id", 1)])["_id"] == "a"          # collation order (in byte order "A" comes first)
    plain = _coll(srv, "strplain", [{"_id": s} for s in ids])
    for d in (1, -1):
        _same_as_full(plain, {}, [("_id", d)], range(1, len(ids) + 1))
        _same_as_full(plain, {}, [("_id", d)], [1, 3], collation={"locale": "en", "strength": 2})
    assert plain.find_one(sort=[("_id", 1)])["_id"] == "A"


def test_unindexed_sort_topk_ties_and_skip(srv):
    docs = [{"_id": i, "k": (i * 7) % 13, "g": i % 4, "arr": [i % 5, 20 - i % 7]} for i in range(400)]
    c = _coll(srv, "ties", docs)
    c.create_index("g")
    for sort in ([("k", 1)], [("k", -1)], [("k", 1), ("_id", -1)], [("arr", 1)], [("arr", -1)], [("missing", 1), ("k", -1)]):
        _same_as_full(c, {}, sort, [1, 2, 13, 50])                        # collection scan
        _same_as_full(c, {"g": 2}, sort, [1, 7, 30])                       # index scan that does not give the order
        _same_as_full(c, {"$or": [{"g": 1}, {"g": 3}]}, sort, [1, 9])     # $or of index scans
        _same_as_full(c, {}, sort, [5], skip=11)


def test_update_with_sort_picks_first(srv):
    c = _coll(srv, "upd", [{"_id": i, "k": (i * 37) % 101, "n": 0} for i in range(300)])
    for sort, want in (({"k": 1}, 0), ({"k": -1}, 100)):
        target = c.find_one({"k": want})["_id"]
        r = c.update_one({}, {"$inc": {"n": 1}}, sort=sort)
        assert r.modified_count == 1 and c.find_one({"_id": target})["n"] == 1
    r = c.replace_one({"k": {"$gt": 50}}, {"k": -1, "n": 9}, sort={"k": 1})
    assert c.find_one({"n": 9})["_id"] == c.find_one({"k": -1})["_id"]
    assert c.find_one_and_delete({}, sort=[("k", -1), ("_id", 1)])["k"] == 100


@pytest.mark.parametrize("indexed", [False, True])
def test_transaction_own_writes_with_limit(srv, indexed):
    c = _coll(srv, "txn", [{"_id": i, "k": i * 10} for i in range(1, 501)])
    if indexed:
        c.create_index("k")
    with srv.mc.start_session() as s:
        s.start_transaction()
        c.delete_one({"_id": 1}, session=s)                       # the current first goes away
        c.update_one({"_id": 300}, {"$set": {"k": 5}}, session=s)  # an old one becomes the first
        c.insert_one({"_id": 1000, "k": 7}, session=s)             # a new one second
        c.update_one({"_id": 2}, {"$set": {"k": 99999}}, session=s)  # the old second moves to the end
        for sort in ([("k", 1)], [("k", -1)], [("_id", 1)], [("_id", -1)]):
            _same_as_full(c, {}, sort, [1, 2, 3, 10], session=s)
        assert [d["_id"] for d in c.find({}, sort=[("k", 1)], limit=3, session=s)] == [300, 1000, 3]
        assert c.find_one({}, sort=[("_id", -1)], session=s)["_id"] == 1000
        r = c.update_one({}, {"$set": {"hit": 1}}, sort={"k": 1}, session=s)
        assert r.modified_count == 1
        assert c.find_one_and_update({}, {"$set": {"hit": 2}}, sort=[("k", -1)], session=s)["_id"] == 2
        assert c.find_one_and_update({"k": {"$gte": 20, "$lt": 50000}}, {"$set": {"hit": 3}}, sort=[("_id", 1)], session=s)["_id"] == 3
        s.commit_transaction()
    assert c.find_one({"hit": 1})["_id"] == 300
    assert c.find_one({"hit": 2})["_id"] == 2
    assert c.find_one({"hit": 3})["_id"] == 3


def test_text_score_sort_with_limit(srv):
    c = _coll(srv, "text", [{"_id": i, "t": "apple " * (i % 5 + 1) + "pear"} for i in range(50)])
    c.create_index([("t", "text")])
    full = list(c.find({"$text": {"$search": "apple"}}, {"s": {"$meta": "textScore"}}, sort=[("s", {"$meta": "textScore"})]))
    got = list(c.find({"$text": {"$search": "apple"}}, {"s": {"$meta": "textScore"}}, sort=[("s", {"$meta": "textScore"})], limit=3))
    assert [d["s"] for d in got] == [d["s"] for d in full[:3]]
