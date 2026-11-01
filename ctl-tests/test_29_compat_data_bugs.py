"""Two compatibility bugs that changed data (found by the S10 compatibility run, fixed on branch fix-compat-dati).

1. Transaction that loses writes. Inside a transaction, a findAndModify (findOneAndUpdate / findOneAndReplace /
   findOneAndDelete) that ends without writing took the transaction's pending writes and never gave them back: the commit
   answered ok and every write made earlier in the transaction was gone. Three paths ended that way: nothing matched
   (delete), nothing matched and no upsert (update/replace), and a match the update does not change.
2. updateOne / replaceOne with `sort` (MongoDB 8.0), also inside bulkWrite: the option was accepted and ignored, so the
   statement changed the first document found instead of the first one in the sort order. Now the sort is applied as in
   MongoDB 8.0; a sort with multi: true, or a sort that is not an object, is refused.

Before the fix every test below fails (measured on main 658c724); after the fix they pass.
"""
import pytest
from pymongo import ReplaceOne, UpdateOne
from pymongo.errors import OperationFailure

from lib import Server, free_port, make_bucket


@pytest.fixture(scope="module")
def srv():
    port = free_port()
    s = Server(make_bucket("compatdata"), "compatdata", idle=600, port=port,
               extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    s.mc = s.client()   # one client: a session works only with the client that started it
    yield s
    s.mc.close()
    s.term()


def _txn(srv, coll, ops):
    with srv.mc.start_session() as s:
        s.start_transaction()
        for op in ops:
            op(coll, s)
        s.commit_transaction()
    return sorted((d["_id"], d["v"]) for d in coll.find())


def _fresh(srv, name):
    coll = srv.mc["compatdata"][name]
    coll.drop()
    coll.insert_many([{"_id": i, "v": 0} for i in (1, 2, 3)])
    return coll


CASES = {
    # earlier write, then a findAndModify that writes nothing: the earlier write must survive the commit
    "delete_then_findOneAndDelete_nomatch": (
        [lambda c, s: c.delete_one({"_id": 3}, session=s), lambda c, s: c.find_one_and_delete({"_id": 9}, session=s)],
        [(1, 0), (2, 0)]),
    "findOneAndDelete_then_findOneAndDelete_nomatch": (
        [lambda c, s: c.find_one_and_delete({"_id": 3}, session=s), lambda c, s: c.find_one_and_delete({"_id": 9}, session=s)],
        [(1, 0), (2, 0)]),
    "findOneAndUpdate_then_findOneAndUpdate_nomatch": (
        [lambda c, s: c.find_one_and_update({"_id": 1}, {"$set": {"v": 1}}, session=s),
         lambda c, s: c.find_one_and_update({"_id": 9}, {"$set": {"v": 1}}, session=s)],
        [(1, 1), (2, 0), (3, 0)]),
    "insert_then_findOneAndReplace_nomatch": (
        [lambda c, s: c.insert_one({"_id": 4, "v": 0}, session=s),
         lambda c, s: c.find_one_and_replace({"_id": 9}, {"v": 5}, session=s)],
        [(1, 0), (2, 0), (3, 0), (4, 0)]),
    "insert_then_findOneAndUpdate_match_without_change": (
        [lambda c, s: c.insert_one({"_id": 4, "v": 0}, session=s),
         lambda c, s: c.find_one_and_update({"_id": 1}, {"$set": {"v": 0}}, session=s)],
        [(1, 0), (2, 0), (3, 0), (4, 0)]),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_txn_keeps_earlier_writes_after_findandmodify_that_writes_nothing(srv, name):
    ops, expected = CASES[name]
    got = _txn(srv, _fresh(srv, name), ops)
    print(name, "->", got)
    assert got == expected, f"{name}: the commit lost writes made earlier in the transaction: {got} instead of {expected}"


def _sort_coll(srv, name):
    coll = srv.mc["compatdata"][name]
    coll.drop()
    coll.insert_many([{"_id": 1, "x": 11}, {"_id": 2, "x": 22}, {"_id": 3, "x": 33}])
    return coll


def _xs(coll):
    return [d["x"] for d in coll.find(sort=[("_id", 1)])]


def test_update_one_with_sort_changes_the_first_in_sort_order(srv):
    coll = _sort_coll(srv, "upd_sort")
    r = coll.update_one({"_id": {"$gt": 1}}, {"$inc": {"x": 1}}, sort={"_id": -1})
    assert (r.matched_count, r.modified_count) == (1, 1)
    assert _xs(coll) == [11, 22, 34], _xs(coll)
    r = coll.update_one({}, {"$inc": {"x": 100}}, sort={"x": -1})
    assert _xs(coll) == [11, 22, 134], _xs(coll)


def test_replace_one_with_sort_changes_the_first_in_sort_order(srv):
    coll = _sort_coll(srv, "rep_sort")
    r = coll.replace_one({"_id": {"$gt": 1}}, {"x": 1}, sort={"_id": -1})
    assert (r.matched_count, r.modified_count) == (1, 1)
    assert _xs(coll) == [11, 22, 1], _xs(coll)


def test_bulk_write_update_and_replace_with_sort(srv):
    coll = _sort_coll(srv, "bulk_sort")
    coll.bulk_write([UpdateOne({"_id": {"$gt": 1}}, {"$inc": {"x": 1}}, sort={"_id": -1}),
                     ReplaceOne({"_id": {"$lt": 3}}, {"x": 7}, sort={"_id": -1})])
    assert _xs(coll) == [11, 7, 34], _xs(coll)


def test_update_with_sort_inside_a_transaction_sees_its_own_writes(srv):
    coll = _sort_coll(srv, "txn_sort")
    with srv.mc.start_session() as s:
        s.start_transaction()
        coll.insert_one({"_id": 4, "x": 44}, session=s)
        coll.update_one({}, {"$set": {"x": 0}}, sort={"_id": -1}, session=s)   # the document inserted in this transaction
        s.commit_transaction()
    assert _xs(coll) == [11, 22, 33, 0], _xs(coll)


def test_update_sort_with_multi_or_wrong_type_is_refused(srv):
    db = srv.mc["compatdata"]
    _sort_coll(srv, "bad_sort")
    with pytest.raises(OperationFailure) as e:
        db.command({"update": "bad_sort", "updates": [{"q": {}, "u": {"$set": {"x": 0}}, "multi": True, "sort": {"_id": 1}}]})
    assert e.value.code == 9, e.value.details
    with pytest.raises(OperationFailure) as e:
        db.command({"update": "bad_sort", "updates": [{"q": {}, "u": {"$set": {"x": 0}}, "sort": 1}]})
    assert e.value.code == 14, e.value.details
    assert _xs(db["bad_sort"]) == [11, 22, 33]
