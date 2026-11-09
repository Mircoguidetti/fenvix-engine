"""Compaction 2, step 2: key layout v2, `_id` unique per customer, the time field, envelopes (custkey.rs, layout2.rs,
envelope.rs). Every test compares with a model or with the same operations on a collection without a customer key.

- `_id` per customer (founder, 2026-11-07): the same `_id` under two customers is accepted, under one customer refused
  with 11000, in plain writes and in transactions (also when a transaction and a plain write race), and a move to a
  customer that already holds the `_id` is refused.
- The time field: `customerKey: {field, order}` stores a customer's documents in time order; reads by customer, by
  time range, by `_id`, after updates that move a document in time or to another customer, equal a model.
- Layout v2 changes no answer: random writes and queries on a customer-keyed collection with co-located indexes equal
  the same on a plain collection (the control).
"""
import datetime as dt
import random

import pytest
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError

from lib import Server, make_bucket


@pytest.fixture(scope="module")
def srv():
    s = Server(make_bucket("layoutv2"), "layoutv2", idle=600, extra=["--replica-set", "rs0", "--memtable-max-mb", "1"]).start()
    yield s
    s.term()


def _keyed(c, name, key="u"):
    c.v2.drop_collection(name)
    c.v2.create_collection(name, customerKey=key)
    return c.v2[name]


def test_same_id_two_customers_accepted_one_customer_refused(srv):
    c = srv.client()
    m = _keyed(c, "ids")
    m.insert_one({"_id": 1, "u": "alice", "x": 1})
    m.insert_one({"_id": 1, "u": "bob", "x": 2})
    with pytest.raises(DuplicateKeyError) as e:
        m.insert_one({"_id": 1, "u": "alice", "x": 3})
    assert e.value.code == 11000
    assert sorted(d["u"] for d in m.find({"_id": 1})) == ["alice", "bob"]
    assert m.count_documents({}) == 2
    # insert_many, unordered: the duplicate under the same customer is the only error
    with pytest.raises(PyMongoError) as e:
        m.insert_many([{"_id": 2, "u": "alice"}, {"_id": 2, "u": "alice"}, {"_id": 2, "u": "carol"}], ordered=False)
    errs = e.value.details["writeErrors"]
    assert [x["code"] for x in errs] == [11000] and errs[0]["index"] == 1
    assert sorted(d["u"] for d in m.find({"_id": 2})) == ["alice", "carol"]
    # an upsert by (customer, _id) finds its own document, not the other customer's
    m.update_one({"_id": 1, "u": "bob"}, {"$set": {"x": 20}}, upsert=True)
    assert m.find_one({"_id": 1, "u": "bob"})["x"] == 20 and m.find_one({"_id": 1, "u": "alice"})["x"] == 1
    # a move to a customer that already holds the `_id` is refused, to a free one accepted
    with pytest.raises(PyMongoError) as e:
        m.update_one({"_id": 1, "u": "bob"}, {"$set": {"u": "alice"}})
    assert getattr(e.value, "code", None) == 11000
    m.update_one({"_id": 1, "u": "bob"}, {"$set": {"u": "dave"}})
    assert sorted(d["u"] for d in m.find({"_id": 1})) == ["alice", "dave"]
    # deleting one customer's document leaves the other's
    m.delete_one({"_id": 1, "u": "alice"})
    assert [d["u"] for d in m.find({"_id": 1})] == ["dave"]
    m.insert_one({"_id": 1, "u": "alice", "back": True})
    assert sorted(d["u"] for d in m.find({"_id": 1})) == ["alice", "dave"]


def test_same_id_in_transactions(srv):
    c = srv.client()
    m = _keyed(c, "idtx")
    m.insert_one({"_id": "seed", "u": 0})
    # two customers, two transactions: both commit
    s1, s2 = c.start_session(), c.start_session()
    s1.start_transaction()
    s2.start_transaction()
    m.insert_one({"_id": 7, "u": "alice"}, session=s1)
    m.insert_one({"_id": 7, "u": "bob"}, session=s2)
    s1.commit_transaction()
    s2.commit_transaction()
    assert sorted(d["u"] for d in m.find({"_id": 7})) == ["alice", "bob"]
    # one customer, two transactions: at most one commits
    s1.start_transaction()
    s2.start_transaction()
    committed = 0
    m.insert_one({"_id": 8, "u": "alice"}, session=s1)
    try:
        m.insert_one({"_id": 8, "u": "alice", "second": True}, session=s2)
        s2.commit_transaction()
        committed += 1
    except PyMongoError:
        try:
            s2.abort_transaction()
        except PyMongoError:
            pass
    try:
        s1.commit_transaction()
        committed += 1
    except PyMongoError:
        pass
    assert committed == 1 and m.count_documents({"_id": 8}) == 1
    # one customer, a transaction against a plain insert: at most one wins
    s1.start_transaction()
    m.insert_one({"_id": 9, "u": "alice"}, session=s1)
    ok = 0
    try:
        m.insert_one({"_id": 9, "u": "alice", "plain": True})
        ok += 1
    except PyMongoError:
        pass
    try:
        s1.commit_transaction()
        ok += 1
    except PyMongoError:
        pass
    assert ok == 1 and m.count_documents({"_id": 9}) == 1
    # inside one transaction: the same customer twice is refused, two customers accepted
    s1.start_transaction()
    m.insert_one({"_id": 10, "u": "alice"}, session=s1)
    m.insert_one({"_id": 10, "u": "bob"}, session=s1)
    with pytest.raises(PyMongoError):
        m.insert_one({"_id": 10, "u": "bob"}, session=s1)
    try:
        s1.abort_transaction()
    except PyMongoError:
        pass
    assert m.count_documents({"_id": 10}) == 0
    s1.end_session()
    s2.end_session()


def test_time_field_orders_a_customer_and_matches_a_model(srv):
    c = srv.client()
    c.v2.drop_collection("timed")
    c.v2.create_collection("timed", customerKey={"field": "u", "order": "ts"})
    m = c.v2.timed
    m.create_index([("u", ASCENDING), ("k", ASCENDING)])
    m.create_index([("k", ASCENDING)])
    r = random.Random(11)
    t0 = dt.datetime(2026, 1, 1)
    model = {}
    nid = 0
    for step in range(3000):
        op = r.random()
        if op < 0.5 or not model:
            nid += 1
            d = {"_id": nid, "u": r.randrange(12), "ts": t0 + dt.timedelta(seconds=r.randrange(100000)), "k": r.randrange(20)}
            m.insert_one(d)
            model[(d["u"], nid)] = d
        elif op < 0.7:
            (u, i) = r.choice(list(model))
            nts = t0 + dt.timedelta(seconds=r.randrange(100000))
            m.update_one({"_id": i, "u": u}, {"$set": {"ts": nts}})
            model[(u, i)]["ts"] = nts
        elif op < 0.8:
            (u, i) = r.choice(list(model))
            nu = r.randrange(12)
            if (nu, i) in model:
                continue
            m.update_one({"_id": i, "u": u}, {"$set": {"u": nu}})
            d = model.pop((u, i))
            d["u"] = nu
            model[(nu, i)] = d
        elif op < 0.9:
            (u, i) = r.choice(list(model))
            m.delete_one({"_id": i, "u": u})
            del model[(u, i)]
        else:
            u = r.randrange(12)
            want = sorted((d for d in model.values() if d["u"] == u), key=lambda d: (d["ts"], d["_id"]), reverse=True)[:10]
            got = list(m.find({"u": u}).sort([("ts", DESCENDING), ("_id", DESCENDING)]).limit(10))
            assert [(d["_id"], d["ts"]) for d in got] == [(d["_id"], d["ts"]) for d in want], f"step {step} customer {u}"
    for u in range(12):
        mine = [d for d in model.values() if d["u"] == u]
        assert sorted(d["_id"] for d in m.find({"u": u})) == sorted(d["_id"] for d in mine)
        cut = t0 + dt.timedelta(seconds=50000)
        assert sorted(d["_id"] for d in m.find({"u": u, "ts": {"$lt": cut}})) == sorted(d["_id"] for d in mine if d["ts"] < cut)
        k = 7
        assert sorted(d["_id"] for d in m.find({"u": u, "k": k})) == sorted(d["_id"] for d in mine if d["k"] == k)
    assert m.count_documents({}) == len(model)
    assert sorted(d["_id"] for d in m.find({"k": 3})) == sorted(d["_id"] for d in model.values() if d["k"] == 3)
    v = c.v2.command("validate", "timed")
    assert v["valid"], v


@pytest.mark.parametrize("seed", [1, 2])
def test_layout_v2_answers_equal_a_plain_collection(srv, seed):
    """Random writes and reads on a customer-keyed collection (two co-located indexes, one shared, customer values of
    several types, arrays and missing) and on a plain collection: every read answers the same."""
    c = srv.client()
    k = _keyed(c, f"same{seed}")
    c.v2.drop_collection(f"plain{seed}")
    p = c.v2[f"plain{seed}"]
    for col in (k, p):
        col.create_index([("u", ASCENDING), ("ts", DESCENDING)])
        col.create_index([("u", ASCENDING), ("k", ASCENDING)], unique=False)
        col.create_index([("k", ASCENDING)])
    r = random.Random(seed)
    custs = ["a", "ab", "b", 1, 2.0, 3, None, "zz", ["a", "b"]]
    ids = []
    for step in range(2500):
        op = r.random()
        if op < 0.45 or not ids:
            i = len(ids) + 1
            d = {"_id": i, "ts": r.randrange(1000), "k": r.randrange(30), "pad": "x" * r.randrange(200)}
            u = r.choice(custs)
            if u is not None or r.random() < 0.5:
                d["u"] = u
            for col in (k, p):
                col.insert_one(dict(d))
            ids.append(i)
        elif op < 0.65:
            i = r.choice(ids)
            upd = {"$set": {"ts": r.randrange(1000)}} if r.random() < 0.5 else {"$set": {"u": r.choice(custs[:-1])}}
            for col in (k, p):
                col.update_one({"_id": i}, upd)
        elif op < 0.72:
            i = r.choice(ids)
            for col in (k, p):
                col.delete_one({"_id": i})
        else:
            u = r.choice(custs)
            q = r.randrange(6)
            def run(col):
                if q == 0:
                    return list(col.find({"u": u}).sort([("ts", -1), ("_id", 1)]).limit(15))
                if q == 1:
                    return list(col.find({"u": u, "k": {"$gte": 10}}).sort([("k", 1), ("_id", 1)]))
                if q == 2:
                    return list(col.find({"k": r2}).sort("_id", 1))
                if q == 3:
                    return list(col.find({"u": {"$gt": "a"}}).sort([("u", 1), ("_id", 1)]).limit(20))
                if q == 4:
                    return list(col.find({}).sort([("u", -1), ("ts", 1), ("_id", 1)]).limit(25))
                return [col.count_documents({"u": u}), col.find_one({"_id": r2})]
            r2 = r.randrange(40)
            assert run(k) == run(p), f"step {step} query {q} customer {u!r}"
    assert sorted(k.find({}), key=lambda d: d["_id"]) == sorted(p.find({}), key=lambda d: d["_id"])
    assert c.v2.command("validate", k.name)["valid"]
