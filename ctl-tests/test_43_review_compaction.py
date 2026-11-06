"""Reviewer tests for compaction step 1 (commit 91414a6): customer-first keys.

Each test states the guarantee it checks. Every server runs with `--customer-key cc.m=u` (collection cc.m stores its
documents customer first; the locator `loc/m/{_id}` is its unique `_id` index).
"""
import threading

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
    assert ok <= 1 and len(scan) <= 1, f"both writes of _id 8 were acknowledged; a collection scan finds {scan}"
