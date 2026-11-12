"""Query defects found by the review of fix-query-limits (fix-query-defects P5 to P8), each compared with mongod 7.0.

The reference mongod is the judge's (judge/setup.sh, replica set rs0 on MONGOD_REF, default 127.0.0.1:27020); the tests
are skipped when it does not answer. Each test runs the same commands on both and fails when the answers differ (the
documents, and their order where mongod gives one).
- P5 order: an `$or` of `_id` clauses, `$nin`, `$not $in` and a one-clause `$nor` on `_id` answer in `_id` order (mongod
  reads the `_id` index), so a limit or a single write picks the same document; a transaction's own writes are in that
  order too.
- P6 an upsert from a one-value `$in` inserts that value.
- P7 min/max with a hint of the `_id` index.
- P8 a Decimal128 compares exactly with a double: `$in [0.1]` does not find Decimal128 0.1, Decimal128 1E+400 is not infinity.
"""
import os

import pytest
from bson import SON, Decimal128
from pymongo import MongoClient

from lib import Server, make_bucket, free_port

REF = os.environ.get("MONGOD_REF", "mongodb://127.0.0.1:27020/?directConnection=true")


def ref_client():
    c = MongoClient(REF, serverSelectionTimeoutMS=2000)
    try:
        c.admin.command("ping")
    except Exception:
        pytest.skip(f"no reference mongod at {REF}")
    return c


@pytest.fixture(scope="module")
def both():
    real = ref_client()
    port = free_port()
    s = Server(make_bucket("qdef"), "qdef", idle=600, port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    ours = s.client()
    yield real, ours
    s.term()


def setup(c, db, docs):
    c.drop_database(db)
    c[db].c.insert_many(docs)
    return c[db]


IDS = [7, 3, 9, 1, 5, 2, 8, 6, 4, 0]   # insertion order differs from _id order


def same(both, db, docs, fn):
    real, ours = both
    a = fn(setup(real, db, docs))
    b = fn(setup(ours, db, docs))
    print(f"{db}: mongod {a}, ours {b}")
    assert a == b


@pytest.mark.parametrize("name,flt", [
    ("or_two_in", {"$or": [{"_id": {"$in": [5, 6]}}, {"_id": {"$in": [1, 2]}}]}),
    ("or_in_and_range", {"$or": [{"_id": {"$in": [8, 6]}}, {"_id": {"$lt": 2}}]}),
    ("nin", {"_id": {"$nin": [3, 4]}}),
    ("not_in", {"_id": {"$not": {"$in": [3, 4]}}}),
    ("nor_one_clause", {"$nor": [{"_id": {"$in": [3, 4]}}]}),
])
def test_p5_order_of_id_queries(both, name, flt):
    docs = [{"_id": i, "k": i % 5} for i in IDS]
    same(both, f"p5_{name}", docs, lambda d: ([x["_id"] for x in d.c.find(flt)], [x["_id"] for x in d.c.find(flt).limit(2)]))
    # a single write picks the same document as mongod
    def one_write(d):
        d.c.update_one(flt, {"$set": {"x": 1}})
        return [x["_id"] for x in d.c.find({"x": 1})]
    same(both, f"p5w_{name}", docs, one_write)


def test_p5_transaction_own_writes_in_id_order(both):
    def run(d):
        c = d.client
        with c.start_session() as s:
            s.start_transaction()
            d.c.insert_many([{"_id": 5.5, "k": 1}, {"_id": 1e6, "k": 2}], session=s)
            got = [x["_id"] for x in d.c.find({"_id": {"$in": [1e6, 9, 7, 5.5, 3, 1, 250]}}, session=s)]
            lim = [x["_id"] for x in d.c.find({"_id": {"$in": [1e6, 9, 7, 5.5, 3, 1, 250]}}, session=s).limit(3)]
            s.abort_transaction()
        return got, lim
    same(both, "p5_txn", [{"_id": i, "k": i} for i in [250, 9, 7, 3, 1]], run)


def test_p6_upsert_from_one_value_in(both):
    def run(d):
        r1 = d.command(SON([("findAndModify", "c"), ("query", {"_id": {"$in": [99999]}}), ("update", {"$set": {"q": 1}}), ("upsert", True), ("new", True)]))
        d.c.update_one({"_id": {"$in": [88888]}, "a": {"$in": ["x"]}}, {"$set": {"m": 1}}, upsert=True)
        return r1["value"], d.c.find_one({"_id": 88888})
    same(both, "p6", [{"_id": 1}], run)


def test_p7_min_max_with_id_hint(both):
    docs = [{"_id": i, "k": i % 5} for i in IDS]
    same(both, "p7", docs, lambda d: ([x["_id"] for x in d.c.find({}).hint([("_id", 1)]).min([("_id", 2)]).max([("_id", 8)])],
                                       [x["_id"] for x in d.c.find({"_id": {"$in": [3, 1, 2, 9]}}).hint([("_id", 1)]).min([("_id", 2)]).max([("_id", 9)])]))


def test_p8_decimal_equality_is_exact(both):
    docs = [{"_id": Decimal128("0.1")}, {"_id": float("inf")}, {"_id": 7}]
    same(both, "p8", docs, lambda d: ([str(x["_id"]) for x in d.c.find({"_id": {"$in": [0.1]}})],
                                       [str(x["_id"]) for x in d.c.find({"_id": {"$in": [Decimal128("1E+400")]}})],
                                       [str(x["_id"]) for x in d.c.find({"_id": Decimal128("7")})],
                                       [str(x["_id"]) for x in d.c.find({"_id": {"$in": [Decimal128("0.1")]}})]))
