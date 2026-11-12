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
import datetime
import random

from bson import SON, Decimal128, Int64, ObjectId, Regex
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


MIXED = ([{"_id": i} for i in range(0, 40, 3)] + [{"_id": Int64(2**53 + 1)}, {"_id": float(2**53)},
         {"_id": Decimal128("10.0000000000000000001")}, {"_id": 10.5}, {"_id": Decimal128("0.1")}, {"_id": 0.30000000000000004},
         {"_id": float("inf")}, {"_id": float("-inf")}, {"_id": float("nan")}, {"_id": "a"}, {"_id": "ab"}, {"_id": "b"}, {"_id": ""},
         {"_id": ObjectId("000000000000000000000001")}, {"_id": ObjectId("ffffffffffffffffffffffff")}, {"_id": datetime.datetime(2020, 1, 1)},
         {"_id": None}, {"_id": True}, {"_id": {"x": 1}}])


@pytest.mark.parametrize("name,flt", [
    ("int_range", {"_id": {"$gte": 9, "$lt": 27}}),
    ("gt_only", {"_id": {"$gt": 30}}),
    ("lte_only", {"_id": {"$lte": 3}}),
    ("decimal_edges", {"_id": {"$gt": Decimal128("10"), "$lte": Decimal128("10.5")}}),
    ("big_int_edges", {"_id": {"$gte": float(2**53), "$lte": Int64(2**53 + 1)}}),
    ("double_vs_decimal", {"_id": {"$gt": 0.1, "$lt": 1}}),
    ("strings", {"_id": {"$gte": "a", "$lt": "b"}}),
    ("objectids", {"_id": {"$gt": ObjectId("000000000000000000000000")}}),
    ("dates", {"_id": {"$lt": datetime.datetime(2030, 1, 1)}}),
    ("and_range", {"$and": [{"_id": {"$gt": 5}}, {"_id": {"$lt": 20}}]}),
    ("range_plus_ne", {"_id": {"$gt": 5, "$lt": 30, "$ne": 12}}),
])
def test_p3_id_ranges_answer_as_mongod(both, name, flt):
    same(both, f"p3_{name}", MIXED, lambda d: ([repr(x["_id"]) for x in d.c.find(flt)], [repr(x["_id"]) for x in d.c.find(flt).limit(2)],
                                                [repr(x["_id"]) for x in d.c.find(flt).sort("_id", -1).limit(3)]))


def test_p3_a_range_reads_only_its_keys(both):
    _, ours = both
    d = ours.p3keys
    d.c.drop()
    ids = list(range(10_000))
    random.Random(5).shuffle(ids)
    for s in range(0, 10_000, 2_000):
        d.c.insert_many([{"_id": i, "pad": "x" * 50} for i in ids[s:s + 2_000]])
    e = d.command("explain", {"find": "c", "filter": {"_id": {"$gte": 5_000, "$lt": 5_100}}}, verbosity="executionStats")
    ex = e["executionStats"]
    print(f"range of 100 in 10,000: returned {ex['nReturned']}, documents examined {ex['totalDocsExamined']}")
    assert ex["nReturned"] == 100
    assert ex["totalDocsExamined"] <= 200, f"the range read {ex['totalDocsExamined']} documents"


P2_DOCS = ([{"_id": i, "v": i, "a": [i, i + 1000]} for i in range(200)] + [{"_id": "s1", "v": "abc"}, {"_id": "s2", "v": "xyz", "a": ["q", None]},
           {"_id": "n1", "v": None}, {"_id": "m1"}, {"_id": "d1", "v": Decimal128("5")}, {"_id": "f1", "v": 7.0}, {"_id": "o1", "v": {"x": 1}},
           {"_id": "arr", "v": [3, 4]}, {"_id": "nan", "v": float("nan")}])
LONG = list(range(0, 400, 3)) + [Int64(150), 9.0, Decimal128("11"), "abc", {"x": 1}, [3, 4], float("nan")]


@pytest.mark.parametrize("name,flt", [
    ("in_long", {"v": {"$in": LONG}}),
    ("in_long_null", {"v": {"$in": LONG + [None]}}),
    ("in_long_regex", {"v": {"$in": LONG + [Regex("^x")]}}),
    ("in_long_on_array", {"a": {"$in": LONG + ["q"]}}),
    ("nin_long", {"v": {"$nin": LONG}}),
    ("nin_long_null", {"a": {"$nin": LONG + [None]}}),
    ("id_in_long", {"_id": {"$in": LONG + ["s1", "nan"]}}),
    ("in_long_and_more", {"v": {"$in": LONG, "$gt": 100}}),
])
def test_p2_long_in_lists_answer_as_mongod(both, name, flt):
    same(both, f"p2_{name}", P2_DOCS, lambda d: sorted(repr(x["_id"]) for x in d.c.find(flt)))


@pytest.mark.parametrize("name,mb,kw,late", [
    ("limit5_batch2", 0.001, {"limit": 5, "batchSize": 2}, False),
    ("limit5", 0.001, {"limit": 5}, False),
    ("skip1_limit4_batch2", 0.001, {"skip": 1, "limit": 4, "batchSize": 2}, False),
    ("limit5_5mb", 5, {"limit": 5}, False),
    ("limit5_inserts_after_open", 0.001, {"limit": 5, "batchSize": 2}, True),
])
def test_p10_tailable_cursor_keeps_its_limit(both, name, mb, kw, late):
    """P10 (added by the coordinator): a tailable cursor's `limit` counts over its whole life, not only its first batch
    (the review's probes/tail_limit.py: limit 5 returned 10). mongod returns the limit in total and keeps the cursor open."""
    import time
    def run(d):
        d.drop_collection("cap")
        d.create_collection("cap", capped=True, size=200 * 1024 * 1024)
        if late:
            d.cap.insert_one({"_id": -1})
        for i in range(0 if late else 10):
            d.cap.insert_one({"_id": i, "p": "x" * int(mb * 1024 * 1024)})
        cmd = SON([("find", "cap"), ("filter", {}), ("tailable", True), ("awaitData", True)])
        cmd.update(kw)
        r = d.command(cmd)
        got = [x["_id"] for x in r["cursor"]["firstBatch"]]
        cid = r["cursor"]["id"]
        if late:
            for i in range(10):
                d.cap.insert_one({"_id": i, "p": "x"})
        t0 = time.time()
        while cid and time.time() - t0 < 3:
            g = d.command(SON([("getMore", cid), ("collection", "cap"), ("maxTimeMS", 200)]))
            got += [x["_id"] for x in g["cursor"]["nextBatch"]]
            cid = g["cursor"]["id"]
        return got, cid != 0
    same(both, f"p10_{name}", [{"_id": "x"}], run)


# Fix round (review of 62b8619). P8: two numbers that compare different are two keys (a Decimal128 and its nearest double
# were one key: the second insert was refused as a duplicate); two that compare equal are one key.
@pytest.mark.parametrize("name,a,b", [
    ("dec01_then_double01", Decimal128("0.1"), 0.1),
    ("int10_then_dec_long", 10, Decimal128("10.0000000000000000001")),
    ("inf_then_dec_1e400", float("inf"), Decimal128("1E+400")),
    ("int64_then_double_near", Int64(9007199254740993), 9007199254740992.0),
    ("dec_then_dec_one_double_apart", Decimal128("0.1"), Decimal128("0.10000000000000001")),
    ("equal_int64_and_decimal", Int64(9007199254740993), Decimal128("9007199254740993")),
    ("equal_double_and_decimal", 0.5, Decimal128("0.50")),
])
def test_p8_numbers_are_one_key_only_when_equal(both, name, a, b):
    def run(d):
        out = []
        for spec in ("_id", "unique"):
            d.drop_collection("k")
            if spec == "unique":
                d.k.create_index("v", unique=True)
            for v in (a, b):
                try:
                    d.k.insert_one({"_id": v} if spec == "_id" else {"v": v})
                    out.append("ok")
                except Exception as e:
                    out.append(getattr(e, "code", "err"))
            f = "_id" if spec == "_id" else "v"
            out.append(sorted(str(x[f]) for x in d.k.find({f: b})))
        return out
    same(both, f"p8k_{name}", [{"_id": 0}], run)


def test_p8_decimal_finds_the_equal_int64(both):
    docs = [{"_id": Int64(9007199254740993)}, {"_id": Int64(9007199254740992)}, {"_id": 1}]
    same(both, "p8i", docs, lambda d: ([str(x["_id"]) for x in d.c.find({"_id": Decimal128("9007199254740993")})],
                                       [str(x["_id"]) for x in d.c.find({"_id": {"$in": [Decimal128("9007199254740993")] + list(range(2, 300))}})]))


# P10: a negative limit on find is refused (51024), tailable or not, as mongod's find command does.
@pytest.mark.parametrize("kw", [{"limit": -3}, {"limit": -1, "tailable": True}, {"limit": -3, "tailable": True, "awaitData": True}, {"limit": -0.5}])
def test_p10_negative_limit(both, kw):
    def run(d):
        d.drop_collection("cap")
        d.create_collection("cap", capped=True, size=1 << 20)
        d.cap.insert_many([{"_id": i} for i in range(5)])
        try:
            r = d.command(SON([("find", "cap")] + list(kw.items())))
            return [x["_id"] for x in r["cursor"]["firstBatch"]]
        except Exception as e:
            return getattr(e, "code", "err")
    same(both, "p10neg", [{"_id": 0}], run)


# Found while lengthening test_50: two indexes whose key patterns have the same fields in another order are two indexes
# (a Document compares equal regardless of order, so the second was refused as "an equivalent index").
def test_index_key_order_matters(both):
    from pymongo import IndexModel
    def run(d):
        names = d.c.create_indexes([IndexModel([("x", 1), ("s", 1)]), IndexModel([("s", 1), ("x", 1)])])
        d.c.create_index([("x", -1), ("s", 1)])
        hinted = [x["_id"] for x in d.c.find({}).hint([("s", 1), ("x", 1)])]
        d.c.drop_index([("s", 1), ("x", 1)])
        return names, hinted, sorted(i["name"] for i in d.c.list_indexes())
    same(both, "idxorder", [{"_id": i, "x": i % 3, "s": str(9 - i)} for i in range(6)], run)


# P8 fix round: ranges on an indexed field around a decimal within one double step of the bound (the decimal's key sits
# after the double's): the answers are mongod's.
@pytest.mark.parametrize("flt", [{"v": {"$lte": 0.1}}, {"v": {"$lt": 0.1}}, {"v": {"$gte": 0.1}}, {"v": {"$gt": 0.1}},
                                 {"v": {"$lte": Decimal128("0.1")}}, {"v": {"$lt": Decimal128("0.1")}}, {"v": {"$gt": Decimal128("0.1")}},
                                 {"v": {"$gte": 10, "$lt": 11}}, {"v": {"$gt": 10, "$lte": Decimal128("10.0000000000000000001")}}])
def test_p8_index_ranges_next_to_decimals(both, flt):
    docs = [{"_id": i, "v": v} for i, v in enumerate([0.1, Decimal128("0.1"), Decimal128("0.10000000000000001"), 0.2, 10, Decimal128("10.0000000000000000001"), 11, Decimal128("1E+400"), float("inf")])]
    def run(d):
        d.c.create_index("v")
        return sorted(x["_id"] for x in d.c.find(flt).hint("v_1")), d.c.count_documents(flt, hint="v_1"), sorted(x["_id"] for x in d.c.find(flt).sort("v", -1).hint("v_1"))
    same(both, "p8r", docs, run)
