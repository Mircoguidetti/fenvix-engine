"""fix-customer-id-plan: equality on the customer key (and `_id`) reads the customer's documents, not the collection.

The defect (MEASURED by the big-speed fix round): `find_one({"thread_id": X, "_id": Y})` on a customer-keyed collection
with no index on `thread_id` read every document of the collection. fix-query-defects P5 answers a query on `_id` in
`_id` order, and customer-first keys are not in `_id` order, so the plan fell back to a scan of the whole collection.

- `test_documents_read`: 20,000 documents of 200 customers, inserted interleaved. Each shape must read at most its
  customer's documents (explain `totalDocsExamined`); the base 0b143c41 reads all 20,000 for each.
- `test_same_answers_as_mongod`: a generated set of query shapes (customer values of every type, arrays, null, missing,
  mixed numbers; `_id` equal, `$in` and ranges; limit, skip, sort, projection, hints, collation; counts, distinct, a
  single update and a single delete) on mongod 7.0 (plain collection) and on ours (customer-keyed): the same documents
  in the same order, and the same document picked by a single write.
- `test_transaction_own_writes`: the same inside a transaction that wrote to the customer.

The reference mongod is the judge's (judge/setup.sh, replica set rs0 on MONGOD_REF, default 127.0.0.1:27020); the
mongod tests are skipped when it does not answer.
"""
import datetime
import os
import random
import time

import pytest
from bson import Decimal128, Int64, ObjectId, Regex
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
def ours():
    port = free_port()
    s = Server(make_bucket("t85"), "t85", idle=600, port=port, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    yield s.client()
    s.term()


def examined(db, coll, flt, **kw):
    cmd = {"find": coll, "filter": flt}
    cmd.update(kw)
    e = db.command("explain", cmd, verbosity="executionStats")
    return e["executionStats"]["totalDocsExamined"]


# ---------------------------------------------------------------------------------------------------------------------
# Documents read
# ---------------------------------------------------------------------------------------------------------------------

def test_documents_read(ours):
    db = ours.t85read
    ours.drop_database("t85read")
    db.create_collection("c", customerKey="thread_id")
    n_cust, per = 200, 100
    docs = [{"_id": f"{c:03d}-{i:04d}", "thread_id": f"t{c:03d}", "i": i, "k": i % 7} for i in range(per) for c in range(n_cust)]
    for j in range(0, len(docs), 2000):
        db.c.insert_many(docs[j:j + 2000])
    # two documents whose customer value is an array: every customer query must also look at them (mongod matches
    # `{thread_id: X}` on an array holding X)
    db.c.insert_many([{"_id": "arr-1", "thread_id": ["t005", "t006"], "i": -1, "k": 0}, {"_id": "arr-2", "thread_id": ["zz"], "i": -2, "k": 0}])
    total = n_cust * per + 2
    cases = [
        ("customer and _id", {"thread_id": "t005", "_id": "005-0042"}, {"limit": 1, "singleBatch": True}, 1, 1),
        ("customer and _id, no limit", {"thread_id": "t005", "_id": "005-0042"}, {}, 1, 1),
        ("customer and _id $eq", {"thread_id": {"$eq": "t005"}, "_id": {"$eq": "005-0042"}}, {}, 1, 1),
        ("customer alone", {"thread_id": "t005"}, {}, per + 2, per + 1),
        ("customer alone, limit 1", {"thread_id": "t007"}, {"limit": 1}, per + 2, 1),
        ("customer $in of 3", {"thread_id": {"$in": ["t001", "t100", "t199"]}}, {}, 3 * per + 2, 3 * per),
        # (a range reads its bounds' keys inclusive, and the array bracket)
        ("customer and _id range", {"thread_id": "t009", "_id": {"$gte": "009-0010", "$lt": "009-0020"}}, {}, 11 + 2, 10),
        ("customer and _id range, sort _id -1 limit 2", {"thread_id": "t009", "_id": {"$gt": "009-0050"}}, {"sort": {"_id": -1}, "limit": 2}, 50 + 2, 2),
        ("customer and _id $in", {"thread_id": "t010", "_id": {"$in": ["010-0001", "010-0002", "011-0001"]}}, {}, 3, 2),
        ("customer, sort k", {"thread_id": "t011"}, {"sort": {"k": 1, "_id": 1}, "limit": 5}, per + 2, 5),
        ("customer and other field", {"thread_id": "t012", "k": 3}, {}, per + 2, 14),
        ("$and of customer and _id", {"$and": [{"thread_id": "t013"}, {"_id": "013-0099"}]}, {}, 1, 1),
    ]
    bad = []
    for name, flt, kw, most, nret in cases:
        t0 = time.time()
        got = list(db.c.find(flt, **{k: v for k, v in kw.items() if k in ("sort", "limit")}))
        ms = (time.time() - t0) * 1000
        x = examined(db, "c", flt, **kw)
        print(f"T85 read {name}: {x} documents examined of {total} (at most {most}), {len(got)} returned, {ms:.1f} ms", flush=True)
        if x > most:
            bad.append((name, x, most))
        if len(got) != nret:
            bad.append((name, "returned", len(got), nret))
    # a single update and a single delete by customer and _id pick the one document, without reading the collection
    t0 = time.time()
    r = db.c.update_one({"thread_id": "t020", "_id": "020-0005"}, {"$set": {"u": 1}})
    up_ms = (time.time() - t0) * 1000
    t0 = time.time()
    d = db.c.delete_one({"thread_id": "t021", "_id": "021-0006"})
    del_ms = (time.time() - t0) * 1000
    print(f"T85 read update_one {up_ms:.1f} ms (matched {r.matched_count}), delete_one {del_ms:.1f} ms (deleted {d.deleted_count})", flush=True)
    assert r.matched_count == 1 and d.deleted_count == 1
    assert db.c.find_one({"u": 1})["_id"] == "020-0005"
    assert not bad, bad


# ---------------------------------------------------------------------------------------------------------------------
# Same answers as mongod
# ---------------------------------------------------------------------------------------------------------------------

DT = datetime.datetime(2024, 5, 1)
OID = ObjectId("65f000000000000000000001")
# customer values of every kind the generator draws from (MISSING: no field)
MISSING = object()
CUSTS = ["a", "b", "A", "c", 1, Int64(1), 1.0, Decimal128("1"), Decimal128("1.0"), 2, 2.5, Decimal128("2.5"),
         Decimal128("0.1"), 0.1, -0.0, 0, Int64(2**53 + 1), float(2**53), None, MISSING, ["a", "b"], ["a"], [], [None],
         [1, 2], [[1, 2]], {"x": 1}, {"x": 1.0}, {"x": 1, "y": 2}, {"y": 2, "x": 1}, OID, True, DT, "", "a\x00b"]
# query values: every customer value plus values nobody holds
QVALS = [v for v in CUSTS if v is not MISSING] + ["zzz", 3, [1], {"x": 2}, [2, 1]]


def gen_docs(seed=85):
    rnd = random.Random(seed)
    docs = []
    ids = list(range(1, 241))
    rnd.shuffle(ids)
    extra_ids = ["s-a", "s-b", "s-m", "s-z", ObjectId("65f0000000000000000000a1"), 7.5, Decimal128("8.25"), DT, Int64(500)]
    for n, i in enumerate(ids + extra_ids):
        d = {"_id": i, "k": rnd.randrange(6), "s": rnd.choice(["p", "q", "P", "r"])}
        c = CUSTS[n % len(CUSTS)] if n < len(CUSTS) * 3 else rnd.choice(CUSTS)
        if c is not MISSING:
            d["thread_id"] = c
        docs.append(d)
    rnd.shuffle(docs)
    return docs


def prepare(c, dbname, ours):
    c.drop_database(dbname)
    db = c[dbname]
    if ours:
        db.create_collection("c", customerKey="thread_id")
    else:
        db.create_collection("c")
    docs = gen_docs()
    db.c.insert_many(docs[:120])
    for d in docs[120:]:
        db.c.insert_one(d)
    # moves between customers, in-place updates, a delete and a re-insert under another customer
    db.c.update_one({"_id": 17}, {"$set": {"thread_id": "b"}})
    db.c.update_one({"_id": 33}, {"$set": {"thread_id": ["a", "c"]}})
    db.c.update_one({"_id": 41}, {"$unset": {"thread_id": ""}})
    db.c.update_many({"k": 5}, {"$set": {"u": True}})
    db.c.delete_one({"_id": 52})
    db.c.insert_one({"_id": 52, "thread_id": "a", "k": 9, "s": "q"})
    return db


def shapes(rnd, opts=None):
    """(name, filter, options) for each customer value and each shape."""
    opts = OPTS if opts is None else opts
    out = []
    for v in QVALS:
        ids_any = [rnd.randrange(1, 260) for _ in range(4)]
        fl = [
            ("eq", {"thread_id": v}),
            ("$eq", {"thread_id": {"$eq": v}}),
            ("$in", {"thread_id": {"$in": [v, "b"]}}),
            ("$in1", {"thread_id": {"$in": [v]}}),
            ("and", {"$and": [{"thread_id": v}, {"k": {"$gte": 2}}]}),
            ("id_eq", {"thread_id": v, "_id": ids_any[0]}),
            ("id_eq_str", {"thread_id": v, "_id": "s-a"}),
            ("id_in", {"thread_id": v, "_id": {"$in": ids_any}}),
            ("id_range", {"thread_id": v, "_id": {"$gte": 20, "$lt": 160}}),
            ("id_gt_str", {"thread_id": v, "_id": {"$gt": "s-b"}}),
            ("id_lt", {"_id": {"$lt": 100}, "thread_id": v}),
            ("other", {"thread_id": v, "k": 2}),
            ("ne_id", {"thread_id": v, "_id": {"$ne": ids_any[1]}}),
            ("nin_id", {"thread_id": v, "_id": {"$nin": ids_any[:2]}}),
            ("type_id", {"thread_id": v, "_id": {"$type": "number"}}),
            ("ne_id_str", {"thread_id": v, "_id": {"$ne": "s-a"}}),
        ]
        for name, f in fl:
            for oname, opt in opts:
                out.append((f"{name}/{oname}/{v!r}", f, opt))
    # shapes the customer plan must leave to the full scan (still compared)
    for f in [{"thread_id": {"$in": [Regex("^a"), "b"]}}, {"thread_id": Regex("^A", "i")}, {"thread_id": {"$gt": "a"}},
              {"thread_id": float("nan")}, {"thread_id": {"$in": [float("nan"), 1]}}, {"thread_id": {"$eq": "a", "$ne": "b"}},
              {"thread_id": {"$in": ["a", "c"], "$nin": ["c"]}}, {"thread_id": {"$exists": False}}, {"thread_id": {"$not": {"$eq": "a"}}},
              {"$or": [{"thread_id": "a"}, {"thread_id": "b"}]}, {"thread_id.0": "a"}, {"thread_id": {"$size": 2}},
              {"thread_id": {"$elemMatch": {"$eq": "a"}}}, {"thread_id": {"$all": ["a", "b"]}}]:
        for oname, opt in opts:
            out.append((f"other/{oname}/{f!r}", f, opt))
    return out


OPTS = [
    ("plain", {}),
    ("limit1", {"limit": 1}),
    ("limit3", {"limit": 3}),
    ("skip1limit2", {"skip": 1, "limit": 2}),
    ("sort_id", {"sort": [("_id", 1)]}),
    ("sort_id_desc_limit2", {"sort": [("_id", -1)], "limit": 2}),
    ("sort_k_id", {"sort": [("k", 1), ("_id", -1)]}),
    ("sort_k_id_limit2", {"sort": [("k", 1), ("_id", 1)], "limit": 2}),
    ("proj", {"projection": {"k": 1, "_id": 0}}),
    ("natural_hint", {"hint": [("$natural", 1)]}),
    ("natural_hint_limit2", {"hint": [("$natural", 1)], "limit": 2}),
    ("natural_desc", {"hint": [("$natural", -1)], "limit": 4}),
    ("natural_desc_all", {"hint": [("$natural", -1)]}),
    ("collation", {"collation": {"locale": "en", "strength": 2}}),
    ("collation_limit1", {"collation": {"locale": "en", "strength": 2}, "limit": 1}),
]
# Differences from mongod that the base 0b143c41 has too, on plain collections as well (not the customer plan's): a hint
# of the `_id` index is ignored (mongod answers in `_id` order), and equal sort keys under a limit come in another order.
# `test_known_differences` keeps them in view (strict xfail: it fails once they are fixed, so this list gets updated).
KNOWN_OPTS = [
    ("id_hint", {"hint": [("_id", 1)], "limit": 3}),
    ("sort_k_limit2", {"sort": [("k", 1)], "limit": 2}),
]


def run_find(coll, f, opt):
    try:
        cur = coll.find(f, opt.get("projection"))
        for k in ("sort", "skip", "limit", "hint", "collation"):
            if k in opt:
                cur = getattr(cur, k)(opt[k])
        return [repr(d) for d in cur]
    except Exception as e:
        return f"error {getattr(e, 'code', None)}"


def answers(db, cases):
    out = []
    for name, f, opt in cases:
        out.append(run_find(db.c, f, opt))
    return out


def write_answers(db, filters):
    """The document a single update, a findOneAndUpdate and a single delete pick, for each filter."""
    out = []
    for f in filters:
        try:
            db.c.update_one(f, {"$set": {"w": 1}})
            a = [repr(d["_id"]) for d in db.c.find({"w": 1})]
            db.c.update_many({"w": 1}, {"$unset": {"w": ""}})
            b = db.c.find_one_and_update(f, {"$inc": {"fu": 1}}, projection={"_id": 1})
            c = db.c.count_documents(f)
            dd = sorted(repr(x) for x in db.c.distinct("k", f))
            # (the deleted document stays deleted on both: the later filters see the same data)
            gone = db.c.find_one_and_delete(f, projection={"_id": 1})
            out.append((a, repr(b), c, dd, repr(gone)))
        except Exception as e:
            out.append(f"error {getattr(e, 'code', None)}")
    return out


@pytest.mark.parametrize("layout", ["customer", "plain"])
def test_same_answers_as_mongod(ours, layout):
    real = ref_client()
    rdb = prepare(real, "t85same", False)
    odb = prepare(ours, "t85same", layout == "customer")
    rnd = random.Random(1)
    cases = shapes(rnd)
    a = answers(rdb, cases)
    b = answers(odb, cases)
    diff = [(cases[i][0], a[i], b[i]) for i in range(len(cases)) if a[i] != b[i]]
    print(f"T85 same ({layout}): {len(cases)} find shapes, {len(cases) - len(diff)} the same as mongod", flush=True)
    for d in diff[:15]:
        print("T85 DIFF", d, flush=True)
    # single writes: the same document picked; done last (they change the data)
    wf = []
    for v in ["a", "b", 1, Decimal128("2.5"), None, ["a"], {"x": 1}, "zzz", 0]:
        wf += [{"thread_id": v}, {"thread_id": v, "_id": {"$gte": 30}}, {"thread_id": {"$in": [v, "c"]}}, {"thread_id": v, "k": {"$lt": 3}}]
    wa = write_answers(rdb, wf)
    wb = write_answers(odb, wf)
    wdiff = [(wf[i], wa[i], wb[i]) for i in range(len(wf)) if wa[i] != wb[i]]
    print(f"T85 same: {len(wf)} write shapes, {len(wf) - len(wdiff)} the same as mongod", flush=True)
    for d in wdiff[:10]:
        print("T85 WDIFF", d, flush=True)
    # and the reads again after the writes moved and removed documents
    a2 = answers(rdb, cases[::7])
    b2 = answers(odb, cases[::7])
    diff2 = [(cases[::7][i][0], a2[i], b2[i]) for i in range(len(a2)) if a2[i] != b2[i]]
    print(f"T85 same after writes: {len(a2)} find shapes, {len(a2) - len(diff2)} the same as mongod", flush=True)
    for d in diff2[:10]:
        print("T85 DIFF2", d, flush=True)
    assert not diff and not wdiff and not diff2, (len(diff), len(wdiff), len(diff2))


def test_transaction_own_writes(ours):
    real = ref_client()
    a = txn_answers(real, False, move=False)
    b = txn_answers(ours, True, move=False)
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            print(f"T85 TXN DIFF {i}: mongod {x}\n ours {y}", flush=True)
    assert a == b


@pytest.mark.xfail(strict=True, reason="pre-existing on the base: a document moved to another customer inside a "
                   "transaction is listed after the others by that transaction's reads, not at its natural-order place")
def test_transaction_move_keeps_natural_place(ours):
    real = ref_client()
    assert txn_answers(real, False, move=True) == txn_answers(ours, True, move=True)


def txn_answers(c, is_ours, move):
    """A transaction's reads of its own writes to customers (inserts, an update, an array customer, a delete)."""
    db = prepare(c, "t85txn", is_ours)
    out = []
    with c.start_session() as s:
        s.start_transaction()
        db.c.insert_many([{"_id": 1000, "thread_id": "a", "k": 1}, {"_id": 999, "thread_id": "b", "k": 2}, {"_id": 1001, "thread_id": ["a"], "k": 0}], session=s)
        if move:
            db.c.update_one({"_id": 17}, {"$set": {"thread_id": "a"}}, session=s)
        db.c.update_one({"_id": 18}, {"$set": {"k": 77}}, session=s)
        db.c.update_one({"thread_id": "a", "_id": {"$gte": 0}}, {"$set": {"z": 1}}, session=s)
        db.c.delete_one({"thread_id": "b", "k": {"$gte": 0}}, session=s)
        for f, opt in [({"thread_id": "a"}, {}), ({"thread_id": "a"}, {"limit": 2}), ({"thread_id": {"$in": ["a", "b"]}}, {}),
                       ({"thread_id": "a", "_id": 1000}, {}), ({"thread_id": "a", "_id": {"$gt": 100}}, {"limit": 3}),
                       ({"thread_id": "b"}, {"sort": [("_id", -1)]}), ({"z": 1}, {})]:
            cur = db.c.find(f, session=s)
            for k in ("sort", "limit"):
                if k in opt:
                    cur = getattr(cur, k)(opt[k])
            out.append([repr(d) for d in cur])
        s.abort_transaction()
    return out


# ---------------------------------------------------------------------------------------------------------------------
# Streamed finds (fix-long-commands' page-by-page natural order) of one customer, and the race of span and scan
# ---------------------------------------------------------------------------------------------------------------------

def stream_data():
    """`big` first (3,000 documents: its first ones come first in natural order), then 200 small customers of 15
    documents interleaved, then `late` (its documents come last in natural order)."""
    docs = [{"_id": f"big-{i:05d}", "thread_id": "big", "k": i % 10, "v": "x" * (i % 40)} for i in range(3000)]
    docs += [{"_id": f"s{c:03d}-{i:03d}", "thread_id": f"s{c:03d}", "k": i % 10} for i in range(15) for c in range(200)]
    docs += [{"_id": f"late-{i:04d}", "thread_id": "late", "k": i % 10} for i in range(300)]
    return docs


def drain(db, cmd):
    r = db.command(cmd)
    first = r["cursor"]["firstBatch"]
    out, cid = list(first), r["cursor"]["id"]
    open_after_first = cid != 0
    while cid:
        g = db.command({"getMore": cid, "collection": cmd["find"], **({"batchSize": cmd["batchSize"]} if "batchSize" in cmd else {})})
        out += g["cursor"]["nextBatch"]
        cid = g["cursor"]["id"]
    return [d.get("_id") for d in out], len(first), open_after_first


STREAM_CASES = [
    ("big", {"filter": {"thread_id": "big"}}),
    ("big_batch", {"filter": {"thread_id": "big", "k": 3}, "batchSize": 37}),
    ("big_limit", {"filter": {"thread_id": "big"}, "limit": 5}),
    ("late", {"filter": {"thread_id": "late"}}),
    ("late_batch", {"filter": {"thread_id": "late"}, "batchSize": 13}),
    ("late_skip_limit", {"filter": {"thread_id": "late"}, "skip": 7, "limit": 250, "batchSize": 13}),
    ("small", {"filter": {"thread_id": "s042"}}),
    ("small_limit_exact", {"filter": {"thread_id": "s042"}, "limit": 15}),
    ("small_batch_exact", {"filter": {"thread_id": "s042"}, "batchSize": 15}),
    ("in", {"filter": {"thread_id": {"$in": ["s001", "late", "s199"]}}, "batchSize": 7}),
    ("in_big", {"filter": {"thread_id": {"$in": ["big", "late"]}}, "batchSize": 500}),
    ("none", {"filter": {"thread_id": "nobody"}}),
    ("single_batch", {"filter": {"thread_id": "late"}, "batchSize": 20, "singleBatch": True}),
    ("batch_zero", {"filter": {"thread_id": "late"}, "batchSize": 0}),
    ("projection", {"filter": {"thread_id": "late"}, "projection": {"k": 1}, "batchSize": 50}),
]


@pytest.fixture(scope="module")
def stream_both(ours):
    real = ref_client()
    docs = stream_data()
    for c, keyed in ((real, False), (ours, True)):
        c.drop_database("t85st")
        if keyed:
            c.t85st.create_collection("c", customerKey="thread_id")
        for b in range(0, len(docs), 1000):
            c.t85st.c.insert_many(docs[b:b + 1000])
    return real, ours


@pytest.mark.parametrize("name,extra", STREAM_CASES)
def test_streamed_customer_find_as_mongod(stream_both, name, extra):
    real, ours = stream_both
    cmd = {"find": "c", **extra}
    a, b = drain(real.t85st, cmd), drain(ours.t85st, cmd)
    print(f"T85 stream {name}: mongod {len(a[0])} first {a[1]} open {a[2]}; ours {len(b[0])} first {b[1]} open {b[2]}", flush=True)
    assert a == b


def test_streamed_customer_writes_between_getmores(stream_both):
    """Inserts, deletes, moves into and out of the customer between getMores: the same documents as mongod, none twice."""
    real, ours = stream_both
    res = []
    for c in (real, ours):
        db = c.t85st
        db.w.drop()
        if c is ours:
            db.create_collection("w", customerKey="thread_id")
        db.w.insert_many([{"_id": i, "thread_id": "a" if i % 3 else "b", "k": i} for i in range(600)])
        r = db.command({"find": "w", "filter": {"thread_id": "a"}, "batchSize": 100})
        got, cid = [d["_id"] for d in r["cursor"]["firstBatch"]], r["cursor"]["id"]
        db.w.insert_many([{"_id": 1000 + i, "thread_id": "a", "k": i} for i in range(50)])
        db.w.delete_many({"_id": {"$gte": 250, "$lt": 280}})
        db.w.update_many({"_id": {"$in": [300, 303, 450]}}, {"$set": {"thread_id": "a"}})   # into a (b before)
        db.w.update_many({"_id": {"$in": [301, 302, 451]}}, {"$set": {"thread_id": "b"}})   # out of a
        while cid:
            g = db.command({"getMore": cid, "collection": "w", "batchSize": 100})
            got += [d["_id"] for d in g["cursor"]["nextBatch"]]
            cid = g["cursor"]["id"]
        res.append(got)
    print(f"T85 stream writes: mongod {len(res[0])}, ours {len(res[1])}", flush=True)
    assert len(res[1]) == len(set(res[1])), "a document came twice"
    assert res[0] == res[1]


def test_race_takes_both_sides(stream_both):
    """`find_one({thread_id: "big"})`: the natural-order scan finds it at once (the span holds 3,000 documents);
    `find_one({thread_id: "late"})`: the span answers (the scan would read the whole collection first)."""
    real, ours = stream_both
    st = lambda: ours.admin.command("serverStatus")["fenvix"]["customerKeys"]
    s0 = st()
    for t in ("big", "late"):
        a = real.t85st.c.find_one({"thread_id": t})
        b = ours.t85st.c.find_one({"thread_id": t})
        assert a == b, (t, a, b)
    s1 = st()
    x_big = examined(ours.t85st, "c", {"thread_id": "big"}, limit=1)
    x_late = examined(ours.t85st, "c", {"thread_id": "late"}, limit=1)
    print(f"T85 race: span won {s1['raceSpanWon'] - s0['raceSpanWon']}, scan won {s1['raceNaturalWon'] - s0['raceNaturalWon']}; "
          f"documents examined: big {x_big}, late {x_late} (of {len(stream_data())})", flush=True)
    assert s1["raceSpanWon"] > s0["raceSpanWon"] and s1["raceNaturalWon"] > s0["raceNaturalWon"]
    assert x_late <= 2 * 300 + 64 and x_big <= 2 * 64 + 64, (x_big, x_late)


@pytest.mark.xfail(strict=True, reason="pre-existing on the base and on plain collections: a hint of the _id index is "
                   "ignored, equal sort keys under a limit come in another order than mongod's")
def test_known_differences(ours):
    real = ref_client()
    rdb = prepare(real, "t85known", False)
    odb = prepare(ours, "t85known", False)
    cases = shapes(random.Random(1), KNOWN_OPTS)
    assert answers(rdb, cases) == answers(odb, cases)
