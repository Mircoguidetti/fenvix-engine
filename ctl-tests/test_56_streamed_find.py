"""A plain find answered page by page in natural order (fix-long-commands), compared with mongod 7.0.

The reference mongod is the judge's (MONGOD_REF, default 127.0.0.1:27020); skipped when it does not answer. Every case
drains the cursor and compares the documents in order, the number of batches' worth of ids, and whether the first
batch's cursor is open. Fails on any difference.
"""
import os

import pytest
from bson import SON
from pymongo import MongoClient

from lib import Server, make_bucket

REF = os.environ.get("MONGOD_REF", "mongodb://127.0.0.1:27020/?directConnection=true")
N = 3000


@pytest.fixture(scope="module")
def both():
    real = MongoClient(REF, serverSelectionTimeoutMS=2000)
    try:
        real.admin.command("ping")
    except Exception:
        pytest.skip(f"no reference mongod at {REF}")
    s = Server(make_bucket("stream"), "stream", idle=600).start()
    ours = s.client()
    docs = [{"_id": (i * 7919) % N, "k": i % 10, "v": "x" * (i % 50)} for i in range(N)]   # natural order != _id order
    for c in (real, ours):
        c.drop_database("st")
        for b in range(0, N, 500):
            c.st.t.insert_many(docs[b:b + 500])
    yield real, ours
    s.term()


def drain(c, cmd):
    r = c.st.command(cmd)
    first = r["cursor"]["firstBatch"]
    out, cid = list(first), r["cursor"]["id"]
    open_after_first = cid != 0
    while cid:
        g = c.st.command(SON([("getMore", cid), ("collection", "t")] + ([("batchSize", cmd["batchSize"])] if "batchSize" in cmd else [])))
        out += g["cursor"]["nextBatch"]
        cid = g["cursor"]["id"]
    return [d.get("_id") for d in out], [sorted(d.keys()) for d in out[:3]], len(first), open_after_first


CASES = [
    ("all", {}),
    ("filter", {"filter": {"k": 3}}),
    ("filter_none", {"filter": {"k": 99}}),
    ("batch", {"filter": {"k": {"$lt": 5}}, "batchSize": 37}),
    ("skip_limit", {"skip": 7, "limit": 250, "batchSize": 13}),
    ("skip_big", {"skip": 2900, "batchSize": 40}),
    ("limit_small", {"limit": 5}),
    ("limit_exact_batch", {"limit": 101}),
    ("projection", {"filter": {"k": 1}, "projection": {"v": 0}, "batchSize": 50}),
    ("projection_incl", {"projection": {"k": 1}, "batchSize": 500}),
    ("single_batch", {"batchSize": 20, "singleBatch": True}),
    ("batch_zero", {"batchSize": 0}),
    ("natural_sort", {"sort": {"$natural": 1}, "batchSize": 100}),
]


@pytest.mark.parametrize("name,extra", CASES)
def test_streamed_find_answers_as_mongod(both, name, extra):
    real, ours = both
    cmd = SON([("find", "t")] + list(extra.items()))
    a, b = drain(real, cmd), drain(ours, cmd)
    print(f"{name}: mongod {len(a[0])} docs first {a[2]} open {a[3]}; ours {len(b[0])} first {b[2]} open {b[3]}")
    assert a == b


def test_inserts_between_getmores_appear_once(both):
    real, ours = both
    res = []
    for c in (real, ours):
        c.st.u.drop()
        c.st.u.insert_many([{"_id": i} for i in range(300)])
        r = c.st.command(SON([("find", "u"), ("batchSize", 100)]))
        got, cid = [d["_id"] for d in r["cursor"]["firstBatch"]], r["cursor"]["id"]
        c.st.u.insert_many([{"_id": 1000 + i} for i in range(50)])
        c.st.u.delete_many({"_id": {"$gte": 250, "$lt": 260}})
        while cid:
            g = c.st.command(SON([("getMore", cid), ("collection", "u"), ("batchSize", 100)]))
            got += [d["_id"] for d in g["cursor"]["nextBatch"]]
            cid = g["cursor"]["id"]
        res.append(got)
    print(f"mongod {len(res[0])}, ours {len(res[1])}")
    assert len(res[1]) == len(set(res[1])), "a document came twice"
    assert res[0] == res[1]
