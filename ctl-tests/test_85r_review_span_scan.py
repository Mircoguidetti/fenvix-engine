"""Review of command-cpu b60a55c (2026-11-13): in a customer-keyed collection, the documents one query wants are read
with one capped span scan instead of one point read each (`coll.rs` docs_by_span; past 2n+32 rows it falls back).

Never a missed or extra document: the same reads on our server with the span scan on, with it off
(FENVIX_NO_SPAN_READS), and on mongod with the same documents. The data has awkward customers: a customer field that is
an array, null, missing or a number, `_id`s of mixed types, documents moved to another customer by an update, deleted
ones, and a customer with many documents between two wanted ones (a sparse span: the fallback).
"""
import os
import random

import pytest
from bson import SON
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket


DB = "span"
EXTRA = ["--customer-key", "c=thread_id"]


def load(c, rnd):
    docs = []
    for i in range(600):
        t = rnd.random()
        if t < 0.05:
            tid = ["t1", "t2"]
        elif t < 0.08:
            tid = None
        elif t < 0.10:
            tid = 7
        else:
            tid = f"t{rnd.randrange(6)}"
        _id = i if i % 3 else (f"s{i}" if i % 2 else SON([("a", i)]))
        d = SON([("_id", _id), ("k", rnd.randrange(40)), ("v", i)])
        if t >= 0.12 or t < 0.10:
            d["thread_id"] = tid
        docs.append(d)
    for i in range(0, len(docs), 50):
        c["c"].insert_many(docs[i:i + 50], ordered=True)
    c["c"].create_index([("k", 1)])
    c["c"].create_index([("thread_id", 1), ("k", 1)])
    # moves: a document changes customer; deletes
    for i in range(1, 600, 37):
        c["c"].update_one({"_id": i}, {"$set": {"thread_id": "t5", "moved": True}})
    for i in range(2, 600, 41):
        c["c"].delete_one({"_id": i})
    # a dense customer whose documents lie between two wanted ones of another customer (a sparse span)
    c["c"].insert_many([SON([("_id", 10_000 + j), ("thread_id", "t3"), ("k", 99), ("v", -j)]) for j in range(400)])
    # wanted documents on both sides of that block: their span holds the 400 rows between them
    c["c"].insert_many([SON([("_id", 20_000 + j), ("thread_id", "t3"), ("k", 3), ("v", j)]) for j in range(5)])
    # the owner's sparse shape: 5 "rare" documents of t4 spread over 300 others (ObjectId _ids in insertion order)
    from bson import ObjectId
    for j in range(300):
        c["c"].insert_one(SON([("_id", ObjectId(f"{0x6500000000 + j * 2:024x}")), ("thread_id", "t4"), ("ns", ""), ("k", j % 40)]))
        if j % 60 == 30:
            c["c"].insert_one(SON([("_id", ObjectId(f"{0x6500000000 + j * 2 + 1:024x}")), ("thread_id", "t4"), ("ns", "rare"), ("k", j % 40)]))
    c["c"].create_index([("thread_id", 1), ("ns", 1), ("_id", 1)])


def reads(c):
    out = {}
    q = c["c"]
    for k in (0, 5, 17, 39, 99):
        out[f"k={k}"] = list(q.find({"k": k}).sort("_id", 1))
        out[f"k>={k} by k"] = list(q.find({"k": {"$gte": k}}).sort([("k", 1), ("_id", 1)]).limit(80))
    for t in ("t0", "t1", "t2", "t3", "t5", None, 7, ["t1", "t2"]):
        out[f"thread {t!r}"] = list(q.find({"thread_id": t}).sort("_id", 1))
        out[f"thread {t!r} k<20"] = list(q.find({"thread_id": t, "k": {"$lt": 20}}).sort([("k", 1), ("_id", 1)]))
    out["no thread"] = list(q.find({"thread_id": {"$exists": False}}).sort("_id", 1))
    out["$in t1,t3"] = list(q.find({"thread_id": {"$in": ["t1", "t3"]}, "k": {"$in": [3, 4, 99]}}).sort("_id", 1))
    out["moved"] = list(q.find({"moved": True}).sort("_id", 1))
    out["t3 k in 3,4 (sparse span)"] = list(q.find({"thread_id": "t3", "k": {"$in": [3, 4]}}).sort("_id", 1))
    out["t4 rare (sparse span)"] = list(q.find({"thread_id": "t4", "ns": "rare"}).sort("_id", 1))
    out["t4 ns '' newest 40"] = list(q.find({"thread_id": "t4", "ns": ""}).sort("_id", -1).limit(40))
    out["t3 k=3 by k"] = list(q.find({"thread_id": "t3", "k": 3}).sort([("k", 1), ("_id", -1)]))
    out["count t3"] = q.count_documents({"thread_id": "t3"})
    return out


def run(bucket, tag, env, rnd_seed):
    s = Server(bucket, tag, idle=600, env=env, extra=EXTRA).start()
    try:
        c = s.client(document_class=SON)
        load(c[DB], random.Random(rnd_seed))
        c.close()
        s.term()
        s = Server(bucket, tag, idle=600, env=env, extra=EXTRA).start()   # reads from storage, as the owner's test
        c = s.client(document_class=SON)
        r = reads(c[DB])
        ck = c.admin.command("serverStatus")["fenvix"]["customerKeys"]
        return r, ck
    finally:
        s.term()


def test_span_scan_never_misses_or_adds_a_document():
    ref = MongoClient("mongodb://127.0.0.1:27020/?directConnection=true", document_class=SON)["rv_span"]
    ref.client.drop_database("rv_span")
    load(ref, random.Random(85))
    want = reads(ref)
    on, ck_on = run(make_bucket("spanon"), "spanon", {"MONGO_NO_FETCH_SCAN": "1"}, 85)
    off, ck_off = run(make_bucket("spanoff"), "spanoff", {"MONGO_NO_FETCH_SCAN": "1", "FENVIX_NO_SPAN_READS": "1"}, 85)
    bad_on = [k for k in want if on.get(k) != want[k]]
    bad_off = [k for k in want if off.get(k) != want[k]]
    sizes = {k: len(v) if isinstance(v, list) else v for k, v in want.items()}
    print(f"span on: spanScans {ck_on.get('spanScans')} spanGaveUp {ck_on.get('spanGaveUp')}; off: {ck_off.get('spanScans')}; "
          f"reads {len(want)}, wrong with the span on {bad_on}, off {bad_off}; sizes {sizes}")
    assert ck_on.get("spanScans", 0) > 0, "the span scan did not run"  # its fallback is shown by the owner's test_70
    assert not bad_on, [(k, len(on.get(k) or []), len(want[k]) if isinstance(want[k], list) else want[k]) for k in bad_on]
    assert not bad_off, bad_off
