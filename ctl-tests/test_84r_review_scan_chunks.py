"""Review of big-speed-on-i4 0b143c41 (2026-11-13): document scans now read chunks of about 8 MiB (`coll.rs` ChunkSize),
starting at 64 rows, instead of a number of rows. A chunk boundary falls every few rows when documents are large, so this
test mixes small documents with 1.5 MiB ones and checks every scan loop that changed against mongod (same documents,
same order of insertion): no document missed, none twice, the same order where the query fixes one, through getMore.
Then a cursor in small batches runs while documents are inserted, updated and deleted: every untouched document comes
back exactly once.
"""
import os
import random
import threading
import time

import pytest
from bson import SON
from pymongo import MongoClient, ASCENDING, DESCENDING

from lib import Server, make_bucket

DB = "chunks"
REF = "rv_chunks"


def docs():
    rnd = random.Random(84)
    out = []
    for i in range(420):
        big = i % 7 == 3
        size = 1_500_000 if big else rnd.choice([200, 2_000, 20_000, 50_000])
        out.append(SON([("_id", i), ("thread_id", f"t{i % 5}"), ("k", rnd.randint(0, 50)), ("v", rnd.random()), ("big", big), ("pad", "x" * size)]))
    return out


def load(c, d, ref=False):
    for coll, kw in (("ck", {} if ref else {"customerKey": "thread_id"}), ("plain", {})):
        try:
            c[coll].drop()
        except Exception:
            pass
        if kw:
            c.create_collection(coll, **kw)
        c[coll].create_index([("k", 1)])
        for i in range(0, len(d), 10):
            c[coll].insert_many(d[i:i + 10], ordered=True)


def proj(cur):
    return [(x["_id"], len(x.get("pad", "")), x.get("k"), x.get("v")) for x in cur]


def test_every_changed_scan_loop_against_mongod():
    bucket = make_bucket("chunks")
    srv = Server(bucket, "chunks", idle=600).start()
    try:
        o = srv.client(document_class=SON)[DB]
        r = MongoClient("mongodb://127.0.0.1:27020/?directConnection=true", document_class=SON)[REF]
        d = docs()
        load(o, d)
        load(r, d, ref=True)
        rnd = random.Random(7)
        wrong = []

        def same(what, f):
            x, y = f(o), f(r)
            if x != y:
                wrong.append((what, len(x) if hasattr(x, "__len__") else x, len(y) if hasattr(y, "__len__") else y))
        ids_some = rnd.sample(range(420), 150)
        for coll in ("ck", "plain"):
            same(f"{coll} all by _id", lambda c: proj(c[coll].find({}).sort("_id", 1)))
            same(f"{coll} all by _id desc, getMore of 3", lambda c: proj(c[coll].find({}).sort("_id", -1).batch_size(3)))
            same(f"{coll} no sort, as a set", lambda c: sorted(proj(c[coll].find({}))))
            same(f"{coll} no sort, getMore of 2, as a set", lambda c: sorted(proj(c[coll].find({}).batch_size(2))))
            same(f"{coll} filter scan", lambda c: sorted(proj(c[coll].find({"v": {"$gt": 0.3}, "big": False}))))
            same(f"{coll} filter on big", lambda c: sorted(proj(c[coll].find({"big": True}).batch_size(5))))  # a 16 MiB batch cap is command-cpu 33be919
            same(f"{coll} _id $in", lambda c: proj(c[coll].find({"_id": {"$in": ids_some}}).sort("_id", 1)))
            same(f"{coll} _id range", lambda c: proj(c[coll].find({"_id": {"$gte": 37, "$lt": 333}}).sort("_id", 1)))
            same(f"{coll} _id range no sort, as a set", lambda c: sorted(proj(c[coll].find({"_id": {"$gte": 37, "$lt": 333}}))))
            same(f"{coll} _id range limit", lambda c: proj(c[coll].find({"_id": {"$gte": 100}}).sort("_id", 1).limit(17)))
            same(f"{coll} index k limit", lambda c: proj(c[coll].find({"k": {"$gte": 10}}).sort([("k", 1), ("_id", 1)]).limit(120)))
            same(f"{coll} index k all, getMore of 4", lambda c: proj(c[coll].find({"k": {"$lt": 40}}).sort([("k", -1), ("_id", 1)]).batch_size(4)))
            same(f"{coll} top 5 by v", lambda c: proj(c[coll].find({}).sort([("v", -1)]).limit(5)))
            same(f"{coll} count", lambda c: c[coll].count_documents({"big": True}))
            for t in ("t0", "t3"):
                same(f"{coll} customer {t}", lambda c: proj(c[coll].find({"thread_id": t}).sort("_id", 1)))
                same(f"{coll} find_one customer+_id", lambda c: proj([c[coll].find_one({"thread_id": t, "_id": 3 if t == "t3" else 0})]))
        if coll == "plain":
            same("plain $natural order", lambda c: proj(c["plain"].find({}).hint([("$natural", 1)]).batch_size(3)))
        print(f"scan loops: {len(wrong)} wrong: {wrong[:6]}")
        assert not wrong, wrong
    finally:
        srv.term()


def test_cursor_in_small_batches_while_documents_change():
    bucket = make_bucket("chunkmut")
    srv = Server(bucket, "chunkmut", idle=600).start()
    try:
        c = srv.client(document_class=SON)[DB]
        d = docs()
        load(c, d)
        for coll in ("ck", "plain"):
            touched = set()
            cur = c[coll].find({}).batch_size(3)
            seen = []
            stop = threading.Event()

            def change():
                w = srv.client(document_class=SON)[DB][coll]
                rnd = random.Random(5)
                k = 1000
                while not stop.is_set():
                    i = rnd.randrange(420)
                    touched.add(i)
                    op = rnd.random()
                    if op < 0.3:
                        w.delete_one({"_id": i})
                    elif op < 0.6:
                        w.update_one({"_id": i}, {"$set": {"pad": "y" * rnd.choice([10, 900_000])}})
                    else:
                        w.insert_one({"_id": k, "thread_id": f"t{k % 5}", "pad": "z" * 30_000})
                        k += 1
                    time.sleep(0.01)
            th = threading.Thread(target=change, daemon=True)
            th.start()
            for x in cur:
                seen.append(x["_id"])
            stop.set()
            th.join()
            dup = len(seen) - len(set(seen))
            untouched = [i for i in range(420) if i not in touched]
            missing = [i for i in untouched if i not in set(seen)]
            print(f"{coll}: cursor returned {len(seen)}, {dup} twice; {len(touched)} touched, {len(untouched)} untouched, {len(missing)} of those missing")
            assert dup == 0, f"{coll}: documents returned twice"
            assert not missing, f"{coll}: untouched documents missing {missing[:10]}"
    finally:
        srv.term()
