#!/usr/bin/env python3
"""Latency of find-by-_id and a filtered find on a 1,000-document collection, warm and cold,
measured end to end through pymongo (client round trip including the server's tenant wake).

Cold = the database's tenant is closed (`tenantAdmin: {close: true}`) right before every sample, so
the sample includes the reader wake from object storage. Warm = the tenant is already open.
Real MongoDB is measured warm only, as a protocol/driver baseline on the same machine.
"""
import argparse
import json
import os
import random
import time

from pymongo import MongoClient

DOCS = 1000
PAD = "x" * 900  # ~1 KB documents


def stats(samples_ns):
    ms = sorted(s / 1e6 for s in samples_ns)
    n = len(ms)
    rank = lambda p: ms[min(n - 1, max(0, -(-int(p * n * 100) // 100) - 1))]
    return {"n": n, "p50_ms": round(rank(0.50), 3), "p99_ms": round(rank(0.99), 3), "min_ms": round(ms[0], 3), "max_ms": round(ms[-1], 3)}


def timed(fn, n, before=None):
    out = []
    for i in range(n):
        if before:
            before()
        t = time.perf_counter_ns()
        fn(i)
        out.append(time.perf_counter_ns() - t)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--real", default="mongodb://localhost:27017")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", "latency.json"))
    ap.add_argument("--warm-n", type=int, default=500)
    ap.add_argument("--cold-n", type=int, default=100)
    a = ap.parse_args()
    rng = random.Random(7)
    results = {}
    for label, uri, is_ours in (("mongo-server (slatedb on MinIO)", a.ours, True), ("MongoDB 7.0 (reference)", a.real, False)):
        c = MongoClient(uri, serverSelectionTimeoutMS=15000)
        db = c["bench_lat"]
        db.drop_collection("docs")
        db.docs.insert_many([{"_id": i, "n": i, "grp": i % 10, "name": f"user{i}", "pad": PAD} for i in range(DOCS)])
        coll = db.docs
        close = (lambda: db.command({"tenantAdmin": 1, "close": True})) if is_ours else None
        assert coll.count_documents({}) == DOCS

        def by_id(_):
            d = coll.find_one({"_id": rng.randrange(DOCS)})
            assert d is not None

        def filtered(_):
            lo = rng.randrange(DOCS - 10)
            r = list(coll.find({"n": {"$gte": lo, "$lt": lo + 10}}))
            assert len(r) == 10

        def filtered_grp(_):
            r = list(coll.find({"grp": rng.randrange(10)}))
            assert len(r) == 100

        def insert_one(i):
            coll.insert_one({"_id": 1_000_000 + rng.randrange(10**9), "n": -1, "pad": PAD})

        r = {}
        timed(by_id, 20)  # warm up connection + tenant
        r["ping (driver + protocol baseline)"] = {"warm": stats(timed(lambda _: db.command("ping"), a.warm_n))}
        r["find by _id"] = {"warm": stats(timed(by_id, a.warm_n))}
        r["filtered find, 10 of 1,000 docs"] = {"warm": stats(timed(filtered, a.warm_n // 2))}
        r["filtered find, 100 of 1,000 docs"] = {"warm": stats(timed(filtered_grp, a.warm_n // 5))}
        r["insert_one (1 KB doc, durable)"] = {"warm": stats(timed(insert_one, a.warm_n // 2))}
        if is_ours:
            r["find by _id"]["cold"] = stats(timed(by_id, a.cold_n, close))
            r["filtered find, 10 of 1,000 docs"]["cold"] = stats(timed(filtered, a.cold_n, close))
            r["filtered find, 100 of 1,000 docs"]["cold"] = stats(timed(filtered_grp, a.cold_n, close))
            r["insert_one (1 KB doc, durable)"]["cold"] = stats(timed(insert_one, a.cold_n, close))
            r["ping (driver + protocol baseline)"]["cold"] = stats(timed(lambda _: db.command("ping"), a.cold_n, close))
        results[label] = r
        c.drop_database("bench_lat")
        c.close()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(results, open(a.out, "w"), indent=1)
    for label, r in results.items():
        print(f"\n{label}")
        for op, st in r.items():
            for state, s in st.items():
                print(f"  {op:40s} {state:5s} n={s['n']:4d} p50={s['p50_ms']:8.3f} ms p99={s['p99_ms']:8.3f} ms")


if __name__ == "__main__":
    main()
