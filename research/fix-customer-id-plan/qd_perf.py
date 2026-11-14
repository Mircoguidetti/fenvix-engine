#!/usr/bin/env python3
"""Object-store reads and latency of common queries on 1,000,000 documents, cold and warm, one binary at a time on the
same MinIO data (run once per binary). Cold: the database is closed before the query. 3 trials each.
Each answer is hashed so that the two binaries can be checked to return the same documents in the same order.
Usage: perf.py --ours URI --label NAME [--load] [--out FILE]"""
import argparse, hashlib, json, random, time
import bson
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

ap = argparse.ArgumentParser()
ap.add_argument("--ours", required=True); ap.add_argument("--label", required=True)
ap.add_argument("--load", action="store_true"); ap.add_argument("--out")
ap.add_argument("--n", type=int, default=1_000_000); ap.add_argument("--trials", type=int, default=3)
a = ap.parse_args()
c = MongoClient(a.ours)
db = c.app_perf
if a.load:
    db.docs.drop(); t = time.time()
    order = list(range(a.n)); random.Random(3).shuffle(order)   # inserted out of _id order
    for s in range(0, a.n, 10_000):
        db.docs.insert_many([{"_id": i, "g": i % 1000, "name": f"user{i}", "pad": "x" * 100} for i in order[s:s + 10_000]], ordered=False)
    db.docs.create_index("g")
    print(f"loaded {a.n} in {time.time() - t:.0f} s"); db.command({"tenantAdmin": 1, "close": True})
docs = db.get_collection("docs", codec_options=CodecOptions(document_class=RawBSONDocument))
rng = random.Random(7)
IN = {k: [rng.sample(range(a.n * 2 if k >= 10_000 else a.n), k) for _ in range(a.trials)] for k in (100, 1000, 10_000, 100_000)}

def q_in(k):
    return lambda t: list(docs.find({"_id": {"$in": IN[k][t]}}).max_time_ms(900_000))
QUERIES = {
    "in 100": q_in(100), "in 1,000": q_in(1000), "in 10,000 (half missing)": q_in(10_000), "in 100,000 (half missing)": q_in(100_000),
    "in 100 + g filter": lambda t: list(docs.find({"_id": {"$in": IN[100][t]}, "g": {"$lt": 500}})),
    "in 100 sort g desc limit 5": lambda t: list(docs.find({"_id": {"$in": IN[100][t]}}).sort("g", -1).limit(5)),
    "agg $match in 100 + $group": lambda t: list(docs.aggregate([{"$match": {"_id": {"$in": IN[100][t]}}}, {"$group": {"_id": None, "s": {"$sum": "$g"}}}])),
    "count in 1,000": lambda t: [docs.count_documents({"_id": {"$in": IN[1000][t]}})],
    "_id equality": lambda t: list(docs.find({"_id": 4242 + t})),
    "_id range 1,000": lambda t: list(docs.find({"_id": {"$gte": 500_000 + t, "$lt": 501_000 + t}})),
    "g equality (index, 1,000 docs)": lambda t: list(docs.find({"g": 17 + t})),
    "g $in 3 values (index)": lambda t: list(docs.find({"g": {"$in": [1 + t, 500, 999]}})),
    "sort _id limit 10": lambda t: list(docs.find({}).sort("_id", 1).limit(10)),
    "sort _id desc limit 10 skip 100": lambda t: list(docs.find({}).sort("_id", -1).skip(100).limit(10)),
    "name equality (collection scan)": lambda t: list(docs.find({"name": f"user{77 + t}"})),
    "_id $nin 3 values, limit 20": lambda t: list(docs.find({"_id": {"$nin": [0, 1, 2]}}).limit(20)),
}

def snap():
    r = c.admin.command("tenantStats")["requests"]
    return r["get"], r["getBytes"]

out = {"label": a.label, "n": a.n, "results": {}}
for name, fn in QUERIES.items():
    rows = []
    for t in range(1 if "0,000" in name else a.trials):   # (10,000 and 100,000 ids: one trial; the old plan takes minutes)
        for mode in ("cold", "warm"):
            if mode == "cold":
                db.command({"tenantAdmin": 1, "close": True}); db.command("ping")
            g0, b0 = snap(); t0 = time.time()
            try:
                res = fn(t)
            except Exception as e:
                res = []; print(f"   error in {name} trial {t} {mode}: {str(e)[:120]}", flush=True)
            ms = (time.time() - t0) * 1000
            g1, b1 = snap()
            h = hashlib.sha1(b"".join(x.raw if hasattr(x, "raw") else bson.encode({"v": x}) for x in res)).hexdigest()[:12]
            rows.append({"trial": t, "mode": mode, "gets": g1 - g0, "bytes": b1 - b0, "ms": round(ms, 1), "n": len(res), "hash": h})
    out["results"][name] = rows
    cold = [r for r in rows if r["mode"] == "cold"]; warm = [r for r in rows if r["mode"] == "warm"]
    print(f"{a.label:6} {name:34} cold GETs {[r['gets'] for r in cold]} MB {[round(r['bytes'] / 1e6, 1) for r in cold]} ms {[r['ms'] for r in cold]} | warm GETs {[r['gets'] for r in warm]} ms {[r['ms'] for r in warm]} | n {cold[0]['n']}", flush=True)
if a.out:
    json.dump(out, open(a.out, "w"), indent=1)
