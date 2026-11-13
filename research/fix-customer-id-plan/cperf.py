#!/usr/bin/env python3
"""fix-customer-id-plan: object-store reads, documents examined and latency of queries by customer on a customer-keyed
collection of N documents (default 1,000,000: 10,000 customers of 100 documents, inserted in random order), cold and
warm, one binary at a time on the same MinIO data (perf.py's method, fix-query-defects). Cold: the database is closed
before the query. Each answer is hashed so the binaries can be checked to return the same documents in the same order.
Usage: cperf.py --ours URI --label NAME [--load] [--out FILE] [--trials 3]"""
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
c = MongoClient(a.ours, socketTimeoutMS=1_800_000)
db = c.app_cperf
CUST = a.n // 100
if a.load:
    db.docs.drop(); db.create_collection("docs", customerKey="thread_id"); t = time.time()
    order = [(cu, i) for cu in range(CUST) for i in range(100)]; random.Random(3).shuffle(order)
    for s in range(0, a.n, 10_000):
        db.docs.insert_many([{"_id": f"{cu:05d}-{i:03d}", "thread_id": f"t{cu:05d}", "i": i, "k": i % 7, "pad": "x" * 100}
                             for cu, i in order[s:s + 10_000]], ordered=False)
    print(f"loaded {a.n} in {time.time() - t:.0f} s"); db.command({"tenantAdmin": 1, "close": True})
docs = db.get_collection("docs", codec_options=CodecOptions(document_class=RawBSONDocument))
rng = random.Random(7)
CS = [[rng.randrange(CUST) for _ in range(3)] for _ in range(a.trials)]
T = lambda cu: f"t{cu:05d}"
QUERIES = {
    "find_one customer and _id": lambda t: [docs.find_one({"thread_id": T(CS[t][0]), "_id": f"{CS[t][0]:05d}-042"})],
    "find customer and _id": lambda t: list(docs.find({"thread_id": T(CS[t][0]), "_id": f"{CS[t][0]:05d}-042"})),
    "customer alone (100 docs)": lambda t: list(docs.find({"thread_id": T(CS[t][0])})),
    "customer alone, limit 1": lambda t: list(docs.find({"thread_id": T(CS[t][1])}).limit(1)),
    "customer $in 3": lambda t: list(docs.find({"thread_id": {"$in": [T(x) for x in CS[t]]}})),
    "customer and _id range 10": lambda t: list(docs.find({"thread_id": T(CS[t][2]), "_id": {"$gte": f"{CS[t][2]:05d}-010", "$lt": f"{CS[t][2]:05d}-020"}})),
    "customer and _id $gt, limit 2": lambda t: list(docs.find({"thread_id": T(CS[t][2]), "_id": {"$gt": f"{CS[t][2]:05d}-050"}}).limit(2)),
    "customer, sort k limit 5": lambda t: list(docs.find({"thread_id": T(CS[t][1])}).sort([("k", 1), ("_id", 1)]).limit(5)),
    "customer and k": lambda t: list(docs.find({"thread_id": T(CS[t][0]), "k": 3})),
    "count customer": lambda t: [docs.count_documents({"thread_id": T(CS[t][1])})],
    "agg $match customer + $group": lambda t: list(docs.aggregate([{"$match": {"thread_id": T(CS[t][2])}}, {"$group": {"_id": None, "s": {"$sum": "$i"}}}])),
    "update_one customer and _id": lambda t: [docs.update_one({"thread_id": T(CS[t][0]), "_id": f"{CS[t][0]:05d}-007"}, {"$inc": {"u": 1}}).matched_count],
    "_id alone (locator)": lambda t: [docs.find_one({"_id": f"{CS[t][1]:05d}-001"})],
    "other field (whole collection)": lambda t: list(docs.find({"i": 1000 + t})),
}


def snap():
    r = c.admin.command("tenantStats")["requests"]
    return r["get"], r["getBytes"]


# the same shapes as explain (documents examined, trial 0)
EXPLAIN = {
    "find_one customer and _id": lambda: ({"thread_id": T(CS[0][0]), "_id": f"{CS[0][0]:05d}-042"}, {"limit": 1}),
    "find customer and _id": lambda: ({"thread_id": T(CS[0][0]), "_id": f"{CS[0][0]:05d}-042"}, {}),
    "customer alone (100 docs)": lambda: ({"thread_id": T(CS[0][0])}, {}),
    "customer alone, limit 1": lambda: ({"thread_id": T(CS[0][1])}, {"limit": 1}),
    "customer $in 3": lambda: ({"thread_id": {"$in": [T(x) for x in CS[0]]}}, {}),
    "customer and _id range 10": lambda: ({"thread_id": T(CS[0][2]), "_id": {"$gte": f"{CS[0][2]:05d}-010", "$lt": f"{CS[0][2]:05d}-020"}}, {}),
    "customer and _id $gt, limit 2": lambda: ({"thread_id": T(CS[0][2]), "_id": {"$gt": f"{CS[0][2]:05d}-050"}}, {"limit": 2}),
    "customer, sort k limit 5": lambda: ({"thread_id": T(CS[0][1])}, {"sort": {"k": 1, "_id": 1}, "limit": 5}),
    "customer and k": lambda: ({"thread_id": T(CS[0][0]), "k": 3}, {}),
    "_id alone (locator)": lambda: ({"_id": f"{CS[0][1]:05d}-001"}, {"limit": 1}),
    "other field (whole collection)": lambda: ({"i": 1000}, {}),
}


def examined(name):
    if name not in EXPLAIN:
        return None
    f, kw = EXPLAIN[name]()
    e = db.command("explain", {"find": "docs", "filter": f, **kw}, verbosity="executionStats")
    return e["executionStats"]["totalDocsExamined"]


out = {"label": a.label, "n": a.n, "results": {}}
for name, fn in QUERIES.items():
    rows = []
    for t in range(a.trials):
        for mode in ("cold", "warm"):
            if mode == "cold":
                db.command({"tenantAdmin": 1, "close": True}); db.command("ping")
            g0, b0 = snap(); t0 = time.time()
            try:
                res = fn(t)
            except Exception as e:
                res = []; print(f"   error in {name} trial {t} {mode}: {str(e)[:160]}", flush=True)
            ms = (time.time() - t0) * 1000
            g1, b1 = snap()
            h = hashlib.sha1(b"".join(x.raw if hasattr(x, "raw") else bson.encode({"v": x}) for x in res)).hexdigest()[:12]
            rows.append({"trial": t, "mode": mode, "gets": g1 - g0, "bytes": b1 - b0, "ms": round(ms, 1), "n": len(res), "hash": h})
    x = examined(name)
    out["results"][name] = {"rows": rows, "examined": x}
    cold = [r for r in rows if r["mode"] == "cold"]; warm = [r for r in rows if r["mode"] == "warm"]
    print(f"{a.label:6} {name:32} cold GETs {[r['gets'] for r in cold]} MB {[round(r['bytes'] / 1e6, 1) for r in cold]} ms {[r['ms'] for r in cold]} "
          f"| warm ms {[r['ms'] for r in warm]} | examined {x} | n {cold[0]['n']}", flush=True)
if a.out:
    json.dump(out, open(a.out, "w"), indent=1)
