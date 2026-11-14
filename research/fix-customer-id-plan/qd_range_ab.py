#!/usr/bin/env python3
"""`_id` range and other plain queries on N documents (inserted out of `_id` order): latency, one server.
Usage: range_ab.py URI DB N"""
import random, sys, time
from pymongo import MongoClient

c = MongoClient(sys.argv[1]); db = c[sys.argv[2]]; N = int(sys.argv[3])
db.docs.drop(); order = list(range(N)); random.Random(3).shuffle(order)
for s in range(0, N, 10_000):
    db.docs.insert_many([{"_id": i, "g": i % 1000, "pad": "x" * 100} for i in order[s:s + 10_000]], ordered=False)
Q = {
    "_id range 1,000": lambda: db.docs.find({"_id": {"$gte": N // 2, "$lt": N // 2 + 1000}}),
    "_id range 1,000 sort _id": lambda: db.docs.find({"_id": {"$gte": N // 2, "$lt": N // 2 + 1000}}).sort("_id", 1),
    "_id $gt, limit 10": lambda: db.docs.find({"_id": {"$gt": N // 2}}).limit(10),
    "g equality (no index)": lambda: db.docs.find({"g": 17}),
    "_id $in 100": lambda: db.docs.find({"_id": {"$in": list(range(0, N, N // 100))}}),
}
for name, q in Q.items():
    ms = []
    for _ in range(2):
        t0 = time.time(); n = len(list(q())); ms.append(round((time.time() - t0) * 1000))
    print(f"{name:28} n {n:5}  ms {ms}", flush=True)
