"""Latency of the LlamaIndex chat-store reads on a collection of 20,000 messages (50 sessions x 400), which the adapter
leaves without a secondary index: find({session_id}) sorted by index, and find_one sorted by index -1.
    probe_scan.py URI   prints p50 and p95 in ms over 200 calls of each"""
import statistics
import sys
import time

import pymongo

c = pymongo.MongoClient(sys.argv[1])
coll = c["probe_scan"]["sessions"]
coll.drop()
coll.insert_many([{"session_id": f"user-{s:03d}", "index": i, "role": "user", "content": "x" * 200, "additional_kwargs": {}}
                  for s in range(50) for i in range(400)])
for name, fn in [("find all of one session, sorted", lambda k: list(coll.find({"session_id": k}, sort=[("index", 1)]))),
                 ("find_one highest index", lambda k: coll.find_one({"session_id": k}, sort=[("index", -1)]))]:
    ts = []
    for n in range(200):
        t = time.perf_counter()
        fn(f"user-{n % 50:03d}")
        ts.append((time.perf_counter() - t) * 1000)
    ts.sort()
    print(f"{name}: p50 {statistics.median(ts):.1f} ms, p95 {ts[189]:.1f} ms")
c.drop_database("probe_scan")
