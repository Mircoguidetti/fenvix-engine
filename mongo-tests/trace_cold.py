#!/usr/bin/env python3
"""Per-request trace of a cold indexed equality lookup (results8, problem 1).
usage: trace_cold.py --docs 100000 --indexes 1 [--wait 0] [--query eq|scan|id]"""
import argparse, collections, json, os, random, signal, subprocess, sys, time
import pymongo
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_cost as B

ap = argparse.ArgumentParser()
ap.add_argument("--docs", type=int, default=100000)
ap.add_argument("--indexes", type=int, default=1)
ap.add_argument("--wait", type=int, default=0)
ap.add_argument("--query", default="eq")
ap.add_argument("--perkey", type=int, default=10)
ap.add_argument("--keep", action="store_true")
ap.add_argument("--raw", action="store_true")
a = ap.parse_args()
B.PERKEY = a.perkey
p = B.start()
c = pymongo.MongoClient(B.URI)
db = f"tr{B.RUN}_{a.docs}_{a.indexes}"
try:
    rnd = random.Random(7)
    d = c[db]
    d.c.create_index([("k", 1)])
    for k in B.IDX[1:a.indexes]:
        d.c.create_index(k)
    for s in range(0, a.docs, 1000):
        d.c.insert_many(B.make_docs(s, min(1000, a.docs - s), random.Random(s), a.docs))
    d.command("tenantAdmin", close=True)
    time.sleep(a.wait)
    q = {"eq": {"find": "c", "filter": {"k": 7}}, "scan": {"find": "c", "filter": {"payload": {"$ne": "x"}}}, "id": {"find": "c", "filter": {"_id": 5}}}[a.query]
    r = d.command("tenantTrace", run=q, cold=True)
    ev = r["events"]
    print(f"elapsed {r['elapsedUs']/1000:.1f} ms, {len(ev)} requests, ok={r['innerOk']}")
    cnt = collections.Counter(); byt = collections.Counter()
    for e in ev:
        kind = e["path"].split("/")[2] if e["path"].count("/") >= 2 else e["path"]
        key = (e["op"], kind)
        cnt[key] += 1; byt[key] += e["bytes"]
    for k, v in cnt.most_common():
        print(f"{k[0]:12s} {k[1]:14s} n={v:5d} bytes={byt[k]}")
    ph = collections.Counter(); phb = collections.Counter()
    for e in ev:
        ph[e["phase"]] += 1; phb[e["phase"]] += e["bytes"]
    print("by phase:", {k: (ph[k], phb[k]) for k in ph})
    if a.raw:
        for e in ev[:80]:
            print(e["startUs"], e["durUs"], e["op"], e["path"], e["bytes"])
    tree = collections.Counter(e["path"].split("/")[-1][:12] for e in ev if "compacted" in e["path"])
    print("distinct sst objects:", len(tree), "top:", tree.most_common(5))
finally:
    p.send_signal(signal.SIGTERM); p.wait(30)
