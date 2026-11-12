#!/usr/bin/env python3
"""Object-store reads of `find({_id: {$in: [...]}})` on a collection of N documents (default 1,000,000).

Usage: in_reads.py --ours mongodb://... [--load] [--n 1000000] [--k 100] [--trials 5]
--load (once) fills the collection `app_big.docs`; later runs (other binaries on the same MinIO data) only measure.
For each trial: close the database (cold: the next command opens it), then one find with k random `_id`s; the server's
request counters (`tenantStats`: GETs and bytes) before and after. Then the same find warm (database open)."""
import argparse, json, random, time
from pymongo import MongoClient

ap = argparse.ArgumentParser()
ap.add_argument("--ours", required=True)
ap.add_argument("--n", type=int, default=1_000_000)
ap.add_argument("--k", type=int, default=100)
ap.add_argument("--trials", type=int, default=5)
ap.add_argument("--load", action="store_true")
ap.add_argument("--label", default="")
a = ap.parse_args()
c = MongoClient(a.ours)
db = c.app_big
if a.load:
    db.docs.drop()
    t = time.time()
    B = 10_000
    for s in range(0, a.n, B):
        db.docs.insert_many([{"_id": i, "g": i % 1000, "name": f"user{i}", "pad": "x" * 100} for i in range(s, min(a.n, s + B))], ordered=False)
    print(f"loaded {a.n} documents in {time.time() - t:.0f} s")
    db.command({"tenantAdmin": 1, "close": True})
assert db.docs.estimated_document_count() >= a.n, "collection not loaded"

def snap():
    r = c.admin.command("tenantStats")["requests"]
    return r["get"], r["getBytes"], r["billedGetTier"]

rng = random.Random(7)
out = {"label": a.label, "n": a.n, "k": a.k, "cold": [], "warm": []}
for trial in range(a.trials):
    ids = rng.sample(range(a.n), a.k)
    for mode in ("cold", "warm"):
        if mode == "cold":
            db.command({"tenantAdmin": 1, "close": True})
            db.command("ping")  # (ping does not open the database)
        g0, b0, t0 = snap()
        t = time.time()
        docs = list(db.docs.find({"_id": {"$in": ids}}))
        ms = (time.time() - t) * 1000
        g1, b1, t1 = snap()
        assert sorted(d["_id"] for d in docs) == sorted(ids), "wrong answer"
        plan = db.docs.find({"_id": {"$in": ids}}).explain()["queryPlanner"]["winningPlan"]
        r = {"gets": g1 - g0, "get_bytes": b1 - b0, "billed_get_tier": t1 - t0, "ms": round(ms, 1), "plan": plan.get("inputStage", plan).get("stage")}
        out[mode].append(r)
        print(f"{a.label} trial {trial} {mode}: {len(docs)} documents, GETs {r['gets']}, bytes {r['get_bytes']:,}, {r['ms']} ms, plan {r['plan']}")
print(json.dumps(out))
