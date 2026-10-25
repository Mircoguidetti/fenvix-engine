#!/usr/bin/env python3
"""Object-store requests per operation, warm and cold, read from mongo-server's request counters
(`tenantAdmin` debug command). Prices are S3 Standard list prices: $0.005 per 1,000 PUT/LIST-tier
requests and $0.0004 per 1,000 GET-tier requests."""
import argparse
import json
import os
import random

from pymongo import MongoClient

PAD = "x" * 900
PUT, GET = 0.005 / 1000, 0.0004 / 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ours", default="mongodb://localhost:27018")
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", "requests.json"))
    a = ap.parse_args()
    rng = random.Random(3)
    c = MongoClient(a.ours)
    db = c["bench_req"]
    c.drop_database("bench_req")
    db.docs.insert_many([{"_id": i, "n": i, "grp": i % 10, "name": f"user{i}", "pad": PAD} for i in range(1000)])
    close = lambda: db.command({"tenantAdmin": 1, "close": True})
    snap = lambda: db.command({"tenantAdmin": 1})["requests"]

    def measure(label, op, cold):
        puts = gets = dels = 0
        for _ in range(a.n):
            if cold:
                close()
            b = snap()
            op()
            e = snap()
            puts += e["putTier"] - b["putTier"]
            gets += e["getTier"] - b["getTier"]
            dels += e["delete"] - b["delete"]
        n = a.n
        r = {"put_tier": puts / n, "get_tier": gets / n, "delete": dels / n, "usd": (puts * PUT + gets * GET) / n}
        print(f"{label:48s} PUT/LIST-tier {r['put_tier']:6.2f}  GET-tier {r['get_tier']:7.2f}  DELETE {r['delete']:5.2f}  ${r['usd']:.8f}")
        return r

    ops = {
        "find by _id": lambda: db.docs.find_one({"_id": rng.randrange(1000)}),
        "filtered find (10 of 1,000 docs)": lambda: list(db.docs.find({"n": {"$gte": (lo := rng.randrange(990)), "$lt": lo + 10}})),
        "insert_one (1 KB, durable)": lambda: db.docs.insert_one({"_id": 10**6 + rng.randrange(10**9), "pad": PAD}),
    }
    out = {}
    for name, op in ops.items():
        op()
        out[name] = {"warm": measure(f"{name} [warm]", op, False), "cold": measure(f"{name} [cold]", op, True)}
    json.dump(out, open(a.out, "w"), indent=1)
    c.drop_database("bench_req")


if __name__ == "__main__":
    main()
