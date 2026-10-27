#!/usr/bin/env python3
"""Cost measurements for indexes (results7, step 8). Starts its own mongo-server (port 27032, no idle close,
no TTL passes, compaction/GC on) against the MinIO bucket and measures with the server's own object-store counters
(`tenantStats`: requests and bytes through the counting store) and with a listing of the bucket.

  A. storage per tenant with 0, 1 and 3 indexes on the same data
  B. wake requests (GET/LIST/PUT counts) for a cold read and the first write, with 0, 1, 3 indexes
  C. write amplification: object-store bytes PUT per inserted document, 0, 1, 3 indexes
  D. indexed query vs full scan: latency and requests, 10k and 100k documents
Output: out/cost.json
"""
import argparse
import json
import os
import random
import signal
import statistics
import subprocess
import time

import boto3
import pymongo

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "mongo-server")
PORT = 27032
URI = f"mongodb://localhost:{PORT}/?directConnection=true"
RUN = f"{int(time.time()) % 100000}"
S3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")


def start(extra=()):
    log = open(os.path.join(HERE, "out", "cost-server.log"), "ab")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "cost", "--no-ttl", *extra], stdout=log, stderr=log)
    for _ in range(100):
        try:
            pymongo.MongoClient(URI, serverSelectionTimeoutMS=300).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("no server")


def stats(c):
    r = c.admin.command("tenantStats")["requests"]
    return dict(put=r["put"], get=r["get"], list=r["list"], head=r["head"], delete=r["delete"], putBytes=r["putBytes"], getBytes=r["getBytes"])


def diff(a, b):
    return {k: b[k] - a[k] for k in a}


def bucket_bytes(db):
    total, n = 0, 0
    for page in S3.get_paginator("list_objects_v2").paginate(Bucket="bench", Prefix=f"tenants/{db}/"):
        for o in page.get("Contents", []):
            total += o["Size"]
            n += 1
    return total, n


PERKEY = 10  # documents per value of `k` in the whole collection


def make_docs(start, n, rnd, total=None):
    total = total or n  # `k` has about 10 documents per value in the whole collection (not per batch)
    return [{"_id": i, "user": f"user{rnd.randint(0, 5000):05d}", "age": rnd.randint(18, 90), "ts": __import__("datetime").datetime(2024, 1, 1) + __import__("datetime").timedelta(seconds=rnd.randint(0, 10**7)), "tags": [f"t{rnd.randint(0, 50)}", f"t{rnd.randint(0, 50)}", f"t{rnd.randint(0, 50)}"], "payload": "x" * 100, "k": rnd.randint(0, max(1, total // PERKEY)), "g": rnd.randint(0, 99)} for i in range(start, start + n)]


IDX = [[("user", 1)], [("age", 1), ("ts", -1)], [("tags", 1)]]


def setup(c, db, n_idx, n_docs, rnd, indexes_first=True):
    d = c[db]
    d.drop_collection("c")
    if indexes_first:
        for k in IDX[:n_idx]:
            d.c.create_index(k)
    for s in range(0, n_docs, 1000):
        d.c.insert_many(make_docs(s, min(1000, n_docs - s), rnd, n_docs), ordered=True)
    if not indexes_first:
        for k in IDX[:n_idx]:
            d.c.create_index(k)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=10000)
    ap.add_argument("--big", type=int, default=100000)
    ap.add_argument("--samples", type=int, default=15)
    ap.add_argument("--compaction-wait", type=int, default=75)
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "out"), exist_ok=True)
    rnd = random.Random(7)
    p = start()
    c = pymongo.MongoClient(URI)
    out = {"run": RUN, "docs": a.docs, "doc_bytes_logical": None}
    try:
        # ---- A + B: storage and wake requests ----------------------------------------------------------------
        out["storage"] = {}
        out["wake"] = {}
        for n_idx in (0, 1, 3):
            db = f"cost{RUN}_s{n_idx}"
            setup(c, db, n_idx, a.docs, rnd)
            c[db].command("tenantAdmin", close=True)
            b0, f0 = bucket_bytes(db)
            time.sleep(a.compaction_wait)  # background compaction / GC of the closed tenant
            b1, f1 = bucket_bytes(db)
            st = c[db].command("collStats", "c")
            out["storage"][str(n_idx)] = {"after_close_bytes": b0, "after_close_objects": f0, "after_compaction_bytes": b1, "after_compaction_objects": f1, "docs_bytes": st["size"], "index_bytes_logical": st["totalIndexSize"]}
            c[db].command("tenantAdmin", close=True)
            # cold point read (wake as reader)
            s0 = stats(c)
            t0 = time.time()
            c[db].c.find_one({"_id": 5})
            t_read = time.time() - t0
            s1 = stats(c)
            # first write (upgrade to writer)
            t0 = time.time()
            c[db].c.insert_one({"_id": 10**9, "user": "new", "age": 30, "ts": __import__("datetime").datetime(2024, 5, 5), "tags": ["a", "b", "c"], "k": 1})
            t_write = time.time() - t0
            s2 = stats(c)
            c[db].command("tenantAdmin", close=True)
            s3 = stats(c)
            # cold indexed query (when an index exists) / cold full scan
            out["wake"][str(n_idx)] = {"cold_point_read": {**diff(s0, s1), "secs": round(t_read, 4)}, "first_write_after_wake": {**diff(s1, s2), "secs": round(t_write, 4)}, "clean_close": diff(s2, s3)}
        # ---- C: write amplification ----------------------------------------------------------------------------
        out["write_amp"] = {}
        for n_idx in (0, 1, 3):
            db = f"cost{RUN}_w{n_idx}"
            c[db].drop_collection("c")
            for k in IDX[:n_idx]:
                c[db].c.create_index(k)
            docs = make_docs(0, 3000, rnd)
            logical = sum(len(__import__("bson").encode(d)) for d in docs)
            s0 = stats(c)
            t0 = time.time()
            for d in docs[:1000]:
                c[db].c.insert_one(d)  # one durable write per document
            t_single = time.time() - t0
            s1 = stats(c)
            t0 = time.time()
            for s in range(1000, 3000, 100):
                c[db].c.insert_many(docs[s:s + 100])  # 100 documents per batch
            t_batch = time.time() - t0
            s2 = stats(c)
            one = diff(s0, s1)
            bat = diff(s1, s2)
            avg_doc = logical / len(docs)
            out["write_amp"][str(n_idx)] = {"avg_doc_bytes": round(avg_doc, 1), "single_insert": {"puts": one["put"], "put_bytes": one["putBytes"], "put_bytes_per_doc": round(one["putBytes"] / 1000, 1), "puts_per_doc": round(one["put"] / 1000, 3), "docs_per_sec": round(1000 / t_single, 1)}, "batch_100": {"puts": bat["put"], "put_bytes": bat["putBytes"], "put_bytes_per_doc": round(bat["putBytes"] / 2000, 1), "puts_per_doc": round(bat["put"] / 2000, 4), "docs_per_sec": round(2000 / t_batch, 1)}}
        # ---- D: indexed query vs full scan ---------------------------------------------------------------------------
        out["query"] = {}
        for n_docs in (a.docs, a.big):
            for indexed in (False, True):
                db = f"cost{RUN}_q{n_docs}_{int(indexed)}"
                d = c[db]
                d.drop_collection("c")
                for s in range(0, n_docs, 1000):
                    d.c.insert_many(make_docs(s, min(1000, n_docs - s), random.Random(s), n_docs))
                if indexed:
                    d.c.create_index([("k", 1)])
                    d.c.create_index([("ts", 1)])
                key = f"{n_docs}_{'indexed' if indexed else 'scan'}"
                res = {}
                queries = {
                    "eq_10_docs": lambda i: d.c.find({"k": i % max(1, n_docs // 10)}).to_list(),
                    "range_100_docs": lambda i: d.c.find({"ts": {"$gte": __import__("datetime").datetime(2024, 1, 1), "$lt": __import__("datetime").datetime(2024, 1, 1) + __import__("datetime").timedelta(seconds=10**7 * 100 // n_docs)}}).to_list(),
                    "sort_limit_10": lambda i: d.c.find({}).sort("ts", -1).limit(10).to_list(),
                    "point_by_id": lambda i: d.c.find_one({"_id": i * 7 % n_docs}),
                }
                for qn, fn in queries.items():
                    cold, warm = [], []
                    cold_req, warm_req = [], []
                    for i in range(a.samples):
                        d.command("tenantAdmin", close=True)
                        s0 = stats(c)
                        t0 = time.time()
                        r = fn(i)
                        cold.append(time.time() - t0)
                        s1 = stats(c)
                        cold_req.append(diff(s0, s1))
                        t0 = time.time()
                        fn(i)
                        warm.append(time.time() - t0)
                        s2 = stats(c)
                        warm_req.append(diff(s1, s2))
                    med = lambda xs: round(statistics.median(xs), 4)
                    res[qn] = {
                        "cold_ms_median": round(med(cold) * 1000, 1), "cold_ms_p95": round(sorted(cold)[int(0.95 * (len(cold) - 1))] * 1000, 1),
                        "warm_ms_median": round(med(warm) * 1000, 1),
                        "cold_gets_median": statistics.median([x["get"] + x["list"] for x in cold_req]), "cold_get_bytes_median": statistics.median([x["getBytes"] for x in cold_req]),
                        "warm_gets_median": statistics.median([x["get"] + x["list"] for x in warm_req]),
                    }
                out["query"][key] = res
        json.dump(out, open(os.path.join(HERE, "out", "cost.json"), "w"), indent=1, default=str)
        print(json.dumps(out, indent=1, default=str))
    finally:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(30)
        except Exception:
            p.kill()


if __name__ == "__main__":
    main()
