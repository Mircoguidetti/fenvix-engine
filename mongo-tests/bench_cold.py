#!/usr/bin/env python3
"""Cold reads with and without indexes (results8, problem 1). For 10k and 100k documents and 0 / 1 / 3 indexes,
builds a tenant, closes it, and measures cold queries (tenant closed before every sample) with the server's own
object-store counters. Run with the cost-based document fetch off (MONGO_NO_FETCH_SCAN, the behaviour before) and on.
Output: out/cold.json"""
import argparse, json, os, random, signal, statistics, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh).
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ.setdefault(_k, _v)
import boto3, pymongo
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_cost as B

PORT = B.PORT
INDEXES = [[("k", 1)], [("g", 1)], [("ts", 1)]]
QUERIES = {
    "point_by_id": lambda d, n, i: d.c.find_one({"_id": (i * 7919) % n}),
    "eq_10_docs": lambda d, n, i: d.c.find({"k": (i * 31) % max(1, n // 10)}).to_list(),
    "eq_1pct_docs": lambda d, n, i: d.c.find({"g": i % 100}).to_list(),
}


def server(extra_env=None):
    env = dict(os.environ, **(extra_env or {}))
    log = open(os.path.join(B.HERE, "out", "cold-server.log"), "ab")
    p = subprocess.Popen([B.BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "cold", "--no-ttl"], stdout=log, stderr=log, env=env)
    for _ in range(100):
        try:
            pymongo.MongoClient(B.URI, serverSelectionTimeoutMS=300).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("no server")


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(30)
    except Exception:
        p.kill()


def ssts(db):
    n = 0
    for page in B.S3.get_paginator("list_objects_v2").paginate(Bucket="bench", Prefix=f"tenants/{db}/compacted/"):
        n += len(page.get("Contents", []))
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="10000,100000")
    ap.add_argument("--samples", type=int, default=9)
    ap.add_argument("--settle", type=int, default=20)
    ap.add_argument("--reuse", default="", help="run id of tenants built by an earlier run (skip the load)")
    ap.add_argument("--modes", default="before,after")
    ap.add_argument("--out", default=os.path.join(B.HERE, "out", "cold.json"))
    a = ap.parse_args()
    B.PERKEY = 10
    sizes = [int(x) for x in a.sizes.split(",")]
    names = {}
    if a.reuse:
        B.RUN = a.reuse
    p = server()
    c = pymongo.MongoClient(B.URI)
    for n in sizes:
        for ni in (0, 1, 3):
            db = f"cold{B.RUN}_{n}_{ni}"
            names[(n, ni)] = db
            if a.reuse:
                continue
            d = c[db]
            for k in INDEXES[:ni]:
                d.c.create_index(k)
            for s in range(0, n, 1000):
                d.c.insert_many(B.make_docs(s, min(1000, n - s), random.Random(s), n))
            d.command("tenantAdmin", close=True)
    if not a.reuse:
        time.sleep(a.settle)
    stop(p)
    out = {"run": B.RUN, "samples": a.samples, "modes": {}}
    for mode, env in (("before", {"MONGO_NO_FETCH_SCAN": "1"}), ("after", {})):
        if mode not in a.modes.split(","):
            continue
        p = server(env)
        c = pymongo.MongoClient(B.URI)
        res = {}
        for (n, ni), db in names.items():
            d = c[db]
            r = {"sst_objects": ssts(db)}
            for qn, fn in QUERIES.items():
                if qn.startswith("eq_") and ni == 0:
                    pass  # no index: the query is a full scan
                gets, byts, lat, docs = [], [], [], []
                for i in range(a.samples):
                    d.command("tenantAdmin", close=True)
                    s0 = B.stats(c)
                    t0 = time.time()
                    out_q = fn(d, n, i + 1)
                    lat.append((time.time() - t0) * 1000)
                    s1 = B.stats(c)
                    x = B.diff(s0, s1)
                    gets.append(x["get"] + x["list"])
                    byts.append(x["getBytes"])
                    docs.append(len(out_q) if isinstance(out_q, list) else (1 if out_q else 0))
                r[qn] = {"gets_median": statistics.median(gets), "get_bytes_median": statistics.median(byts), "ms_median": round(statistics.median(lat), 1), "ms_max": round(max(lat), 1), "docs_returned_median": statistics.median(docs)}
            res[f"{n}_{ni}"] = r
        out["modes"][mode] = res
        stop(p)
    json.dump(out, open(a.out, "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
