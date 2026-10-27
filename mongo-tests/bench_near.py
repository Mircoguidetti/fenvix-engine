#!/usr/bin/env python3
"""Cost of $near / $nearSphere without $maxDistance on MinIO (results10): the number of object-store requests, bytes read
and latency, cold (tenant closed before each sample) and warm, for 10k and 100k documents with a 2dsphere and a 2d index.
The same data is used for the "before" and "after" binaries (--bin, --skip-load).
Output: out/cost_near_<label>.json"""
import argparse, json, os, random, statistics, sys, time
import pymongo
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_cost as B
import bench_geo_text as G


def docs(start, n, rnd):
    out = []
    for i in range(start, start + n):
        out.append({"_id": i, "k": i % 97, "loc": {"type": "Point", "coordinates": [round(rnd.uniform(-180, 180), 5), round(rnd.uniform(-85, 85), 5)]},
                    "xy": [round(rnd.uniform(-100, 100), 5), round(rnd.uniform(-100, 100), 5)], "payload": "x" * 100})
    return out


CENTER = lambda i: [-150 + (i * 31) % 300, -70 + (i * 13) % 140]
QUERIES = {
    "near_sphere_limit1": lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}}}}, {"_id": 1}).limit(1))),
    "near_sphere_limit10": lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}}}}, {"_id": 1}).limit(10))),
    "near_sphere_limit100": lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}}}}, {"_id": 1}).limit(100))),
    "near_geometry_limit10_and_filter": lambda c, i: len(list(c.find({"k": 5, "loc": {"$near": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}}}}, {"_id": 1}).limit(10))),
    "near_sphere_maxdistance_100km_limit10": lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}, "$maxDistance": 100000}}}, {"_id": 1}).limit(10))),
    "near_2d_limit10": lambda c, i: len(list(c.find({"xy": {"$near": [CENTER(i)[0] / 2, CENTER(i)[1] / 2]}}, {"_id": 1}).limit(10))),
    "near_sphere_no_limit": lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": CENTER(i)}}}}, {"_id": 1}))),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--sizes", default="10000,100000")
    ap.add_argument("--samples", type=int, default=5)
    ap.add_argument("--skip-load", action="store_true")
    ap.add_argument("--prefix", default="nr")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    B.BIN = a.bin
    sizes = [int(x) for x in a.sizes.split(",")]
    p = G.server()
    c = pymongo.MongoClient(B.URI)
    try:
        for n in sizes:
            db = f"{a.prefix}_{n}"
            d = c[db]
            if not a.skip_load:
                d.c.drop()
                d.c.create_index([("loc", "2dsphere")])
                d.c.create_index([("xy", "2d")])
                d.c.create_index([("k", 1)])
                rnd = random.Random(11)
                for s in range(0, n, 1000):
                    d.c.insert_many(docs(s, min(1000, n - s), rnd))
                d.command("tenantAdmin", close=True)
        time.sleep(5)
        out = {"label": a.label, "bin": a.bin, "samples": a.samples, "results": {}}
        for n in sizes:
            d = c[f"{a.prefix}_{n}"]
            res = {}
            for qn, fn in QUERIES.items():
                if a.only and a.only not in qn:
                    continue
                cold, warm, hits = {"gets": [], "bytes": [], "ms": []}, {"gets": [], "bytes": [], "ms": []}, []
                for i in range(a.samples):
                    d.command("tenantAdmin", close=True)
                    s0 = B.stats(c)
                    t0 = time.time()
                    hits.append(fn(d.c, i + 1))
                    ms = (time.time() - t0) * 1000
                    x = B.diff(s0, B.stats(c))
                    cold["gets"].append(x["get"] + x["list"]); cold["bytes"].append(x["getBytes"]); cold["ms"].append(ms)
                    s0 = B.stats(c)
                    t0 = time.time()
                    fn(d.c, i + 1)
                    ms = (time.time() - t0) * 1000
                    x = B.diff(s0, B.stats(c))
                    warm["gets"].append(x["get"] + x["list"]); warm["bytes"].append(x["getBytes"]); warm["ms"].append(ms)
                med = lambda v: round(statistics.median(v), 1)
                res[qn] = {"results": sorted(set(hits)), "cold": {"gets": med(cold["gets"]), "bytes": med(cold["bytes"]), "ms": med(cold["ms"])}, "warm": {"gets": med(warm["gets"]), "bytes": med(warm["bytes"]), "ms": med(warm["ms"])}}
                print(n, qn, json.dumps(res[qn]), flush=True)
            out["results"][str(n)] = res
        json.dump(out, open(os.path.join(B.HERE, "out", f"cost_near_{a.label}.json"), "w"), indent=1)
    finally:
        G.stop(p)


if __name__ == "__main__":
    main()
