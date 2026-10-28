#!/usr/bin/env python3
"""Cost of text and geo queries on MinIO (results9): cold (tenant closed before every sample) and warm (same query repeated
on the open tenant), with and without the index, for 10k and 100k documents. The numbers come from the server's own
object-store counters (`tenantStats`): GETs (+LISTs), bytes read, and client-side latency.
Output: out/cost_geo_text.json"""
import argparse, json, os, random, signal, statistics, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
import pymongo
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_cost as B

PORT = B.PORT
WORDS = [f"w{i:04d}" for i in range(3000)]


def docs(start, n, rnd):
    out = []
    for i in range(start, start + n):
        # Zipf-ish: a few common words, a long tail of rare ones (each rare word is in about 10 documents per 10k)
        text = " ".join(WORDS[min(int(rnd.paretovariate(0.7)) - 1, len(WORDS) - 1)] if rnd.random() < 0.5 else WORDS[rnd.randrange(len(WORDS))] for _ in range(8))
        out.append({"_id": i, "k": i % 97, "t": text, "loc": {"type": "Point", "coordinates": [round(rnd.uniform(-180, 180), 5), round(rnd.uniform(-85, 85), 5)]}, "payload": "x" * 100})
    return out


def server():
    log = open(os.path.join(B.HERE, "out", "cost-geo-server.log"), "wb")
    p = subprocess.Popen([B.BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "geo", "--no-ttl"], stdout=log, stderr=log)
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


POLY = lambda i: (lambda x, y: {"type": "Polygon", "coordinates": [[[x, y], [x + 4, y], [x + 4, y + 4], [x, y + 4], [x, y]]]})(-170 + (i * 37) % 330, -80 + (i * 17) % 150)
QUERIES = {
    # name: (needs_index, fn(collection, sample index) -> number of results)
    "text_rare_term": (True, lambda c, i: len(list(c.find({"$text": {"$search": WORDS[2000 + i % 900]}}, {"_id": 1})))),
    "text_common_term": (True, lambda c, i: len(list(c.find({"$text": {"$search": WORDS[i % 5]}}, {"_id": 1})))),
    "text_phrase_regex_scan": (False, lambda c, i: len(list(c.find({"t": {"$regex": WORDS[2000 + i % 900]}}, {"_id": 1})))),
    "search_stage_scan": (False, lambda c, i: len(list(c.aggregate([{"$search": {"text": {"query": WORDS[2000 + i % 900], "path": "t"}}}, {"$project": {"_id": 1}}])))),
    "geo_within_small_polygon": (True, lambda c, i: len(list(c.find({"loc": {"$geoWithin": {"$geometry": POLY(i)}}}, {"_id": 1})))),
    "geo_near_100km": (True, lambda c, i: len(list(c.find({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [-150 + (i * 31) % 300, -70 + (i * 13) % 140]}, "$maxDistance": 100000}}}, {"_id": 1})))),
    "geo_near_limit10": (True, lambda c, i: len(list(c.find({"loc": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": [-150 + (i * 31) % 300, -70 + (i * 13) % 140]}}}}, {"_id": 1}).limit(10)))),
    "geo_within_scan_centersphere": (False, lambda c, i: len(list(c.find({"loc": {"$geoWithin": {"$centerSphere": [[-150 + (i * 31) % 300, -70 + (i * 13) % 140], 0.02]}}}, {"_id": 1})))),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="10000,100000")
    ap.add_argument("--samples", type=int, default=7)
    ap.add_argument("--out", default=os.path.join(B.HERE, "out", "cost_geo_text.json"))
    a = ap.parse_args()
    sizes = [int(x) for x in a.sizes.split(",")]
    names = {}
    p = server()
    c = pymongo.MongoClient(B.URI)
    for n in sizes:
        for indexed in (False, True):
            db = f"gt{B.RUN}_{n}_{int(indexed)}"
            names[(n, indexed)] = db
            d = c[db]
            if indexed:
                d.c.create_index([("t", "text")])
                d.c.create_index([("loc", "2dsphere")])
            rnd = random.Random(7)
            t0 = time.time()
            for s in range(0, n, 1000):
                d.c.insert_many(docs(s, min(1000, n - s), rnd))
            names[(n, indexed, "load_s")] = round(time.time() - t0, 1)
            d.command("tenantAdmin", close=True)
    time.sleep(20)
    stop(p)
    p = server()
    c = pymongo.MongoClient(B.URI)
    out = {"run": B.RUN, "samples": a.samples, "results": {}}
    for (n, indexed), db in [(k, v) for k, v in names.items() if len(k) == 2]:
        d = c[db]
        res = {"load_seconds": names[(n, indexed, "load_s")]}
        for qn, (needs_index, fn) in QUERIES.items():
            if needs_index and not indexed:
                res[qn] = "not possible without the index (the server answers with an error, as MongoDB does)"
                continue
            if not needs_index and indexed:
                pass  # scans are measured with the indexes present too (they cannot use them)
            cold, warm = {"gets": [], "bytes": [], "ms": []}, {"gets": [], "bytes": [], "ms": []}
            hits = []
            for i in range(a.samples):
                d.command("tenantAdmin", close=True)
                s0 = B.stats(c)
                t0 = time.time()
                hits.append(fn(d.c, i + 1))
                ms = (time.time() - t0) * 1000
                x = B.diff(s0, B.stats(c))
                cold["gets"].append(x["get"] + x["list"]); cold["bytes"].append(x["getBytes"]); cold["ms"].append(ms)
                # warm: the same query again on the open tenant
                s0 = B.stats(c)
                t0 = time.time()
                fn(d.c, i + 1)
                ms = (time.time() - t0) * 1000
                x = B.diff(s0, B.stats(c))
                warm["gets"].append(x["get"] + x["list"]); warm["bytes"].append(x["getBytes"]); warm["ms"].append(ms)
            med = lambda v: round(statistics.median(v), 1)
            res[qn] = {"results_median": statistics.median(hits),
                       "cold": {"gets": med(cold["gets"]), "bytes": med(cold["bytes"]), "ms": med(cold["ms"])},
                       "warm": {"gets": med(warm["gets"]), "bytes": med(warm["bytes"]), "ms": med(warm["ms"])}}
        out["results"][f"{n}_{'indexed' if indexed else 'no_index'}"] = res
        print(n, indexed, json.dumps(res)[:300], flush=True)
    stop(p)
    json.dump(out, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
