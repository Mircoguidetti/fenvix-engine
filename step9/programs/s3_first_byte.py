"""Step 9 · program "s3-first-byte": how long S3 takes to send the FIRST byte and the whole object, by object size,
from a client machine in eu-west-1 (the same zone as the cook). This is the floor of every cold read of our design.
For each size it writes OBJECTS distinct objects (random bytes, so nothing compresses), then reads READS of them in a
random order, each read on a NEW object as far as possible (S3 has no per-object warm-up we can see, but this avoids
measuring the client's own connection cache on one key). Also ranged GETs of RANGE_KIB inside a big object, which is
what a block read of a sealed run is (see sealed_run_read.py for the one-trip read itself).
Every read: time to first byte (ttfb) and time to last byte (total), with the UTC time. Raw lines in
OUT/s3-first-byte.jsonl, summary (p50, p95, p99, max, n) in OUT/s3-first-byte-summary.json.
Cost (ESTIMATED with the DECLARED S3 prices $0.005 per 1,000 PUT and $0.0004 per 1,000 GET): with the defaults about
10 sizes x 40 PUTs + 10 x 300 GETs = 400 PUTs and 3,000 GETs, about $0.0032; storage less than 1 GB for an hour.
Usage: s3_first_byte.py BUCKET OUT [--prefix P] [--sizes-kib 1,4,...] [--objects 40] [--reads 300] [--seed 9]"""
import argparse
import json
import os
import random
import statistics
import time

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("bucket")
ap.add_argument("out")
ap.add_argument("--prefix", default="step9-s3fb/")
ap.add_argument("--sizes-kib", default="1,4,16,64,256,1024,4096,16384")
ap.add_argument("--objects", type=int, default=40)
ap.add_argument("--reads", type=int, default=300)
ap.add_argument("--range-kib", default="64,256,1024")
ap.add_argument("--big-mib", type=int, default=256, help="size of the object for the ranged GETs")
ap.add_argument("--seed", type=int, default=9)
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
rng = random.Random(a.seed)
raw = open(os.path.join(a.out, "s3-first-byte.jsonl"), "a")


def timed_get(key, rng_header=None):
    t0 = time.perf_counter()
    kw = {"Bucket": a.bucket, "Key": key}
    if rng_header:
        kw["Range"] = rng_header
    r = s3.get_object(**kw)
    body = r["Body"]
    first = body.read(1)
    t1 = time.perf_counter()
    n = len(first) + len(body.read())
    t2 = time.perf_counter()
    return (t1 - t0) * 1000, (t2 - t0) * 1000, n


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))] if v else None


def summ(rows):
    ttfb = [r["ttfb_ms"] for r in rows]
    tot = [r["total_ms"] for r in rows]
    return {"n": len(rows), "ttfb_p50": pct(ttfb, 50), "ttfb_p95": pct(ttfb, 95), "ttfb_p99": pct(ttfb, 99), "ttfb_max": max(ttfb),
            "total_p50": pct(tot, 50), "total_p95": pct(tot, 95), "total_p99": pct(tot, 99), "total_max": max(tot),
            "ttfb_mean": round(statistics.mean(ttfb), 3)}


summary = {"what": "S3 first byte and whole object by size, from the client machine", "bucket_region": "eu-west-1",
           "objects": {}, "ranged": {}}
# 1. whole objects by size
for kib in [int(x) for x in a.sizes_kib.split(",")]:
    keys = []
    for i in range(a.objects):
        k = f"{a.prefix}obj-{kib}k-{i:04d}"
        s3.put_object(Bucket=a.bucket, Key=k, Body=os.urandom(kib * 1024))
        keys.append(k)
    rows = []
    order = [keys[i % len(keys)] for i in range(a.reads)]
    rng.shuffle(order)
    for k in order:
        ttfb, total, n = timed_get(k)
        row = {"t": time.time(), "kind": "object", "kib": kib, "key": k, "ttfb_ms": round(ttfb, 3), "total_ms": round(total, 3), "bytes": n}
        raw.write(json.dumps(row) + "\n")
        rows.append(row)
    summary["objects"][str(kib)] = summ(rows)
    print(f"object {kib:>6} KiB: {json.dumps(summary['objects'][str(kib)])}", flush=True)
# 2. ranged GETs at random offsets of one big object (a sealed run's block read)
big = f"{a.prefix}big-{a.big_mib}m"
s3.upload_file(__file__, a.bucket, f"{a.prefix}placeholder")  # makes sure the prefix is writable before the big upload
tmp = os.path.join(a.out, ".big.bin")
with open(tmp, "wb") as f:
    for _ in range(a.big_mib):
        f.write(os.urandom(1 << 20))
s3.upload_file(tmp, a.bucket, big)
os.remove(tmp)
size = a.big_mib << 20
for kib in [int(x) for x in a.range_kib.split(",")]:
    rows = []
    for _ in range(a.reads):
        off = rng.randrange(0, size - kib * 1024)
        ttfb, total, n = timed_get(big, f"bytes={off}-{off + kib * 1024 - 1}")
        row = {"t": time.time(), "kind": "range", "kib": kib, "offset": off, "ttfb_ms": round(ttfb, 3), "total_ms": round(total, 3), "bytes": n}
        raw.write(json.dumps(row) + "\n")
        rows.append(row)
    summary["ranged"][str(kib)] = summ(rows)
    print(f"range  {kib:>6} KiB: {json.dumps(summary['ranged'][str(kib)])}", flush=True)
json.dump(summary, open(os.path.join(a.out, "s3-first-byte-summary.json"), "w"), indent=1)
# the objects go away with the run bucket at the end; delete them now anyway (they are not needed any more)
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix=a.prefix):
    objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objs:
        s3.delete_objects(Bucket=a.bucket, Delete={"Objects": objs})
print("S3-FIRST-BYTE: DONE")
