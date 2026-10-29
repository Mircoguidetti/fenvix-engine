"""Extra: does the S3 cost of an active database grow with its history? With --retention-window-secs > 0 the engine
keeps WAL objects for the window and its WAL cleanup of an open writer takes slatedb's own pass (lists the WAL
directory, reads manifests) every --wal-gc-secs (20 s), whatever --wal-gc-mode says. Two databases on a fresh engine
with the scenario flags: `walnew` (fresh, 200 documents) and `walold` (WRITES single-document inserts first, i.e.
about WRITES WAL objects kept). Each then gets the same traffic, one database at a time: 1 update/s for SECS seconds;
MinIO request counters are read before and after (nothing else runs; the other databases are closed).
usage: wal_growth.py WRITES SECS"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine
from sampler import minio
from setup_dbs import admin
from pymongo import MongoClient


def count_objects(prefix):
    import boto3
    s = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    out = {}
    for page in s.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            parts = o["Key"].split("/")
            k = parts[2] if len(parts) > 3 else parts[-1]
            out[k] = out.get(k, 0) + 1
    return out


def main():
    writes, secs = int(sys.argv[1]), float(sys.argv[2])
    p = engine.start("wal_growth_load")
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER, password=ROOT_PW, authSource="admin")
    for db in ("walnew", "walold"):
        if admin("POST", "/v1/databases", {"name": db})[0] == 201:
            c[db].items.insert_many([{"_id": j, "k": j % 50, "pad": "x" * 900} for j in range(200)])
    t0 = time.time()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(lambda j: c.walold.events.insert_one({"j": j, "pad": "y" * 900}), range(writes)))
    load_s = time.time() - t0
    c.close()
    engine.stop(p, 600)
    res = {"writes_before": writes, "load_secs": round(load_s, 1), "secs": secs, "objects": {}, "runs": {}}
    for db in ("walnew", "walold"):
        res["objects"][db] = count_objects(f"tenants/{db}/")
    p = engine.start("wal_growth")
    c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username=ROOT_USER, password=ROOT_PW, authSource="admin")
    for db in ("walnew", "walold"):
        c[db].items.find_one({"_id": 1})          # open it (cold wake, not counted)
        c[db].items.update_one({"_id": 1}, {"$inc": {"n": 1}})
        time.sleep(5)
        a = minio()
        t = time.time()
        i = 0
        while time.time() - t < secs:
            c[db].items.update_one({"_id": i % 200}, {"$inc": {"n": 1}})
            i += 1
            time.sleep(max(0, t + i - time.time()))
        b = minio()
        span = time.time() - t
        d = {k: b.get(k, 0) - a.get(k, 0) for k in b if b.get(k, 0) - a.get(k, 0)}
        res["runs"][db] = {"span_s": round(span, 1), "updates": i, "minio": d, "per_min": {k: round(v / span * 60, 1) for k, v in d.items()}}
        print(db, res["runs"][db], flush=True)
        time.sleep(70)                               # let it close before the next one
    c.close()
    engine.stop(p, 600)
    os.makedirs(os.path.join(RESULTS, "extra_wal_growth"), exist_ok=True)
    with open(os.path.join(RESULTS, "extra_wal_growth", "wal_growth.json"), "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps(res))


if __name__ == "__main__":
    main()
