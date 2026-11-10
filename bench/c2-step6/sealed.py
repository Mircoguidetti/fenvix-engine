#!/usr/bin/env python3
"""Compaction 2 step 6: publish a version of our manifest through the server (the stub of step 4's reseal trigger).

  sealed.py publish --uri URI --db chat [--reseal all|none] [--block-kb 256]
      retries while E1's announcement waits (rule 26), prints the build report (JSON) and the staged publish's part.
  sealed.py counter --uri URI [--clear]
      what the counter of our runs holds (fenvixCounter).
"""
import argparse
import json
import time

from pymongo import MongoClient


def publish(admin, db, reseal="all", block_kb=None, tries=120):
    cmd = {"fenvixSealedPublish": db, "reseal": reseal}
    if block_kb:
        cmd["sealedBlockKb"] = int(block_kb)
    for _ in range(tries):
        r = admin.command(cmd)
        if r.get("published"):
            rep = json.loads(r["report"])
            rep["millis"] = r["millis"]
            return rep
        time.sleep(0.5)
    raise SystemExit(f"no publish of {db} after {tries} tries")


def gc(bucket, db):
    """Bench hygiene, not the product's GC (step 4): delete the run objects of `db` that the newest version of our
    manifest no longer lists. Safe in the two-visit bench only: it holds no transaction across a publish, and a
    publish installs its version before this runs. Returns (objects, bytes) deleted."""
    import boto3
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    pre = f"tenants/{db}/fx/"
    mans = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre + "manifest/") for o in p.get("Contents", [])]
    if not mans:
        return 0, 0
    v = json.loads(s3.get_object(Bucket=bucket, Key=sorted(mans)[-1])["Body"].read())
    listed = {g["sealed"]["path"] for g in v["groups"] if g.get("sealed")} | {d["path"] for d in v["delta"]}
    runs = [o for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre + "run/") for o in p.get("Contents", [])]
    gone = [o for o in runs if o["Key"] not in listed]
    for i in range(0, len(gone), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in gone[i:i + 1000]]})
    return len(gone), sum(o["Size"] for o in gone)


def counter(admin, clear=False, trim=False):
    return admin.command({"fenvixCounter": 1, "clear": bool(clear), "trim": bool(trim)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["publish", "counter"])
    ap.add_argument("--uri", required=True)
    ap.add_argument("--db", default="chat")
    ap.add_argument("--reseal", default="all")
    ap.add_argument("--block-kb", type=int)
    ap.add_argument("--clear", action="store_true")
    a = ap.parse_args()
    admin = MongoClient(a.uri).admin
    if a.cmd == "publish":
        print(json.dumps(publish(admin, a.db, a.reseal, a.block_kb)), flush=True)
    else:
        c = counter(admin, a.clear)
        c.pop("perCustomer", None)
        print(json.dumps({k: v for k, v in c.items()}), flush=True)


if __name__ == "__main__":
    main()
