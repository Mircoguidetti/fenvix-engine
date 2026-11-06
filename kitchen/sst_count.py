"""K2b (reviewer's C6) · how many SST files our server has in the pantry (the data bucket), and how big, per database
and per kind: compacted SSTs (memtable flushes and compaction output), WAL SSTs, manifests, other. Run by the server
machine before our server starts and after it stops, at every size: if the counts move a lot during a round, the
compaction was still busy (the cold reads depend on how many level-0 SSTs there are).
One LIST of the bucket (about 1 request per 1,000 objects: $0.005 per 1,000, DECLARED S3 price).
Usage: sst_count.py BUCKET OUT_JSON LABEL   (prints one summary line)
OUT ending in .jsonl: the record is appended as one line (K5: the count every 5 minutes during a size)."""
import datetime as dt
import json
import os
import sys

import boto3

KINDS = (("/compacted/", "compacted"), ("/wal/", "wal"), ("/manifest/", "manifest"))


def kind(key):
    for frag, name in KINDS:
        if frag in key:
            return name
    return "other"


def main():
    bucket, out, label = sys.argv[1:4]
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
    per_db = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.startswith("kitchen-states/"):
                continue
            parts = k.split("/")
            db = parts[1] if parts[0] == "tenants" and len(parts) > 2 else f"({parts[0]})"
            d = per_db.setdefault(db, {})
            kd = kind(k)
            x = d.setdefault(kd, {"objects": 0, "bytes": 0, "sst": 0, "sst_bytes": 0})
            x["objects"] += 1
            x["bytes"] += o["Size"]
            if k.endswith(".sst"):
                x["sst"] += 1
                x["sst_bytes"] += o["Size"]
    tot = {"objects": 0, "bytes": 0, "sst": 0, "sst_bytes": 0}
    for d in per_db.values():
        for x in d.values():
            for f in tot:
                tot[f] += x[f]
    rec = {"label": label, "t": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "total": tot,
           "per_database": per_db}
    if out.endswith(".jsonl"):
        with open(out, "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    else:
        json.dump(rec, open(out, "w"), indent=1)
    l0 = sum(d.get("compacted", {}).get("sst", 0) for d in per_db.values())
    print(f"SST {label}: {tot['sst']} SST ({tot['sst_bytes'] / 2**30:.2f} GiB), compacted {l0}, "
          f"{tot['objects']} objects ({tot['bytes'] / 2**30:.2f} GiB) in {len(per_db)} databases")


if __name__ == "__main__":
    main()
