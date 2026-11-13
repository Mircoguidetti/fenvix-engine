"""Objects of a database's run folder that its newest version does not list (left by a killed generator: GC removes them
after the publish, once a compactor runs). Usage: orphans.py BUCKET DB  (local MinIO)"""
import json
import sys

import boto3

s = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
b, db = sys.argv[1], sys.argv[2]
root = f"tenants/{db}/fx/"
objs = {}
for p in s.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=root):
    for o in p.get("Contents", []):
        objs[o["Key"]] = o["Size"]
mans = sorted(k for k in objs if k.startswith(root + "manifest/"))
v = json.loads(s.get_object(Bucket=b, Key=mans[-1])["Body"].read())
listed = {g["sealed"]["path"] for g in v["groups"] if g.get("sealed")} | {d["path"] for d in v.get("delta", [])}
runs = {k: n for k, n in objs.items() if k.startswith(root + "run/")}
orph = {k: n for k, n in runs.items() if k not in listed}
print(json.dumps({"version": mans[-1].rsplit("/", 1)[-1], "runs_listed": len(listed), "run_objects": len(runs),
                  "orphans": len(orph), "orphan_bytes": sum(orph.values()), "listed_missing": sorted(listed - set(runs))}))
