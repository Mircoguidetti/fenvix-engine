"""Sizes of one e2e database. sizes.py dbstats DB  -> dbStats of the target on 27017 (logical bytes, as the server reports)
sizes.py s3 DB                                     -> bytes our server keeps in MinIO under tenants/DB/, by kind (run after a clean stop)"""
import collections
import json
import sys

kind, db = sys.argv[1], sys.argv[2]
if kind == "dbstats":
    import pymongo
    s = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")[db].command("dbStats")
    print(json.dumps({k: s.get(k) for k in ("collections", "objects", "dataSize", "storageSize", "indexSize", "totalSize")}))
else:
    import boto3
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
    size, count = collections.Counter(), collections.Counter()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="adapters", Prefix=f"tenants/{db}/"):
        for o in page.get("Contents", []):
            part = o["Key"].split("/")[2]
            size[part] += o["Size"]
            count[part] += 1
    print(json.dumps({"total": sum(size.values()), "objects": sum(count.values()), "by_kind": dict(size), "count_by_kind": dict(count)}))
