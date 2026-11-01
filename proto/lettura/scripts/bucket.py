#!/usr/bin/env python3
"""Crea (o svuota con --svuota) i bucket di prova su MinIO locale. Uso: bucket.py [--svuota] nome..."""
import sys
import boto3
from botocore.config import Config
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin",
                  aws_secret_access_key="minioadmin", region_name="us-east-1", config=Config(retries={"max_attempts": 2}))
args = sys.argv[1:]
svuota = "--svuota" in args
for b in [a for a in args if not a.startswith("--")]:
    try:
        s3.create_bucket(Bucket=b)
    except Exception:
        pass
    if svuota:
        n = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=b, Delete={"Objects": keys, "Quiet": True})
                n += len(keys)
        print(b, "svuotato", n)
    else:
        print(b, "pronto")
