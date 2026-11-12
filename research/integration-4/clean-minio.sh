#!/bin/bash
# integration-4: between long runs, empty the local judge MinIO (127.0.0.1:9000): every bucket the tests left (some test
# files name theirs without the ctl- prefix, so ctl-rerun.sh misses them) and MinIO's deleted-object trash, which filled
# the disk three times on 2026-11-11 (P-integration-4). Only the local test store; never a real bucket.
/root/.fenvix-judge/env/bin/python - <<'P'
import boto3
s = boto3.resource("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
n = 0
for b in s.buckets.all():
    b.objects.all().delete()
    try:
        b.object_versions.all().delete()
    except Exception:
        pass
    b.delete(); n += 1
print(f"local MinIO: {n} buckets deleted")
P
rm -rf /root/.fenvix-judge/data/minio/.minio.sys/tmp/.trash/* 2>/dev/null
df -h / | tail -1
