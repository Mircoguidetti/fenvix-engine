"""K2b (reviewer's risk 1 and C6) · on the client machine, right after the load and BEFORE cook 0 (the loader) is
switched off: wait until the compaction of our server calms down, so that no size starts with the loader's compaction
still to do (the cold reads depend on how many SSTs there are; the local test of K2b MEASURED the count moving a lot
after the load). Every STEP seconds one LIST of the pantry ($0.005 per 1,000 objects listed, DECLARED); calm = the
number and the bytes of the SSTs did not change for three counts in a row. At most MAX minutes, then it goes on anyway
and says so.
K2c: calm also needs at least MIN_SECONDS (default 0; the client uses 150) since the start. Our server closes a database
after 60 idle seconds (--idle-secs) and only then asks the compaction of a closed writer with 4 or more L0 SSTs
(src/lifecycle.rs): with a short step (5 s in the local test) three equal counts came BEFORE that close, so "calm" said
nothing (the reviewer's E2).
Usage: settle.py PANTRY MAX_MINUTES STEP_SECONDS OUT_JSON [MIN_SECONDS]"""
import json
import os
import sys
import time

import boto3


def count(s3, bucket):
    n = b = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="tenants/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".sst"):
                n += 1
                b += o["Size"]
    return n, b


def main():
    bucket, most, step, out = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
    least = float(sys.argv[5]) if len(sys.argv) > 5 else 0.0
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
    t0 = time.time()
    seen = []
    calm = False
    while True:
        n, b = count(s3, bucket)
        seen.append({"s": round(time.time() - t0), "sst": n, "bytes": b})
        print(f"settle: {seen[-1]['s']} s, {n} SST, {b / 2**30:.3f} GiB", flush=True)
        if seen[-1]["s"] >= least and len(seen) >= 3 and seen[-1]["sst"] == seen[-2]["sst"] == seen[-3]["sst"] and \
                seen[-1]["bytes"] == seen[-2]["bytes"] == seen[-3]["bytes"]:
            calm = True
            break
        if time.time() - t0 >= most * 60:
            break
        time.sleep(step)
    json.dump({"calm": calm, "seconds": round(time.time() - t0), "counts": seen}, open(out, "w"), indent=1)
    print(f"SETTLE: {'calm' if calm else 'NOT calm'} after {time.time() - t0:.0f} s ({seen[-1]['sst']} SST)")


if __name__ == "__main__":
    main()
