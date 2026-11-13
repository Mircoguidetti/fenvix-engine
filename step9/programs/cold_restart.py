"""Step 9 · the cold phase's restart on AWS (total review B9, P-bench-local 2.5), called by bench.py as --cold-cmd:
asks a machine of the round, through the run bucket, to stop its server, drop its page cache and start again
(machine/common.sh serve_cold_restart, in cook.sh and pgbox.sh), and waits until it answers that it serves again.
Aurora has no such path (a reboot needs an IAM permission the workflow does not have): its target runs no cold phase.
Usage: cold_restart.py BUCKET MACHINE [--wait-s 900]   (MACHINE: the machine's folder in the bucket: server-1, pgbox)
Exit 0 once the machine answers ok; 1 on a refusal or no answer in time (bench.py then marks the run failed)."""
import argparse
import json
import sys
import time
import uuid

import boto3
from botocore.exceptions import ClientError


def restart(s3, bucket, machine, wait_s=900, poll_s=2.0):
    rid = uuid.uuid4().hex
    t0 = time.time()
    s3.put_object(Bucket=bucket, Key=f"{machine}/restart-request", Body=rid.encode())
    while time.time() - t0 < wait_s:
        try:
            ans = json.loads(s3.get_object(Bucket=bucket, Key=f"{machine}/restarted-{rid}")["Body"].read())
            return {"machine": machine, "id": rid, "seconds": round(time.time() - t0, 1), **ans}
        except ClientError:
            time.sleep(poll_s)
    return {"machine": machine, "id": rid, "ok": False, "error": f"no answer in {wait_s} s"}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("bucket")
    ap.add_argument("machine")
    ap.add_argument("--wait-s", type=int, default=900)
    a = ap.parse_args()
    r = restart(boto3.client("s3", region_name="eu-west-1"), a.bucket, a.machine, a.wait_s)
    print(json.dumps(r))
    sys.exit(0 if r.get("ok") else 1)
