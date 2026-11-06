"""Kitchen (K2/K2b) · S3 from a machine, with the machine's role (boto3, credentials from IMDS). No keys.
  s3io.py push BUCKET FOLDER PREFIX        upload the new or changed files of FOLDER (size or mtime)
  s3io.py get BUCKET KEY DEST              download one object
  s3io.py get-dir BUCKET PREFIX DEST       download every object under PREFIX into DEST (same relative paths)
  s3io.py text BUCKET KEY                  print the object (no final newline); exit 1 if missing
  s3io.py exists BUCKET KEY                exit 0 if the object exists, 1 if not
  s3io.py put BUCKET KEY FILE              upload a file (FILE "-" = stdin)
AWS_ENDPOINT_URL (local test only) points to a fake S3."""
import json
import os
import sys

import boto3


def client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))


def push(bucket, folder, prefix):
    s3 = client()
    memo_p = os.path.join(folder, ".pushed.json")
    try:
        memo = json.load(open(memo_p))
    except Exception:
        memo = {}
    for root, _, names in os.walk(folder):
        for n in names:
            p = os.path.join(root, n)
            if p == memo_p:
                continue
            rel = os.path.relpath(p, folder)
            try:
                st = os.stat(p)
            except OSError:
                continue
            sig = [st.st_size, st.st_mtime_ns]
            if memo.get(rel) == sig:
                continue
            try:
                s3.upload_file(p, bucket, prefix + rel)
                memo[rel] = sig
            except Exception as e:  # retried at the next push
                print(f"push {rel}: {type(e).__name__}", file=sys.stderr)
    with open(memo_p + ".tmp", "w") as f:
        json.dump(memo, f)
    os.replace(memo_p + ".tmp", memo_p)


def main():
    cmd, bucket = sys.argv[1], sys.argv[2]
    if cmd == "push":
        push(bucket, sys.argv[3], sys.argv[4])
    elif cmd == "get":
        os.makedirs(os.path.dirname(os.path.abspath(sys.argv[4])), exist_ok=True)
        client().download_file(bucket, sys.argv[3], sys.argv[4])
    elif cmd == "get-dir":
        s3, prefix, dest = client(), sys.argv[3], sys.argv[4]
        n = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                p = os.path.join(dest, o["Key"][len(prefix):])
                os.makedirs(os.path.dirname(p), exist_ok=True)
                s3.download_file(bucket, o["Key"], p)
                n += 1
        print(f"{n} files")
        if n == 0:
            sys.exit(1)
    elif cmd == "text":
        try:
            v = client().get_object(Bucket=bucket, Key=sys.argv[3])["Body"].read().decode().strip()
        except Exception as e:
            sys.exit(f"text {sys.argv[3]}: {type(e).__name__}")
        sys.stdout.write(v)
    elif cmd == "exists":
        try:
            client().head_object(Bucket=bucket, Key=sys.argv[3])
        except Exception:
            sys.exit(1)
    elif cmd == "put":
        data = sys.stdin.buffer.read() if sys.argv[4] == "-" else open(sys.argv[4], "rb").read()
        client().put_object(Bucket=bucket, Key=sys.argv[3], Body=data)
    else:
        sys.exit(f"unknown command: {cmd}")


if __name__ == "__main__":
    main()
