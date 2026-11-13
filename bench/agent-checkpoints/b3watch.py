"""P-B3 (diagnosis of the 2026-11-13 loss): every 30 s, list every object under tenants/app_demo/ in the bench's MinIO
and record what disappeared (count and bytes by kind), the newest version of our manifest (number, kind, writer epoch,
X_d, groups, listed runs, their bytes, how many of them are missing) and keep a copy of every new newest version.

  b3watch.py OUT_DIR
"""
import gzip
import json
import os
import sys
import time

import boto3

S3 = dict(endpoint_url="http://172.31.77.10:9000", region_name="eu-west-1", aws_access_key_id="minio-local", aws_secret_access_key="minio-local-secret")
B = "fxbench"
PFX = "tenants/app_demo/"


def kind(k: str) -> str:
    rest = k[len(PFX):].split("/")
    if rest and rest[0].startswith("i") and rest[0][1:].isdigit():
        rest = rest[1:]
    if not rest:
        return "?"
    if rest[0] == "fx" and len(rest) > 2:
        return "fx/" + rest[1] + ("/" + rest[2] if rest[1] == "run" else "")
    return rest[0]


def main():
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    s3 = boto3.client("s3", **S3)
    prev = {}
    seen_newest = None
    log = open(os.path.join(out, "watch.jsonl"), "a")
    while True:
        t = time.time()
        try:
            cur = {}
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix=PFX):
                for o in page.get("Contents", []):
                    cur[o["Key"]] = o["Size"]
        except Exception as e:  # noqa: BLE001
            log.write(json.dumps({"t": t, "error": repr(e)[:300]}) + "\n")
            log.flush()
            time.sleep(30)
            continue
        gone = {}
        for k, sz in prev.items():
            if k not in cur:
                g = gone.setdefault(kind(k), [0, 0])
                g[0] += 1
                g[1] += sz
        new = {}
        for k, sz in cur.items():
            if k not in prev:
                g = new.setdefault(kind(k), [0, 0])
                g[0] += 1
                g[1] += sz
        rec = {"t": round(t, 1), "objects": len(cur), "bytes": sum(cur.values()), "gone": gone, "new": new}
        mans = sorted(k for k in cur if "/fx/manifest/" in k)
        if mans:
            nk = mans[-1]
            try:
                body = s3.get_object(Bucket=B, Key=nk)["Body"].read()
                v = json.loads(body)
                runs = [g["sealed"] for g in v["groups"] if g.get("sealed")] + v["delta"]
                missing = [r["path"] for r in runs if r["path"] not in cur]
                rec["newest"] = {"key": nk, "version": v["version"], "kind": v["kind"], "writer_epoch": v.get("writer_epoch"), "x_d": v["x_d"],
                                 "groups": len(v["groups"]), "runs": len(runs), "run_bytes": sum(r["size"] for r in runs), "missing": len(missing),
                                 "missing_first": missing[:3], "versions_listed": len(mans)}
                if nk != seen_newest:
                    seen_newest = nk
                    with gzip.open(os.path.join(out, f"version-{v['version']:06d}.json.gz"), "wb") as f:
                        f.write(body)
            except Exception as e:  # noqa: BLE001
                rec["newest_error"] = repr(e)[:300]
        run_bytes = sum(sz for k, sz in cur.items() if "/fx/run/" in k)
        rec["fx_run_bytes_in_store"] = run_bytes
        log.write(json.dumps(rec) + "\n")
        log.flush()
        prev = cur
        time.sleep(max(1, 30 - (time.time() - t)))


if __name__ == "__main__":
    main()
