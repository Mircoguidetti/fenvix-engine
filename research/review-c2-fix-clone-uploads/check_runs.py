"""Every run a stored version lists exists; counts runs listed by no version (orphans). Usage: check_runs.py BUCKET_PREFIX..."""
import boto3, json, sys
s = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin", region_name="us-east-1")
def ls(b, p):
    out = []
    for pg in s.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=p):
        out += [o["Key"] for o in pg.get("Contents", [])]
    return out
bad = 0
for b in [x["Name"] for x in s.list_buckets()["Buckets"] if any(x["Name"].startswith(a) for a in sys.argv[1:])]:
    dbs = {k.split("/")[1] for k in ls(b, "tenants/") if "/fx/manifest/" in k}
    for db in sorted(dbs):
        root = f"tenants/{db}/fx"
        runs = set(ls(b, root + "/run/"))
        listed, missing, nv = set(), [], 0
        for k in ls(b, root + "/manifest/"):
            body = s.get_object(Bucket=b, Key=k)["Body"].read()
            try:
                v = json.loads(body)
            except Exception:
                continue
            nv += 1
            paths = [g["sealed"]["path"] for g in v.get("groups", []) if g.get("sealed")] + [d["path"] for d in v.get("delta", [])]
            for p in paths:
                listed.add(p)
                if p.startswith(root) and p not in runs:
                    missing.append((k.rsplit("/", 1)[1], p))
        orphans = runs - listed
        bad += len(missing)
        print(f"{b} {db}: versions {nv}, runs {len(runs)}, listed {len(listed)}, MISSING {len(missing)} {missing[:3]}, orphans {len(orphans)}")
sys.exit(1 if bad else 0)
