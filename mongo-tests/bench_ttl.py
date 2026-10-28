#!/usr/bin/env python3
"""How often does expiry wake a sleeping tenant? (results7, step 3).

A synthetic population of tenants, each with a TTL index (expireAfterSeconds) on a collection that receives
inserts for a while and then goes quiet. The server runs with short idle/maintenance intervals so minutes of
behaviour fit in a short run. For every policy (the minimum gap between expiry wakes of one tenant) it reports:
 - tenants that were woken only to expire documents (`wakes`) and wakes per tenant,
 - object-store requests made by the maintenance passes (one LIST per pass) and by the wakes,
 - documents removed, and tenants that were never woken for expiry.
Nothing here is production traffic: the numbers describe this workload only.
Output: out/ttl.json
"""
import argparse
import datetime
import json
import os

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
import random
import signal
import subprocess
import time

import pymongo

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "mongo-server")
PORT = 27033
URI = f"mongodb://localhost:{PORT}/?directConnection=true"
RUN = f"{int(time.time()) % 100000}"


def run_policy(min_gap, tenants, active_secs, observe_secs, ttl_secs, rate):
    log = open(os.path.join(HERE, "out", "ttl-server.log"), "ab")
    # min_gap < 0: the default (sleeping tenants are never woken for expiry); otherwise --ttl-wake-sleeping with that gap
    wake = ["--ttl-wake-sleeping", "--ttl-maintenance-secs", "5", "--ttl-min-gap-secs", str(min_gap), "--ttl-max-wakes", "1000"] if min_gap >= 0 else []
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "3", "--holder", f"ttl{min_gap}", "--ttl-sweep-secs", "2", *wake], stdout=log, stderr=log)
    for _ in range(100):
        try:
            pymongo.MongoClient(URI, serverSelectionTimeoutMS=300).admin.command("ping")
            break
        except Exception:
            time.sleep(0.2)
    c = pymongo.MongoClient(URI, maxPoolSize=20)
    rnd = random.Random(3)
    names = [f"ttl{RUN}_{min_gap}_{i}" for i in range(tenants)]
    for n in names:
        c[n].c.create_index("t", expireAfterSeconds=ttl_secs)
    # the first window: each tenant gets inserts at Poisson times
    events = sorted((rnd.uniform(0, active_secs), n) for n in names for _ in range(max(1, int(rnd.expovariate(1 / rate)))))
    start = time.time()
    inserted = 0
    for at, n in events:
        d = at - (time.time() - start)
        if d > 0:
            time.sleep(d)
        c[n].c.insert_one({"t": datetime.datetime.now(datetime.timezone.utc), "v": inserted})
        inserted += 1
    time.sleep(max(0, observe_secs - (time.time() - start)))
    ts = c.admin.command("tenantStats")
    left = sum(c[n].c.estimated_document_count() for n in names)
    res = {"min_gap_secs": min_gap, "tenants": tenants, "documents_inserted": inserted, "documents_left_visible": left, "observed_secs": round(time.time() - start, 1), "ttl": ts["ttl"], "requests": ts["requests"]}
    res["wakes_per_tenant"] = round(ts["ttl"]["wakes"] / tenants, 2)
    p.send_signal(signal.SIGTERM)
    p.wait(60)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenants", type=int, default=60)
    ap.add_argument("--active", type=int, default=60)
    ap.add_argument("--observe", type=int, default=150)
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--gaps", default="-1,0,30,100")
    ap.add_argument("--rate", type=float, default=6.0, help="mean inserts per tenant in the active window")
    a = ap.parse_args()
    os.makedirs(os.path.join(HERE, "out"), exist_ok=True)
    out = []
    for gap in [int(x) for x in a.gaps.split(",")]:
        out.append(run_policy(gap, a.tenants, a.active, a.observe, a.ttl, a.rate))
        print(json.dumps(out[-1], default=str))
    json.dump(out, open(os.path.join(HERE, "out", "ttl.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
