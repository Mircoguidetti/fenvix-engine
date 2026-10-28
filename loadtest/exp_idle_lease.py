#!/usr/bin/env python3
"""Idle cost of leases and failover time, with the per-process lease off and on.

Idle: N tenants opened (written once) by one server process and then left idle; object-store PUTs of the lease category
(and all PUTs) over a window, per tenant-hour. Failover: SIGKILL the owner of a tenant, time until a second process
serves its first write (kill to first success), repeated.

    exp_idle_lease.py idle [--tenants 100,400] [--window 120] [--ttl 30]
    exp_idle_lease.py failover [--trials 8] [--ttl 10]
"""
import argparse, json, os, signal, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
from pymongo import MongoClient
from pymongo.errors import OperationFailure

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
LOADGEN = os.environ.get("LOADGEN", os.path.join(ROOT, "target", "release", "loadgen"))
OUT = os.path.join(ROOT, "loadtest", "out", "lease-cost"); RES = os.path.join(ROOT, "loadtest", "results", "lease-cost")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")


def start(port, holder, ttl, flags):
    f = open(os.path.join(OUT, f"server-{port}.log"), "ab")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--idle-secs", "100000", "--holder", holder, "--lease-ttl-secs", str(ttl), "--endpoint", EP] + flags, stdout=f, stderr=f)
    for _ in range(100):
        try:
            MongoClient(f"mongodb://127.0.0.1:{port}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def idle(a):
    rows = []
    for mode, flags in [m for m in [("off", []), ("on", ["--process-lease"])] if m[0] in os.environ.get("MODES", "off,on").split(",")]:
        for n in [int(x) for x in a.tenants.split(",")]:
            port = 28051
            p = start(port, f"I{mode}", a.ttl, flags)
            try:
                prefix = f"idle{mode}{n}x{int(time.time())}"
                subprocess.run([LOADGEN, "load", "--uris", f"mongodb://127.0.0.1:{port}", "--prefix", prefix, "--tenants", str(n), "--first", "0", "--out", "/tmp/idle-load.json", "--close-after", "false"], check=True, stdout=subprocess.DEVNULL)
                c = MongoClient(f"mongodb://127.0.0.1:{port}")
                s0 = c.admin.command("tenantStats"); t0 = time.time()
                time.sleep(a.window)
                s1 = c.admin.command("tenantStats"); secs = time.time() - t0
                assert s1["openWriters"] + s1["openReaders"] >= n, (s1["openWriters"], s1["openReaders"])
                d = lambda k: s1["putByCategory"][k] - s0["putByCategory"][k]
                tot = s1["requests"]["put"] - s0["requests"]["put"]
                get = s1["requests"]["get"] - s0["requests"]["get"]
                r = {"process_lease": mode, "tenants_open": s1["openWriters"] + s1["openReaders"], "window_s": round(secs, 1), "ttl_s": a.ttl,
                     "lease_puts": d("lease"), "all_puts": tot, "all_gets": get,
                     "lease_puts_per_tenant_hour": round(d("lease") / n * 3600 / secs, 1), "all_puts_per_tenant_hour": round(tot / n * 3600 / secs, 1),
                     "gets_per_tenant_hour": round(get / n * 3600 / secs, 1),
                     "put_by_category": {k: s1["putByCategory"][k] - s0["putByCategory"][k] for k in s1["putByCategory"]},
                     "get_by_category": {k: s1["getByCategory"][k] - s0["getByCategory"][k] for k in s1["getByCategory"]}}
                print(r, flush=True); rows.append(r)
            finally:
                p.terminate(); p.wait(60)
    json.dump(rows, open(os.path.join(RES, os.environ.get("IDLE_OUT", "idle.json")), "w"), indent=1)


def failover(a):
    rows = []
    for mode, flags in [("off", []), ("on", ["--process-lease"])]:
        times = []
        for t in range(a.trials):
            pa = start(28061, "FA", a.ttl, flags); pb = start(28062, "FB", a.ttl, flags)
            ca = MongoClient("mongodb://127.0.0.1:28061", retryWrites=False, serverSelectionTimeoutMS=3000)
            cb = MongoClient("mongodb://127.0.0.1:28062", retryWrites=False, retryReads=False, serverSelectionTimeoutMS=3000)
            db = f"fo{mode}{t}x{int(time.time())}"
            try:
                for i in range(20):
                    ca[db].c.insert_one({"_id": i})
                time.sleep(a.ttl / 3 + 0.5 if t % 2 else 0.1)  # vary where in the renewal cycle the kill lands
                pa.send_signal(signal.SIGKILL); k = time.time(); pa.wait()
                while True:
                    try:
                        cb[db].c.insert_one({"_id": 1000 + t})
                        break
                    except OperationFailure as e:
                        if e.code != 134:
                            raise
                        time.sleep(0.05)
                    if time.time() - k > a.ttl * 4:
                        raise RuntimeError("no failover")
                dt = time.time() - k
                n = cb[db].c.count_documents({})
                assert n == 21, n
                times.append(round(dt, 2)); print(mode, t, round(dt, 2), "s, docs", n, flush=True)
            finally:
                for p in (pa, pb):
                    try: p.terminate(); p.wait(60)
                    except Exception: p.kill()
        rows.append({"process_lease": mode, "ttl_s": a.ttl, "skew_allowance_s": 1.5, "trials": times, "min": min(times), "median": sorted(times)[len(times) // 2], "max": max(times)})
        print(rows[-1], flush=True)
    json.dump(rows, open(os.path.join(RES, "failover.json"), "w"), indent=1)


ap = argparse.ArgumentParser(); ap.add_argument("what", choices=["idle", "failover"]); ap.add_argument("--tenants", default="100,400")
ap.add_argument("--window", type=int, default=120); ap.add_argument("--ttl", type=int, default=30); ap.add_argument("--trials", type=int, default=8)
a = ap.parse_args()
{"idle": idle, "failover": failover}[a.what](a)
