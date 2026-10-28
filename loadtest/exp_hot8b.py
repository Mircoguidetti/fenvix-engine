#!/usr/bin/env python3
"""Task 5: single-tenant throughput with 1, 8 and 64 connections for reads only, writes only and a 90/10 mix,
against MinIO, a local directory and an in-memory store, with reads serialized (before) and shared (after)."""
import json, os, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
OUT = os.path.join(ROOT, "loadtest", "out", "t5_results8_run2")
RES = os.path.join(ROOT, "results8", "hot_run2")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
DUR = int(os.environ.get("HOT_SECS", "15"))
URI = "mongodb://127.0.0.1:28001"
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")

def server(store, shared):
    args = [os.path.join(BIN, "mongo-server"), "--listen", "127.0.0.1:28001", "--idle-secs", "3600", "--holder", "H", "--endpoint", ENDPOINT, "--store", store, "--local-dir", "/tmp/hot-store"]
    if not shared: args.append("--serialize-reads")
    if store != "s3": args.append("--no-leases")  # LocalFileSystem has no conditional update; single process anyway
    p = subprocess.Popen(args, stdout=open(os.path.join(OUT, f"server-{store}-{'shared' if shared else 'serial'}.log"), "w"), stderr=subprocess.STDOUT)
    time.sleep(1.0)
    return p

def run():
    stores = os.environ.get("STORES", "s3,local,memory").split(",")
    path = os.path.join(RES, "hot.json")
    results = [r for r in json.load(open(path)) if r["store"] not in stores] if os.path.exists(path) and os.environ.get("STORES") else []
    for store in stores:
        for shared in [False, True]:
            if store == "local":
                subprocess.run("rm -rf /tmp/hot-store", shell=True)
            p = server(store, shared)
            try:
                if True:  # the tenant must exist in every store (the MinIO bucket is empty in this environment)
                    subprocess.run([os.path.join(BIN, "loadgen"), "load", "--uris", URI, "--tenants", "1", "--first", "49999", "--out", "/tmp/hot-load.json", "--close-after", "false"], check=True, stdout=subprocess.DEVNULL)
                for conns in [1, 8, 64]:
                    for label, rp in [("reads", 100), ("writes", 0), ("mix90_10", 90)]:
                        out = f"/tmp/hot-{store}-{conns}-{label}.json"
                        subprocess.run([os.path.join(BIN, "loadgen"), "hot", "--uris", URI, "--tenant", "49999", "--connections", str(conns), "--read-pct", str(rp), "--duration-secs", str(DUR), "--warmup-secs", "3", "--out", out], check=True, stdout=subprocess.DEVNULL)
                        r = json.load(open(out)); r.update({"store": store, "reads": "shared" if shared else "serialized", "mix": label})
                        results.append(r)
                        print(store, "shared" if shared else "serial", conns, label, round(r["ops_per_sec"]), "ops/s p50", r["p50_ms"], "p99", r["p99_ms"], flush=True)
                        json.dump(results, open(os.path.join(RES, "hot.json"), "w"), indent=1)
            finally:
                p.terminate()
                try: p.wait(25)
                except subprocess.TimeoutExpired:
                    open(os.path.join(OUT, "hung-shutdown.txt"), "a").write(f"{store} {shared} did not exit within 25 s of SIGTERM; killed\n"); p.kill(); p.wait()

run()
