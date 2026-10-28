#!/usr/bin/env python3
"""Task 2 (engine-cost): single-tenant write throughput with 1, 8 and 64 connections, and object-store PUTs per write,
for one server binary. Run once with the binary from before group commit and once with the current one.

    exp_groupcommit.py <label> <mongo-server binary> [extra server args...]
Env: STORES (s3,local), DUR (15), MINIO_ENDPOINT (http://127.0.0.1:9000)
"""
import json, os, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh).
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ.setdefault(_k, _v)
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOADGEN = os.environ.get("LOADGEN", os.path.join(ROOT, "target", "release", "loadgen"))
OUT = os.path.join(ROOT, "loadtest", "out", "gc")
RES = os.path.join(ROOT, "loadtest", "results", "gc")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
DUR = int(os.environ.get("DUR", "15"))
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PORT = 28011
URI = f"mongodb://127.0.0.1:{PORT}"


def stats():
    return MongoClient(URI).admin.command("tenantStats")


def main():
    label, binary, extra = sys.argv[1], sys.argv[2], sys.argv[3:]
    stores = os.environ.get("STORES", "s3,local").split(",")
    results = []
    for store in stores:
        for conns in [int(x) for x in os.environ.get("CONNS", "1,8,64").split(",")]:
            prefix = f"g{label}{store}{conns}x"
            args = [binary, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "3600", "--holder", "H", "--endpoint", ENDPOINT,
                    "--store", store, "--local-dir", "/tmp/gc-store"] + extra
            if store != "s3":
                args.append("--no-leases")
                subprocess.run("rm -rf /tmp/gc-store", shell=True)
            log = open(os.path.join(OUT, f"server-{label}-{store}-{conns}.log"), "w")
            p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
            time.sleep(1.0)
            try:
                subprocess.run([LOADGEN, "load", "--uris", URI, "--prefix", prefix, "--tenants", "1", "--first", "0", "--out", "/tmp/gc-load.json", "--close-after", "false"], check=True, stdout=subprocess.DEVNULL)
                before = stats()
                out = f"/tmp/gc-{label}-{store}-{conns}.json"
                subprocess.run([LOADGEN, "hot", "--uris", URI, "--prefix", prefix, "--tenant", "0", "--connections", str(conns), "--read-pct", os.environ.get("READ_PCT", "0"),
                                "--duration-secs", str(DUR), "--warmup-secs", "0", "--out", out], check=True, stdout=subprocess.DEVNULL)
                after = stats()
                r = json.load(open(out))
                d = lambda k: after["putByCategory"][k] - before["putByCategory"][k]
                wal, tot = d("wal"), after["requests"]["put"] - before["requests"]["put"]
                gcs = {k: after.get("groupCommit", {}).get(k, 0) - before.get("groupCommit", {}).get(k, 0) for k in ("commits", "flushes")}
                if gcs["flushes"]:
                    r.update(group_commits=gcs["commits"], group_flushes=gcs["flushes"], writes_per_flush=round(gcs["commits"] / gcs["flushes"], 2))
                r.update(label=label, store=store, wal_puts=wal, total_puts=tot, wal_puts_per_write=round(wal / max(r["ops"], 1), 3),
                         total_puts_per_write=round(tot / max(r["ops"], 1), 3), put_by_category={k: d(k) for k in after["putByCategory"]})
                results.append(r)
                print(label, store, conns, round(r["ops_per_sec"], 1), "ops/s p50", r["p50_ms"], "p99", r["p99_ms"], "errors", r["errors"],
                      "writes/flush", r.get("writes_per_flush"), "wal PUT/write", r["wal_puts_per_write"], "all PUT/write", r["total_puts_per_write"], flush=True)
            finally:
                p.terminate()
                try:
                    p.wait(25)
                except subprocess.TimeoutExpired:
                    open(os.path.join(OUT, "hung-shutdown.txt"), "a").write(f"{label} {store} {conns}: no exit 25 s after SIGTERM\n")
                    p.kill(); p.wait()
    json.dump(results, open(os.path.join(RES, f"{label}.json"), "w"), indent=1)


main()
