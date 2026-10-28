#!/usr/bin/env python3
"""Task 1: object-store requests per second, by source, for an idle open tenant, a tenant at 1 op/s and a tenant at
0.1 op/s, for several WAL flush settings. Also the effect of the flush interval on a write-heavy tenant, and the
durability window of acknowledging before durable.

Each cell: a fresh server process (MinIO, idle close off so tenants stay open, lease TTL 60 s), 200 tenants opened and
exercised, then counters read from `tenantStats` over a measurement window. Requests are attributed by object
category (WAL, manifest, compacted SST, lease, clean marker) and the standalone compaction/GC totals.

    exp_cost.py [--tenants 200] [--window 90]
"""
import argparse
import json
import os

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh).
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ.setdefault(_k, _v)
import signal
import subprocess
import time

from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
OUT = os.path.join(ROOT, "loadtest", "out", "t1")
RES = os.path.join(ROOT, "loadtest", "results", "t1")
os.makedirs(OUT, exist_ok=True)
os.makedirs(RES, exist_ok=True)
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")
PORT = 28001
URI = f"mongodb://127.0.0.1:{PORT}"

# name -> server flags. Durable-ack configurations first (the acknowledged write is durable), then fast-ack.
CONFIGS = [
    ("baseline: explicit flush per write (100 ms periodic)", []),
    ("durable ack, flush every 10 ms", ["--wal-flush-ms", "10"]),
    ("durable ack, flush every 100 ms", ["--wal-flush-ms", "100"]),
    ("durable ack, flush every 1000 ms", ["--wal-flush-ms", "1000"]),
]


def start(flags, ttl=60):
    f = open(os.path.join(OUT, "server.log"), "ab")
    p = subprocess.Popen([os.path.join(BIN, "mongo-server"), "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "C", "--lease-ttl-secs", str(ttl), "--endpoint", ENDPOINT] + flags, stdout=f, stderr=f)
    for _ in range(100):
        try:
            MongoClient(URI, serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(300)
    except Exception:
        p.kill()


def stats(c):
    return c.admin.command("tenantStats")


def delta(a, b):
    out = {}
    for k in ("putByCategory", "getByCategory"):
        out[k] = {n: b[k][n] - a[k][n] for n in b[k]}
    out["maintenance"] = {n: b["maintenance"][n] - a["maintenance"][n] for n in b["maintenance"]}
    out["req"] = {n: b["requests"][n] - a["requests"][n] for n in b["requests"]}
    out["lease"] = {n: b["leases"][n] - a["leases"][n] for n in b["leases"]}
    out["open"] = (b["openReaders"], b["openWriters"])
    return out


def run_load(args, rate, read_pct, first, hold, tag):
    """Open `tenants` tenants with `loadgen run` in burst mode and keep them at `rate` ops/s for `hold` seconds."""
    d = os.path.join(OUT, tag)
    os.makedirs(d, exist_ok=True)
    cmd = [os.path.join(BIN, "loadgen"), "run", "--name", tag, "--uris", URI, "--out", d, "--mode", "burst", "--start", str(first), "--pool", str(args.tenants), "--active", str(args.tenants),
           "--ramp-secs", "5", "--hold-secs", str(hold), "--rate", str(rate), "--read-pct", str(read_pct), "--duration-secs", str(hold + 8), "--sample-secs", "5", "--worker-threads", "1"]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def window_from_samples(d, t_from, t_to):
    rows = [json.loads(l) for l in open(os.path.join(d, "samples.jsonl"))]
    rows = [r for r in rows if r["servers"][0].get("up")]
    a = next(r for r in rows if r["t"] >= t_from)
    b = [r for r in rows if r["t"] <= t_to][-1]
    sa, sb = a["servers"][0], b["servers"][0]
    dt = b["t"] - a["t"]
    return dt, {
        "put": {n: (sb["put_by_category"][n] - sa["put_by_category"][n]) / dt for n in sb["put_by_category"]},
        "get": {n: (sb["get_by_category"][n] - sa["get_by_category"][n]) / dt for n in sb["get_by_category"]},
        "maintenance": {n: (sb["maintenance"][n] - sa["maintenance"][n]) / dt for n in sb["maintenance"]},
        "list": (sb["req"]["list"] - sa["req"]["list"]) / dt, "delete": (sb["req"]["delete"] - sa["req"]["delete"]) / dt,
        "open": (sb["open_readers"], sb["open_writers"]),
    }


def cell_rate(args, name, flags, rate, label):
    p = start(flags)
    try:
        hold = 40 + args.window
        lg = run_load(args, rate, 80, args.first, hold, f"{label}")
        lg.wait()
        d = os.path.join(OUT, label)
        dt, w = window_from_samples(d, 40, 40 + args.window)
        sm = json.load(open(os.path.join(d, "summary.json")))
        lat = {k: {"p50_ms": v["p50_ms"], "p99_ms": v["p99_ms"], "count": v["count"]} for k, v in sm["normal"]["steady_by_type"].items()}
        return {"config": name, "scenario": f"{rate} op/s per tenant (80% reads)", "tenants": args.tenants, "window_secs": dt, "per_sec": w, "latency_open_tenants": lat, "errors": sm["errors_by_code"]}
    finally:
        stop(p)


def cell_idle(args, name, flags, read_pct, label):
    p = start(flags)
    try:
        c = MongoClient(URI)
        # open the tenants: one op each in a short burst, then nothing
        lg = run_load(args, 1.0, read_pct, args.first, 3, label)
        lg.wait()
        time.sleep(15)
        a = stats(c)
        t0 = time.time()
        time.sleep(args.window)
        b = stats(c)
        dt = time.time() - t0
        d = delta(a, b)
        return {"config": name, "scenario": "idle open " + ("readers" if read_pct == 100 else "writers"), "tenants": args.tenants, "window_secs": dt, "open_readers_writers": d["open"],
                "per_sec": {"put": {n: v / dt for n, v in d["putByCategory"].items()}, "get": {n: v / dt for n, v in d["getByCategory"].items()},
                            "maintenance": {n: v / dt for n, v in d["maintenance"].items()}, "list": d["req"]["list"] / dt, "delete": d["req"]["delete"] / dt}}
    finally:
        stop(p)


def cell_write_heavy(args, name, flags):
    """One tenant, 8 connections writing as fast as possible: how many writes share a WAL object."""
    p = start(flags)
    try:
        c = MongoClient(URI)
        subprocess.run([os.path.join(BIN, "loadgen"), "hot", "--uris", URI, "--tenant", str(args.first), "--connections", "1", "--read-pct", "100", "--duration-secs", "2", "--warmup-secs", "0", "--out", "/tmp/w0.json"], stdout=subprocess.DEVNULL)
        a = stats(c)
        t0 = time.time()
        subprocess.run([os.path.join(BIN, "loadgen"), "hot", "--uris", URI, "--tenant", str(args.first), "--connections", "8", "--read-pct", "0", "--duration-secs", "20", "--warmup-secs", "0", "--out", "/tmp/w1.json"], stdout=subprocess.DEVNULL, check=True)
        dt = time.time() - t0
        b = stats(c)
        r = json.load(open("/tmp/w1.json"))
        d = delta(a, b)
        return {"config": name, "scenario": "one tenant, 8 connections writing", "writes_per_sec": r["ops_per_sec"], "p50_ms": r["p50_ms"], "p99_ms": r["p99_ms"],
                "wal_puts_per_sec": d["putByCategory"]["wal"] / dt, "writes_per_wal_put": r["ops"] / max(d["putByCategory"]["wal"], 1)}
    finally:
        stop(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenants", type=int, default=200)
    ap.add_argument("--window", type=int, default=90)
    ap.add_argument("--first", type=int, default=47000)
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    results = []
    path = os.path.join(RES, "cost.json")
    for ci, (name, flags) in enumerate(CONFIGS):
        for kind, arg, label in [("idle", 100, "idle_readers"), ("idle", 0, "idle_writers"), ("rate", 1.0, "rate1"), ("rate", 0.1, "rate01")]:
            tag = f"c{ci}_{label}"
            if args.only and args.only not in tag:
                continue
            print("running", tag, flush=True)
            if kind == "idle":
                r = cell_idle(args, name, flags, arg, tag)
            else:
                r = cell_rate(args, name, flags, arg, tag)
            r["tag"] = tag
            results.append(r)
            json.dump(results, open(path, "w"), indent=1)
        if not args.only or "heavy" in args.only:
            print("running write-heavy", ci, flush=True)
            r = cell_write_heavy(args, name, flags)
            r["tag"] = f"c{ci}_heavy"
            results.append(r)
            json.dump(results, open(path, "w"), indent=1)


if __name__ == "__main__":
    main()
