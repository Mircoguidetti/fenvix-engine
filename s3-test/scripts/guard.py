#!/usr/bin/env python3
"""On-instance spend guard, independent of GitHub. Started BEFORE the engine; every 10 s it reads the engine's
request counters (`tenantStats`, billedPutTier / billedGetTier) from every engine port, computes the gross cost of the
run (PUT and LIST at the PUT price, GET at the GET price, plus instance hours) and uploads the counters to S3 for the
workflow's own watchdog. Above the cap it stops the engine, uploads the final counters and powers the instance off
(InstanceInitiatedShutdownBehavior=terminate turns that into a termination).

An engine restart resets its counters: a drop of a counter is treated as a restart and the previous value is kept.
Requests made in the 10 s before a kill are lost; the cap therefore leaves headroom (see results-s3.md).
Fail closed: if an engine process runs but its counters cannot be read 12 times in a row (2 min), it stops too.

    guard.py --bucket B --ports 27018,27019 [--cap 5.0] [--interval 10]
Extra requests made outside the engine (the s3probe, aws cli listings) are added through /var/lib/s3test/extra.json
({"put_tier": n, "get_tier": n}).
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cost import PRICES, gross  # noqa: E402

STATE = "/var/lib/s3test"
os.makedirs(STATE, exist_ok=True)


def uptime():
    return float(open("/proc/uptime").read().split()[0])


def engine_running():
    return subprocess.run(["pgrep", "-x", "mongo-server"], capture_output=True).returncode == 0


def read_extra():
    try:
        return json.load(open(os.path.join(STATE, "extra.json")))
    except Exception:
        return {"put_tier": 0, "get_tier": 0}


def upload(bucket, path, key):
    r = subprocess.run(["aws", "s3", "cp", path, f"s3://{bucket}/{key}", "--only-show-errors"], capture_output=True, text=True, timeout=60)
    return r.returncode == 0


def stop_everything(reason):
    subprocess.run(["pkill", "-TERM", "-x", "mongo-server"])
    for _ in range(20):
        if not engine_running():
            break
        time.sleep(1)
    subprocess.run(["pkill", "-KILL", "-x", "mongo-server"])
    return reason


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--ports", default="27018,27019")
    ap.add_argument("--cap", type=float, default=PRICES["run_cost_cap_usd"])
    # The engine's rate limiter bounds what can accrue between two looks: 2 processes x 300 req/s x ~35 s (10 s interval +
    # shutdown) x $5.4e-6 = ~$0.11. Stopping at cap - margin keeps the run's gross cost under the cap with room to spare.
    ap.add_argument("--margin", type=float, default=0.5)
    ap.add_argument("--interval", type=float, default=10)
    a = ap.parse_args()
    from pymongo import MongoClient

    ports = [int(p) for p in a.ports.split(",")]
    acc = {p: {"acc_put": 0, "acc_get": 0, "last_put": 0, "last_get": 0} for p in ports}
    clients = {}
    guard_uploads = 0
    blind = 0
    stopped = None
    while True:
        t0 = time.time()
        readable = 0
        for p in ports:
            try:
                c = clients.get(p) or MongoClient(f"mongodb://127.0.0.1:{p}", serverSelectionTimeoutMS=3000, connectTimeoutMS=3000, socketTimeoutMS=5000)
                clients[p] = c
                ts = c.admin.command("tenantStats")
                # HTTP-level counts (retries included, POST ?delete at the PUT price); the CountingStore numbers only as fallback.
                s = ts.get("s3Http") or ts["requests"]
                put, get = int(s["billedPutTier"]), int(s["billedGetTier"])
                st = acc[p]
                if put < st["last_put"]:
                    st["acc_put"] += st["last_put"]
                if get < st["last_get"]:
                    st["acc_get"] += st["last_get"]
                st["last_put"], st["last_get"] = put, get
                readable += 1
            except Exception:
                clients.pop(p, None)
        if engine_running() and readable == 0:
            blind += 1
        else:
            blind = 0
        extra = read_extra()
        put_tier = sum(v["acc_put"] + v["last_put"] for v in acc.values()) + extra["put_tier"] + guard_uploads
        get_tier = sum(v["acc_get"] + v["last_get"] for v in acc.values()) + extra["get_tier"]
        up = uptime() + 60  # boot time before the guard started is billed too
        cost = gross(put_tier, get_tier, up)
        if cost > a.cap - a.margin:
            stopped = f"cost {cost:.4f} > cap {a.cap} - margin {a.margin}"
        elif blind >= 12:
            stopped = "engine running but counters unreadable for 2 minutes"
        rec = {"ts": int(time.time()), "uptime_secs": up, "put_tier": put_tier, "get_tier": get_tier, "cost_usd": cost, "cap_usd": a.cap, "per_port": acc, "extra": extra, "guard_uploads": guard_uploads, "stopped": stopped}
        path = os.path.join(STATE, "guard.json")
        json.dump(rec, open(path, "w"))
        guard_uploads += 1  # counted before the next record so the PUT is never missing from the sum
        upload(a.bucket, path, "_s3test/guard/counters.json")
        if stopped:
            stop_everything(stopped)
            open(os.path.join(STATE, "STOPPED"), "w").write(stopped)
            upload(a.bucket, path, "_s3test/guard/final.json")
            subprocess.run(["shutdown", "-h", "now"])
            return
        time.sleep(max(0.0, a.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
