#!/usr/bin/env python3
"""Task 4: cold-wake breakdown by step (object-store trace), and the number of concurrent wakes at which latency degrades.

  trace:  `tenantTrace` runs a find on a cold tenant with the object-store trace on and returns every request in order,
          for a tenant that was closed cleanly (marker present) and one that was not (marker missing: WAL replay).
  knee:   waves of K simultaneous first requests to K distinct cold tenants, K = 4 ... 256 (loadgen burst, ramp 0).
  limit:  a 5,000-tenant burst over 60 s (S2) without and with `--max-concurrent-wakes`.

    exp_wake.py trace|knee|limit
"""
import json
import os

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
import signal
import statistics
import subprocess
import sys
import time

from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
OUT = os.path.join(ROOT, "loadtest", "out", "t4")
RES = os.path.join(ROOT, "loadtest", "results", "t4")
os.makedirs(OUT, exist_ok=True)
os.makedirs(RES, exist_ok=True)
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")
PORT = 28001
URI = f"mongodb://127.0.0.1:{PORT}"


def start(flags=(), ttl=60, idle=30):
    f = open(os.path.join(OUT, "server.log"), "ab")
    p = subprocess.Popen([os.path.join(BIN, "mongo-server"), "--listen", f"127.0.0.1:{PORT}", "--idle-secs", str(idle), "--holder", "W", "--lease-ttl-secs", str(ttl), "--endpoint", ENDPOINT] + list(flags), stdout=f, stderr=f)
    for _ in range(100):
        try:
            MongoClient(URI, serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(300)
    except Exception:
        p.kill()


def prefetch(lo, hi):
    """Read the tenants' objects once so the disk (page cache) is warm and the measurement is not a cold-disk one."""
    subprocess.run(f"docker exec minio sh -c 'for i in $(seq {lo} {hi}); do cat /data/bench/tenants/t$(printf %06d $i)/*/* 2>/dev/null; done > /dev/null'", shell=True)


def step(path, op):
    if path.endswith("/_lease"):
        return "lease acquire (GET + conditional PUT)"
    if "/manifest" in path or path.endswith("manifest") or path.endswith("/manifest/"):
        return "manifest (list + read)"
    if path.endswith("/_clean"):
        return "clean-marker HEAD"
    if "/wal/" in path or path.endswith("/wal"):
        return "WAL replay (reads)"
    if "/compacted/" in path or path.endswith("/compacted"):
        return "first read (SST index, filter, blocks)"
    return "other"


def trace():
    p = start()
    c = MongoClient(URI, serverSelectionTimeoutMS=30000)
    allruns = {}
    try:
        for variant, marker, first in [("clean (marker present)", "present", 45000), ("unclean (marker missing, WAL replay)", "missing", 45100)]:
            runs = []
            for i in range(40):
                db = f"t{first + i:06d}"
                r = c[db].command({"tenantTrace": 1, "cold": True, "marker": marker, "run": {"find": "c", "filter": {"_id": 1}, "limit": 1}})
                runs.append(r)
                c[db].command({"tenantAdmin": 1, "close": True})
            allruns[variant] = runs
    finally:
        stop(p)
    out = {}
    for variant, runs in allruns.items():
        steps = {}
        for r in runs[5:]:  # the first few runs warm connection pools and the disk
            by = {}
            for e in r["events"]:
                s = step(e["path"], e["op"])
                x = by.setdefault(s, {"n": 0, "dur_us": 0, "first_us": 10**12, "last_us": 0, "ops": {}})
                x["n"] += 1
                x["dur_us"] += e["durUs"]
                x["first_us"] = min(x["first_us"], e["startUs"])
                x["last_us"] = max(x["last_us"], e["startUs"] + e["durUs"])
                x["ops"][e["op"]] = x["ops"].get(e["op"], 0) + 1
            for s, x in by.items():
                z = steps.setdefault(s, {"n": [], "dur_us": [], "first_us": [], "last_us": [], "ops": {}})
                z["n"].append(x["n"])
                z["dur_us"].append(x["dur_us"])
                z["first_us"].append(x["first_us"])
                z["last_us"].append(x["last_us"])
                for o, k in x["ops"].items():
                    z["ops"].setdefault(o, []).append(k)
        el = sorted(r["elapsedUs"] for r in runs[5:])
        out[variant] = {
            "runs": len(runs) - 5,
            "total_ms": {"p50": statistics.median(el) / 1000, "p99": el[int(len(el) * 0.99) - 1] / 1000 if len(el) > 1 else el[0] / 1000, "max": el[-1] / 1000},
            "requests_per_wake": statistics.median([len(r["events"]) for r in runs[5:]]),
            "steps": {s: {"requests_p50": statistics.median(z["n"]), "sum_of_request_ms_p50": statistics.median(z["dur_us"]) / 1000,
                          "starts_at_ms_p50": statistics.median(z["first_us"]) / 1000, "ends_at_ms_p50": statistics.median(z["last_us"]) / 1000,
                          "by_type_p50": {o: statistics.median(v) for o, v in z["ops"].items()}} for s, z in steps.items()},
        }
    json.dump({"summary": out, "raw_first_run_clean": allruns["clean (marker present)"][10]["events"], "raw_first_run_unclean": allruns["unclean (marker missing, WAL replay)"][10]["events"]}, open(os.path.join(RES, "trace.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


def burst(k, first, tag, flags=(), ramp=0, hold=2, duration=40):
    d = os.path.join(OUT, tag)
    os.makedirs(d, exist_ok=True)
    cmd = [os.path.join(BIN, "loadgen"), "run", "--name", tag, "--uris", URI, "--out", d, "--mode", "burst", "--start", str(first), "--pool", str(k), "--active", str(k), "--ramp-secs", str(ramp),
           "--hold-secs", str(hold), "--rate", "1", "--duration-secs", str(duration), "--sample-secs", "5", "--worker-threads", "2", "--pool-size", "600", "--read-pct", "100"]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
    return json.load(open(os.path.join(d, "summary.json")))


def knee():
    res = []
    first = 30000
    for k in [4, 8, 16, 32, 64, 128, 256, 512]:
        prefetch(first, first + k)
        p = start()
        try:
            s = burst(k, first, f"knee_{k}", hold=1, duration=20)
        finally:
            stop(p)
        w = s["normal"]["wake"]
        res.append({"concurrent_wakes": k, "wake_p50_ms": w["p50_ms"], "wake_p90_ms": w["p90_ms"], "wake_p99_ms": w["p99_ms"], "wake_max_ms": w["max_ms"], "count": w["count"], "errors": s["errors_by_code"]})
        print(res[-1], flush=True)
        first += 600
        json.dump(res, open(os.path.join(RES, "knee.json"), "w"), indent=1)


def limit():
    if len(sys.argv) > 2 and sys.argv[2] != "auto":
        lim = int(sys.argv[2])
    else:
        # the largest wave whose p99 first-request latency is within 2x of the smallest wave's, and not above 1 s
        knee = json.load(open(os.path.join(RES, "knee.json")))
        base = knee[0]["wake_p99_ms"]
        lim = max([k["concurrent_wakes"] for k in knee if k["wake_p99_ms"] <= max(2 * base, 1000) and all(j["wake_p99_ms"] <= max(2 * base, 1000) for j in knee if j["concurrent_wakes"] <= k["concurrent_wakes"])] or [knee[0]["concurrent_wakes"]])
        print("limit chosen from the knee:", lim, flush=True)
    res = {}
    for label, flags in [("unlimited", []), (f"limit {lim}", ["--max-concurrent-wakes", str(lim)])]:
        prefetch(32000, 36999)
        p = start(flags, idle=30)
        try:
            s = burst(5000, 32000, "limit_" + label.replace(" ", "_"), flags, ramp=60, hold=60, duration=135)
            st = MongoClient(URI).admin.command("tenantStats")
        finally:
            stop(p)
        res[label] = {"wake": s["normal"]["wake"], "all_ops": s["normal"]["all_ops"], "errors": s["errors_by_code"], "failures": s["failures_by_code"], "ops": s["normal"]["ops"], "wake_queue": st["wakeQueue"]}
        print(label, json.dumps({k: v for k, v in res[label].items() if k != "wake"}), json.dumps(s["normal"]["wake"]), flush=True)
        json.dump(res, open(os.path.join(RES, "limit.json"), "w"), indent=1)


def overload():
    """5,000 cold tenants all within 10 s (about 500 wakes per second, more than the server can do): without a limit and
    with limits at the throughput plateau of the knee test (32) and at the p99 rule's choice (128)."""
    res = {}
    for label, flags in [("unlimited", []), ("limit 32", ["--max-concurrent-wakes", "32"]), ("limit 128", ["--max-concurrent-wakes", "128"]), ("unlimited (repeat, to see run-to-run spread)", [])]:
        prefetch(32000, 36999)
        p = start(flags, idle=30)
        try:
            s = burst(5000, 32000, "overload_" + label.replace(" ", "_"), flags, ramp=10, hold=30, duration=60)
            st = MongoClient(URI).admin.command("tenantStats")
        finally:
            stop(p)
        res[label] = {"wake": s["normal"]["wake"], "all_ops": s["normal"]["all_ops"], "errors": s["errors_by_code"], "failures": s["failures_by_code"], "ops": s["normal"]["ops"], "wake_queue": st["wakeQueue"], "seconds": s["seconds"]}
        print(label, json.dumps({k: v for k, v in res[label].items() if k != "wake"}), json.dumps(s["normal"]["wake"]), flush=True)
        json.dump(res, open(os.path.join(RES, "overload.json"), "w"), indent=1)


if __name__ == "__main__":
    {"trace": trace, "knee": knee, "limit": limit, "overload": overload}[sys.argv[1]]()
