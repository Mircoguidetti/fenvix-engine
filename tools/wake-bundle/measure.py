#!/usr/bin/env python3
"""Wake-bundle exploration: requests, sequential steps and time of a cold wake (open + first find), on local MinIO with a
simulated extra delay per request (`delay_all MS` in MONGO_TEST_FAULTS_FILE). A MODEL, NOT REAL S3.

    measure.py --bin target/release/mongo-server --out results.json [--n 25] [--delays 0,10,20,30] [--modes baseline,bundle]

Needs MinIO on 127.0.0.1:9000 (minioadmin/minioadmin) with a bucket `wbx`. Each (mode, delay) uses fresh tenants:
setup (insert 50 documents, clean close) runs without delay, then the faults file sets the delay and every tenant is
woken once with `tenantTrace` (find by _id). The first 5 wakes of each series are discarded (connection warm-up).

"Sequential steps" = the longest chain of requests in which each one starts after the previous one ended (from the trace
timestamps): the critical path in requests. Close cost = request and byte deltas of `tenantStats` around a clean close
of a tenant whose writer is open.
"""
import argparse
import json
import os
import signal
import statistics
import subprocess
import tempfile
import time

for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN"):
    os.environ.pop(_k, None)

from pymongo import MongoClient  # noqa: E402

PORT = 28311
URI = f"mongodb://127.0.0.1:{PORT}"


def start(binary, faults, flags, log):
    env = dict(os.environ, MONGO_TEST_FAULTS_FILE=faults)
    p = subprocess.Popen([binary, "--listen", f"127.0.0.1:{PORT}", "--bucket", "wbx", "--endpoint", "http://127.0.0.1:9000", "--holder", "W", "--idle-secs", "600"] + flags,
                         stdout=log, stderr=log, env=env)
    for _ in range(200):
        try:
            MongoClient(URI, serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def stop(p):
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(120)
    except Exception:
        p.kill()


def set_delay(faults, ms):
    with open(faults, "w") as f:
        f.write(f"delay_all {ms}\n" if ms else "")
    # the server re-reads the file when its mtime changes
    t = time.time() + 1
    os.utime(faults, (t, t))


def critical_path(events):
    """Longest chain of requests where each starts after the previous one ended."""
    ev = sorted(events, key=lambda e: e["startUs"])
    best = []
    for i, e in enumerate(ev):
        b = 1
        for j in range(i):
            if ev[j]["startUs"] + ev[j]["durUs"] <= e["startUs"]:
                b = max(b, best[j] + 1)
        best.append(b)
    return max(best) if best else 0


def series(c, prefix, n, marker):
    out = []
    for i in range(n):
        db = f"{prefix}{i:03d}"
        r = c[db].command({"tenantTrace": 1, "cold": True, "marker": marker, "run": {"find": "c", "filter": {"_id": 1}, "limit": 1}})
        assert r["innerOk"], r
        out.append({"elapsed_ms": r["elapsedUs"] / 1000, "requests": len(r["events"]), "steps": critical_path(r["events"]),
                    "by_op": {o: sum(1 for e in r["events"] if e["op"] == o) for o in {e["op"] for e in r["events"]}},
                    "events": [{k: e[k] for k in ("op", "path", "startUs", "durUs", "bytes")} for e in r["events"]]})
        c[db].command({"tenantAdmin": 1, "close": True})
    return out


def summarize(runs):
    runs = runs[5:]
    el = sorted(r["elapsed_ms"] for r in runs)
    return {
        "wakes": len(runs),
        "time_ms_p50": round(statistics.median(el), 2), "time_ms_max": round(el[-1], 2),
        "requests_p50": statistics.median(r["requests"] for r in runs),
        "requests_min": min(r["requests"] for r in runs), "requests_max": max(r["requests"] for r in runs),
        "steps_p50": statistics.median(r["steps"] for r in runs),
        "by_op_p50": {o: statistics.median(r["by_op"].get(o, 0) for r in runs) for o in sorted({o for r in runs for o in r["by_op"]})},
    }


def stats(c):
    return c.admin.command({"tenantStats": 1})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=25)
    ap.add_argument("--delays", default="0,10,20,30")
    ap.add_argument("--modes", default="baseline,bundle")
    ap.add_argument("--extra-flags", default="")
    args = ap.parse_args()
    tmp = tempfile.mkdtemp()
    faults = os.path.join(tmp, "faults")
    log = open(os.path.join(os.path.dirname(os.path.abspath(args.out)), "server.log"), "ab")
    results = {"note": "local MinIO + simulated delay per request (delay_all); a model, NOT real S3", "n_per_series": args.n, "discarded_per_series": 5, "series": {}}
    run_id = f"{int(time.time()) % 100000:05d}"
    for mode in args.modes.split(","):
        flags = (["--wake-bundle"] if mode == "bundle" else []) + (args.extra_flags.split() if args.extra_flags else [])
        for delay in [int(x) for x in args.delays.split(",")]:
            set_delay(faults, 0)
            p = start(args.bin, faults, flags, log)
            try:
                c = MongoClient(URI, serverSelectionTimeoutMS=60000)
                prefix_c = f"w{run_id}{mode[:2]}{delay:02d}c"
                prefix_u = f"w{run_id}{mode[:2]}{delay:02d}u"
                close_cost = []
                for prefix in (prefix_c, prefix_u):
                    for i in range(args.n):
                        db = f"{prefix}{i:03d}"
                        c[db]["c"].insert_many([{"_id": k, "v": "x" * 40, "n": k} for k in range(50)])
                        if prefix == prefix_c and i < 10:
                            # close cost of a writer with this mode (no delay; requests and bytes only)
                            s0 = stats(c)["requests"]
                            c[db].command({"tenantAdmin": 1, "close": True})
                            s1 = stats(c)["requests"]
                            close_cost.append({k: s1[k] - s0[k] for k in s0})
                        else:
                            c[db].command({"tenantAdmin": 1, "close": True})
                set_delay(faults, delay)
                time.sleep(1.5)
                clean = series(c, prefix_c, args.n, "")
                unclean = series(c, prefix_u, args.n, "missing")
                wb = stats(c).get("wakeBundle")
                results["series"][f"{mode}/{delay}ms"] = {
                    "mode": mode, "delay_ms": delay,
                    "clean": summarize(clean), "unclean": summarize(unclean),
                    "close_cost_p50": {k: statistics.median(x[k] for x in close_cost) for k in close_cost[0]},
                    "wake_bundle_counters": wb,
                    "example_clean": clean[min(10, len(clean) - 1)]["events"], "example_unclean": unclean[min(10, len(unclean) - 1)]["events"],
                }
                print(mode, delay, json.dumps({k: results["series"][f"{mode}/{delay}ms"][k] for k in ("clean", "unclean", "close_cost_p50", "wake_bundle_counters")}), flush=True)
            finally:
                set_delay(faults, 0)
                stop(p)
    json.dump(results, open(args.out, "w"), indent=1)


if __name__ == "__main__":
    main()
