#!/usr/bin/env python3
"""Test 3: dormant databases on real S3 (runs on the c6i.large, engine started through mongo-tests/server.sh with
SERVER_BIN=engine-wrap.sh). Results are written to --out after every phase, so a guard shutdown keeps what was measured.

Phases
  stages   10, 100, 1000 databases (empty: one createCollection each), left dormant; S3 objects/bytes, requests and
           engine CPU/RSS while all are dormant.
  sizes    one database empty, one with 1,000 and one with 100,000 documents (~1 KB each): S3 bytes right after close and
           after a settle period (compaction / GC).
  wake     cold wake: first find on a closed database; latency and S3 requests (engine counters, before/after).
  awake    how many databases can be kept open together under light load (1 find per database every 10 s); CPU, RSS,
           request rate and latency per level; stops at the first level that breaks a limit.
"""
import argparse
import concurrent.futures as cf
import json
import os
import statistics
import subprocess
import threading
import time

from pymongo import MongoClient

ROOT = os.environ.get("S3T_ROOT", "/opt/s3test")
B = os.environ["S3T_BUCKET"]
SERVER = f"{ROOT}/mongo-tests/server.sh"
PORT = 27018
URI = f"mongodb://127.0.0.1:{PORT}"
CLK = os.sysconf("SC_CLK_TCK")
SMOKE = os.environ.get("S3T_SMOKE") == "1"  # tiny sizes, for a dry check of this script against a local store; never used in the real run
STAGES, SIZES, SETTLE, LEVELS, LOAD_SECS, WARMUP = ((3, 6, 10), (0, 100, 1000), 5, (5, 10), 10, 5) if SMOKE else ((10, 100, 1000), (0, 1000, 100000), 300, (100, 250, 500, 1000, 2000), 90, 40)
RES = {"flags": {}, "phases": {}, "smoke": SMOKE}
LIMITS = {"p99_ms": 1000.0, "error_rate": 0.005, "rss_mb": 3000.0, "cpu_pct_of_one_core": 150.0}  # c6i.large: 2 vCPU, 4 GiB


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def add_extra(put, get):
    sh(f"add_extra {put} {get}")


def save(path):
    json.dump(RES, open(path, "w"), indent=1)
    sh(f"aws s3 cp {path} s3://{B}/_s3test/out/test3.partial.json --only-show-errors")
    add_extra(1, 0)


def start_server(idle_secs, flags=""):
    os.environ["S3T_ENGINE_FLAGS"] = flags
    r = sh(f"{SERVER} {PORT} {idle_secs}")
    if r.returncode:
        raise RuntimeError(r.stdout + r.stderr)
    RES["flags"] = {"idle_secs": idle_secs, "extra_flags": flags, "base": "--store s3-aws --region eu-central-1 (engine defaults otherwise)"}


def stop_server():
    sh("pkill -TERM -x mongo-server")
    for _ in range(60):
        if sh("pgrep -x mongo-server").returncode:
            return
        time.sleep(1)
    sh("pkill -KILL -x mongo-server")


def pid():
    return int(sh("pgrep -x mongo-server").stdout.split()[0])


def proc_cpu_rss(p):
    f = open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()
    cpu = (int(f[11]) + int(f[12])) / CLK
    rss = [int(l.split()[1]) for l in open(f"/proc/{p}/status") if l.startswith("VmRSS")][0] / 1024
    return cpu, rss


def stats(c):
    return c.admin.command("tenantStats")


def counts(c):
    st = stats(c)
    r = st["requests"]
    out = {k: int(r[k]) for k in ("billedPutTier", "billedGetTier", "put", "get", "head", "list", "delete", "copy")}
    h = st.get("s3Http") or {}  # HTTP attempts incl. retries (real S3 only); what S3 bills
    out["httpPutTier"], out["httpGetTier"], out["throttled"] = int(h.get("billedPutTier", 0)), int(h.get("billedGetTier", 0)), int(h.get("throttled", 0))
    return out


def diff(a, b):
    return {k: b[k] - a[k] for k in a}


def open_count(c):
    s = stats(c)
    return int(s["openReaders"]) + int(s["openWriters"])


def wait_dormant(c, timeout=300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = stats(c)
        if int(s["openReaders"]) + int(s["openWriters"]) == 0 and int(s["leasesHeld"]) == 0:
            return time.time() - t0
        time.sleep(1)
    raise RuntimeError("tenants did not close")


def s3_usage(prefix):
    """(objects, bytes) under tenants/<prefix>; `aws s3 ls` pages are LIST requests: counted into extra.json."""
    r = sh(f"aws s3 ls s3://{B}/tenants/{prefix} --recursive --summarize | tail -2")
    objs = size = 0
    for l in r.stdout.splitlines():
        if "Total Objects" in l:
            objs = int(l.split(":")[1])
        if "Total Size" in l:
            size = int(l.split(":")[1])
    add_extra(objs // 1000 + 1, 0)
    return objs, size


def idle_window(c, secs):
    p = pid()
    c0, r0 = proc_cpu_rss(p)
    q0 = counts(c)
    t0 = time.time()
    time.sleep(secs)
    c1, r1 = proc_cpu_rss(p)
    el = time.time() - t0
    q = diff(q0, counts(c))
    return {"secs": el, "cpu_pct_of_one_core": 100 * (c1 - c0) / el, "rss_mb": r1, "requests": q,
            "put_tier_per_hour": q["billedPutTier"] / el * 3600, "get_tier_per_hour": q["billedGetTier"] / el * 3600}


def doc(i):
    return {"_id": i, "n": i, "name": f"user{i}", "payload": "x" * 900}


def make_db(c, name, ndocs):
    db = c[name]
    if ndocs == 0:
        db.create_collection("c")
    else:
        for s in range(0, ndocs, 1000):
            db.c.insert_many([doc(i) for i in range(s, min(ndocs, s + 1000))], ordered=True)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * q))] if xs else None


def phase_stages(c, out):
    res = []
    made = 0
    for target in STAGES:
        t0 = time.time()
        q0 = counts(c)
        with cf.ThreadPoolExecutor(32) as ex:
            list(ex.map(lambda i: make_db(c, f"dorm_{i:05d}", 0), range(made, target)))
        create_secs = time.time() - t0
        made = target
        closed_after = wait_dormant(c)
        q = diff(q0, counts(c))
        objs, size = s3_usage("dorm_")
        w = idle_window(c, 60)
        res.append({"databases": target, "create_secs": create_secs, "close_wait_secs": closed_after, "create_requests": q,
                    "s3_objects": objs, "s3_bytes": size, "bytes_per_db": size / target, "objects_per_db": objs / target, "idle_60s": w})
        RES["phases"]["stages"] = res
        save(out)
    return res


def phase_sizes(c, out):
    res = {}
    for name, n in zip(("size_empty", "size_1k", "size_100k"), SIZES):
        t0 = time.time()
        q0 = counts(c)
        make_db(c, name, n)
        res[name] = {"docs": n, "load_secs": time.time() - t0, "requests": diff(q0, counts(c))}
    wait_dormant(c)
    for name in res:
        res[name]["after_close"] = dict(zip(("objects", "bytes"), s3_usage(name + "/")))
    time.sleep(SETTLE)  # settle: compaction and GC of the closed tenants run in the background
    for name in res:
        res[name]["after_settle"] = dict(zip(("objects", "bytes"), s3_usage(name + "/")))
    RES["phases"]["sizes"] = res
    save(out)
    return res


def phase_wake(c, out):
    res = {}
    for label, names, closer in (("empty_dormant", [f"dorm_{i:05d}" for i in range(STAGES[-1] // 2, STAGES[-1] // 2 + (3 if SMOKE else 20))], False),
                                  ("empty", ["size_empty"] * 10, True), ("docs_1k", ["size_1k"] * 10, True), ("docs_100k", ["size_100k"] * 10, True)):
        runs = []
        for name in names:
            if closer:
                c[name].command({"tenantAdmin": 1, "close": True})
                wait_dormant(c)
            q0 = counts(c)
            t0 = time.perf_counter()
            c[name].c.find_one({"_id": 1})
            lat = (time.perf_counter() - t0) * 1000
            runs.append({"ms": lat, "requests": diff(q0, counts(c))})
            if not closer:
                c[name].command({"tenantAdmin": 1, "close": True})
                wait_dormant(c)
        ms = [r["ms"] for r in runs]
        res[label] = {"n": len(runs), "ms_p50": statistics.median(ms), "ms_max": max(ms), "ms_first": ms[0],
                      "billedPutTier_median": statistics.median(r["requests"]["billedPutTier"] for r in runs),
                      "billedGetTier_median": statistics.median(r["requests"]["billedGetTier"] for r in runs),
                      "requests_median": {k: statistics.median(r["requests"][k] for r in runs) for k in runs[0]["requests"]}, "runs": runs}
        RES["phases"]["wake"] = res
        save(out)
    return res


def light_load(c, names, secs, period=10.0):
    lat, errs, stop = [], [0], threading.Event()
    lock = threading.Lock()
    workers = min(64, len(names))
    chunks = [names[i::workers] for i in range(workers)]

    def work(mine):
        cl = MongoClient(URI, maxPoolSize=2, serverSelectionTimeoutMS=10000)
        slot = period / len(mine)
        nxt = time.time()
        while not stop.is_set():
            for n in mine:
                nxt += slot
                t0 = time.perf_counter()
                try:
                    cl[n].c.find_one({"_id": 1})
                    ms = (time.perf_counter() - t0) * 1000
                    with lock:
                        lat.append(ms)
                except Exception:
                    with lock:
                        errs[0] += 1
                d = nxt - time.time()
                if d > 0:
                    stop.wait(d)
                if stop.is_set():
                    return

    ths = [threading.Thread(target=work, args=(m,)) for m in chunks]
    p = pid()
    for t in ths:
        t.start()
    time.sleep(WARMUP)  # warm-up: every database woken once, leases taken
    with lock:
        lat.clear()
        errs[0] = 0
    c0, _ = proc_cpu_rss(p)
    q0 = counts(c)
    t0 = time.time()
    time.sleep(secs)
    el = time.time() - t0
    c1, r1 = proc_cpu_rss(p)
    q = diff(q0, counts(c))
    opened = open_count(c)
    with lock:
        ls = list(lat)
        e = errs[0]
    stop.set()
    for t in ths:
        t.join()
    return {"databases": len(names), "open_tenants": opened, "ops": len(ls), "errors": e, "error_rate": e / max(1, len(ls) + e),
            "ms_p50": pct(ls, 0.5), "ms_p99": pct(ls, 0.99), "cpu_pct_of_one_core": 100 * (c1 - c0) / el, "rss_mb": r1,
            "put_tier_per_s": q["billedPutTier"] / el, "get_tier_per_s": q["billedGetTier"] / el, "requests": q}


def phase_awake(c, out):
    levels, res = LEVELS, []
    stop_server()
    start_server(100000)  # tenants stay open
    c = MongoClient(URI, serverSelectionTimeoutMS=30000)
    have = STAGES[-1]
    for k in levels:
        if k > have:
            with cf.ThreadPoolExecutor(32) as ex:
                list(ex.map(lambda i: make_db(c, f"dorm_{i:05d}", 0), range(have, k)))
            have = k
        names = [f"dorm_{i:05d}" for i in range(k)]
        r = light_load(c, names, LOAD_SECS)
        r["limits_exceeded"] = [m for m, bad in (("p99_ms", (r["ms_p99"] or 0) > LIMITS["p99_ms"]), ("error_rate", r["error_rate"] > LIMITS["error_rate"]),
                                                  ("rss_mb", r["rss_mb"] > LIMITS["rss_mb"]), ("cpu_pct_of_one_core", r["cpu_pct_of_one_core"] > LIMITS["cpu_pct_of_one_core"])) if bad]
        res.append(r)
        RES["phases"]["awake"] = {"limits": LIMITS, "levels": res}
        save(out)
        if r["limits_exceeded"]:
            break
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    start_server(20)  # idle close after 20 s so created databases go dormant quickly
    c = MongoClient(URI, serverSelectionTimeoutMS=30000, maxPoolSize=64)
    c.admin.command("ping")
    phase_stages(c, a.out)
    phase_sizes(c, a.out)
    phase_wake(c, a.out)
    phase_awake(c, a.out)
    stop_server()
    RES["finished"] = True
    save(a.out)


if __name__ == "__main__":
    main()
