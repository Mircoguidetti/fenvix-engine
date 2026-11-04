"""Noisy neighbours again (S4, isolation): tools/round3/noisy.py adapted to a 2-CPU machine and to the per-database quotas.

Same load as round3: N heavy databases, each with 4 connections sending back to back (50/50 reads by _id and 1 KB
inserts), plus 1 light database at 2 requests/s (50/50, open loop, Poisson). The engine runs alone on CPU 0
(taskset); the heavy clients, the light client and the S3 stand-in share CPU 1. On a shared CPU the light client's own
scheduling would add to its latency, so it runs at high priority (nice -15) and the heavy clients at nice 10.

Each mode is NAME=FLAGS (engine flags separated by spaces, may be empty) and BIN may be given as NAME@BIN=FLAGS.
Per step: the light database's p50/p99 (also reads and inserts apart), the heavy ones' throughput and p99, engine CPU,
and the server signals of GET /v1/metrics/databases.

usage: noisy_quota.py --heavy 50 --secs 40 --mode "off=--fair-lag-ms 0" --mode "quota=" [--out FILE]"""
import argparse
import json
import multiprocessing as mp
import os
import random
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "round3"))
from common import *  # noqa
import engine as E
import run as R

PAD = "y" * 900
ENGINE_CPUS = os.environ.get("ISO_ENGINE_CPUS", "0")
# s3: the S3 stand-in on 127.0.0.1:9000 (MinIO or moto); local: --store local in ISO_LOCAL_DIR (its writes all pass one
# lock of the process, see localcas.rs); memory: --store memory (no S3 latency, no stand-in competing for CPU 1; the data
# lives in the engine process and goes at its stop, so every engine start prefills again).
STORE = os.environ.get("ISO_STORE", "s3")
LOCAL_DIR = os.environ.get("ISO_LOCAL_DIR", "/tmp/iso-store")


class Engine(E.Engine):
    def cmd(self):
        c = super().cmd()
        if STORE in ("local", "memory"):
            i = c.index("--store")
            c[i + 1] = STORE
            j = c.index("--bucket")
            del c[j:j + 2]
            j = c.index("--endpoint")
            del c[j:j + 4]
            if STORE == "local":
                c += ["--local-dir", os.path.join(LOCAL_DIR, self.bucket)]
        return c
CLIENT_CPUS = {int(x) for x in os.environ.get("ISO_CLIENT_CPUS", "1").split(",")}


LOADGEN = os.environ.get("ISO_LOADGEN", os.path.join(ROOT, "target", "release", "loadgen"))
# python: the round3 client (pymongo async, 4 processes); rust: loadgen noisy (one process), cheap enough that one CPU
# of clients saturates one CPU of engine.
HEAVY_CLIENT = os.environ.get("ISO_HEAVY_CLIENT", "rust")


def heavy_rust(port, dbs, secs, out):
    import subprocess
    cmd = ["taskset", "-c", ",".join(map(str, sorted(CLIENT_CPUS))), "nice", "-n", "10", LOADGEN, "noisy", "--uris", f"mongodb://127.0.0.1:{port}",
           "--worker-threads", "1", "--dbs", ",".join(dbs), "--conns-per-db", "4", "--secs", str(int(secs)), "--out", out]
    return subprocess.Popen(cmd)


def heavy_proc(port, dbs, secs, out):
    import asyncio
    from pymongo import AsyncMongoClient
    try:
        os.sched_setaffinity(0, CLIENT_CPUS)
        os.setpriority(os.PRIO_PROCESS, 0, 10)
    except Exception:
        pass

    async def main():
        lat, errs = [], [0]
        end = time.time() + secs

        async def loop(c, d, k):
            rng = random.Random(k)
            i = 0
            while time.time() < end:
                i += 1
                t0 = time.time()
                try:
                    if rng.random() < 0.5:
                        await c[d].items.find_one({"_id": rng.randrange(200)})
                    else:
                        await c[d].ev.insert_one({"_id": f"{int(end)}-{k}-{i}", "pad": PAD})
                    lat.append((time.time() - t0) * 1000)
                except Exception:
                    errs[0] += 1
                    await asyncio.sleep(0.01)
        cs = {d: AsyncMongoClient("127.0.0.1", port, directConnection=True, maxPoolSize=6, retryWrites=False, retryReads=False) for d in dbs}
        await asyncio.gather(*(loop(cs[d], d, hash((d, k))) for d in dbs for k in range(4)))
        json.dump({"lat": lat, "errors": errs[0]}, open(out, "w"))
    asyncio.run(main())


def light(port, secs, res):
    from pymongo import MongoClient
    c = MongoClient("127.0.0.1", port, directConnection=True)
    rng = random.Random(1)
    end = time.time() + secs
    nxt = time.time()
    i = 0
    while time.time() < end:
        nxt += rng.expovariate(2.0)
        time.sleep(max(0, nxt - time.time()))
        i += 1
        t0 = time.time()
        kind = "read" if i % 2 else "insert"
        try:
            if kind == "read":
                c.light.items.find_one({"_id": rng.randrange(200)})
            else:
                c.light.ev.insert_one({"_id": f"{int(end)}-{i}", "pad": PAD})
            res.append((kind, (time.time() - t0) * 1000))
        except Exception:
            res.append((kind, None))


def parse_mode(s):
    """NAME[@BIN]=FLAGS; a flag ENV:K=V sets an environment variable of the engine instead."""
    name, _, flags = s.partition("=")
    name, _, bin_ = name.partition("@")
    toks = flags.split()
    env = dict(t[4:].split("=", 1) for t in toks if t.startswith("ENV:"))
    return name, (bin_ or None), [t for t in toks if not t.startswith("ENV:")], env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--heavy", default="50")
    ap.add_argument("--secs", type=float, default=40)
    ap.add_argument("--mode", action="append", required=True)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--out", default=os.path.join(ROOT, "results-isolamento", "noisy.json"))
    a = ap.parse_args()
    try:
        os.sched_setaffinity(0, CLIENT_CPUS)
        os.setpriority(os.PRIO_PROCESS, 0, -15)
    except Exception as e:
        print("priority/affinity not set:", e, file=sys.stderr)
    E.ENGINE_CPUS = ENGINE_CPUS
    out = {"store": STORE, "runs": [], "engine_cpus": ENGINE_CPUS, "client_cpus": sorted(CLIENT_CPUS), "cpus": os.cpu_count()}
    if STORE in ("local", "memory"):
        import shutil
        bucket = "iso-noisy"
        shutil.rmtree(os.path.join(LOCAL_DIR, bucket), ignore_errors=True)
    else:
        bucket = E.make_bucket("iso-noisy")
    flags, _ = R.mode_flags("immediate", "noisy")
    nmax = max(int(x) for x in a.heavy.split(","))
    if STORE != "memory":
        eng = Engine("iso-setup", bucket, flags)
        eng.pin = True
        eng.start()
        E.prefill(eng, [f"h{i:02d}" for i in range(nmax)] + ["light"], parallel=8)
        eng.stop()
    modes = [parse_mode(m) for m in a.mode]
    for rep in range(a.repeat):
        for name, bin_, extra, env in modes:
            for n in [int(x) for x in a.heavy.split(",")]:
                eng = Engine(f"iso-{name}-{n}", bucket, flags + extra, bin=bin_, env=env)
                eng.pin = True
                eng.start()
                if STORE == "memory":
                    E.prefill(eng, [f"h{i:02d}" for i in range(n)] + ["light"], parallel=8)
                    time.sleep(2)
                c = eng.client()
                for d in [f"h{i:02d}" for i in range(n)] + ["light"]:
                    c[d].items.find_one({"_id": 0})   # open them all first (no cold wake inside the window)
                heavy = [f"h{i:02d}" for i in range(n)]
                procs = 4 if HEAVY_CLIENT == "python" else 1
                outs = [os.path.join(WORK, f"iso-h{k}.json") for k in range(procs)]
                for o in outs:
                    if os.path.exists(o):
                        os.remove(o)
                p0, t0 = proc_stat(eng.pid), time.time()
                m0 = eng.admin("GET", "/v1/metrics/databases")[1]
                if HEAVY_CLIENT == "python":
                    ps = [mp.Process(target=heavy_proc, args=(eng.port, heavy[k::procs], a.secs, outs[k])) for k in range(procs) if heavy[k::procs]]
                    [p.start() for p in ps]
                else:
                    ps = [heavy_rust(eng.port, heavy, a.secs, outs[0])]
                lres = []
                th = threading.Thread(target=light, args=(eng.port, a.secs, lres))
                th.start()
                lags = []
                while th.is_alive():
                    s = eng.admin("GET", "/v1/metrics/databases")[1]["server"]
                    lags.append(s["schedulingLagMs"])
                    time.sleep(2)
                [p.join() if HEAVY_CLIENT == "python" else p.wait() for p in ps]
                dt = time.time() - t0
                p1 = proc_stat(eng.pid)
                m1 = eng.admin("GET", "/v1/metrics/databases")[1]
                hl, herr, hstat = [], 0, None
                for o in outs:
                    if os.path.exists(o):
                        h = json.load(open(o))
                        if "lat" in h:
                            hl += h["lat"]
                        else:
                            hstat = h
                        herr += h["errors"]
                        os.remove(o)
                ok = [x for _, x in lres if x is not None]
                rd = [x for k, x in lres if x is not None and k == "read"]
                ins = [x for k, x in lres if x is not None and k == "insert"]
                ld = m1["databases"].get("light", {})
                r = {"mode": name, "flags": " ".join(extra + [f"ENV:{k}={v}" for k, v in env.items()]), "bin": bin_ or "current", "rep": rep, "heavy_dbs": n,
                     "heavy_client": HEAVY_CLIENT,
                     "heavy_req_per_s": round((hstat["ops"] / hstat["secs"]) if hstat else len(hl) / dt, 1),
                     "heavy_p50_ms": round(hstat["p50_ms"] if hstat else pct(hl, 50), 1), "heavy_p99_ms": round(hstat["p99_ms"] if hstat else pct(hl, 99), 1), "heavy_errors": herr,
                     "light_requests": len(lres), "light_errors": len(lres) - len(ok), "light_p50_ms": round(pct(ok, 50), 1), "light_p99_ms": round(pct(ok, 99), 1),
                     "light_max_ms": round(max(ok), 1), "light_read_p99_ms": round(pct(rd, 99), 1), "light_insert_p50_ms": round(pct(ins, 50), 1), "light_insert_p99_ms": round(pct(ins, 99), 1),
                     "engine_cpu_pct_of_1_core": round((p1["cpu_s"] - p0["cpu_s"]) / dt * 100, 1), "engine_rss_mb": round(p1["rss_mb"], 1),
                     "sched_lag_ms_avg": round(sum(lags) / max(1, len(lags)), 2), "sched_lag_ms_max_sample": round(max(lags), 2) if lags else None,
                     "saturated_s": round((m1["server"]["saturatedMs"] - m0["server"]["saturatedMs"]) / 1000, 1),
                     "queued_commands": m1["server"]["queuedCommands"] - m0["server"]["queuedCommands"],
                     "light_queue_waits": ld.get("queueWaits"), "light_queue_wait_max_ms": ld.get("queueWaitMaxMs"),
                     "quota_waits_heavy": sum(v.get("quotaWaits", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                     "quota_rejections_heavy": sum(v.get("quotaRejections", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                     "sched_lag_max_ms_server": m1["server"].get("schedulingLagMaxMs"),
                     "light_points": ld.get("points"), "heavy_points": round(sum(v.get("points", 0) or 0 for k, v in m1["databases"].items() if k.startswith("h")), 1),
                     "heavy_db_commands_min_max": [min(v["commands"] - m0["databases"].get(k, {}).get("commands", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                                                   max(v["commands"] - m0["databases"].get(k, {}).get("commands", 0) for k, v in m1["databases"].items() if k.startswith("h"))],
                     "heavy_queue_wait_max_ms": max(v.get("queueWaitMaxMs", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                     "heavy_flush_wait_max_ms": max(v.get("flushWaitMaxMs", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                     "heavy_flush_waits": sum(v.get("flushWaits", 0) for k, v in m1["databases"].items() if k.startswith("h")),
                     "light_latencies_ms": [[k, None if x is None else round(x, 2)] for k, x in lres]}
                print(json.dumps({k: v for k, v in r.items() if k != "light_latencies_ms"}), flush=True)
                out["runs"].append(r)
                eng.stop()
                dump(a.out, out)


if __name__ == "__main__":
    main()
