"""Noisy neighbours (MinIO, engine on 2 CPUs): N heavy databases, each with 4 connections sending back to back (50/50 reads
and 1 KB inserts), plus 1 light database at 2 requests/s (50/50, open loop). N = 5, 10, 25, 50, with the fair queue off
(--fair-lag-ms 0) and on (default). Per step: the light database's p50/p99, the heavy ones' throughput and p99, engine CPU,
and the server signals of GET /v1/metrics/databases (scheduling lag, saturated time, queued commands).
usage: noisy.py [--heavy 5,10,25,50] [--secs 40]"""
import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine as E
import run as R

PAD = "y" * 900
# The engine gets ROUND3_ENGINE_CPUS (default here: 1 CPU, so that it saturates before the Python clients do); the heavy
# clients NOISY_HEAVY_CPUS, the light client (this process) NOISY_LIGHT_CPUS.
HEAVY_CPUS = {int(x) for x in os.environ.get("NOISY_HEAVY_CPUS", "2,3").split(",")}
LIGHT_CPUS = {int(x) for x in os.environ.get("NOISY_LIGHT_CPUS", "1").split(",")}


def heavy_proc(port, dbs, secs, out):
    from pymongo import AsyncMongoClient
    try:
        os.sched_setaffinity(0, HEAVY_CPUS)
    except Exception:
        pass

    async def main():
        lat, n = [], [0]
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
                    pass
        cs = {d: AsyncMongoClient("127.0.0.1", port, directConnection=True, maxPoolSize=6) for d in dbs}
        await asyncio.gather(*(loop(cs[d], d, hash((d, k))) for d in dbs for k in range(4)))
        json.dump(lat, open(out, "w"))
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
        try:
            if i % 2:
                c.light.items.find_one({"_id": rng.randrange(200)})
            else:
                c.light.ev.insert_one({"_id": f"{int(end)}-{i}", "pad": PAD})
            res.append((time.time() - t0) * 1000)
        except Exception:
            res.append(None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--heavy", default="5,10,25,50")
    ap.add_argument("--secs", type=float, default=40)
    ap.add_argument("--out", default=os.path.join(ROOT, "results-round3", "minio", "noisy.json"))
    a = ap.parse_args()
    os.environ.setdefault("ROUND3_ENGINE_CPUS", "0")
    E.ENGINE_CPUS = os.environ["ROUND3_ENGINE_CPUS"]
    try:
        os.sched_setaffinity(0, LIGHT_CPUS)
    except Exception:
        pass
    out = {"runs": [], "engine_cpus": E.ENGINE_CPUS, "heavy_client_cpus": sorted(HEAVY_CPUS), "light_client_cpus": sorted(LIGHT_CPUS)}
    bucket = E.make_bucket("r3-noisy")
    flags, _ = R.mode_flags("immediate", "noisy")
    nmax = max(int(x) for x in a.heavy.split(","))
    eng = E.Engine("noisy-setup", bucket, flags).start()
    E.prefill(eng, [f"h{i:02d}" for i in range(nmax)] + ["light"], parallel=16)
    eng.stop()
    for fair in ("off", "on"):
        for n in [int(x) for x in a.heavy.split(",")]:
            eng = E.Engine(f"noisy-{fair}-{n}", bucket, flags + (["--fair-lag-ms", "0"] if fair == "off" else [])).start()
            c = eng.client()
            for d in [f"h{i:02d}" for i in range(n)] + ["light"]:
                c[d].items.find_one({"_id": 0})   # open them all first (no cold wake inside the window)
            heavy = [f"h{i:02d}" for i in range(n)]
            procs = 4
            outs = [os.path.join(WORK, f"noisy-h{k}.json") for k in range(procs)]
            ps = [mp.Process(target=heavy_proc, args=(eng.port, heavy[k::procs], a.secs, outs[k])) for k in range(procs) if heavy[k::procs]]
            p0, t0 = proc_stat(eng.pid), time.time()
            m0 = eng.admin("GET", "/v1/metrics/databases")[1]["server"]
            [p.start() for p in ps]
            lres = []
            th = threading.Thread(target=light, args=(eng.port, a.secs, lres))
            th.start()
            lags = []
            while th.is_alive():
                s = eng.admin("GET", "/v1/metrics/databases")[1]["server"]
                lags.append(s["schedulingLagMs"])
                time.sleep(2)
            [p.join() for p in ps]
            dt = time.time() - t0
            p1 = proc_stat(eng.pid)
            m1 = eng.admin("GET", "/v1/metrics/databases")[1]
            hl = []
            for o in outs:
                if os.path.exists(o):
                    hl += json.load(open(o))
                    os.remove(o)
            ok = [x for x in lres if x is not None]
            r = {"fair": fair, "heavy_dbs": n, "heavy_req_per_s": round(len(hl) / dt, 1), "heavy_p50_ms": round(pct(hl, 50), 1), "heavy_p99_ms": round(pct(hl, 99), 1),
                 "light_requests": len(lres), "light_errors": len(lres) - len(ok), "light_p50_ms": round(pct(ok, 50), 1), "light_p99_ms": round(pct(ok, 99), 1),
                 "light_max_ms": round(max(ok), 1), "engine_cpu_pct_of_1_core": round((p1["cpu_s"] - p0["cpu_s"]) / dt * 100, 1),
                 "sched_lag_ms_avg": round(sum(lags) / max(1, len(lags)), 2), "sched_lag_ms_max_sample": round(max(lags), 2) if lags else None,
                 "saturated_s": round((m1["server"]["saturatedMs"] - m0["saturatedMs"]) / 1000, 1), "queued_commands": m1["server"]["queuedCommands"] - m0["queuedCommands"],
                 "light_queue_waits": m1["databases"].get("light", {}).get("queueWaits"), "light_queue_wait_max_ms": m1["databases"].get("light", {}).get("queueWaitMaxMs")}
            print(json.dumps(r), flush=True)
            out["runs"].append(r)
            eng.stop()
            dump(a.out, out)


if __name__ == "__main__":
    main()
