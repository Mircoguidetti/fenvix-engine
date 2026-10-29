"""Ceiling of ONE database (MinIO): one database, one client with C parallel connections each sending its next request as soon
as the previous one is answered (closed loop), C = 1, 4, 16, 64, 256, STEP seconds each. Profiles: read, write, half. Write
modes: immediate, adaptive, relaxed, local. Engine pinned to 2 CPUs (0-1) or 4 (0-3; the client then shares them).
Per step: requests/s served, engine CPU, RSS, p50/p99, S3 requests per request (MinIO counters) and USD/month at that rate.
usage: ceiling.py [--cpus 2,4] [--modes immediate,adaptive,relaxed,local] [--profiles read,write,half] [--step 20]"""
import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine as E
import run as R

PAD = "y" * 900


def client_proc(port, conc, secs, profile, wc, out, seed):
    from pymongo import AsyncMongoClient, WriteConcern

    async def main():
        c = AsyncMongoClient("127.0.0.1", port, directConnection=True, maxPoolSize=conc + 2, retryWrites=False, retryReads=False)
        w = c.cdb.get_collection("ev", write_concern=WriteConcern(w=1, j=False)) if wc == "j0" else c.cdb.ev
        items = c.cdb.items
        lat, errs = [], [0]
        end = time.time() + secs
        rng = random.Random(seed)

        async def loop(k):
            i = 0
            while time.time() < end:
                i += 1
                t0 = time.time()
                try:
                    if profile == "read" or (profile == "half" and rng.random() < 0.5):
                        await items.find_one({"_id": rng.randrange(2000)})
                    else:
                        await w.insert_one({"_id": f"{seed}-{k}-{i}", "pad": PAD})
                    lat.append((time.time() - t0) * 1000)
                except Exception:
                    errs[0] += 1
        await asyncio.gather(*(loop(k) for k in range(conc)))
        await c.close()
        json.dump({"lat": lat, "errs": errs[0]}, open(out, "w"))
    asyncio.run(main())


def step(eng, conc, secs, profile, wc):
    procs = max(1, min(4, conc // 16))
    per = [conc // procs + (1 if i < conc % procs else 0) for i in range(procs)]
    outs = [os.path.join(WORK, f"ceil-{i}.json") for i in range(procs)]
    m0, p0, t0 = minio_counts(), proc_stat(eng.pid), time.time()
    ps = [mp.Process(target=client_proc, args=(eng.port, per[i], secs, profile, wc, outs[i], int(t0) * 10 + i)) for i in range(procs)]
    [p.start() for p in ps]
    rss = 0
    while any(p.is_alive() for p in ps):
        rss = max(rss, proc_stat(eng.pid)["rss_mb"])
        time.sleep(1)
    t1, p1 = time.time(), proc_stat(eng.pid)
    m1 = minio_counts()
    lat, errs = [], 0
    for o in outs:
        d = json.load(open(o))
        lat += d["lat"]
        errs += d["errs"]
    dt = t1 - t0
    put, get, _, _ = classify(diff(m0, m1))
    rps = len(lat) / dt
    return {"connections": conc, "req_per_s": round(rps, 1), "engine_cpu_pct_of_1_core": round((p1["cpu_s"] - p0["cpu_s"]) / dt * 100, 1),
            "rss_mb_max": round(rss, 1), "p50_ms": round(pct(lat, 50), 2) if lat else None, "p99_ms": round(pct(lat, 99), 2) if lat else None,
            "errors": errs, "s3_per_request": round((put + get) / max(1, len(lat)), 3),
            "usd_month_at_this_rate": round(usd(put, get) / dt * MONTH_S, 1), "put_per_s": round(put / dt, 1), "get_per_s": round(get / dt, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cpus", default="2,4")
    ap.add_argument("--modes", default="immediate,adaptive,relaxed,local")
    ap.add_argument("--profiles", default="read,write,half")
    ap.add_argument("--conc", default="1,4,16,64,256")
    ap.add_argument("--step", type=float, default=20)
    ap.add_argument("--out", default=os.path.join(ROOT, "results-round3", "minio", "ceiling.json"))
    a = ap.parse_args()
    res = json.load(open(a.out)) if os.path.exists(a.out) else {"runs": []}
    done = {(r["cpus"], r["mode"], r["profile"]) for r in res["runs"]}
    for cpus in [int(x) for x in a.cpus.split(",")]:
        os.environ["ROUND3_ENGINE_CPUS"] = "0,1" if cpus == 2 else "0-3"
        E.ENGINE_CPUS = os.environ["ROUND3_ENGINE_CPUS"]
        for mode in a.modes.split(","):
            for profile in a.profiles.split(","):
                if profile == "read" and mode != "immediate":
                    continue  # no write: every write mode serves reads the same way
                if (cpus, mode, profile) in done:
                    continue
                name = f"ceil-{cpus}cpu-{mode}-{profile}"
                flags, _ = R.mode_flags(mode, name)
                bucket = E.make_bucket(f"r3-{name}")
                eng = E.Engine(name, bucket, flags).start()
                c = eng.client()
                c.cdb.items.insert_many([{"_id": i, "pad": "x" * 900} for i in range(2000)])
                c.cdb.items.find_one({"_id": 1})
                steps = []
                for conc in [int(x) for x in a.conc.split(",")]:
                    s = step(eng, conc, a.step, profile, "j0" if mode == "relaxed" else "default")
                    steps.append(s)
                    print(json.dumps({"run": name, **s}), flush=True)
                eng.stop()
                best = max(steps, key=lambda s: s["req_per_s"])
                res["runs"].append({"cpus": cpus, "mode": mode, "profile": profile, "steps": steps, "max_req_per_s": best["req_per_s"],
                                    "at_connections": best["connections"]})
                dump(a.out, res)


if __name__ == "__main__":
    main()
