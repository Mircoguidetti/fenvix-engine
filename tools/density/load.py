"""Load clients: one pymongo AsyncMongoClient per database (TLS + SCRAM-SHA-256 with that database's own user),
spread over several processes. Every request is recorded: wall time, database, type, latency, outcome, and the
delay between its planned start and its real start (a client that cannot keep up shows here, not as engine latency).

Modes (per database):
  steady  RATE requests/s with exponential gaps (Poisson); 90% reads (find by _id 50%, find by the indexed k 40%),
          10% writes (update by _id 5%, insert 5%)
  burst   like steady, but only 10% of the databases are active in each minute, in turn (minute m: databases whose
          index % 10 == m % 10)
  period  one request every PERIOD seconds, first one at a random offset in [0, PERIOD); same mix

usage: load.py MODE N DURATION_S OUT_PREFIX [--rate R] [--period P] [--procs K] [--offset O]"""
import argparse
import asyncio
import csv
import multiprocessing as mp
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa

OPS = [("read_id", 0.50), ("read_idx", 0.40), ("update", 0.05), ("insert", 0.05)]
MIX = OPS  # --mix replaces it (diagnostic runs only)


def pick(rng):
    x = rng.random()
    for name, p in MIX:
        if x < p:
            return name
        x -= p
    return MIX[-1][0]


async def do_op(d, op, rng, i):
    if op == "read_id":
        await d.items.find_one({"_id": rng.randrange(100)})
    elif op == "read_idx":
        await d.items.find({"k": rng.randrange(50)}).limit(10).to_list()
    elif op == "update":
        await d.items.update_one({"_id": rng.randrange(100)}, {"$inc": {"n": 1}})
    else:
        await d.events.insert_one({"t": time.time(), "c": i, "pad": "y" * 900})


async def db_loop(i, args, t_start, t_end, rows, rng):
    from pymongo import AsyncMongoClient
    db = db_name(i)
    c = AsyncMongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, username="u", password=user_pw(i),
                         authSource=db, maxPoolSize=2, serverMonitoringMode="poll", heartbeatFrequencyMS=30000,
                         serverSelectionTimeoutMS=60000, socketTimeoutMS=120000, appname=f"density-{db}")
    d = c[db]
    # connection, TLS and SCRAM before the measured window (not counted)
    while time.time() < t_start - 0.5:
        try:
            await c.admin.command("ping")
            break
        except Exception:
            await asyncio.sleep(1)
    now = max(time.time(), t_start)
    if args.mode == "period":
        nxt = t_start + rng.uniform(0, args.period)
    else:
        nxt = t_start + rng.expovariate(args.rate)
    while True:
        if args.mode == "burst":
            # skip to the next minute where this database is active
            while True:
                m = int((nxt - t_start) // 60)
                if m % 10 == i % 10:
                    break
                nxt = t_start + (m + 1) * 60 + rng.uniform(0, 1.0 / args.rate)
        if nxt >= t_end or (args.stop_file and os.path.exists(args.stop_file)):
            break
        wait = nxt - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        op = pick(rng)
        t0 = time.time()
        ok, err = 1, ""
        try:
            await do_op(d, op, rng, i)
        except Exception as e:  # recorded, not fatal
            ok, err = 0, type(e).__name__ + ":" + str(e)[:80].replace(",", ";").replace("\n", " ")
        t1 = time.time()
        rows.append((round(t0 - t_start, 4), i, op, round((t1 - t0) * 1000, 3), ok, round((t0 - nxt) * 1000, 1), err))
        nxt = nxt + (args.period if args.mode == "period" else rng.expovariate(args.rate))
    await c.close()


def proc_main(idx, args, t_start, t_end, out):
    dbs = [i for i in range(args.offset, args.offset + args.n) if i % args.procs == idx]
    rows = []
    rng = random.Random(1000 + idx)

    async def run():
        await asyncio.gather(*(db_loop(i, args, t_start, t_end, rows, random.Random(i * 7919 + idx)) for i in dbs))
    asyncio.run(run())
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "db", "op", "lat_ms", "ok", "late_ms", "err"])
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["steady", "burst", "period"])
    ap.add_argument("n", type=int)
    ap.add_argument("duration", type=float)
    ap.add_argument("out_prefix")
    ap.add_argument("--rate", type=float, default=1.0)
    ap.add_argument("--period", type=float, default=70.0)
    ap.add_argument("--procs", type=int, default=2)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--warmup", type=float, default=None, help="seconds for connect+auth before the window")
    ap.add_argument("--t-start", type=float, default=None)
    ap.add_argument("--stop-file", default=None, help="stop early (rows kept) once this file exists")
    ap.add_argument("--mix", default=None, help="e.g. read_id=1 or update=0.5,insert=0.5 (diagnostic runs)")
    args = ap.parse_args()
    if args.mix:
        global MIX
        MIX = [(k, float(v)) for k, v in (x.split("=") for x in args.mix.split(","))]
    warm = args.warmup if args.warmup is not None else max(20.0, args.n / 25)
    t_start = args.t_start or (time.time() + warm)
    t_end = t_start + args.duration
    print(f"T_START {t_start:.3f}", flush=True)
    ps = [mp.Process(target=proc_main, args=(k, args, t_start, t_end, f"{args.out_prefix}.p{k}.csv")) for k in range(args.procs)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    print("load done", [p.exitcode for p in ps], flush=True)


if __name__ == "__main__":
    main()
