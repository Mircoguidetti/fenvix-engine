"""Open-loop load on N databases (pymongo AsyncMongoClient, no TLS, no auth: the S3 request counts do not depend on them).

Every database gets RATE requests/s with exponential gaps (Poisson), started on schedule whatever the previous request is
doing (a slow engine shows up as latency, not as a lower rate). Mix: `read` (find_one by _id of a prefilled document),
`write` (insert_one of a ~1 KB document), `half` (50/50). Write concern: `default` (none sent: durable) or `j0`
({w: 1, j: false}: the relaxed mode acknowledges these before they are durable when the database has it switched on).

Every acknowledged insert is appended to ACKED (one `_id` per line, flushed per line) so a kill test can check which
acknowledged writes survived.

usage: load.py --port P --dbs c0000,c0001,... --rate R --mix half --secs S --out OUT.json [--wc j0] [--acked F]
               [--procs K] [--pattern steady|burst] [--burst-on 30 --burst-every 300]"""
import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import pct  # noqa

PAD = "y" * 900


async def db_loop(db, args, t_start, t_end, lat, errs, acked, rng, k):
    from pymongo import AsyncMongoClient, WriteConcern
    c = AsyncMongoClient("127.0.0.1", args.port, directConnection=True, maxPoolSize=args.pool, serverSelectionTimeoutMS=60000,
                         socketTimeoutMS=120000, retryWrites=False, retryReads=False, appname=f"round3-{db}")
    wc = WriteConcern(w=1, j=False) if args.wc == "j0" else None
    coll = c[db].get_collection("events", write_concern=wc) if wc else c[db].events
    items = c[db].items
    try:
        await c.admin.command("ping")
    except Exception:
        pass
    seq = 0
    inflight = set()
    nxt = t_start + (0.3 * rng.random() if args.first_at_start else rng.expovariate(args.rate))
    first = [True]

    async def one(op, planned, seq):
        t0 = time.time()
        try:
            if op == "r":
                await items.find_one({"_id": rng.randrange(args.prefill)})
            else:
                _id = f"{db}-{k}-{seq}"
                await coll.insert_one({"_id": _id, "t": t0, "pad": PAD})
                if acked is not None:
                    acked.write(_id + "\n")
                    acked.flush()
            ms = (time.time() - t0) * 1000
            lat[op].append((round(t0 - t_start, 3), round(ms, 3)))
            if first[0]:
                first[0] = False
                lat["first"].append(ms)
        except Exception as e:  # recorded, not fatal
            errs.append(f"{op}:{type(e).__name__}:{str(e)[:120]}")

    while True:
        if args.pattern == "burst":
            # active for `burst_on` seconds out of every `burst_every`, each database with its own phase
            phase = zlib.crc32(db.encode()) % int(args.burst_every)
            while True:
                pos = (nxt - t_start + phase) % args.burst_every
                if pos < args.burst_on:
                    break
                nxt += args.burst_every - pos + rng.uniform(0, 0.5)
        if nxt >= t_end:
            break
        w = nxt - time.time()
        if w > 0:
            await asyncio.sleep(w)
        if args.mix == "read":
            op = "r"
        elif args.mix == "write":
            op = "w"
        else:
            op = "w" if rng.random() < 0.5 else "r"
        seq += 1
        t = asyncio.create_task(one(op, nxt, seq))
        inflight.add(t)
        t.add_done_callback(inflight.discard)
        nxt += rng.expovariate(args.rate)
    if inflight:
        await asyncio.wait(inflight, timeout=120)
    await c.close()


def proc_main(idx, args, dbs, t_start, t_end, out):
    lat = {"r": [], "w": [], "first": []}
    errs = []
    acked = open(f"{args.acked}.p{idx}", "a") if args.acked else None

    async def run():
        await asyncio.gather(*(db_loop(d, args, t_start, t_end, lat, errs, acked, random.Random(zlib.crc32(d.encode()) ^ idx), idx) for d in dbs))
    asyncio.run(run())
    with open(out, "w") as f:
        json.dump({"lat": lat, "errs": errs}, f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dbs", required=True)
    ap.add_argument("--rate", type=float, default=1.0)
    ap.add_argument("--mix", choices=["read", "write", "half"], default="half")
    ap.add_argument("--secs", type=float, default=60)
    ap.add_argument("--wc", choices=["default", "j0"], default="default")
    ap.add_argument("--acked", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--procs", type=int, default=1)
    ap.add_argument("--pool", type=int, default=4)
    ap.add_argument("--prefill", type=int, default=200)
    ap.add_argument("--pattern", choices=["steady", "burst"], default="steady")
    ap.add_argument("--burst-on", type=float, default=30)
    ap.add_argument("--burst-every", type=float, default=300)
    ap.add_argument("--t-start", type=float, default=None)
    ap.add_argument("--first-at-start", action="store_true", help="first request of every database within 0.3 s of the start")
    ap.add_argument("--lat-after", type=float, default=0, help="also report percentiles of the requests started after this many seconds")
    args = ap.parse_args()
    dbs = args.dbs.split(",")
    t_start = args.t_start or time.time() + 3
    t_end = t_start + args.secs
    procs = min(args.procs, len(dbs))
    parts = [f"{args.out}.part{k}" for k in range(procs)]
    ps = [mp.Process(target=proc_main, args=(k, args, dbs[k::procs], t_start, t_end, parts[k])) for k in range(procs)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    lat = {"r": [], "w": [], "first": []}
    errs = []
    for f in parts:
        try:
            d = json.load(open(f))
        except Exception:
            continue
        for k in lat:
            lat[k] += d["lat"][k]
        errs += d["errs"]
        os.remove(f)
    res = {"t_start": t_start, "t_end": t_end, "dbs": len(dbs), "rate": args.rate, "mix": args.mix, "wc": args.wc,
           "n_read": len(lat["r"]), "n_write": len(lat["w"]), "errors": len(errs), "error_samples": errs[:10]}
    for k, name in (("r", "read"), ("w", "write")):
        xs = [ms for _, ms in lat[k]]
        st = [ms for t, ms in lat[k] if t >= args.lat_after]
        m1 = [ms for t, ms in lat[k] if t < 60]
        for p in (50, 95, 99):
            res[f"{name}_p{p}_ms"] = round(pct(xs, p), 2) if xs else None
            res[f"{name}_steady_p{p}_ms"] = round(pct(st, p), 2) if st else None
        res[f"minute1_{name}_p99_ms"] = round(pct(m1, 99), 2) if m1 else None
        res[f"{name}_max_ms"] = round(max(xs), 1) if xs else None
    if lat["first"]:
        for p in (50, 99):
            res[f"first_p{p}_ms"] = round(pct(lat["first"], p), 1)
        res["first_max_ms"] = round(max(lat["first"]), 1)
    with open(args.out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps({k: res[k] for k in ("n_read", "n_write", "errors", "read_p50_ms", "write_p50_ms", "write_p99_ms")}), flush=True)


if __name__ == "__main__":
    main()
