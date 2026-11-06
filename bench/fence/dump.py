#!/usr/bin/env python3
"""Fence-layout test: write the chat dataset of the compaction bench to files instead of loading it into a server.

The messages come from `cold_customer.gen_batches`, the same generator and the same random sequence as
`cold_customer.py load` with the same --gb/--customers/--zipf/--seed/--workers, so the files and a server loaded by
`cold_customer.py load` hold the same documents.

Output in OUT:
  part-WWW-PPP.rec   records of worker W whose userId falls in partition P (P = userId * parts // customers):
                     u32 bson length, i64 userId, i64 n (global order = ts order), 12-byte ObjectId, the BSON bytes
  model.json         the exact model: per user the message count and its 20 newest messages (by ts) as
                     [ObjectId hex, sha256 of the BSON bytes]; the 300 warm-up users and the 400 measured users
                     (200 drawn by activity, 200 uniform), drawn exactly as `cold_customer.py measure` draws them
"""
import argparse
import collections
import datetime as dt
import hashlib
import json
import multiprocessing as mp
import os
import random
import struct
import sys
import time

import bson

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "compaction"))
import cold_customer as cc  # noqa: E402

HDR = struct.Struct("<Iqq12s")


def work(args):
    out, wid, nw, per, customers, s, seed, parts = args
    t0 = dt.datetime(2026, 1, 1)
    files = {}
    tail = collections.defaultdict(lambda: collections.deque(maxlen=20))
    counts = collections.Counter()
    nbytes = 0
    for batch in cc.gen_batches(wid, nw, per, customers, s, seed, t0):
        for n, d in batch:
            u = d["userId"]
            raw = bson.encode(d)
            p = u * parts // customers
            f = files.get(p)
            if f is None:
                f = files[p] = open(os.path.join(out, f"part-{wid:03d}-{p:03d}.rec"), "wb", buffering=1 << 20)
            f.write(HDR.pack(len(raw), u, n, d["_id"].binary))
            f.write(raw)
            nbytes += len(raw)
            counts[u] += 1
            tail[u].append((n, str(d["_id"]), hashlib.sha256(raw).hexdigest()))
    for f in files.values():
        f.close()
    return dict(counts), {u: list(q) for u, q in tail.items()}, nbytes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--gb", type=float, default=10.0)
    ap.add_argument("--customers", type=int, default=20000)
    ap.add_argument("--zipf", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--parts", type=int, default=64)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    avg = 470  # as cold_customer.load
    total = int(a.gb * (1 << 30) / avg)
    per = total // a.workers
    t = time.time()
    with mp.Pool(a.workers) as p:
        res = p.map(work, [(a.out, w, a.workers, per, a.customers, a.zipf, a.seed, a.parts) for w in range(a.workers)])
    counts, tails, nbytes = collections.Counter(), collections.defaultdict(list), 0
    for c, tl, nb in res:
        counts.update(c)
        for u, q in tl.items():
            tails[u].extend(q)
        nbytes += nb
    newest = {u: [[oid, h] for _, oid, h in sorted(q, reverse=True)[:20]] for u, q in tails.items()}
    # the users, drawn exactly as cold_customer.measure draws them (same seed, same order of draws)
    r = random.Random(a.seed)
    eligible = [u for u, n in counts.items() if n >= 20]
    weights = [counts[u] for u in eligible]
    if len(eligible) < 700:
        sys.exit(f"only {len(eligible)} customers have 20 messages: too few to draw 700 distinct ones")
    picked = set()

    def pick(weighted):
        while True:
            u = r.choices(eligible, weights=weights)[0] if weighted else r.choice(eligible)
            if u not in picked:
                picked.add(u)
                return u

    warmup = [pick(False) for _ in range(300)]
    active = [pick(True) for _ in range(200)]
    uniform = [pick(False) for _ in range(200)]
    model = {"docs": per * a.workers, "bson_bytes": nbytes, "customers": a.customers, "zipf": a.zipf, "seed": a.seed, "workers": a.workers,
             "gb": a.gb, "parts": a.parts, "dump_secs": time.time() - t, "counts": {str(u): n for u, n in counts.items()},
             "newest": {str(u): v for u, v in newest.items()}, "warmup": warmup, "active": active, "uniform": uniform}
    json.dump(model, open(os.path.join(a.out, "model.json"), "w"))
    print(f"dumped {per * a.workers} messages, {len(counts)} customers, {nbytes / 2**30:.2f} GiB of BSON in {time.time() - t:.0f} s")


if __name__ == "__main__":
    main()
