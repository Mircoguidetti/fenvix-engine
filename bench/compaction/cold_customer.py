#!/usr/bin/env python3
"""Owner · Compaction, task 1: S3 GETs of a cold find of one customer's recent messages.

Chat-like data (many customers, zipf activity) is loaded through mongo-server into a local MinIO; then the server is
restarted (nothing in its caches) and, for customers never read before, the find

    db.messages.find({userId: X}).sort({ts: -1}).limit(20)        (index {userId: 1, ts: -1})

is run one at a time while the server is quiet. The GETs of each find are read from the server's own meter of
object-store reads (serverStatus fenvix.waiter.reads: every GET the process sends, counted where it leaves the process).
Before measuring, `--warmup` finds of other customers load the SST indexes and filters into the server's metadata cache
(as in M21/M29: index blocks and filters warm, data blocks cold).

Every answer is checked against what was loaded (20 documents of that customer, newest first).

Usage:
  cold_customer.py load    --uri URI --gb 2 --customers 20000 --state state.json [--customer-key]
  cold_customer.py measure --uri URI --state state.json --n 200 --warmup 300 --out result.json
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import time
import datetime as dt

from bson import ObjectId
from pymongo import MongoClient

DB, COLL = "chat", "messages"
WORDS = None


def vocab(seed=7, n=4000):
    r = random.Random(seed)
    letters = "abcdefghijklmnopqrstuvwxyz"
    words = ["".join(r.choice(letters) for _ in range(r.randint(2, 9))) for _ in range(n)]
    weights = [1.0 / (i + 1) for i in range(n)]
    return words, weights


def zipf_weights(n, s):
    return [1.0 / ((i + 1) ** s) for i in range(n)]


def worker(args):
    uri, wid, nworkers, n_docs, customers, s, seed, t0 = args
    words, ww = vocab()
    r = random.Random(seed * 1000 + wid)
    cw = zipf_weights(customers, s)
    # a fixed shuffle of customer ids, so the busiest customers are not the smallest ids
    perm = list(range(customers))
    random.Random(seed).shuffle(perm)
    c = MongoClient(uri, w=1)
    coll = c[DB][COLL]
    batch, done = [], 0
    counts = {}
    last = {}
    i = 0
    while i < n_docs:
        k = min(1000, n_docs - i)
        cust = r.choices(range(customers), weights=cw, k=k)
        for j in range(k):
            n = (i + j) * nworkers + wid  # global order: workers interleave in time
            ts = t0 + dt.timedelta(milliseconds=n * 10)
            u = perm[cust[j]]
            text = " ".join(r.choices(words, weights=ww, k=r.randint(30, 80)))
            oid = ObjectId(int(ts.timestamp()).to_bytes(4, "big") + n.to_bytes(8, "big"))
            batch.append({"_id": oid, "userId": u, "convId": u * 16 + r.randint(0, 15), "role": "user" if n % 2 else "assistant", "ts": ts, "text": text, "tokens": len(text) // 4})
            counts[u] = counts.get(u, 0) + 1
            last[u] = max(last.get(u, 0), n)
        coll.insert_many(batch, ordered=False)
        batch = []
        i += k
    return counts, last


def load(a):
    t0 = dt.datetime(2026, 1, 1)
    avg = 470  # bytes per message as BSON (MEASURED on the generator: 469.8 over 2,000 messages)
    total = int(a.gb * (1 << 30) / avg)
    c = MongoClient(a.uri)
    c[DB][COLL].drop()
    if a.customer_key:
        c[DB].create_collection(COLL, customerKey="userId")
    c[DB][COLL].create_index([("userId", 1), ("ts", -1)])
    nw = a.workers
    per = total // nw
    t = time.time()
    with mp.Pool(nw) as p:
        res = p.map(worker, [(a.uri, w, nw, per, a.customers, a.zipf, a.seed, t0) for w in range(nw)])
    counts, last = {}, {}
    for cn, ls in res:
        for u, n in cn.items():
            counts[u] = counts.get(u, 0) + n
        for u, n in ls.items():
            last[u] = max(last.get(u, 0), n)
    secs = time.time() - t
    st = c[DB].command("collStats", COLL)
    state = {"docs": per * nw, "customers": a.customers, "zipf": a.zipf, "seed": a.seed, "customer_key": a.customer_key, "load_secs": secs,
             "counts": counts, "data_bytes": st.get("size"), "storage_bytes": st.get("storageSize")}
    json.dump(state, open(a.state, "w"))
    print(f"loaded {per * nw} messages, {len(counts)} customers, {st.get('size', 0) / 2**30:.2f} GiB of BSON in {secs:.0f} s", flush=True)


def fen(admin):
    return admin.command("serverStatus").get("fenvix", {})


def quiet(admin, timeout=120.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        f = fen(admin)
        w = f.get("waiter", {})
        busy = w.get("readsInFlight", 0) + w.get("readAhead", {}).get("busy", 0) + w.get("maps", {}).get("busy", 0) + f.get("compaction", {}).get("running", 0)
        if busy == 0:
            return f
        time.sleep(0.05)
    return fen(admin)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))]


def measure(a):
    state = json.load(open(a.state))
    counts = {int(k): v for k, v in state["counts"].items()}
    c = MongoClient(a.uri)
    admin, coll = c.admin, c[DB][COLL]
    r = random.Random(a.seed)
    eligible = [u for u, n in counts.items() if n >= 20]
    weights = [counts[u] for u in eligible]
    picked = set()

    def pick(weighted):
        while True:
            u = r.choices(eligible, weights=weights)[0] if weighted else r.choice(eligible)
            if u not in picked:
                picked.add(u)
                return u

    def run(u):
        docs = list(coll.find({"userId": u}).sort("ts", -1).limit(20))
        assert len(docs) == 20, (u, len(docs))
        assert all(d["userId"] == u for d in docs), u
        assert all(docs[i]["ts"] >= docs[i + 1]["ts"] for i in range(19)), u
        return docs

    t = time.time()
    for _ in range(a.warmup):
        run(pick(False))
    print(f"warmup {a.warmup} finds in {time.time() - t:.0f} s", flush=True)
    rows = []
    for group, weighted in (("active (customers drawn by activity)", True), ("any (customers drawn uniformly)", False)):
        for _ in range(a.n):
            u = pick(weighted)
            f1 = quiet(admin)
            t1 = time.perf_counter()
            run(u)
            ms = (time.perf_counter() - t1) * 1000
            f2 = quiet(admin)
            gets = f2["waiter"]["reads"] - f1["waiter"]["reads"]
            rows.append({"group": group, "customer": u, "messages": counts[u], "gets": gets, "ms": ms})
    out = {"state": {k: v for k, v in state.items() if k != "counts"}, "n": a.n, "warmup": a.warmup, "groups": {}}
    for g in sorted(set(x["group"] for x in rows)):
        gs = [x["gets"] for x in rows if x["group"] == g]
        ms = [x["ms"] for x in rows if x["group"] == g]
        out["groups"][g] = {"gets_mean": statistics.mean(gs), "gets_p50": pct(gs, 50), "gets_p95": pct(gs, 95), "gets_max": max(gs), "ms_p50": pct(ms, 50), "ms_p95": pct(ms, 95)}
        print(f"{g}: GETs mean {statistics.mean(gs):.2f}, p50 {pct(gs, 50)}, p95 {pct(gs, 95)}, max {max(gs)}; ms p50 {pct(ms, 50):.1f}", flush=True)
    out["rows"] = rows
    json.dump(out, open(a.out, "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["load", "measure"])
    ap.add_argument("--uri", default="mongodb://127.0.0.1:27041/?directConnection=true")
    ap.add_argument("--gb", type=float, default=2.0)
    ap.add_argument("--customers", type=int, default=20000)
    ap.add_argument("--zipf", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--customer-key", action="store_true")
    ap.add_argument("--state", required=True)
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=300)
    ap.add_argument("--out", default="result.json")
    a = ap.parse_args()
    load(a) if a.cmd == "load" else measure(a)


if __name__ == "__main__":
    main()
