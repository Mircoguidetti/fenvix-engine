#!/usr/bin/env python3
"""Fence-layout test, the baseline: the same 400 users as `fence_layout measure`, read through mongo-server.

    find({userId: u}).sort({ts: -1}).limit(20)     (index {userId: 1, ts: -1}, customer key on)

Warm-up: the model's 300 warm-up users (SST indexes and filters warm, as cold_customer.py). Then:
  cold pass: each of the 400 users once, server quiet before and after; GETs = serverStatus fenvix.waiter.reads delta,
             trips = fenvix.waiter.trips delta, chain = fenvix.waiter.lastChainDepth (reads waited for one after another)
  hot pass:  the same 400 users again (from the server's cache), timed
  ping:      200 `ping` commands, the client and protocol floor of a read
Every answer is checked against the model of bench/fence/dump.py: the same 20 _ids, newest first, the same BSON bytes.
"""
import argparse
import hashlib
import json
import statistics
import sys
import time

from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

sys.path.insert(0, __file__.rsplit("/", 2)[0] + "/compaction")
from cold_customer import quiet, pct  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--passes", default="cold,hot")
    a = ap.parse_args()
    m = json.load(open(a.model))
    c = MongoClient(a.uri)
    admin = c.admin
    coll = c.get_database("chat", codec_options=CodecOptions(document_class=RawBSONDocument))["messages"]

    def read(u):
        return list(coll.find({"userId": u}).sort("ts", -1).limit(20))

    def ok(u, docs):
        want = m["newest"][str(u)]
        return len(docs) == len(want) and all(str(d["_id"]) == w[0] and hashlib.sha256(d.raw).hexdigest() == w[1] for d, w in zip(docs, want))

    t = time.time()
    wrong = 0
    for u in m["warmup"]:
        wrong += not ok(u, read(u))
    print(f"warmup {len(m['warmup'])} finds in {time.time() - t:.0f} s", flush=True)
    users = [("active", u) for u in m["active"]] + [("uniform", u) for u in m["uniform"]]
    rows = []
    for p in a.passes.split(","):
        for g, u in users:
            f1 = quiet(admin)
            t1 = time.perf_counter()
            docs = read(u)
            ms = (time.perf_counter() - t1) * 1000
            f2 = quiet(admin)
            w1, w2 = f1["waiter"], f2["waiter"]
            good = ok(u, docs)
            wrong += not good
            rows.append({"pass": p, "group": g, "user": u, "messages": m["counts"][str(u)], "gets": w2["reads"] - w1["reads"], "trips": w2["trips"] - w1["trips"],
                         "chain": w2.get("lastChainDepth", 0) if w2["reads"] > w1["reads"] else 0, "ms": ms, "ok": good})
    pings = []
    for _ in range(200):
        t1 = time.perf_counter()
        admin.command("ping")
        pings.append((time.perf_counter() - t1) * 1000)
    out = {"model": a.model, "wrong": wrong, "ping_ms_p50": pct(pings, 50), "ping_ms_p99": pct(pings, 99), "summary": {}, "rows": rows}
    for p in a.passes.split(","):
        for g in ("active", "uniform", "all"):
            rs = [r for r in rows if r["pass"] == p and (g == "all" or r["group"] == g)]
            gets = [r["gets"] for r in rs]
            ms = [r["ms"] for r in rs]
            ch = [r["chain"] for r in rs]
            s = {"n": len(rs), "gets_mean": statistics.mean(gets), "gets_p95": pct(gets, 95), "gets_max": max(gets), "trips_mean": statistics.mean(r["trips"] for r in rs),
                 "chain_mean": statistics.mean(ch), "chain_max": max(ch), "one_get_share": sum(x <= 1 for x in gets) / len(gets), "one_chain_share": sum(x <= 1 for x in ch) / len(ch),
                 "ms_p50": pct(ms, 50), "ms_p95": pct(ms, 95), "ms_p99": pct(ms, 99), "ms_max": max(ms)}
            out["summary"][f"{p}_{g}"] = s
            print(f"{p} {g}: {json.dumps(s)}", flush=True)
    print(f"ping p50 {out['ping_ms_p50']:.3f} ms, p99 {out['ping_ms_p99']:.3f} ms; answers checked: {len(rows) + len(m['warmup'])}, wrong {wrong}", flush=True)
    json.dump(out, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
