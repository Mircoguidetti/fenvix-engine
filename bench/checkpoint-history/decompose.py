#!/usr/bin/env python3
"""Where the trips of a cold get_tuple go: the checkpointer's two queries, run one by one exactly as `get_tuple` sends
them (same filter, sort, limit), each with the server's trip meter before and after.

  1. checkpoints.find({thread_id, checkpoint_ns}).sort({checkpoint_id: -1}).limit(1)   (index, then the document)
  2. checkpoint_writes.find({thread_id, checkpoint_ns, checkpoint_id})                  (index probe, then documents)

Usage: decompose.py --uri URI --plan plan.json --out out.json [--n 100] [--seed 23]
Threads are drawn from those ckpt.py measure did not touch (seed differs); long threads are included once each.
"""
import argparse
import json
import random
import statistics
import time

from pymongo import MongoClient

import ckpt


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", required=True)
    p.add_argument("--plan", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--n", type=int, default=100)
    p.add_argument("--seed", type=int, default=23)
    p.add_argument("--skip", type=int, default=900, help="threads at the front of ckpt.py's shuffle (already read)")
    a = p.parse_args()
    plan = json.load(open(a.plan))
    long_set = set(plan["long"])
    short = [t for t in range(plan["threads"]) if t not in long_set]
    random.Random(11).shuffle(short)  # ckpt.py measure's default seed: skip what it read
    pool = short[a.skip:]
    random.Random(a.seed).shuffle(pool)
    c = MongoClient(a.uri)
    db = c[ckpt.DB]
    meter = ckpt.Meter(c, True)
    rows = []
    for kind, t in [("short", t) for t in pool[:a.n]] + [("long", t) for t in plan["long"]]:
        tn = ckpt.thread_name(t)
        time.sleep(0.02)
        docs, m1 = ckpt.timed(lambda: list(db.checkpoints.find({"thread_id": tn, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)), meter)
        time.sleep(0.02)
        w, m2 = ckpt.timed(lambda: list(db.checkpoint_writes.find({"thread_id": tn, "checkpoint_ns": "", "checkpoint_id": docs[0]["checkpoint_id"]})), meter)
        rows.append({"kind": kind, "thread": t, "ckpt": m1, "writes": m2, "nwrites": len(w)})
    out = {"rows": rows, "summary": {}}
    for kind in ("short", "long"):
        rs = [r for r in rows if r["kind"] == kind]
        if not rs:
            continue
        out["summary"][kind] = {q: {f: round(statistics.mean(r[q][f] for r in rs), 3) for f in ("trips", "gets", "ms")} | {"max_trips": max(r[q]["trips"] for r in rs)}
                                for q in ("ckpt", "writes")}
        out["summary"][kind]["n"] = len(rs)
    json.dump(out, open(a.out, "w"), indent=1)
    print(json.dumps(out["summary"], indent=1))


if __name__ == "__main__":
    main()
