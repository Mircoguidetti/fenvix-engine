#!/usr/bin/env python3
"""Keep every agent checkpoint: does reading the latest state stay one trip however long the history?

The published `langgraph-checkpoint-mongodb` (0.5.1, unchanged) writes a deterministic checkpoint history into a target
(our server or the reference mongod) through its own `MongoDBSaver.put` and `put_writes`; then `get_tuple` and `list`
are measured, one call at a time, and every answer is reduced to a digest so the two targets can be compared.

Shape (from sessions/2026-11-07-fable-pains-first, P1): many threads with about 17.7 checkpoints each (1 + Poisson(16.7)),
checkpoint bodies drawn uniformly between 20 kB and 300 kB of text-like data, then LONG of those threads grown to 10,000
checkpoints each. Every checkpoint but the newest of a thread has pending writes (two small writes of one task), as a
LangGraph run leaves them: the writes of step N are stored before checkpoint N+1.

The data is a pure function of (seed, thread, step): both targets receive the same documents, byte for byte.

Usage:
  ckpt.py load    --uri URI --threads N --long L --long-steps 10000 --plan plan.json [--workers 8]
  ckpt.py measure --uri URI --plan plan.json --out result.json [--meter] [--short-n 500] [--long-rounds 3] ...
  ckpt.py compare A.json B.json
"""
import argparse
import hashlib
import json
import math
import multiprocessing as mp
import os
import random
import statistics
import sys
import time

DB = "checkpointing_db"
SEED = 20261107
MIN_BODY, MAX_BODY = 20_000, 300_000
CORPUS = None


def corpus():
    """64 MiB of word text (zipf vocabulary), sliced at random offsets for the bodies: compressible like chat text,
    never repeating within an SST block."""
    global CORPUS
    if CORPUS is None:
        r = random.Random(7)
        letters = "abcdefghijklmnopqrstuvwxyz"
        words = ["".join(r.choice(letters) for _ in range(r.randint(2, 9))) for _ in range(4000)]
        weights = [1.0 / (i + 1) for i in range(4000)]
        parts, n = [], 0
        while n < 64 << 20:
            s = " ".join(r.choices(words, weights=weights, k=20000))
            parts.append(s)
            n += len(s) + 1
        CORPUS = " ".join(parts)
    return CORPUS


def thread_name(t):
    return f"thread-{t:06d}"


def ckpt_id(t, i):
    # uuid6-like, sortable: the checkpointer sorts by checkpoint_id, newest last within a thread
    return f"1f0{t:05x}-{(i >> 16) & 0xffff:04x}-6{(i >> 4) & 0xfff:03x}-8{i & 0xf:03x}-{(t * 1_000_003 + i) & 0xffffffffffff:012x}"


def make_checkpoint(t, i):
    r = random.Random(f"{SEED}/{t}/{i}")
    size = r.randint(MIN_BODY, MAX_BODY)
    c = corpus()
    off = r.randrange(0, len(c) - size)
    text = c[off:off + size]
    cid = ckpt_id(t, i)
    checkpoint = {
        "v": 4, "id": cid, "ts": f"2026-11-07T{(i // 3600) % 24:02d}:{(i // 60) % 60:02d}:{i % 60:02d}.000000+00:00",
        "channel_values": {"messages": [{"role": "assistant" if i % 2 else "user", "content": text}], "step": i},
        "channel_versions": {"messages": f"{i:032d}.0", "step": f"{i:032d}.0"},
        "versions_seen": {"agent": {"messages": f"{max(i - 1, 0):032d}.0"}},
        "updated_channels": ["messages", "step"],
    }
    metadata = {"source": "loop", "step": i, "parents": {}}
    return checkpoint, metadata, size


def make_writes(t, i):
    r = random.Random(f"{SEED}/w/{t}/{i}")
    return f"task-{t}-{i}", [("messages", {"role": "assistant", "content": "w" * r.randint(200, 1500)}), ("step", i + 1)]


def plan_threads(threads, long_n, long_steps):
    r = random.Random(SEED)
    counts = []
    for _ in range(threads):
        # 1 + Poisson(16.7): mean 17.7 checkpoints per thread
        L, k, p = math.exp(-16.7), 0, 1.0
        while True:
            p *= r.random()
            if p <= L:
                break
            k += 1
        counts.append(1 + k)
    long_ids = sorted(r.sample(range(threads), long_n))
    return counts, long_ids, long_steps


def saver_for(uri):
    from langgraph.checkpoint.mongodb import MongoDBSaver
    from pymongo import MongoClient
    client = MongoClient(uri)
    return client, MongoDBSaver(client, db_name=DB)


def write_steps(saver, t, start, stop):
    """Write checkpoints start..stop-1 of thread t, as a LangGraph loop would: writes of step i-1, then checkpoint i."""
    import bson
    written = 0
    tn = thread_name(t)
    for i in range(start, stop):
        if i > 0:
            task, writes = make_writes(t, i - 1)
            saver.put_writes({"configurable": {"thread_id": tn, "checkpoint_ns": "", "checkpoint_id": ckpt_id(t, i - 1)}}, writes, task)
            for idx, (ch, v) in enumerate(writes):
                ty, ser = saver.serde.dumps_typed(v)
                written += len(bson.encode({"thread_id": tn, "checkpoint_ns": "", "checkpoint_id": ckpt_id(t, i - 1), "task_id": task,
                                            "task_path": "", "idx": idx, "channel": ch, "type": ty, "value": ser}))
        checkpoint, metadata, _ = make_checkpoint(t, i)
        parent = {"configurable": {"thread_id": tn, "checkpoint_ns": ""}}
        if i > 0:
            parent["configurable"]["checkpoint_id"] = ckpt_id(t, i - 1)
        saver.put(parent, checkpoint, metadata, {})
        ty, ser = saver.serde.dumps_typed(checkpoint)
        written += len(bson.encode({"thread_id": tn, "checkpoint_ns": "", "checkpoint_id": checkpoint["id"],
                                    "parent_checkpoint_id": ckpt_id(t, i - 1) if i else None, "type": ty, "checkpoint": ser,
                                    "metadata": {"source": "loop", "step": i, "parents": {}}}))
    return written


def load_worker(args):
    uri, jobs, rate = args  # rate: bytes per second for this worker (0 = no limit)
    t0 = time.time()
    corpus()
    client, saver = saver_for(uri)
    total = 0
    # round-robin over the worker's threads, a few steps at a time, so threads interleave in time
    pos = {t: a for t, a, _ in jobs}
    end = {t: b for t, _, b in jobs}
    live = [t for t, _, _ in jobs]
    while live:
        nxt = []
        for t in live:
            b = min(pos[t] + 4, end[t])
            total += write_steps(saver, t, pos[t], b)
            pos[t] = b
            if rate:
                ahead = total / rate - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
            if b < end[t]:
                nxt.append(t)
        live = nxt
    client.close()
    return total


def cmd_load(a):
    counts, long_ids, long_steps = plan_threads(a.threads, a.long, a.long_steps)
    corpus()
    t0 = time.time()
    written = 0
    with mp.Pool(a.workers) as pool:
        # phase 1: every thread at its short length
        jobs = [(t, 0, counts[t]) for t in range(a.threads)]
        chunks = [(a.uri, jobs[w::a.workers], a.rate_mb * 1e6 / a.workers) for w in range(a.workers)]
        written += sum(pool.map(load_worker, chunks))
        t1 = time.time()
        print(f"phase 1: {a.threads} threads, {sum(counts)} checkpoints, {written / 1e9:.2f} GB written, {t1 - t0:.0f} s", flush=True)
        # phase 2: the long threads grow to long_steps checkpoints
        jobs = [(t, counts[t], long_steps) for t in long_ids]
        nw = min(a.workers, len(jobs))
        chunks = [(a.uri, jobs[w::nw], a.rate_mb * 1e6 / nw) for w in range(nw)]
        w2 = sum(pool.map(load_worker, chunks))
        written += w2
        print(f"phase 2: {len(long_ids)} threads grown to {long_steps}, {w2 / 1e9:.2f} GB written, {time.time() - t1:.0f} s", flush=True)
    plan = {"threads": a.threads, "counts": counts, "long": long_ids, "long_steps": long_steps, "bytes_written_bson": written,
            "checkpoints": sum(counts) + sum(long_steps - counts[t] for t in long_ids), "load_secs": time.time() - t0}
    json.dump(plan, open(a.plan, "w"))
    print(json.dumps({k: v for k, v in plan.items() if k not in ("counts",)}))


# ---------------------------------------------------------------- measure

def digest_tuple(tup):
    if tup is None:
        return None
    h = hashlib.sha256()
    h.update(json.dumps(tup.config, sort_keys=True).encode())
    h.update(json.dumps(tup.checkpoint, sort_keys=True, default=str).encode())
    h.update(json.dumps(tup.metadata, sort_keys=True, default=str).encode())
    h.update(json.dumps(tup.parent_config, sort_keys=True).encode())
    h.update(json.dumps(sorted([list(map(str, w)) for w in tup.pending_writes]), sort_keys=True).encode())
    return h.hexdigest()[:24]


def expected_latest(plan, t):
    n = plan["long_steps"] if t in set(plan["long"]) else plan["counts"][t]
    return ckpt_id(t, n - 1), n


class Meter:
    def __init__(self, client, on):
        self.c, self.on = client, on

    def snap(self):
        if not self.on:
            return (0, 0)
        w = self.c.admin.command("serverStatus")["fenvix"]["waiter"]
        return (w["trips"], w["reads"])


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    return xs[min(len(xs) - 1, int(math.ceil(p / 100 * len(xs))) - 1)]


def summary(rows, key):
    xs = [r[key] for r in rows]
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "p50": pct(xs, 50), "p95": pct(xs, 95), "p99": pct(xs, 99), "max": max(xs)}


def timed(fn, meter):
    a = meter.snap()
    t = time.perf_counter()
    out = fn()
    ms = (time.perf_counter() - t) * 1e3
    b = meter.snap()
    return out, {"ms": round(ms, 3), "trips": b[0] - a[0], "gets": b[1] - a[1]}


def cmd_measure(a):
    plan = json.load(open(a.plan))
    client, saver = saver_for(a.uri)
    meter = Meter(client, a.meter)
    long_set = set(plan["long"])
    r = random.Random(a.seed)
    short = [t for t in range(plan["threads"]) if t not in long_set]
    r.shuffle(short)
    warm, sample = short[:a.warmup], short[a.warmup:a.warmup + a.short_n]
    out = {"uri": a.uri, "short": [], "long": [], "hot_short": [], "hot_long": [], "list_full": [], "list20": [], "answers": {}}

    def cfg(t):
        return {"configurable": {"thread_id": thread_name(t), "checkpoint_ns": ""}}

    def check(t, tup):
        cid, _ = expected_latest(plan, t)
        assert tup is not None and tup.config["configurable"]["checkpoint_id"] == cid, (t, tup and tup.config)
        return digest_tuple(tup)

    # warm-up: SST indexes and filters into the metadata cache, through reads of threads that are not measured
    t0 = time.time()
    for t in warm:
        saver.get_tuple(cfg(t))
    out["warmup"] = {"threads": len(warm), "secs": round(time.time() - t0, 1)}
    time.sleep(0.05)

    # cold get_tuple: short threads, with the long threads interleaved (each long thread once per round)
    longs = [t for _ in range(a.long_rounds) for t in plan["long"]]
    every = max(1, len(sample) // (len(longs) + 1))
    order, li = [], 0
    for k, t in enumerate(sample):
        order.append(("short", t))
        if (k + 1) % every == 0 and li < len(longs):
            order.append(("long", longs[li]))
            li += 1
    order += [("long", t) for t in longs[li:]]
    for kind, t in order:
        time.sleep(0.02)  # the meter's chains end after 10 ms of quiet
        tup, m = timed(lambda: saver.get_tuple(cfg(t)), meter)
        d = check(t, tup)
        m["thread"] = t
        m["pending"] = len(tup.pending_writes)
        out[kind].append(m)
        out["answers"].setdefault(f"get/{t}", d)
        # hot: the same call again at once
        time.sleep(0.02)
        tup2, m2 = timed(lambda: saver.get_tuple(cfg(t)), meter)
        assert digest_tuple(tup2) == d
        m2["thread"] = t
        out["hot_" + kind].append(m2)

    # list: limit 20, then the full history, of a few long threads
    for t in plan["long"][:a.list_threads]:
        time.sleep(0.02)
        res, m = timed(lambda: list(saver.list(cfg(t), limit=20)), meter)
        m["thread"] = t
        m["n"] = len(res)
        out["list20"].append(m)
        out["answers"][f"list20/{t}"] = hashlib.sha256("".join(digest_tuple(x) for x in res).encode()).hexdigest()[:24]
        assert [x.config["configurable"]["checkpoint_id"] for x in res] == [ckpt_id(t, plan["long_steps"] - 1 - k) for k in range(20)]
    for t in plan["long"][:a.list_full_threads]:
        time.sleep(0.02)
        h = hashlib.sha256()
        n = [0]

        def full():
            for x in saver.list(cfg(t)):
                h.update(digest_tuple(x).encode())
                n[0] += 1
        _, m = timed(full, meter)
        m["thread"] = t
        m["n"] = n[0]
        assert n[0] == plan["long_steps"], n[0]
        out["list_full"].append(m)
        out["answers"][f"list/{t}"] = h.hexdigest()[:24]

    out["summary"] = {}
    for k in ("short", "long", "hot_short", "hot_long", "list20", "list_full"):
        if out[k]:
            out["summary"][k] = {f: summary(out[k], f) for f in ("ms", "trips", "gets")}
    if out["short"]:
        for k in ("short", "long"):
            out["summary"][k]["share_le2_trips"] = round(sum(1 for x in out[k] if x["trips"] <= 2) / len(out[k]), 4)
    json.dump(out, open(a.out, "w"), indent=1)
    print(json.dumps(out["summary"], indent=1))


def cmd_compare(a):
    x, y = json.load(open(a.a))["answers"], json.load(open(a.b))["answers"]
    common = sorted(set(x) & set(y))
    diff = [k for k in common if x[k] != y[k]]
    print(f"{len(common)} answers in both, {len(diff)} different; only in A: {len(set(x) - set(y))}, only in B: {len(set(y) - set(x))}")
    for k in diff[:20]:
        print("  differs:", k)
    return 1 if diff else 0


def main():
    p = argparse.ArgumentParser()
    s = p.add_subparsers(dest="cmd", required=True)
    l = s.add_parser("load")
    l.add_argument("--uri", required=True)
    l.add_argument("--threads", type=int, default=16000)
    l.add_argument("--long", type=int, default=100)
    l.add_argument("--long-steps", type=int, default=10000)
    l.add_argument("--workers", type=int, default=8)
    l.add_argument("--plan", required=True)
    l.add_argument("--rate-mb", type=float, default=0, help="total write rate cap in MB/s (0 = none): lets the SST cleanup keep up on a small disk")
    m = s.add_parser("measure")
    m.add_argument("--uri", required=True)
    m.add_argument("--plan", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--meter", action="store_true", help="read trips and GETs from our server's serverStatus")
    m.add_argument("--seed", type=int, default=11)
    m.add_argument("--warmup", type=int, default=300)
    m.add_argument("--short-n", type=int, default=500)
    m.add_argument("--long-rounds", type=int, default=3)
    m.add_argument("--list-threads", type=int, default=5)
    m.add_argument("--list-full-threads", type=int, default=2)
    c = s.add_parser("compare")
    c.add_argument("a")
    c.add_argument("b")
    a = p.parse_args()
    if a.cmd == "load":
        cmd_load(a)
    elif a.cmd == "measure":
        cmd_measure(a)
    else:
        sys.exit(cmd_compare(a))


if __name__ == "__main__":
    main()
