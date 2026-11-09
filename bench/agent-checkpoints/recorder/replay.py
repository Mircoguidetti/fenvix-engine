"""Part C · replay a recording (record.py) against any LangGraph checkpointer: the same put / put_writes calls, the same
bytes (deserialized with LangGraph's serializer and handed to the target's saver, which serializes them again as it
always does), in the recorded order, with the recorded time between calls (scaled by --time-scale; 0 = as fast as
possible). No model call. --copies N replays every conversation N times under new thread ids (volume without paying the
model again). Then for every conversation: resume (latest checkpoint), time travel (a random earlier checkpoint), list.
Prints latency p50 / p99 per operation and writes OUT/replay-ops.jsonl and OUT/replay-summary.json.
Usage: replay.py RECORDING_DIR OUT --target fenvix|mongo|postgres --uri URI [--copies 1] [--time-scale 0] [--seed 3]"""
import argparse
import base64
import glob
import gzip
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import targets  # noqa: E402  (bench/agent-checkpoints/targets.py: the official savers)

ap = argparse.ArgumentParser()
ap.add_argument("recording")
ap.add_argument("out")
ap.add_argument("--target", required=True, choices=["fenvix", "mongo", "postgres"])
ap.add_argument("--uri", required=True)
ap.add_argument("--copies", type=int, default=1)
ap.add_argument("--time-scale", type=float, default=0.0)
ap.add_argument("--seed", type=int, default=3)
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
saver, conn = {"fenvix": targets.fenvix_saver, "mongo": targets.mongodb_saver, "postgres": targets.postgres_saver}[a.target](a.uri)
serde = saver.serde
ops = open(os.path.join(a.out, "replay-ops.jsonl"), "w")
lat = {}
rng = random.Random(a.seed)


def typed(v):
    return serde.loads_typed((v[0], base64.b64decode(v[1])))


def timed(op, fn):
    t0 = time.perf_counter()
    r = fn()
    ms = (time.perf_counter() - t0) * 1000
    lat.setdefault(op, []).append(ms)
    ops.write(json.dumps({"op": op, "ms": round(ms, 3), "t": time.time()}) + "\n")
    return r


def cfg(c, thread):
    d = dict(c["configurable"])
    d["thread_id"] = thread
    return {"configurable": d}


threads = []
files = sorted(glob.glob(f"{a.recording}/*.jsonl.gz"))
for copy in range(a.copies):
    for f in files:
        thread = f"{os.path.basename(f)[:-9]}-r{copy}"
        threads.append(thread)
        last_t = 0.0
        for line in gzip.open(f, "rt"):
            ev = json.loads(line)
            if "t" in ev and a.time_scale > 0:
                time.sleep(max(0.0, (ev["t"] - last_t) * a.time_scale))
            last_t = ev.get("t", last_t)
            if ev["ev"] == "put":
                timed("put", lambda: saver.put(cfg(ev["config"], thread), typed(ev["checkpoint"]), typed(ev["metadata"]), ev["new_versions"]))
            elif ev["ev"] == "put_writes":
                writes = [(ch, typed(v)) for ch, v in ev["writes"]]
                timed("put_writes", lambda: saver.put_writes(cfg(ev["config"], thread), writes, ev["task_id"], ev.get("task_path", "")))
for thread in threads:
    c = {"configurable": {"thread_id": thread, "checkpoint_ns": ""}}
    timed("resume", lambda: saver.get_tuple(c))
    hist = timed("list", lambda: list(saver.list(c)))
    if hist:
        k = rng.choice(hist)
        timed("time_travel", lambda: saver.get_tuple(k.config))


def pct(v, p):
    v = sorted(v)
    return round(v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))], 3)


summary = {"target": a.target, "conversations": len(files), "copies": a.copies, "threads": len(threads),
           "ops": {op: {"n": len(v), "p50_ms": pct(v, 50), "p99_ms": pct(v, 99)} for op, v in lat.items()}}
json.dump(summary, open(os.path.join(a.out, "replay-summary.json"), "w"), indent=1)
print(json.dumps(summary))
