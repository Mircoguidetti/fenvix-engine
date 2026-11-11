"""Part C · replay a recording (record.py) against any LangGraph checkpointer: the same put / put_writes calls, the same
bytes (deserialized with LangGraph's serializer and handed to the target's saver, which serializes them again as it
always does), in the recorded order, with the recorded time between calls (scaled by --time-scale; 0 = as fast as
possible). No model call. --copies N replays every conversation N times under new thread ids (volume without paying the
model again). Then for every conversation: resume (latest checkpoint), time travel (a random earlier checkpoint), list.
Prints latency p50 / p99 per operation and writes OUT/replay-ops.jsonl and OUT/replay-summary.json.
aws-fixes (B11 of the total review, 2026-11-11):
  - channel versions are RENUMBERED with the target saver's own `get_next_version`: the recorder's saver handed
    LangGraph the base class's integer versions (1, 2, 3), while e.g. the Postgres saver writes 51-byte versions
    ("00000000000000000000000000000001.0.6542104956684704") into every row key and every checkpoint; the reviewer
    measured the replay storing 12.4% less than the same conversations run directly on Postgres. Per thread and channel
    the n-th recorded version becomes the target's n-th version, in the checkpoint's `channel_versions`, its
    `versions_seen` and the put's `new_versions` (the MongoDB saver keeps integers: nothing changes for it);
    --keep-recorded-versions turns it off (to compare);
  - only COMPLETE recordings are replayed: a file whose last event is not {"ev": "end", "status": "done"} is refused
    and counted (recordings made before this change have no end event: --accept-unmarked takes them).
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
ap.add_argument("--keep-recorded-versions", action="store_true")
ap.add_argument("--accept-unmarked", action="store_true", help="replay recordings that have no end event (made before aws-fixes)")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
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


vmap, cur, renum = {}, {}, {"renumbered": 0, "unmapped": 0}


def vkey(v):
    return json.dumps(v)


def tv(thread, ch, v):
    """The target's version for the recorded version v of channel ch in this thread (made in recorded order)."""
    if a.keep_recorded_versions:
        return v
    k = (thread, ch, vkey(v))
    if k not in vmap:
        vmap[k] = saver.get_next_version(cur.get((thread, ch)), None)
        cur[(thread, ch)] = vmap[k]
        renum["renumbered"] += 1
    return vmap[k]


def renumber(thread, ckpt, new_versions):
    if a.keep_recorded_versions:
        return ckpt, new_versions
    # new versions first, in the order of their recorded numbers (they are made in that order)
    for ch, v in sorted(new_versions.items(), key=lambda kv: (str(type(kv[1])), kv[1])):
        tv(thread, ch, v)
    nv = {ch: tv(thread, ch, v) for ch, v in new_versions.items()}
    ck = dict(ckpt)
    for ch, v in ckpt.get("channel_versions", {}).items():
        if (thread, ch, vkey(v)) not in vmap:
            renum["unmapped"] += 1
    ck["channel_versions"] = {ch: tv(thread, ch, v) for ch, v in ckpt.get("channel_versions", {}).items()}
    ck["versions_seen"] = {node: {ch: tv(thread, ch, v) for ch, v in seen.items()}
                           for node, seen in ckpt.get("versions_seen", {}).items()}
    return ck, nv


def complete(f):
    last = None
    for line in gzip.open(f, "rt"):
        last = line
    try:
        ev = json.loads(last) if last else {}
    except ValueError:
        return False
    if ev.get("ev") == "end":
        return ev.get("status") == "done"
    return a.accept_unmarked


threads = []
files_all = sorted(glob.glob(f"{a.recording}/*.jsonl.gz"))
files = [f for f in files_all if complete(f)]
refused = sorted(set(files_all) - set(files))
if refused:
    print(f"refused (not a complete recording): {[os.path.basename(f) for f in refused]}", flush=True)
saver, conn = {"fenvix": targets.fenvix_saver, "mongo": targets.mongodb_saver, "postgres": targets.postgres_saver}[a.target](a.uri)
serde = saver.serde
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
                ck, nv = renumber(thread, typed(ev["checkpoint"]), ev["new_versions"])
                timed("put", lambda: saver.put(cfg(ev["config"], thread), ck, typed(ev["metadata"]), nv))
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


summary = {"target": a.target, "conversations": len(files), "refused_incomplete": [os.path.basename(f) for f in refused],
           "versions": "recorded (unchanged)" if a.keep_recorded_versions else f"renumbered by {type(saver).__name__}.get_next_version",
           "versions_renumbered": renum["renumbered"], "versions_unmapped": renum["unmapped"],
           "copies": a.copies, "threads": len(threads),
           "ops": {op: {"n": len(v), "p50_ms": pct(v, 50), "p99_ms": pct(v, 99)} for op, v in lat.items()}}
json.dump(summary, open(os.path.join(a.out, "replay-summary.json"), "w"), indent=1)
print(json.dumps(summary))
