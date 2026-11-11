"""Agent checkpoint benchmark: replays a generator.py plan against one target through the real LangGraph checkpointer.

Phases (in this order, all recorded in ops.jsonl with timestamps):
  1. write  - every planned turn, in virtual-time order, through graph.invoke() (graph_runner.py). Records each put,
              put_writes, the composite "save_step" (put_writes of a superstep + its put) and the "resume" read LangGraph
              does at the start of every turn (get_tuple without checkpoint_id), tagged with the gap before the turn
              (first / none / short / long).
  2. read   - on a deterministic sample of threads (the longest ones always included): resume (load latest),
              list the whole history, list the last 10, time travel to 3 random earlier checkpoints.
  3. sizes  - logical bytes written vs bytes the target reports it stores (targets.py), then the per-thread
              "agent checkpoint delta test" (delta_hook.py -> delta_test.json).
  4. erase  - "erase all of one end user's data": delete_thread for each thread of a sampled user, timed as a whole,
              then checks that nothing of those threads can be read back. Sizes again afterwards.

Output in --out: ops.jsonl (one line per operation), summary.json (percentiles, bytes, throughput), plan digest.
All latencies include the client (Python, pymongo/psycopg) and the network between client and database.

Example:
  python bench.py --target fenvix --uri mongodb://127.0.0.1:27100/?directConnection=true --profile small --out results/x
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import random
import sys
import threading
import time
from collections import defaultdict

import delta_hook
import generator
import graph_runner
import targets

VALIDATION_NOTE = ("LOCAL runs are validation, not results: one sandbox, client and database on the same machine, "
                   "MinIO instead of S3. Only the AWS round (runbooks/) produces comparable numbers.")


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3) if xs else None


def stats(xs: list[float]) -> dict:
    return {"n": len(xs), "p50": pct(xs, 0.5), "p90": pct(xs, 0.9), "p99": pct(xs, 0.99),
            "max": round(max(xs), 3) if xs else None, "mean": round(sum(xs) / len(xs), 3) if xs else None}


class Recorder:
    """Thread-safe JSONL sink plus in-memory copy for the summary."""

    def __init__(self, path: str):
        self.f = open(path, "w")
        self.lock = threading.Lock()
        self.rows: list[dict] = []

    def __call__(self, r: dict) -> None:
        with self.lock:
            self.rows.append(r)
            self.f.write(json.dumps(r) + "\n")

    def close(self):
        self.f.close()


def write_phase(target: targets.Target, seed: int, threads: dict, events: list[dict], rec: Recorder, workers: int) -> dict:
    """Replay the plan. With workers > 1 the users are split across workers (one saver/connection each); every thread
    stays on one worker, so each thread's turns keep their order."""
    shards = defaultdict(list)
    for e in events:
        shards[int(e["user"][4:]) % workers].append(e)
    errors = []

    def run(shard: list[dict]):
        saver, handle = target.make_saver()
        ts = graph_runner.TimedSaver(saver, rec)
        script = graph_runner.Script()
        graph = graph_runner.build_graph(ts, script)
        try:
            for e in shard:
                ts.context = {"phase": "write", "gap": e["gap"], "kind": e["kind"]}
                graph_runner.run_turn(graph, script, seed, threads[e["thread_id"]], e["turn"])
        except Exception as ex:  # noqa: BLE001 - reported in the summary, the run is then marked failed
            errors.append(repr(ex))
        finally:
            handle.close()

    t0 = time.time()
    ths = [threading.Thread(target=run, args=(shards[w],)) for w in range(workers)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    return {"seconds": round(time.time() - t0, 3), "errors": errors}


def read_phase(target: targets.Target, threads: dict, rec: Recorder, sample: int, seed: int) -> dict:
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec)
    ids = sorted(threads)
    longest = sorted(ids, key=lambda t: -threads[t].turns)[: max(1, sample // 5)]
    rest = [t for t in ids if t not in longest]
    r = random.Random(seed)
    chosen = longest + r.sample(rest, min(len(rest), sample - len(longest)))
    bad = 0
    for tid in chosen:
        cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
        ts.context = {"phase": "read", "kind": threads[tid].kind, "turns": threads[tid].turns}
        latest = ts.get_tuple(cfg)
        hist = list(ts.list(cfg))
        list(ts.list(cfg, limit=10))
        if latest is None or not hist or hist[0].config["configurable"]["checkpoint_id"] != latest.config["configurable"]["checkpoint_id"]:
            bad += 1
        for k in sorted(r.sample(range(len(hist)), min(3, len(hist)))):
            c = hist[k].config
            got = ts.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": "", "checkpoint_id": c["configurable"]["checkpoint_id"]}})
            if got is None or got.checkpoint["id"] != c["configurable"]["checkpoint_id"]:
                bad += 1
    handle.close()
    return {"threads_read": len(chosen), "inconsistencies": bad}


def erase_phase(target: targets.Target, threads: dict, rec: Recorder, n_users: int, seed: int) -> dict:
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec)
    by_user = defaultdict(list)
    for t in threads.values():
        by_user[t.user].append(t.thread_id)
    users = random.Random(seed + 1).sample(sorted(by_user), min(n_users, len(by_user)))
    leftovers = 0
    for u in users:
        ts.context = {"phase": "erase", "user": u, "threads": len(by_user[u])}
        t0 = time.perf_counter()
        for tid in by_user[u]:
            ts.delete_thread(tid)
        rec({"ts": time.time(), "op": "erase_user", "user": u, "threads": len(by_user[u]),
             "ms": round((time.perf_counter() - t0) * 1000, 3), "phase": "erase"})
        ts.context = {"phase": "erase_verify"}
        for tid in by_user[u]:
            cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
            if ts.get_tuple(cfg) is not None or list(ts.inner.list(cfg, limit=1)):
                leftovers += 1
    handle.close()
    return {"users_erased": len(users), "threads_erased": sum(len(by_user[u]) for u in users), "threads_left_readable": leftovers}


def summarize(rows: list[dict]) -> dict:
    by = defaultdict(list)
    for r in rows:
        key = r["op"]
        if r["op"] == "resume":
            key = f"resume[{r.get('phase')}{'/' + r['gap'] if r.get('gap') else ''}]"
        by[key].append(r["ms"])
        if r["op"] in ("save_step", "put"):
            by[f"{r['op']}[{r.get('kind')}]"].append(r["ms"])
    return {k: stats(v) for k, v in sorted(by.items())}


def logical_bytes(rows: list[dict]) -> dict:
    put = [r for r in rows if r["op"] == "put"]
    pw = [r for r in rows if r["op"] == "put_writes"]
    per_thread = defaultdict(lambda: {"logical_bytes": 0, "checkpoints": 0, "last_messages_bytes": 0})
    for r in put:
        p = per_thread[r["thread_id"]]
        p["logical_bytes"] += r["logical_bytes"]
        p["checkpoints"] += 1
        p["last_messages_bytes"] = r["message_bytes"]
    for r in pw:
        per_thread[r["thread_id"]]["logical_bytes"] += r["logical_bytes"]
    return {
        "checkpoints": len(put),
        "put_writes_calls": len(pw),
        "checkpoint_bytes": sum(r["logical_bytes"] for r in put),
        "writes_bytes": sum(r["logical_bytes"] for r in pw),
        "message_payload_bytes_all_checkpoints": sum(r["message_bytes"] for r in put),
        "unique_message_bytes": sum(p["last_messages_bytes"] for p in per_thread.values()),
        "per_thread": dict(per_thread),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", required=True, choices=["fenvix", "mongo", "postgres"])
    ap.add_argument("--uri", default=os.environ.get("BENCH_URI"), help="connection string (or env BENCH_URI)")
    ap.add_argument("--label", default="", help="free text describing the target, stored in the summary")
    ap.add_argument("--durability", default="", help="what an acknowledged write means on this target (e.g. 'single node, "
                    "no copy'); stored in the summary and printed on the last line")
    ap.add_argument("--profile", default="small", choices=sorted(generator.PROFILES))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--read-sample", type=int, default=30)
    ap.add_argument("--erase-users", type=int, default=0, help="0 = 10%% of users, at least 2")
    ap.add_argument("--no-reset", action="store_true", help="do not drop the app_demo database first")
    ap.add_argument("--local", action="store_true", help="mark every output as local validation")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if not a.uri:
        sys.exit("--uri or BENCH_URI is required")
    os.makedirs(a.out, exist_ok=True)

    target = targets.from_args(a.target, a.uri, a.label)
    thread_list, events = generator.plan(a.seed, a.profile)
    threads = {t.thread_id: t for t in thread_list}
    if not a.no_reset:
        target.reset()
    rec = Recorder(os.path.join(a.out, "ops.jsonl"))
    s3_before = targets.fenvix_s3_counters(a.uri) if a.target == "fenvix" else None

    w = write_phase(target, a.seed, threads, events, rec, a.workers)
    s3_after = targets.fenvix_s3_counters(a.uri) if a.target == "fenvix" else None
    rd = read_phase(target, threads, rec, a.read_sample, a.seed)
    targets.wait_settled(target)
    sizes = target.sizes()
    # per-thread logical vs stored ("agent checkpoint delta test"), before the erase removes threads
    with open(os.path.join(a.out, "per_thread_bytes.json"), "w") as f:
        json.dump(logical_bytes(rec.rows)["per_thread"], f)
    delta = delta_hook.run(a.target, a.uri, a.out)
    n_erase = a.erase_users or max(2, generator.PROFILES[a.profile].users // 10)
    er = erase_phase(target, threads, rec, n_erase, a.seed)
    targets.wait_settled(target)
    sizes_after_erase = target.sizes()
    rec.close()

    lb = logical_bytes(rec.rows)
    logical_total = lb["checkpoint_bytes"] + lb["writes_bytes"]
    stored = sizes.get("stored_bytes")
    lb.pop("per_thread")
    s3_puts = None
    if s3_before and s3_after and "put" in s3_after and "put" in s3_before:
        s3_puts = s3_after["put"] - s3_before["put"]
    summary = {
        "note": VALIDATION_NOTE if a.local else "AWS round result",
        "target": a.target, "label": a.label, "durability": a.durability or "not stated", "profile": a.profile, "seed": a.seed, "workers": a.workers,
        "plan": {"threads": len(threads), "turns": len(events), "digest": generator.plan_digest(events)},
        "client": {"python": platform.python_version(), "host": platform.node()},
        "write": w | {"steps_per_second": round(lb["checkpoints"] / w["seconds"], 2) if w["seconds"] else None},
        "read": rd, "erase": er,
        "latency_ms": summarize(rec.rows),
        "bytes": lb | {"logical_total": logical_total, "stored": stored, "stored_from": sizes.get("stored_from"),
                       "stored_per_logical": round(stored / logical_total, 4) if stored and logical_total else None,
                       "stored_per_checkpoint": round(stored / lb["checkpoints"], 1) if stored and lb["checkpoints"] else None},
        "sizes": sizes, "sizes_after_erase": sizes_after_erase,
        "s3_put_requests_during_write": s3_puts,
        "delta_test": {k: v for k, v in delta.items() if k != "rows"},
        "ok": not w["errors"] and rd["inconsistencies"] == 0 and er["threads_left_readable"] == 0,
    }
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    lat = summary["latency_ms"]
    print(f"[{a.target}{' | ' + a.durability if a.durability else ''}] {'VALIDATION, NOT RESULTS: ' if a.local else ''}ok={summary['ok']} "
          f"checkpoints={lb['checkpoints']} write={w['seconds']}s "
          f"save_step p50/p99={lat['save_step']['p50']}/{lat['save_step']['p99']} ms "
          f"stored/logical={summary['bytes']['stored_per_logical']} -> {a.out}/summary.json")
    if w["errors"]:
        print("errors:", w["errors"][:3], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
