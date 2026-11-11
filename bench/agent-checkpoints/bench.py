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
  5. cold   - only with --cold-cmd: the command restarts every container of the line and drops the host's page cache
              (bench/agent-checkpoints/lines.py restart); then a resume of --cold-threads threads (fixed seed, the same
              for every target: threads neither read in phase 2 nor erased in phase 4, so none was read since its last
              write), each read once, the first read after the restart. Op "resume[cold]".
Every read of every phase is compared with a hash of what was written (graph_runner.Expected): "wrong_reads" in the
summary must be 0, and it is part of "ok". Before the sizes, --settle waits until our server's compactor is idle
(targets.wait_compaction_idle); the wait is recorded in summary["settle"].

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
import tempfile
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


class ClientCpu(threading.Thread):
    """bench-local review F7: the client's own CPU. Every `every` seconds: the host's CPU busy share and each client
    process's CPU (this process and the worker processes, from /proc/<pid>/stat), written to `path`. A process at 0.9
    core or more on average, or a host at 95% busy or more in more than 10% of the samples, makes the line
    `client_limited` (RULES: the sender must not be the limit). Written before any w8 result was read."""

    def __init__(self, path: str, pids, every: float = 2.0):
        super().__init__(daemon=True)
        self.path, self.pids, self.every, self.stop = path, pids, every, threading.Event()
        self.rows: list[dict] = []
        self.tick = os.sysconf("SC_CLK_TCK")

    @staticmethod
    def _host():
        v = list(map(int, open("/proc/stat").readline().split()[1:]))
        return sum(v), v[3] + v[4]

    def _proc(self, pid):
        try:
            f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
            return (int(f[11]) + int(f[12])) / self.tick
        except (OSError, IndexError, ValueError):
            return None

    def run(self):
        tot, idle = self._host()
        prev = {p: self._proc(p) for p in self.pids()}
        t_prev = time.time()
        with open(self.path, "w") as f:
            while not self.stop.wait(self.every):
                now = time.time()
                t2, i2 = self._host()
                busy = 1 - (i2 - idle) / max(1, t2 - tot)
                tot, idle = t2, i2
                row = {"t": round(now, 1), "host_busy": round(busy, 3), "procs": {}}
                for p in self.pids():
                    c = self._proc(p)
                    if c is not None and prev.get(p) is not None:
                        row["procs"][str(p)] = round((c - prev[p]) / (now - t_prev), 3)
                    prev[p] = c
                t_prev = now
                self.rows.append(row)
                f.write(json.dumps(row) + "\n")

    def summary(self) -> dict:
        if not self.rows:
            return {"samples": 0}
        per = defaultdict(list)
        for r in self.rows:
            for p, c in r["procs"].items():
                per[p].append(c)
        means = {p: round(sum(v) / len(v), 3) for p, v in per.items() if v}
        hb = [r["host_busy"] for r in self.rows]
        hot = round(sum(x >= 0.95 for x in hb) / len(hb), 3)
        limited = any(m >= 0.9 for m in means.values()) or hot > 0.10
        return {"samples": len(self.rows), "process_cores_mean": means, "process_cores_max_mean": max(means.values()) if means else None,
                "host_busy_mean": round(sum(hb) / len(hb), 3), "host_busy_ge_95pct_share": hot, "client_limited": limited,
                "rule": "limited if a client process averages >= 0.9 core or the host is >= 95% busy in > 10% of samples"}


def _worker(target, seed, threads, shard, out_path, w):
    """One worker process of the write phase (--workers N > 1): its own saver, Recorder and Expected; at the end its
    rows and write-side state go to out_path for the parent to merge."""
    import pickle

    rec = Recorder(out_path + ".ops.jsonl")
    exp = graph_runner.Expected()
    errors = []
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec, expected=exp)
    script = graph_runner.Script()
    graph = graph_runner.build_graph(ts, script)
    try:
        for e in shard:
            ts.context = {"phase": "write", "gap": e["gap"], "kind": e["kind"], "worker": w}
            graph_runner.run_turn(graph, script, seed, threads[e["thread_id"]], e["turn"])
    except Exception as ex:  # noqa: BLE001 - reported in the summary, the run is then marked failed
        errors.append(repr(ex))
    finally:
        handle.close()
        rec.close()
    with open(out_path, "wb") as f:
        pickle.dump({"errors": errors, "expected": exp.export_state()}, f)


def write_phase(target: targets.Target, seed: int, threads: dict, events: list[dict], rec: Recorder, workers: int,
                expected: graph_runner.Expected, out_dir: str | None = None) -> dict:
    """Replay the plan. With workers > 1 the users are split across workers, each a separate PROCESS with its own
    saver and connection (bench-local review F7: threads of one Python process serialize on the GIL, so the timed calls
    waited behind other threads' hashing and serialization); every thread stays on one worker, so each thread's turns
    keep their order. The client's CPU is sampled (ClientCpu) in both cases."""
    shards = defaultdict(list)
    for e in events:
        shards[int(e["user"][4:]) % workers].append(e)
    errors = []
    procs = []
    cpu = ClientCpu(os.path.join(out_dir, "client_cpu.jsonl") if out_dir else os.devnull,
                    lambda: [os.getpid()] + [p.pid for p in procs if p.pid])
    out_dir = out_dir or tempfile.mkdtemp()
    if workers > 1:
        import multiprocessing as mp
        import pickle

        ctx = mp.get_context("fork")
        t0 = time.time()
        paths = [os.path.join(out_dir, f".worker-{w}.state") for w in range(workers)]
        procs.extend(ctx.Process(target=_worker, args=(target, seed, threads, shards[w], paths[w], w)) for w in range(workers))
        cpu.start()
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        seconds = round(time.time() - t0, 3)
        cpu.stop.set()
        cpu.join()
        for w, (p, path) in enumerate(zip(procs, paths)):
            if p.exitcode != 0 or not os.path.exists(path):
                errors.append(f"worker {w} exited {p.exitcode}")
                continue
            st = pickle.load(open(path, "rb"))
            errors += st["errors"]
            expected.merge_state(st["expected"])
            with open(path + ".ops.jsonl") as f:
                for line in f:
                    rec(json.loads(line))
            os.remove(path)
            os.remove(path + ".ops.jsonl")
        return {"seconds": seconds, "errors": errors, "processes": workers, "client_cpu": cpu.summary()}

    def run(shard: list[dict]):
        saver, handle = target.make_saver()
        ts = graph_runner.TimedSaver(saver, rec, expected=expected)
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
    cpu.start()
    run(shards[0])
    cpu.stop.set()
    cpu.join()
    return {"seconds": round(time.time() - t0, 3), "errors": errors, "processes": 1, "client_cpu": cpu.summary()}


def read_phase(target: targets.Target, threads: dict, rec: Recorder, sample: int, seed: int,
               expected: graph_runner.Expected) -> dict:
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec, expected=expected)
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
    return {"threads_read": len(chosen), "inconsistencies": bad, "chosen": chosen}


def erase_phase(target: targets.Target, threads: dict, rec: Recorder, n_users: int, seed: int,
                expected: graph_runner.Expected) -> dict:
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec, expected=expected)
    by_user = defaultdict(list)
    for t in threads.values():
        by_user[t.user].append(t.thread_id)
    users = random.Random(seed + 1).sample(sorted(by_user), min(n_users, len(by_user)))
    leftovers, ids_left = 0, 0
    for u in users:
        # every checkpoint id written for these threads, kept before delete_thread makes Expected forget them (F6)
        written = {tid: expected.written_ids(tid) for tid in by_user[u]}
        ts.context = {"phase": "erase", "user": u, "threads": len(by_user[u])}
        t0 = time.perf_counter()
        for tid in by_user[u]:
            ts.delete_thread(tid)
        rec({"ts": time.time(), "op": "erase_user", "user": u, "threads": len(by_user[u]),
             "ms": round((time.perf_counter() - t0) * 1000, 3), "phase": "erase"})
        ts.context = {"phase": "erase_verify"}
        for tid in by_user[u]:
            cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
            left = ts.get_tuple(cfg) is not None or bool(list(ts.inner.list(cfg, limit=1)))
            # bench-local review F6: each written checkpoint asked by its id must be gone too (untimed: the saver itself)
            for ns, cid in written[tid]:
                if ts.inner.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": ns, "checkpoint_id": cid}}) is not None:
                    ids_left += 1
                    left = True
            leftovers += left
    handle.close()
    return {"users_erased": len(users), "threads_erased": sum(len(by_user[u]) for u in users), "threads_left_readable": leftovers,
            "checkpoints_left_readable_by_id": ids_left,
            "erased": sorted(t for u in users for t in by_user[u])}


def cold_phase(target: targets.Target, threads: dict, rec: Recorder, n: int, seed: int, cmd: str, not_these: set,
               expected: graph_runner.Expected) -> dict:
    """Restart the line (cmd), then resume n threads once each: the first reads after the restart."""
    import subprocess

    eligible = sorted(t for t in threads if t not in not_these)
    chosen = random.Random(seed + 2).sample(eligible, min(n, len(eligible)))
    t0 = time.time()
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    restart = {"cmd": cmd, "rc": r.returncode, "seconds": round(time.time() - t0, 1), "output": (r.stdout + r.stderr)[-4000:]}
    if r.returncode != 0:
        return {"restart": restart, "threads": 0, "error": "restart failed"}
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec, expected=expected)
    errors = []
    for tid in chosen:
        ts.context = {"phase": "cold", "kind": threads[tid].kind, "turns": threads[tid].turns}
        try:
            ts.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": ""}})
        except Exception as ex:  # noqa: BLE001 - reported, the run is then marked failed
            errors.append(f"{tid}: {ex!r}")
    handle.close()
    return {"restart": restart, "threads": len(chosen), "eligible": len(eligible), "errors": errors[:10],
            "n_errors": len(errors), "first_thread": chosen[0] if chosen else None}


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


def run_ok(w: dict, rd: dict, er: dict, check: dict, log_failed: dict, health: list, cold: dict | None) -> bool:
    """A run is ok only if: no write error; no read inconsistency and no wrong read; no erased thread readable, also by
    checkpoint id (F6); our 3-zone log refused nothing (F5: quaderno.failed 0 after write, before the cold restart and
    at the end); every member healthy after write and at the end (F10); the cold restart and every cold read worked."""
    return bool(not w["errors"] and rd["inconsistencies"] == 0 and er["threads_left_readable"] == 0
                and check["wrong_reads"] == 0 and er.get("checkpoints_left_readable_by_id", 0) == 0
                and all(not v for v in (log_failed or {}).values()) and all(h["rc"] == 0 for h in health)
                and (cold is None or (cold["restart"]["rc"] == 0 and not cold.get("n_errors") and cold.get("threads", 0) > 0)))


def run_health(cmd: str, when: str) -> dict:
    import subprocess

    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return {"when": when, "rc": r.returncode, "output": (r.stdout + r.stderr)[-4000:]}


def ours_shares(sizes: dict, stored) -> dict:
    """F9: of our stored bytes, how much is still WAL and how much is compaction 2's own objects (fx/)."""
    by = ((sizes.get("s3") or {}).get("by_kind")) or {}
    if not stored or not by:
        return {}
    return {"wal_bytes": by.get("wal", 0), "fx_bytes": by.get("fx", 0), "compacted_bytes": by.get("compacted", 0),
            "wal_share": round(by.get("wal", 0) / stored, 4), "fx_share": round(by.get("fx", 0) / stored, 4),
            "compacted_share": round(by.get("compacted", 0) / stored, 4)}


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
    ap.add_argument("--settle", action="store_true", help="fenvix: before the sizes, wait until the compactor is idle")
    ap.add_argument("--settle-cap-s", type=int, default=2700, help="cap of the --settle wait (default 45 minutes)")
    ap.add_argument("--cold-cmd", default="", help="shell command that restarts the line; then the cold phase runs")
    ap.add_argument("--cold-threads", type=int, default=200)
    ap.add_argument("--health-cmd", default="", help="shell command that checks every member of the line (bench-local "
                    "review F10); run after the write phase and at the end, a non-zero exit makes ok false")
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
    expected = graph_runner.Expected()
    s3_before = targets.fenvix_s3_counters(a.uri) if a.target == "fenvix" else None
    status_before = targets.fenvix_status(a.uri) if a.target == "fenvix" else None

    w = write_phase(target, a.seed, threads, events, rec, a.workers, expected, a.out)
    s3_after = targets.fenvix_s3_counters(a.uri) if a.target == "fenvix" else None
    status_after_write = targets.fenvix_status(a.uri) if a.target == "fenvix" else None
    health = [run_health(a.health_cmd, "after write")] if a.health_cmd else []
    rd = read_phase(target, threads, rec, a.read_sample, a.seed, expected)
    read_chosen = rd.pop("chosen")
    settle = targets.wait_compaction_idle(target, a.settle_cap_s) if a.settle else None
    targets.wait_settled(target)
    sizes = target.sizes()
    # per-thread logical vs stored ("agent checkpoint delta test"), before the erase removes threads
    with open(os.path.join(a.out, "per_thread_bytes.json"), "w") as f:
        json.dump(logical_bytes(rec.rows)["per_thread"], f)
    delta = delta_hook.run(a.target, a.uri, a.out)
    n_erase = a.erase_users or max(2, generator.PROFILES[a.profile].users // 10)
    er = erase_phase(target, threads, rec, n_erase, a.seed, expected)
    erased = er.pop("erased")
    targets.wait_settled(target)
    sizes_after_erase = target.sizes()
    # the cook's counters start again at 0 after the cold restart: read them before it too (F5)
    status_before_cold = targets.fenvix_status(a.uri) if a.target == "fenvix" else None
    cold = None
    if a.cold_cmd:
        cold = cold_phase(target, threads, rec, a.cold_threads, a.seed, a.cold_cmd, set(read_chosen) | set(erased), expected)
    status_end = targets.fenvix_status(a.uri) if a.target == "fenvix" else None
    if a.health_cmd:
        health.append(run_health(a.health_cmd, "end"))
    rec.close()
    check = expected.report()
    # F5: our 3-zone log refused a batch: the engine acknowledged it through the S3-WAL fallback, not at 2 of 3 copies
    log_failed = {k: ((s or {}).get("quaderno") or {}).get("failed") for k, s in
                  (("after_write", status_after_write), ("before_cold", status_before_cold), ("end", status_end))} \
        if a.target == "fenvix" else {}

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
                       **ours_shares(sizes, stored),
                       "stored_per_logical": round(stored / logical_total, 4) if stored and logical_total else None,
                       "stored_per_checkpoint": round(stored / lb["checkpoints"], 1) if stored and lb["checkpoints"] else None},
        "sizes": sizes, "sizes_after_erase": sizes_after_erase,
        "s3_put_requests_during_write": s3_puts,
        "server_status_before": status_before, "server_status_after_write": status_after_write,
        "server_status_before_cold": status_before_cold, "server_status_end": status_end, "log_failed": log_failed,
        "health": health, "client_cpu": w.get("client_cpu"), "client_limited": bool((w.get("client_cpu") or {}).get("client_limited")),
        "settle": settle, "cold": cold,
        "read_sample": a.read_sample, "erase_users": n_erase,
        "correctness": check, "wrong_reads": check["wrong_reads"],
        "delta_test": {k: v for k, v in delta.items() if k != "rows"},
        "ok": run_ok(w, rd, er, check, log_failed, health, cold),
    }
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    lat = summary["latency_ms"]
    print(f"[{a.target}{' | ' + a.durability if a.durability else ''}] {'VALIDATION, NOT RESULTS: ' if a.local else ''}ok={summary['ok']} "
          f"checkpoints={lb['checkpoints']} write={w['seconds']}s "
          f"save_step p50/p99={lat['save_step']['p50']}/{lat['save_step']['p99']} ms "
          f"stored/logical={summary['bytes']['stored_per_logical']} wrong_reads={check['wrong_reads']} -> {a.out}/summary.json")
    if check["wrong_reads"]:
        # RULES.md 8 / P-bench-local 0: a read that returns a checkpoint different from the one written is a STOP
        print(f"STOP: {check['wrong_reads']} wrong reads {check['by_kind']}: {check['examples'][:3]}", file=sys.stderr)
        sys.exit(3)
    if w["errors"]:
        print("errors:", w["errors"][:3], file=sys.stderr)
        sys.exit(1)
    if not summary["ok"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
