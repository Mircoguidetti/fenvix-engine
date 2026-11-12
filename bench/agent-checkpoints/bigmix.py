"""P-bench-big (2026-11-12): the measured mix on data that does not fit in memory, and per-operation resource probes.

After the load (bench.py's write phase, every planned turn) and a cold restart of the whole line, `mix_round` runs one
round of the same mix on every target, one call at a time (client --workers 1):

  - write tasks (W threads): one new turn on an old thread (its resume is "resume[long]", the first read of a thread not
    used since the load; its saves are "save_step[old]"), then at once a second turn on the same thread ("resume[none]",
    "save_step[none]");
  - read tasks (R threads, R/4 per kind): ONE read per thread, so each is the first read of that thread since the load:
    "resume[read]" (load the latest checkpoint), "list" (the whole history), "list_limit10" (the last 10) or
    "time_travel" (one earlier checkpoint picked from what was written).

The threads are drawn uniformly from the whole set with a fixed seed per round (the same for every target), never a
thread of an earlier round; read and write tasks are interleaved in a fixed shuffled order. Every read is checked
against the hash of what was written (graph_runner.Expected), as in every other phase.

Probes (outside the timed region of each call, graph_runner.TimedSaver.probe): before and after every saver call,
  - every container of the line: CPU (cgroup v1 cpuacct.usage) and the bytes its processes read from the disk
    (/proc/<pid>/io read_bytes over the cgroup's processes; the blkio counters read 0 on this kernel);
  - ours: serverStatus().fenvix s3 (GETs, bytes), cache (block cache hits and misses), sealedRuns, backgroundReads,
    firstCourse; the delta goes into the op's row as "res".
The cgroup files are read just before t0 and just after t1 (the server counters outside them), so the probe's own
serverStatus call is not counted in the CPU of the op. Background work that runs during a call (compaction, a
checkpoint, WAL GC) is counted in that call: the summary gives medians as well as means.
Cache hit rates of mongod (WiredTiger) and Postgres (pg_statio, shared buffers) are taken per round, before and after
(Postgres flushes its per-backend statistics at most once a second, so per call they would be wrong).
"""
from __future__ import annotations

import json
import os
import random
import time
from collections import defaultdict

import generator
import graph_runner

CG = "/sys/fs/cgroup"


def _flat(d, prefix="") -> dict:
    out = {}
    for k, v in (d or {}).items():
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            out[prefix + k] = v
        elif isinstance(v, dict):
            out |= _flat(v, prefix + k + ".")
    return out


class Probe:
    """Counters before and after one call. containers: {name: docker id}. uri: our server (None for the others)."""

    SERVER_KEYS = ("s3", "cache", "sealedRuns", "backgroundReads", "firstCourse", "waiter")

    def __init__(self, containers: dict[str, str], uri: str | None = None):
        self.containers = containers
        self.client = None
        if uri:
            from pymongo import MongoClient
            self.client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    def _cgroup(self) -> dict:
        out = {}
        for n, cid in self.containers.items():
            try:
                out[f"cpu_ns.{n}"] = int(open(f"{CG}/cpuacct/docker/{cid}/cpuacct.usage").read())
            except OSError:
                continue
            # bytes the container's processes really fetched from the storage layer (/proc/<pid>/io read_bytes, summed
            # over cgroup.procs; exited children are folded into their parent). The cgroup's blkio counters stay at 0
            # on this sandbox's kernel (tested: a 200 MB read after drop_caches counted 0), so they are not used.
            try:
                tot = 0
                for pid in open(f"{CG}/cpuacct/docker/{cid}/cgroup.procs").read().split():
                    try:
                        for line in open(f"/proc/{pid}/io"):
                            if line.startswith("read_bytes:"):
                                tot += int(line.split()[1])
                    except OSError:
                        continue
                out[f"rd_bytes.{n}"] = tot
            except OSError:
                pass
        return out

    def _server(self) -> dict:
        if not self.client:
            return {}
        try:
            f = self.client.admin.command("serverStatus").get("fenvix", {})
        except Exception:  # noqa: BLE001 - a probe never fails a call; the row then has no server counters
            return {}
        return _flat({k: f.get(k) for k in self.SERVER_KEYS if k in f}, "srv.")

    def before(self) -> dict:
        s = self._server()
        return s | self._cgroup()

    def after(self) -> dict:
        c = self._cgroup()
        return c | self._server()

    @staticmethod
    def delta(a: dict, b: dict) -> dict:
        out = {}
        for k, v in b.items():
            if k in a and isinstance(v, (int, float)):
                d = v - a[k]
                if d:
                    out[k] = d
        return out


def add_res(a: dict | None, b: dict | None) -> dict:
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = out.get(k, 0) + v
    return out


def pick_threads(threads: dict, used: set, n_write: int, n_read: int, seed: int) -> tuple[list, list]:
    eligible = sorted(t for t in threads if t not in used)
    r = random.Random(seed)
    n = min(len(eligible), n_write + n_read)
    chosen = r.sample(eligible, n)
    return chosen[:n_write], chosen[n_write:n]


READ_KINDS = ("resume_read", "list", "list_limit10", "time_travel")


def mix_round(target, threads: dict, rec, expected: graph_runner.Expected, seed: int, round_name: str,
              n_write: int, n_read: int, used: set, probe: Probe | None, extra_turns: dict) -> dict:
    """One round of the mix (see the module doc). `extra_turns[tid]`: turns added to a thread by earlier rounds."""
    write_ids, read_ids = pick_threads(threads, used, n_write, n_read, seed)
    used.update(write_ids)
    used.update(read_ids)
    r = random.Random(seed + 7)
    tasks = [("write", t) for t in write_ids] + [("read", READ_KINDS[i % 4], t) for i, t in enumerate(read_ids)]
    r.shuffle(tasks)
    saver, handle = target.make_saver()
    ts = graph_runner.TimedSaver(saver, rec, expected=expected)
    ts.probe = probe
    script = graph_runner.Script()
    graph = graph_runner.build_graph(ts, script)
    errors = []
    t0 = time.time()
    phase = f"mix:{round_name}"
    for task in tasks:
        try:
            if task[0] == "write":
                tid = task[1]
                base = threads[tid]
                k = extra_turns.get(tid, 0)
                th = generator.extend_thread(seed=SEED_OF_PLAN[0], th=base, extra=k + 2)
                for j, label in ((0, "long"), (1, "none")):
                    ts.context = {"phase": phase, "gap": label, "kind": base.kind, "task": "write", "turn_in_task": j}
                    graph_runner.run_turn(graph, script, SEED_OF_PLAN[0], th, base.turns + k + j)
                extra_turns[tid] = k + 2
            else:
                kind, tid = task[1], task[2]
                cfg = {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}
                ts.context = {"phase": phase, "gap": "read", "kind": threads[tid].kind, "task": kind,
                              "turns": threads[tid].turns + extra_turns.get(tid, 0)}
                if kind == "resume_read":
                    ts.get_tuple(cfg)
                elif kind == "list":
                    list(ts.list(cfg))
                elif kind == "list_limit10":
                    list(ts.list(cfg, limit=10))
                else:
                    ids = sorted(cid for ns, cid in expected.written_ids(tid) if ns == "")
                    cid = random.Random(f"{seed}:{tid}").choice(ids)
                    ts.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": "", "checkpoint_id": cid}})
        except Exception as ex:  # noqa: BLE001 - reported; the run is then not ok
            errors.append(f"{task}: {ex!r}")
    handle.close()
    return {"round": round_name, "seed": seed, "write_threads": len(write_ids), "read_threads": len(read_ids),
            "tasks": len(tasks), "seconds": round(time.time() - t0, 1), "errors": errors[:20], "n_errors": len(errors)}


SEED_OF_PLAN = [42]   # set by bench.py to --seed: the content of every new turn comes from the plan's seed


def op_key(row: dict) -> str | None:
    """The name an op of the mix is reported under."""
    op, gap = row["op"], row.get("gap")
    if op == "resume":
        return f"resume[{gap}]"
    if op == "save_step":
        return "save_step[old]" if gap == "long" else "save_step[none]"
    if op in ("list", "list_limit10", "time_travel"):
        return op
    return None


def _pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3) if xs else None


def summarize_round(rows: list[dict], round_name: str) -> dict:
    """Latency per op (p50, p90, p99, max, n) and, per op, the probe's resources: mean and median per call."""
    by = defaultdict(list)
    phase = f"mix:{round_name}"
    for r in rows:
        if r.get("phase") != phase:
            continue
        k = op_key(r)
        if k:
            by[k].append(r)
            if k.startswith("save_step"):
                by["save_step"].append(r)
    out = {}
    for k, rs in sorted(by.items()):
        ms = [r["ms"] for r in rs]
        d = {"n": len(ms), "p50": _pct(ms, 0.5), "p90": _pct(ms, 0.9), "p99": _pct(ms, 0.99), "max": round(max(ms), 3),
             "mean": round(sum(ms) / len(ms), 3)}
        res = defaultdict(list)
        for r in rs:
            for kk, v in (r.get("res") or {}).items():
                res[kk].append(v)
        d["res_mean"] = {kk: round(sum(v) / len(rs), 3) for kk, v in sorted(res.items())}   # zeros count (absent = 0)
        d["res_p50"] = {kk: _pct(v + [0] * (len(rs) - len(v)), 0.5) for kk, v in sorted(res.items())}
        out[k] = d
    return out


# -- per-round cache counters of the other systems ---------------------------------------------------------------------

def mongo_cache(uri: str) -> dict:
    from pymongo import MongoClient
    c = MongoClient(uri, serverSelectionTimeoutMS=10000)
    ss = c.admin.command("serverStatus")
    wc = ss["wiredTiger"]["cache"]
    keys = ("pages requested from the cache", "pages read into cache", "bytes read into cache",
            "bytes currently in the cache", "maximum bytes configured", "pages written from cache")
    return {k: wc.get(k) for k in keys} | {"host": ss.get("host")}


def pg_cache(uri: str) -> dict:
    import psycopg
    with psycopg.connect(uri, autocommit=True) as c:
        row = c.execute("SELECT coalesce(sum(heap_blks_read),0), coalesce(sum(heap_blks_hit),0), coalesce(sum(idx_blks_read),0),"
                        " coalesce(sum(idx_blks_hit),0), coalesce(sum(toast_blks_read),0), coalesce(sum(toast_blks_hit),0),"
                        " coalesce(sum(tidx_blks_read),0), coalesce(sum(tidx_blks_hit),0) FROM pg_statio_user_tables").fetchone()
        db = c.execute("SELECT blks_read, blks_hit FROM pg_stat_database WHERE datname = current_database()").fetchone()
    names = ("heap_read", "heap_hit", "idx_read", "idx_hit", "toast_read", "toast_hit", "tidx_read", "tidx_hit")
    return dict(zip(names, map(int, row))) | {"db_blks_read": int(db[0]), "db_blks_hit": int(db[1])}


def fenvix_cache(uri: str) -> dict:
    from pymongo import MongoClient
    f = MongoClient(uri, serverSelectionTimeoutMS=10000).admin.command("serverStatus").get("fenvix", {})
    return _flat({k: f.get(k) for k in Probe.SERVER_KEYS if k in f})


def round_counters(target_name: str, uri: str) -> dict:
    try:
        if target_name == "mongo":
            return mongo_cache(uri)
        if target_name == "postgres":
            return pg_cache(uri)
        return fenvix_cache(uri)
    except Exception as e:  # noqa: BLE001 - recorded
        return {"error": repr(e)}


def cache_rates(target_name: str, before: dict, after: dict) -> dict:
    """Hit rates over one round from the counters before and after it."""
    d = {k: after[k] - before[k] for k in after if isinstance(after.get(k), (int, float)) and isinstance(before.get(k), (int, float))}
    if target_name == "mongo":
        req, miss = d.get("pages requested from the cache", 0), d.get("pages read into cache", 0)
        return {"wt_pages_requested": req, "wt_pages_read_into_cache": miss,
                "wt_hit_rate": round(1 - miss / req, 4) if req else None, "wt_bytes_read_into_cache": d.get("bytes read into cache")}
    if target_name == "postgres":
        rd = sum(d.get(k, 0) for k in ("heap_read", "idx_read", "toast_read", "tidx_read"))
        hit = sum(d.get(k, 0) for k in ("heap_hit", "idx_hit", "toast_hit", "tidx_hit"))
        return {"shared_buffers_hits": hit, "shared_buffers_reads": rd, "shared_buffers_hit_rate": round(hit / (hit + rd), 4) if hit + rd else None,
                "by_kind": {k: d.get(k, 0) for k in ("heap_read", "heap_hit", "idx_read", "idx_hit", "toast_read", "toast_hit", "tidx_read", "tidx_hit")},
                "note": "a shared-buffers read may still be served by the page cache of the member's cgroup; rd_bytes (/proc/<pid>/io) is the disk"}
    hits, miss = d.get("cache.hits", 0), d.get("cache.misses", 0)
    gets = d.get("s3.get", 0) + d.get("s3.getRange", 0)
    return {"block_cache_hits": hits, "block_cache_misses": miss,
            "block_cache_hit_rate": round(hits / (hits + miss), 4) if hits + miss else None,
            "sealed_block_hits": d.get("sealedRuns.blockHits"), "sealed_run_gets": d.get("sealedRuns.runGets"),
            "s3_gets": gets, "s3_get_bytes": d.get("s3.getBytes"), "background_gets": d.get("backgroundReads.gets"),
            "delta": d}


def write_json(path: str, obj) -> None:
    with open(path, "w") as f:
        json.dump(obj, f, indent=1, default=str)
