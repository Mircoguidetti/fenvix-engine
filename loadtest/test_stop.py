#!/usr/bin/env python3
"""Task 2: can a process that lost ownership serve a read? SIGSTOP the owner for longer than the lease TTL,
let the other process take the tenant over and write a newer value, SIGCONT the first process and read through it.

Reads queued at the stopped process (sent while it was frozen) and reads sent right after it resumes must return
the new value or fail with a retryable error, never the old value. With --unsafe-skip-ownership-checks on the first
process (negative control) the old value is served, which shows the test can detect it.

    test_stop.py [--rounds 5] [--ttl 4] [--serialize-reads is used for the "serialized reads" rows]
"""
import argparse
import json
import os
import signal
import subprocess
import threading
import time
import uuid

from pymongo import MongoClient
from pymongo.errors import OperationFailure

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release", "mongo-server")
OUT = os.path.join(ROOT, "loadtest", "out", "t2")
RES = os.path.join(ROOT, "loadtest", "results", "t2")
os.makedirs(OUT, exist_ok=True)
os.makedirs(RES, exist_ok=True)
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")


def start(port, holder, ttl, extra):
    f = open(os.path.join(OUT, f"server-{port}.log"), "ab")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--idle-secs", "600", "--holder", holder, "--lease-ttl-secs", str(ttl), "--endpoint", ENDPOINT] + extra, stdout=f, stderr=f)
    for _ in range(100):
        try:
            MongoClient(f"mongodb://127.0.0.1:{port}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def client(port, timeout_ms=40000):
    return MongoClient(f"mongodb://127.0.0.1:{port}", serverSelectionTimeoutMS=timeout_ms, socketTimeoutMS=timeout_ms, retryWrites=False, retryReads=False, maxPoolSize=200)


def scenario(unsafe, shared, rounds, ttl):
    extra_a = (["--unsafe-skip-ownership-checks"] if unsafe else []) + ([] if shared else ["--serialize-reads"])
    extra_b = [] if shared else ["--serialize-reads"]
    a = start(29101, "A", ttl, extra_a)
    b = start(29102, "B", ttl, extra_b)
    ca, cb = client(29101), client(29102)
    tot = {"stale": 0, "new_value": 0, "refused_134": 0, "other_error": 0, "queued_reads": 0, "post_resume_reads": 0}
    examples = []
    try:
        for r in range(rounds):
            db = f"stop_{uuid.uuid4().hex[:8]}"
            ca[db].c.insert_one({"_id": 1, "v": 1})
            assert ca[db].c.find_one({"_id": 1})["v"] == 1  # A owns the tenant and has it open
            os.kill(a.pid, signal.SIGSTOP)
            t_stop = time.time()
            results = []
            lock = threading.Lock()

            def reader(tag):
                try:
                    d = ca[db].c.find_one({"_id": 1})
                    res = ("value", d["v"] if d else None)
                except OperationFailure as e:
                    res = ("refused" if e.code == 134 else "error", e.code)
                except Exception as e:  # noqa
                    res = ("error", type(e).__name__)
                with lock:
                    results.append((tag, res))
            # reads sent while A is frozen: they sit in the socket buffer until it resumes
            queued = [threading.Thread(target=reader, args=("queued",)) for _ in range(40)]
            [t.start() for t in queued]
            # B takes the tenant over once the lease has expired, and writes a newer value
            deadline = time.time() + ttl + 20
            took = None
            while time.time() < deadline:
                try:
                    cb[db].c.update_one({"_id": 1}, {"$set": {"v": 2}})
                    took = time.time() - t_stop
                    break
                except OperationFailure as e:
                    if e.code != 134:
                        raise
                    time.sleep(0.25)
            assert took is not None, "B never took the tenant over"
            assert cb[db].c.find_one({"_id": 1})["v"] == 2
            os.kill(a.pid, signal.SIGCONT)
            post = [threading.Thread(target=reader, args=("post",)) for _ in range(40)]
            [t.start() for t in post]
            [t.join() for t in queued + post]
            for tag, (kind, val) in results:
                key = "queued_reads" if tag == "queued" else "post_resume_reads"
                tot[key] += 1
                if kind == "value":
                    if val == 2:
                        tot["new_value"] += 1
                    else:
                        tot["stale"] += 1
                        if len(examples) < 10:
                            examples.append(f"round {r} {tag}: returned v={val} after B had written v=2")
                elif kind == "refused":
                    tot["refused_134"] += 1
                else:
                    tot["other_error"] += 1
            print(f"  round {r}: B took over {took:.1f}s after the stop; stale so far {tot['stale']}", flush=True)
    finally:
        for p in (a, b):
            try:
                os.kill(p.pid, signal.SIGCONT)
            except OSError:
                pass
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(60)
            except Exception:
                p.kill()
    return {"unsafe_skip_ownership_checks": unsafe, "shared_reads": shared, "rounds": rounds, "ttl_secs": ttl, **tot, "examples": examples}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--ttl", type=int, default=4)
    a = ap.parse_args()
    out = []
    for unsafe, shared in [(False, False), (False, True), (True, False)]:
        print("unsafe" if unsafe else "safe", "shared reads" if shared else "serialized reads", flush=True)
        out.append(scenario(unsafe, shared, a.rounds, a.ttl))
    json.dump(out, open(os.path.join(RES, "stop_cont.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
