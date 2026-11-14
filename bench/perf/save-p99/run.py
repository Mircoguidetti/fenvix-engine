"""save-p99: where the tail of an acknowledged write (insert_one, replicated log on, 2 of 3 on disk) comes from.

One run: MinIO (must be up on :9000), three `pq replica` copies of the log on this machine (as ctl-tests test_91), the
engine with --quaderno and FX_WAITS=0 (every command prints its WAITS line: admit, lock_w, stage, log_ack, visible, ...,
and a WAITS_MSG line with handle / write_reply), a paced insert_one load (RATE/s over THREADS connections, open loop:
the latency is measured from the scheduled send time too) plus a background writer (insert_many into another database),
for SECS seconds. Then every acknowledged _id is read back (0 lost is required) and the leader's log timings are taken.

  python3 run.py OUT [--secs 600] [--rate 100] [--bg-docs 20] [--bg-every-ms 50] [--log-dirs shared|tmpfs|sep]
                     [--server-flags "..."] [--env K=V ...] [--pq-env K=V ...]
"""
import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(ROOT, "ctl-tests"))
from lib import make_bucket, free_port  # noqa: E402  (sets the MinIO AWS_* variables)
from pymongo import MongoClient, InsertOne  # noqa: E402

BIN = os.environ.get("MONGO_SERVER_BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
PQ = os.environ.get("PQ_BIN", os.path.join(ROOT, "proto", "quaderno", "target", "release", "pq"))


def wait_port(p, t=60):
    t0 = time.time()
    while time.time() - t0 < t:
        try:
            socket.create_connection(("127.0.0.1", p), timeout=1).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {p} did not open")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--secs", type=int, default=600)
    ap.add_argument("--rate", type=float, default=100.0)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--bg-docs", type=int, default=20)
    ap.add_argument("--bg-every-ms", type=int, default=50)
    ap.add_argument("--bg-size", type=int, default=1000)
    ap.add_argument("--log-dirs", default="shared", help="shared: the three copies under one dir of the root disk; sep: given by --dirs")
    ap.add_argument("--dirs", default="", help="comma list of three dirs for the copies (with --log-dirs sep)")
    ap.add_argument("--server-flags", default="")
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--pq-env", action="append", default=[])
    ap.add_argument("--pq-flags", default="")
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)

    # ---- the log: three copies
    if a.log_dirs == "sep":
        dirs = a.dirs.split(",")
    else:
        base = tempfile.mkdtemp(prefix="svlog", dir=os.environ.get("SV_LOG_BASE"))
        dirs = [os.path.join(base, f"r{i}") for i in range(3)]
    peer = [free_port() for _ in range(3)]
    cli = [free_port() for _ in range(3)]
    pqenv = dict(os.environ, **dict(kv.split("=", 1) for kv in a.pq_env))
    procs = []
    for i in range(3):
        os.makedirs(dirs[i], exist_ok=True)
        peers = ",".join(f"{j}=127.0.0.1:{peer[j]}" for j in range(3) if j != i)
        cmd = [PQ, "replica", "--id", str(i), "--dir", dirs[i], "--peer-listen", f"127.0.0.1:{peer[i]}", "--client-listen", f"127.0.0.1:{cli[i]}",
               "--peers", peers, "--mem-cap-mb", "256"] + a.pq_flags.split()
        procs.append(subprocess.Popen(cmd, stdout=open(os.path.join(out, f"pq{i}.log"), "ab"), stderr=subprocess.STDOUT, env=pqenv))
    addrs = ",".join(f"127.0.0.1:{p}" for p in cli)
    for p in cli:
        wait_port(p)

    # ---- the engine
    bucket = make_bucket("sv")
    port, aport = free_port(), free_port()
    kept = tempfile.mkdtemp(prefix="svkept")
    cmd = [BIN, "--listen", f"127.0.0.1:{port}", "--admin-listen", f"127.0.0.1:{aport}", "--idle-secs", "600", "--bucket", bucket,
           "--endpoint", "http://127.0.0.1:9000", "--holder", f"sv-{port}", "--no-ttl",
           "--quaderno", addrs, "--quaderno-id", "sv", "--local-log-dir", kept] + a.server_flags.split()
    env = dict(os.environ, MONGO_ADMIN_TOKEN="test-token-0123456789abcdef", FX_WAITS="0")
    env.update(dict(kv.split("=", 1) for kv in a.env))
    srv_log = os.path.join(out, "server.log")
    srv = subprocess.Popen(cmd, stdout=open(srv_log, "ab"), stderr=subprocess.STDOUT, env=env)
    json.dump({"cmd": cmd, "pq_dirs": dirs, "args": vars(a), "env": a.env, "pq_env": a.pq_env}, open(os.path.join(out, "config.json"), "w"), indent=1)
    wait_port(port, 120)
    uri = f"mongodb://127.0.0.1:{port}/?directConnection=true"

    # ---- warm: create both databases (first write opens them)
    c0 = MongoClient(uri, serverSelectionTimeoutMS=30000)
    c0.sv.t.insert_one({"_id": "warm"})
    c0.bg.t.insert_one({"_id": "warm"})
    time.sleep(2)

    stop = threading.Event()
    lock = threading.Lock()
    acked_main, acked_bg, errors = [], [], []
    rows = []      # (sched_unix_ms, send_unix_ms, service_ms, from_sched_ms)
    t_start = time.time() + 1.0
    per = a.rate / a.threads

    def writer(k):
        c = MongoClient(uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=60000)
        coll = c.sv.t
        n = 0
        while not stop.is_set():
            sched = t_start + (n * a.threads + k) / a.rate
            d = sched - time.time()
            if d > 0:
                time.sleep(d)
            if stop.is_set():
                break
            _id = f"m{k}-{n}"
            t0 = time.time()
            try:
                coll.insert_one({"_id": _id, "v": "x" * 200, "k": k, "n": n})
                t1 = time.time()
                with lock:
                    acked_main.append(_id)
                    rows.append((sched * 1000, t0 * 1000, (t1 - t0) * 1000, (t1 - sched) * 1000))
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(f"{_id}: {e}")
            n += 1
        c.close()

    def bg():
        c = MongoClient(uri, serverSelectionTimeoutMS=30000, socketTimeoutMS=60000)
        coll = c.bg.t
        n = 0
        while not stop.is_set():
            t0 = time.time()
            ids = [f"b-{n}-{j}" for j in range(a.bg_docs)]
            try:
                coll.insert_many([{"_id": i, "p": os.urandom(a.bg_size // 2).hex()} for i in ids], ordered=True)
                with lock:
                    acked_bg.extend(ids)
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(f"bg {n}: {e}")
            n += 1
            d = a.bg_every_ms / 1000 - (time.time() - t0)
            if d > 0:
                time.sleep(d)
        c.close()

    ths = [threading.Thread(target=writer, args=(k,)) for k in range(a.threads)] + ([threading.Thread(target=bg)] if a.bg_docs > 0 else [])
    for t in ths:
        t.start()
    # leader's log timers from now on
    subprocess.run([PQ, "reset", "--replicas", addrs], capture_output=True, timeout=30)
    t_end = t_start + a.secs
    while time.time() < t_end:
        time.sleep(5)
        if srv.poll() is not None:
            print("server exited", srv.returncode)
            break
    stop.set()
    for t in ths:
        t.join(120)
    st = subprocess.run([PQ, "status", "--replicas", addrs], capture_output=True, timeout=30, text=True).stdout
    open(os.path.join(out, "pq_status.jsonl"), "w").write(st)

    # ---- read back every acknowledged _id
    lost_main = lost_bg = None
    try:
        got = set(d["_id"] for d in c0.sv.t.find({}, {"_id": 1}))
        lost_main = [i for i in acked_main if i not in got]
        got = set(d["_id"] for d in c0.bg.t.find({}, {"_id": 1}))
        lost_bg = [i for i in acked_bg if i not in got]
    except Exception as e:  # noqa: BLE001
        print("read back failed:", e)
    c0.close()
    with open(os.path.join(out, "client.jsonl"), "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    summ = {"acked_main": len(acked_main), "acked_bg": len(acked_bg), "errors": len(errors), "errors_first": errors[:5],
            "lost_main": None if lost_main is None else len(lost_main), "lost_bg": None if lost_bg is None else len(lost_bg),
            "lost_first": (lost_main or [])[:5] + (lost_bg or [])[:5]}
    json.dump(summ, open(os.path.join(out, "readback.json"), "w"), indent=1)
    print(json.dumps(summ))
    srv.send_signal(signal.SIGTERM)
    try:
        srv.wait(60)
    except subprocess.TimeoutExpired:
        srv.kill()
    for p in procs:
        p.kill()
        p.wait()
    if a.log_dirs != "sep":
        shutil.rmtree(os.path.dirname(dirs[0]), ignore_errors=True)
    shutil.rmtree(kept, ignore_errors=True)


if __name__ == "__main__":
    main()
