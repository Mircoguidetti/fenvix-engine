"""P-big-speed dev bench: our server on the host (no Docker), MinIO, an optional S3-like delay (s3delay), a small plan.

Not a measured result: it is the loop the engine fixes are developed and checked with (S3 GETs and trips per call,
FIRST and NEXT request of a customer). The measured results come from bench_big.sh on the Docker line.

  devbench.py up      --server BIN [--delay-ms N] [--flags "..."]   start MinIO (+ s3delay) and the server
  devbench.py load    --profile small --users N                      write the plan's turns of the first N users
  devbench.py restart --server BIN [--delay-ms N] [--flags "..."]   cold restart of the server (and s3delay)
  devbench.py probe   --threads K                                     FIRST and NEXT of every op on K threads
  devbench.py down

State in $DEVBENCH_DIR (default /tmp/claude-0/devbench).
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import random
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import generator  # noqa: E402
import graph_runner  # noqa: E402

D = os.environ.get("DEVBENCH_DIR", "/tmp/claude-0/devbench")
MINIO = os.path.expanduser("~/.fenvix-judge/env/bin/minio")
S3DELAY = os.path.join(HERE, "s3delay/target/release/s3delay")
PORT_MINIO, PORT_PROXY, PORT_SRV = 9302, 9300, 27317
KEY, SECRET, BUCKET = "devbench", "devbench-secret", "fxdev"
URI = f"mongodb://127.0.0.1:{PORT_SRV}/?directConnection=true"
SEED = 42


def pidfile(n):
    return os.path.join(D, f"{n}.pid")


def start(n, cmd, env=None):
    log = open(os.path.join(D, f"{n}.log"), "a")
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, **(env or {})), start_new_session=True)
    open(pidfile(n), "w").write(str(p.pid))
    return p


def stop(n, sig=signal.SIGTERM, wait=60):
    try:
        pid = int(open(pidfile(n)).read())
    except OSError:
        return
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass
    t = time.time()
    while time.time() - t < wait:
        try:
            os.kill(pid, 0)
            time.sleep(0.2)
        except ProcessLookupError:
            break
    os.remove(pidfile(n))


def wait_port(port, s=60):
    import socket
    t = time.time()
    while time.time() - t < s:
        try:
            socket.create_connection(("127.0.0.1", port), 1).close()
            return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"port {port} not up")


def server_up(a):
    endpoint = f"http://127.0.0.1:{PORT_PROXY if a.delay_ms is not None else PORT_MINIO}"
    if a.delay_ms is not None:
        start("s3delay", [S3DELAY, f"127.0.0.1:{PORT_PROXY}", f"127.0.0.1:{PORT_MINIO}", str(a.delay_ms)])
        wait_port(PORT_PROXY)
    cmd = [a.server, "--listen", f"127.0.0.1:{PORT_SRV}", "--store", "s3", "--bucket", BUCKET, "--region", "eu-west-1",
           "--endpoint", endpoint, "--customer-key", "checkpoints=thread_id,checkpoint_writes=thread_id"] + a.flags.split()
    start("server", cmd, env={"AWS_ACCESS_KEY_ID": KEY, "AWS_SECRET_ACCESS_KEY": SECRET, "AWS_REGION": "eu-west-1"})
    wait_port(PORT_SRV, 120)


def cmd_up(a):
    os.makedirs(D, exist_ok=True)
    os.makedirs(os.path.join(D, "minio"), exist_ok=True)
    start("minio", [MINIO, "server", os.path.join(D, "minio"), "--address", f"127.0.0.1:{PORT_MINIO}", "--console-address", "127.0.0.1:9303"],
          env={"MINIO_ROOT_USER": KEY, "MINIO_ROOT_PASSWORD": SECRET, "MINIO_API_DELETE_CLEANUP_INTERVAL": "10s"})
    wait_port(PORT_MINIO)
    import boto3
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT_MINIO}", aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="eu-west-1")
    for _ in range(50):
        try:
            s3.create_bucket(Bucket=BUCKET)
            break
        except s3.exceptions.BucketAlreadyOwnedByYou:
            break
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    server_up(a)


def cmd_restart(a):
    stop("server")
    stop("s3delay")
    server_up(a)


def cmd_down(a):
    for n in ("server", "s3delay", "minio"):
        stop(n)


def saver():
    from langgraph.checkpoint.mongodb import MongoDBSaver
    from pymongo import MongoClient
    c = MongoClient(URI)
    return MongoDBSaver(c, db_name="app_demo"), c


def cmd_load(a):
    threads, events = generator.plan(SEED, a.profile)
    users = sorted({e["user"] for e in events})[: a.users]
    events = [e for e in events if e["user"] in users]
    th = {t.thread_id: t for t in threads}
    exp = graph_runner.Expected()

    class Rec:
        def __call__(self, row):
            pass

    s, c = saver()
    ts = graph_runner.TimedSaver(s, Rec(), expected=exp)
    script = graph_runner.Script()
    graph = graph_runner.build_graph(ts, script)
    t0 = time.time()
    for i, e in enumerate(events):
        ts.context = {"phase": "write"}
        graph_runner.run_turn(graph, script, SEED, th[e["thread_id"]], e["turn"])
        if i % 200 == 0:
            print(f"{i}/{len(events)} turns, {time.time() - t0:.0f} s", flush=True)
    c.close()
    written = sorted({e["thread_id"] for e in events})
    pickle.dump({"threads": written, "expected": exp.export_state(), "profile": a.profile, "users": a.users}, open(os.path.join(D, "plan.pkl"), "wb"))
    print(f"loaded {len(events)} turns of {len(written)} threads in {time.time() - t0:.0f} s")


def status(c):
    f = c.admin.command("serverStatus").get("fenvix", {})
    s3 = f.get("s3", {})
    w = f.get("waiter", {})
    return {"gets": s3.get("get", 0) + s3.get("getRange", 0), "get_bytes": s3.get("getBytes", 0), "trips": w.get("trips", 0),
            "puts": s3.get("put", 0) + s3.get("multipartPart", 0)}


def cmd_probe(a):
    """One round of the measured mix (bigmix.mix_round) on threads of the loaded plan, with per-call probes."""
    import bigmix
    st = pickle.load(open(os.path.join(D, "plan.pkl"), "rb"))
    exp = graph_runner.Expected()
    exp.merge_state(st["expected"])
    threads_all, _ = generator.plan(SEED, st["profile"])
    threads = {t.thread_id: t for t in threads_all if t.thread_id in set(st["threads"])}
    rows = []

    class Rec:
        def __call__(self, row):
            rows.append(row)

    class T:
        name = "fenvix"

        def make_saver(self):
            return saver()

    bigmix.SEED_OF_PLAN[0] = SEED
    used = set(st.get("used", []))
    extra = st.get("extra", {})
    probe = bigmix.Probe({}, URI)
    r = bigmix.mix_round(T(), threads, Rec(), exp, a.seed, "dev", a.writes, a.reads, used, probe, extra)
    st["used"], st["extra"], st["expected"] = sorted(used), extra, exp.export_state()
    pickle.dump(st, open(os.path.join(D, "plan.pkl"), "wb"))
    print(json.dumps({k: r[k] for k in ("tasks", "seconds", "n_errors", "errors")}))
    print("rows", len(rows), "wrong reads", sum(exp.wrong.values()), dict(exp.wrong), "reads checked", exp.reads_checked)
    summ = bigmix.summarize_round(rows, "dev")
    for k, d in summ.items():
        m = d["res_p50"]
        mx = {}
        for x in rows:
            if bigmix.op_key(x) == k:
                for kk, v in (x.get("res") or {}).items():
                    mx[kk] = max(mx.get(kk, 0), v)
        gets = (m.get("srv.s3.get") or 0) + (m.get("srv.s3.getRange") or 0)
        print(f"{k:22s} n={d['n']:4d} ms p50 {d['p50']:7.1f} p99 {d['p99']:7.1f} | GETs p50 {gets} max {mx.get('srv.s3.get', 0) + mx.get('srv.s3.getRange', 0)}"
              f" | trips p50 {m.get('srv.waiter.trips') or 0} max {mx.get('srv.waiter.trips', 0)} | KB p50 {(m.get('srv.s3.getBytes') or 0) // 1024}")
    json.dump(rows, open(os.path.join(D, f"probe-{int(time.time())}.json"), "w"), default=str)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["up", "load", "restart", "probe", "down"])
    ap.add_argument("--server", default=os.path.expanduser("/home/user/fenvix-engine/target/release/mongo-server"))
    ap.add_argument("--delay-ms", type=int, default=None)
    ap.add_argument("--flags", default="")
    ap.add_argument("--profile", default="small")
    ap.add_argument("--users", type=int, default=20)
    ap.add_argument("--writes", type=int, default=10)
    ap.add_argument("--reads", type=int, default=20)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    {"up": cmd_up, "load": cmd_load, "restart": cmd_restart, "probe": cmd_probe, "down": cmd_down}[a.cmd](a)


if __name__ == "__main__":
    main()
