#!/usr/bin/env python3
"""WAL objects of one hot tenant over time, and recovery time after a SIGKILL.

    exp_wal_growth.py <label> <binary> <minutes> [--conns 8] [-- extra server args]

A server on MinIO, one tenant, `loadgen hot` writes only (closed loop) for <minutes>; every 10 s the number of WAL
objects of the tenant is counted in MinIO's data directory (MINIO_DATA_DIR). At the end the server is SIGKILLed, a new
one is started on the same bucket and the time to the first successful read of the tenant is measured (the unclean
wake replays the WAL tail). Writes: results/wal/<label>-<minutes>m.json
"""
import json, os, signal, subprocess, sys, threading, time
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOADGEN = os.environ.get("LOADGEN", os.path.join(ROOT, "target", "release", "loadgen"))
DATA = os.environ.get("MINIO_DATA_DIR", "/tmp/minio-data") + "/bench/tenants"
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
OUT = os.path.join(ROOT, "loadtest", "out", "wal"); RES = os.path.join(ROOT, "loadtest", "results", "wal")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
PORT = 28081


def start(binary, extra, tag):
    p = subprocess.Popen([binary, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "3600", "--holder", "W", "--lease-ttl-secs", "30", "--endpoint", EP] + extra,
                         stdout=open(os.path.join(OUT, f"{tag}.log"), "ab"), stderr=subprocess.STDOUT)
    return p


def wait_ping(timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200).admin.command("ping")
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("no start")


def main():
    args = sys.argv[1:]
    extra = args[args.index("--") + 1:] if "--" in args else []
    args = args[: args.index("--")] if "--" in args else args
    label, binary, minutes = args[0], args[1], float(args[2])
    conns = int(args[args.index("--conns") + 1]) if "--conns" in args else 8
    tag = f"{label}-{minutes:g}m"
    prefix = f"wg{label}{int(minutes*10)}x{int(time.time())%100000}"
    name = f"{prefix}000000"
    p = start(binary, extra, tag); wait_ping()
    uri = f"mongodb://127.0.0.1:{PORT}"
    subprocess.run([LOADGEN, "load", "--uris", uri, "--prefix", prefix, "--tenants", "1", "--first", "0", "--out", "/tmp/wg-load.json", "--close-after", "false"], check=True, stdout=subprocess.DEVNULL)
    samples, stop = [], threading.Event()
    walp = f"{DATA}/{name}/wal"

    def sampler():
        t0 = time.time()
        while not stop.is_set():
            try:
                n = len(os.listdir(walp))
            except OSError:
                n = 0
            samples.append((round(time.time() - t0), n)); stop.wait(10)
    th = threading.Thread(target=sampler); th.start()
    hot = subprocess.run([LOADGEN, "hot", "--uris", uri, "--prefix", prefix, "--tenant", "0", "--connections", str(conns), "--read-pct", "0",
                          "--duration-secs", str(int(minutes * 60)), "--warmup-secs", "0", "--out", f"/tmp/wg-{tag}.json"], check=True, stdout=subprocess.DEVNULL)
    stop.set(); th.join()
    r = json.load(open(f"/tmp/wg-{tag}.json"))
    wal_at_kill = len(os.listdir(walp)); comp = len(os.listdir(f"{DATA}/{name}/compacted")) if os.path.isdir(f"{DATA}/{name}/compacted") else 0
    p.send_signal(signal.SIGKILL); p.wait()
    t0 = time.time()
    p2 = start(binary, extra, tag + "-restart"); wait_ping(); t_up = time.time() - t0
    c = MongoClient(uri, serverSelectionTimeoutMS=600000, socketTimeoutMS=1800000)
    d = c[name].c.find_one({"_id": 0}); t_read = time.time() - t0
    n_docs = c[name].c.count_documents({})
    p2.terminate()
    try: p2.wait(60)
    except Exception: p2.kill()
    res = {"label": label, "minutes": minutes, "connections": conns, "extra_args": extra, "writes": r["ops"], "ops_per_sec": r["ops_per_sec"], "errors": r["errors"],
           "wal_samples": samples, "wal_max": max(n for _, n in samples), "wal_at_kill": wal_at_kill, "compacted_ssts_at_kill": comp,
           "restart_to_ping_s": round(t_up, 2), "recovery_s": round(t_read, 2), "docs_after": n_docs, "doc0_ok": bool(d)}
    json.dump(res, open(os.path.join(RES, f"{tag}.json"), "w"), indent=1)
    print({k: v for k, v in res.items() if k != "wal_samples"}, flush=True)


main()
