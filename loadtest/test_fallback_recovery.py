#!/usr/bin/env python3
"""Is everything recovered after the shutdown fallback (close gives up on the final flush after 20 s and closes uncleanly)?

The stall is forced for real: the server runs with --no-maintenance --writer-poll-secs 3600, so nothing compacts its
tenant and, once L0 is full (--l0-max-ssts 1 here: after 2 x 4096 WAL objects instead of 8 x 4096), the memtable flush stalls. With writers still running, SIGTERM is
sent; the server must log "closing without it" and exit. A new server (default flags) is started on the same bucket and
every acknowledged document (inserts, then an update of each) is compared with what is read back.

    test_fallback_recovery.py [--trials 10] [--secs 100] [--threads 48] [--store s3]
"""
import argparse, json, os, signal, subprocess, threading, time
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
OUT = os.path.join(ROOT, "loadtest", "out", "fallback"); RES = os.path.join(ROOT, "loadtest", "results", "fallback")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PORT = 28091


def start(flags, tag):
    import socket
    with socket.socket() as so:
        if so.connect_ex(("127.0.0.1", PORT)) == 0:
            raise RuntimeError("port in use")
    log = os.path.join(OUT, f"{tag}.log")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "F", "--lease-ttl-secs", "30", "--endpoint", EP] + flags,
                         stdout=open(log, "wb"), stderr=subprocess.STDOUT)
    for _ in range(300):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p, log
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def trial(n, secs, threads):
    p, log = start(["--no-maintenance", "--writer-poll-secs", "3600"] + os.environ.get("STALL_FLAGS", "--l0-max-ssts 1").split(), f"t{n}-stall")
    uri = f"mongodb://127.0.0.1:{PORT}"
    db = f"fb{n}_{int(time.time())}"
    c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=3000, maxPoolSize=threads + 2)
    c[db].c.insert_one({"_id": -1, "v": 0})
    acked, lock, stop = {-1: 0}, threading.Lock(), threading.Event()

    def worker(w):
        i = 0
        while not stop.is_set():
            _id = w * 1_000_000 + i
            try:
                c[db].c.insert_one({"_id": _id, "v": 0})
                with lock: acked[_id] = 0
                c[db].c.update_one({"_id": _id}, {"$set": {"v": 1}})
                with lock: acked[_id] = 1
            except Exception:
                return
            i += 1
    ts = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
    [t.start() for t in ts]
    time.sleep(secs)
    t_term = time.time(); p.send_signal(signal.SIGTERM)
    try:
        p.wait(120); exited = round(time.time() - t_term, 1)
    except subprocess.TimeoutExpired:
        p.kill(); p.wait(); exited = None
    stop.set(); [t.join() for t in ts]
    with lock: final = dict(acked)
    txt = open(log, errors="replace").read()
    fallback = "closing without it" in txt
    p2, log2 = start([], f"t{n}-recover")
    t0 = time.time()
    c2 = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=600000, socketTimeoutMS=1800000)
    present = {d["_id"]: d.get("v", 0) for d in c2[db].c.find({}, {"v": 1})}
    t_read = round(time.time() - t0, 1)
    p2.terminate(); p2.wait(60)
    lost = [k for k in final if k not in present]
    stale = [k for k, v in final.items() if k in present and present[k] < v]
    return {"fallback_used": fallback, "exit_s_after_sigterm": exited, "acked_docs": len(final), "acked_updates": sum(final.values()), "found_docs": len(present),
            "lost_inserts": len(lost), "lost_updates": len(stale), "first_full_read_s": t_read}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--trials", type=int, default=10); ap.add_argument("--secs", type=float, default=100); ap.add_argument("--threads", type=int, default=48)
    a = ap.parse_args()
    rs = []
    for n in range(a.trials):
        r = trial(n, a.secs, a.threads); rs.append(r); print(n, r, flush=True)
        json.dump(rs, open(os.path.join(RES, "fallback.json"), "w"), indent=1)
    print("TOTAL", {"trials": len(rs), "fallback_used": sum(r["fallback_used"] for r in rs), "acked_docs": sum(r["acked_docs"] for r in rs),
                    "lost_inserts": sum(r["lost_inserts"] for r in rs), "lost_updates": sum(r["lost_updates"] for r in rs)})


main()
