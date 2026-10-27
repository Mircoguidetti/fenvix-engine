#!/usr/bin/env python3
"""Cleanup of the compacted SSTs and manifests of an open writer: reads stay right, a SIGKILL during a pass loses nothing.

Server flags make everything fast: memtable flush at 1 MiB, writers re-read the manifest every 2 s, SST/manifest and WAL
cleanup every 5 s, an object must be unreferenced for 20 s. Writers insert 2 KB documents and update a counter in each;
readers check, for documents whose latest acknowledged counter they know, that the document exists, its padding is
intact and its counter is not older than the acknowledged one (a read through an SST that was deleted too early fails
or returns nothing). After `--secs` seconds the test waits for the next "SSTGC begin" line of the server log and sends
SIGKILL at once (so the kill falls inside a cleanup pass), restarts a normal server and compares every acknowledged write.
Negative control: --control starts the server with --gc-control-delete-live (the cleanup also deletes a referenced SST).

    test_open_gc.py [--trials 4] [--secs 100] [--writers 12] [--readers 8] [--control]
"""
import argparse, json, os, random, signal, subprocess, threading, time
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
OUT = os.path.join(ROOT, "loadtest", "out", "open-gc"); RES = os.path.join(ROOT, "loadtest", "results", "open-gc")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PORT = 28121
PAD = "x" * 2000
FAST = ["--sst-gc-secs", "5", "--sst-gc-min-unref-secs", "20", "--writer-poll-secs", "2", "--l0-sst-bytes", "1048576", "--wal-gc-secs", "5"]


def start(flags, log):
    import socket
    with socket.socket() as so:
        if so.connect_ex(("127.0.0.1", PORT)) == 0:
            raise RuntimeError("port in use")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "G", "--lease-ttl-secs", "30", "--endpoint", EP] + flags,
                         stdout=open(log, "wb"), stderr=subprocess.STDOUT)
    for _ in range(300):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("no start")


def trial(n, secs, writers, readers, control):
    log = os.path.join(OUT, f"t{n}{'-control' if control else ''}.log")
    p = start(FAST + (["--gc-control-delete-live"] if control else []), log)
    uri = f"mongodb://127.0.0.1:{PORT}"
    db = f"og{n}_{int(time.time())}"
    c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=5000, maxPoolSize=writers + readers + 4)
    c[db].c.insert_one({"_id": -1, "v": 0, "pad": PAD})
    acked, ids, lock, stop = {-1: 0}, [-1], threading.Lock(), threading.Event()
    viol = {"missing": 0, "stale": 0, "padding": 0, "errors": 0, "count_below_acked": 0}

    def writer(w):
        i = 0
        while not stop.is_set():
            _id = w * 1_000_000 + i
            try:
                c[db].c.insert_one({"_id": _id, "v": 0, "pad": PAD})
                with lock: acked[_id] = 0; ids.append(_id)
                for v in (1, 2):
                    c[db].c.update_one({"_id": _id}, {"$set": {"v": v}})
                    with lock: acked[_id] = v
            except Exception:
                return
            i += 1

    def reader(r):
        rnd = random.Random(r)
        while not stop.is_set():
            with lock:
                _id = rnd.choice(ids); v_ack = acked[_id]; n_ack = len(ids) if rnd.random() < 0.02 else None
            try:
                if n_ack is not None:
                    if c[db].c.count_documents({}) < n_ack:
                        with lock: viol["count_below_acked"] += 1
                    continue
                d = c[db].c.find_one({"_id": _id})
            except Exception:
                if not stop.is_set():
                    with lock: viol["errors"] += 1
                continue
            with lock:
                if d is None: viol["missing"] += 1
                elif d["v"] < v_ack: viol["stale"] += 1
                elif d.get("pad") != PAD: viol["padding"] += 1
    ts = [threading.Thread(target=writer, args=(w,)) for w in range(writers)] + [threading.Thread(target=reader, args=(r,)) for r in range(readers)]
    [t.start() for t in ts]
    time.sleep(secs)
    # kill inside the next cleanup pass: wait for a "begin" line that has no "end" yet
    t_wait = time.time(); done_before = open(log, errors="replace").read().count("SSTGC end"); in_pass = False
    while time.time() - t_wait < 60 and not in_pass:
        txt = open(log, errors="replace").read()
        in_pass = txt.rfind("SSTGC begin") > txt.rfind("SSTGC end") and txt.count("SSTGC end") >= done_before
        time.sleep(0.002)
    p.send_signal(signal.SIGKILL); p.wait()
    txt = open(log, errors="replace").read()
    killed_in_pass = txt.rfind("SSTGC begin") > txt.rfind("SSTGC end")
    stop.set(); [t.join() for t in ts]
    passes = txt.count("SSTGC end"); deleted = sum(int(l.split("deleted_ssts=")[1].split()[0]) for l in txt.splitlines() if "SSTGC end" in l)
    mdel = sum(int(l.split("deleted_manifests=")[1].split()[0]) for l in txt.splitlines() if "SSTGC end" in l)
    with lock: final = dict(acked)
    p2 = start([], os.path.join(OUT, f"t{n}-recover.log"))
    c2 = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=120000, socketTimeoutMS=600000)
    try:
        present = {d["_id"]: d.get("v", 0) for d in c2[db].c.find({}, {"v": 1})}
        read_err = None
    except Exception as e:
        present, read_err = {}, repr(e)[:200]
    p2.terminate(); p2.wait(60)
    lost = [k for k in final if k not in present]
    stale = [k for k, v in final.items() if k in present and present[k] < v]
    return {"killed_inside_cleanup_pass": killed_in_pass, "cleanup_passes": passes, "ssts_deleted": deleted, "manifests_deleted": mdel, "acked_docs": len(final),
            "acked_updates": sum(1 for v in final.values() if v), "lost_inserts": len(lost), "lost_updates": len(stale), "read_violations": dict(viol), "recovery_read_error": read_err,
            "control_deleted_referenced": "SSTGC CONTROL" in txt}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=4); ap.add_argument("--secs", type=float, default=100)
    ap.add_argument("--writers", type=int, default=12); ap.add_argument("--readers", type=int, default=8); ap.add_argument("--control", action="store_true")
    a = ap.parse_args()
    rs = []
    for n in range(a.trials):
        r = trial(n, a.secs, a.writers, a.readers, a.control); rs.append(r); print(n, r, flush=True)
    tot = {"trials": len(rs), "killed_inside_cleanup_pass": sum(r["killed_inside_cleanup_pass"] for r in rs), "ssts_deleted": sum(r["ssts_deleted"] for r in rs),
           "acked_docs": sum(r["acked_docs"] for r in rs), "lost_inserts": sum(r["lost_inserts"] for r in rs), "lost_updates": sum(r["lost_updates"] for r in rs),
           "read_violations": {k: sum(r["read_violations"][k] for r in rs) for k in rs[0]["read_violations"]}}
    print("TOTAL", "control" if a.control else "safe", tot)
    json.dump({"control": a.control, "trials": rs, "total": tot}, open(os.path.join(RES, f"open-gc-{'control' if a.control else 'safe'}.json"), "w"), indent=1)


main()
