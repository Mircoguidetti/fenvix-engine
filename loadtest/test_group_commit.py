#!/usr/bin/env python3
"""No acknowledged write is lost across SIGKILL, with concurrent writers sharing WAL objects (group commit).

N threads insert unique documents (and update them) against one tenant; the server is SIGKILLed at a random moment;
after a restart every document whose insert (or update) was acknowledged must be present with the acknowledged
value. Also reports WAL PUTs per acknowledged write before the kill, to show that writes did share objects.
Negative control: --ack-before-durable acknowledges before the flush, and the same check must find lost writes.

    test_group_commit.py [--store s3|local] [--trials 6] [--threads 32] [--binary PATH] [--control]
"""
import argparse, json, os, random, signal, subprocess, threading, time
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "loadtest", "out", "gc"); RES = os.path.join(ROOT, "loadtest", "results", "gc")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PORT = 28021


def start(binary, store, flags):
    import socket
    with socket.socket() as so:
        if so.connect_ex(("127.0.0.1", PORT)) == 0:
            raise RuntimeError(f"port {PORT} is in use (stale server?)")
    args = [binary, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "D", "--lease-ttl-secs", "30", "--endpoint", EP,
            "--store", store, "--local-dir", "/tmp/gcdur-store"] + flags + (["--no-leases"] if store != "s3" else [])
    p = subprocess.Popen(args, stdout=open(os.path.join(OUT, f"dur-{store}.log"), "ab"), stderr=subprocess.STDOUT)
    for _ in range(200):
        try:
            MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200).admin.command("ping")
            return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("no start")


def trial(binary, store, flags, n, threads):
    p = start(binary, store, flags)
    uri = f"mongodb://127.0.0.1:{PORT}"
    db = f"dur{n}_{int(time.time())}"
    c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=3000, maxPoolSize=threads + 2)
    c[db].c.insert_one({"_id": -1})
    s0 = c.admin.command("tenantStats")["putByCategory"]["wal"]
    acked, lock = {}, threading.Lock()  # _id -> last acknowledged value
    stop = threading.Event()

    def worker(w):
        i = 0
        while not stop.is_set():
            _id = w * 1_000_000 + i
            try:
                c[db].c.insert_one({"_id": _id, "v": 0})
                with lock:
                    acked[_id] = 0
                c[db].c.update_one({"_id": _id}, {"$set": {"v": 1}})
                with lock:
                    acked[_id] = 1
            except Exception:
                return
            i += 1

    ts = [threading.Thread(target=worker, args=(w,)) for w in range(threads)]
    [t.start() for t in ts]
    time.sleep(random.uniform(2.5, 4.0))
    wal = c.admin.command("tenantStats")["putByCategory"]["wal"] - s0
    with lock:
        snap = dict(acked)  # acknowledged before the kill
    p.send_signal(signal.SIGKILL); p.wait()
    stop.set(); [t.join() for t in ts]
    with lock:
        # a thread may have been acknowledged between the snapshot and the kill; those count too
        final = dict(acked)
    p = start(binary, store, flags)
    c2 = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=30000)
    present = {d["_id"]: d.get("v", 0) for d in c2[db].c.find({}, {"v": 1})}
    p.terminate(); p.wait(30)
    lost = [k for k, v in final.items() if k not in present]
    stale = [k for k, v in final.items() if k in present and present[k] < v]
    writes = sum(1 + (1 if v == 1 else 0) for v in snap.values())
    return {"acked_ids": len(final), "acked_writes_before_kill": writes, "lost_inserts": len(lost), "lost_updates": len(stale),
            "wal_puts_per_write": round(wal / max(writes, 1), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", default="s3"); ap.add_argument("--trials", type=int, default=6); ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--binary", default=os.path.join(ROOT, "target", "release", "mongo-server"))
    ap.add_argument("--control", action="store_true", help="negative control: acknowledge before durable")
    a = ap.parse_args()
    if a.store == "local":
        subprocess.run("rm -rf /tmp/gcdur-store", shell=True)
    flags = ["--ack-before-durable", "--wal-flush-ms", "1500"] if a.control else []
    rs = []
    for n in range(a.trials):
        r = trial(a.binary, a.store, flags, n, a.threads)
        rs.append(r)
        print(a.store, "control" if a.control else "durable", n, r, flush=True)
    tot = {k: sum(r[k] for r in rs) for k in ("acked_ids", "lost_inserts", "lost_updates")}
    print("TOTAL", a.store, "control" if a.control else "durable", tot)
    json.dump({"store": a.store, "control": a.control, "trials": rs, "total": tot}, open(os.path.join(RES, f"durability-{a.store}-{'control' if a.control else 'durable'}.json"), "w"), indent=1)


main()
