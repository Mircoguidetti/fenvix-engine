"""Reproduction of "engine stuck after losing the lease" (revisore S7, point 1): the object store stops answering for longer
than the lease (40 s against a 30 s lease) while a client inserts; then, after the store is back, check that the engine
answers tenantStats, accepts writes again and stops on SIGTERM.

    MONGO_SERVER_BIN=target/release/mongo-server python3 tools/lease-stall/repro.py [--ttl 30] [--freeze 40]

Needs MinIO (or another S3) on MINIO_ENDPOINT (default http://127.0.0.1:9000). Prints one JSON line with the results."""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "ctl-tests"))
sys.path.insert(0, HERE)

from pymongo import MongoClient  # noqa: E402
from lib import Server, make_bucket, MINIO  # noqa: E402
from blockproxy import BlockProxy  # noqa: E402


def stats_ok(srv, timeout_s):
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=int(timeout_s * 1000), socketTimeoutMS=int(timeout_s * 1000), retryReads=False)
    t0 = time.time()
    try:
        c.admin.command("tenantStats")
        return True, round(time.time() - t0, 2)
    except Exception as e:
        return False, f"{type(e).__name__}"
    finally:
        c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ttl", type=int, default=30)
    ap.add_argument("--freeze", type=int, default=40)
    ap.add_argument("--after", type=int, default=15, help="seconds to wait after the store is back")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--size", type=int, default=1000)
    a = ap.parse_args()
    host, port = MINIO.split("//")[1].split(":")
    px = BlockProxy((host, int(port)))
    b = make_bucket("lease-stall")
    srv = Server(b, "lstall", idle=600, endpoint=px.endpoint, extra=["--lease-ttl-secs", str(a.ttl)]).start()
    res = {"ttl": a.ttl, "freeze": a.freeze, "log": srv.log_path}
    stop = threading.Event()
    counts = {"ok": 0, "err": 0, "last_err": None}

    lock = threading.Lock()
    nxt = [0]

    def writer():
        c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False, w=1, journal=True)
        while not stop.is_set():
            with lock:
                i = nxt[0]
                nxt[0] += 1
            try:
                c.d.c.insert_many([{"_id": i * a.batch + k, "x": os.urandom(a.size // 2).hex()} for k in range(a.batch)], ordered=False)
                counts["ok"] += 1
            except Exception as e:
                counts["err"] += 1
                counts["last_err"] = str(e)[:160]
                time.sleep(0.5)
            i += 1
        c.close()

    for _ in range(a.threads):
        threading.Thread(target=writer, daemon=True).start()
    time.sleep(5)
    res["batches_before"] = counts["ok"]
    px.freeze()
    time.sleep(a.freeze)
    res["stats_while_frozen"] = stats_ok(srv, 10)
    px.thaw()
    t_thaw = time.time()
    time.sleep(a.after)
    res["stats_after_thaw"] = stats_ok(srv, 10)
    ok_at_thaw = counts["ok"]
    time.sleep(5)
    res["batches_in_5s_after"] = counts["ok"] - ok_at_thaw
    res["last_err"] = counts["last_err"]
    stop.set()
    t0 = time.time()
    srv.proc.send_signal(signal.SIGTERM)
    try:
        srv.proc.wait(30)
        res["sigterm_s"] = round(time.time() - t0, 1)
    except subprocess.TimeoutExpired:
        res["sigterm_s"] = None
        res["stats_after_sigterm"] = stats_ok(srv, 5)
        dump = srv.log_path + ".gdb.txt"
        with open(dump, "w") as f:
            subprocess.run(["gdb", "-p", str(srv.proc.pid), "-batch", "-ex", "thread apply all bt"], stdout=f, stderr=subprocess.STDOUT, timeout=120)
        res["gdb"] = dump
        srv.kill9()
    res["since_thaw_s"] = round(time.time() - t_thaw, 1)
    print(json.dumps(res))


if __name__ == "__main__":
    main()
