"""Review of PR #34: the normal commit must not slow down. Writes per second and p99 of (a) single inserts (b) transactions
(insert + commitTransaction) and (c) retryable inserts (lsid + txnNumber, as the drivers send them), against one binary.

usage: python3 tools_bench_commit.py BINARY SECONDS PROCS THREADS
prints one JSON line per phase.
"""
import json
import multiprocessing as mp
import os
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from bson import Binary, Int64
from pymongo import MongoClient

from lib import Server, free_port, make_bucket


def worker(uri, phase, secs, threads, q, seed):
    cl = MongoClient(uri, maxPoolSize=threads + 2)
    db = cl["bench"]
    lat, errors = [], []
    lock = threading.Lock()
    stop = time.time() + secs

    def go(t):
        lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
        i, local, errs = 0, [], []
        while time.time() < stop:
            i += 1
            k = f"{seed}-{t}-{i}"
            s = time.perf_counter()
            try:
                if phase == "insert":
                    db.command({"insert": "c", "documents": [{"_id": k, "v": "x" * 200}]})
                elif phase == "retryable":
                    db.command({"insert": "c", "documents": [{"_id": k, "v": "x" * 200}], "lsid": lsid, "txnNumber": Int64(i)})
                else:
                    if phase == "txn_newsession":
                        lsid = {"id": Binary(uuid.uuid4().bytes, 4)}
                    db.command({"insert": "c", "documents": [{"_id": k, "v": "x" * 200}], "lsid": lsid, "txnNumber": Int64(i), "startTransaction": True, "autocommit": False})
                    cl.admin.command({"commitTransaction": 1, "lsid": lsid, "txnNumber": Int64(i), "autocommit": False})
            except Exception as e:
                errs.append(f"{type(e).__name__}: {str(e)[:140]}")
            else:
                local.append(time.perf_counter() - s)
        with lock:
            lat.extend(local)
            errors.extend(errs)

    ths = [threading.Thread(target=go, args=(t,)) for t in range(threads)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    q.put((lat, errors))


def main():
    binary, secs, procs, threads = sys.argv[1], float(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    port = free_port()
    srv = Server(make_bucket("bench41"), "bench41", port=port, binary=binary, extra=["--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
    try:
        uri = srv.uri
        MongoClient(uri)["bench"].command({"insert": "c", "documents": [{"_id": "warm"}]})
        for phase in (os.environ.get("BENCH_PHASES", "insert,txn,retryable").split(",")):
            q = mp.Queue()
            ps = [mp.Process(target=worker, args=(uri, phase, secs, threads, q, f"{phase}{p}")) for p in range(procs)]
            [p.start() for p in ps]
            lat, errors = [], []
            for _ in ps:
                a, b = q.get()
                lat.extend(a); errors.extend(b)
            [p.join() for p in ps]
            lat.sort()
            pct = lambda x: round(lat[min(len(lat) - 1, int(len(lat) * x))] * 1000, 1)
            print(json.dumps({"binary": binary, "phase": phase, "ops": len(lat), "ops_s": round(len(lat) / secs, 1), "p50_ms": pct(0.5), "p99_ms": pct(0.99), "max_ms": round(lat[-1] * 1000, 1), "errors": len(errors), "error_sample": errors[:2]}), flush=True)
    finally:
        srv.kill9()


if __name__ == "__main__":
    main()
