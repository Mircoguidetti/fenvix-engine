"""Review of K1 (rev-server-per-script): checks for the M30 short program (carico_massimo and parallelo patterns).

1. Drop race: several "copies" insert batches with many threads into one collection while each copy drops it now and
   then (what the 5 copies of carico_massimo do with the writes collection). Afterwards the storage counters
   (collStats count / size) must equal a recount from the data.
2. Profiler cost under concurrency: N concurrent finds, profiler level 2 against level 0: client latency p50 / p95 and
   the profiler entries found.
Usage: python3 rev_k1_m30_test.py BINARY
"""
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

from bson import ObjectId
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from bson.codec_options import CodecOptions

BIN = sys.argv[1]
FAILS = []
RAW = CodecOptions(document_class=RawBSONDocument)


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what, flush=True)
    if not cond:
        FAILS.append(what)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def start(d):
    port = free_port()
    log = open(os.path.join(d, "server.log"), "a")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir",
                          os.path.join(d, "data")], stdout=log, stderr=log)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    return p, port


def recount(db, coll):
    n, size = 0, 0
    for d in db.get_collection(coll, codec_options=RAW).find({}):
        n += 1
        size += len(d.raw)
    return n, size


def drop_race(port, copies=5, threads=16, seconds=20):
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", maxPoolSize=copies * threads + 10)
    db = c["app_demo"]
    stop = time.time() + seconds
    errs = []
    lock = threading.Lock()

    def worker(k):
        r = random.Random(k)
        while time.time() < stop:
            try:
                if r.random() < 0.01:
                    db.drop_collection("writes_normal")
                else:
                    db["writes_normal"].insert_many(
                        [{"cliente": f"u{r.randrange(10**6):07d}", "ts": time.time(), "txt": "x" * r.randrange(200, 400)}
                         for _ in range(100)], ordered=False)
            except Exception as e:  # noqa: BLE001
                with lock:
                    errs.append(repr(e)[:200])
    ths = [threading.Thread(target=worker, args=(k,)) for k in range(copies * threads)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    st = db.command("collStats", "writes_normal")
    n, size = recount(db, "writes_normal")
    print(f"     drop race: errors {len(errs)} {sorted(set(errs))[:3]}; collStats count {st.get('count')} size "
          f"{st.get('size')}; recount {n} docs {size} bytes", flush=True)
    check(st.get("count") == n, f"drop race: collStats count equals recount ({st.get('count')} vs {n})")
    check(st.get("size") == size, f"drop race: collStats size equals recount ({st.get('size')} vs {size})")
    ds = db.command("dbStats")
    check(ds.get("objects", -1) >= n, f"drop race: dbStats objects counts the collection ({ds.get('objects')})")
    c.close()


def profiler_cost(port, n=100, rounds=10):
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", maxPoolSize=n + 10, minPoolSize=n)
    db = c["app_demo2"]
    db["msgs"].create_index([("cliente", 1), ("ts", -1)])
    for b in range(20):
        db["msgs"].insert_many([{"cliente": f"c{i % 2000:05d}", "ts": b * 10000 + i, "txt": "y" * 300}
                                for i in range(b * 2000, (b + 1) * 2000)])
    out = {}
    for level in (0, 2, 0, 2):
        db.command("profile", 0)
        db.drop_collection("system.profile")
        db.create_collection("system.profile", capped=True, size=64 * 1024 * 1024)
        db.command("profile", level, slowms=0, sampleRate=1.0)
        lat = []
        lk = threading.Lock()
        for r in range(rounds):
            bar = threading.Barrier(n)

            def one(i):
                coll = db.get_collection("msgs", codec_options=RAW)
                bar.wait()
                t0 = time.perf_counter()
                list(coll.find({"cliente": f"c{(r * n + i) % 2000:05d}"}, sort=[("ts", -1)], limit=20, batch_size=20))
                with lk:
                    lat.append((time.perf_counter() - t0) * 1000)
            ths = [threading.Thread(target=one, args=(i,)) for i in range(n)]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
        db.command("profile", 0)
        found = db["system.profile"].count_documents({"op": "query", "ns": "app_demo2.msgs"})
        lat.sort()
        key = f"level{level}"
        out.setdefault(key, []).append((statistics.median(lat), lat[int(0.95 * len(lat))], found))
        print(f"     profiler level {level}: {n} at once x {rounds}: p50 {statistics.median(lat):.1f} ms, p95 "
              f"{lat[int(0.95 * len(lat))]:.1f} ms; entries found {found}", flush=True)
        if level == 2:
            check(found == n * rounds, f"profiler: every concurrent find has its entry ({found} of {n * rounds})")
    c.close()
    return out


def main():
    d = tempfile.mkdtemp(prefix="revk1-")
    p, port = start(d)
    try:
        drop_race(port)
        profiler_cost(port)
    finally:
        p.terminate()
        p.wait(timeout=30)
        shutil.rmtree(d, ignore_errors=True)
    print(f"\n{len(FAILS)} failed")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
