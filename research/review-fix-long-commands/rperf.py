"""rperf.py BUCKET BIN LABEL [--load N]: first-batch and full time of finds, and count / $group, on one server (lease ttl 6 s,
the ctl default; sealed runs as the build's default). Cold = database closed first (tenantAdmin close). 3 trials each.
Documents: {_id: i, k: i % 7, r: i % 1000, pad: 100 x}."""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server
from bson import SON
from pymongo import MongoClient
bucket, binary, label = sys.argv[1], sys.argv[2], sys.argv[3]
s = Server(bucket, "rperf-" + label, idle=600, binary=binary, extra=["--lease-ttl-secs", os.environ.get("RPERF_TTL", "6")]).start()
CASES = [("all-default", {}, None), ("all-b10k", {}, 10000), ("k3", {"k": 3}, None), ("sparse-r0", {"r": 0}, None), ("none", {"r": -1}, None)]
try:
    c = MongoClient(s.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=900000)
    if "--load" in sys.argv:
        n = int(sys.argv[sys.argv.index("--load") + 1])
        for b in range(0, n, 10_000):
            c.d.t.insert_many([{"_id": i, "k": i % 7, "r": i % 1000, "pad": "x" * 100} for i in range(b, min(n, b + 10_000))])
        print(f"{label}: loaded {n}", flush=True)
    for trial in range(3):
        for name, flt, batch in CASES:
            for mode in ("cold", "warm"):
                if mode == "cold":
                    c.d.command("tenantAdmin", close=True)
                    c.d.command("ping")
                t0 = time.time()
                try:
                    cur = c.d.t.find(flt)
                    if batch:
                        cur = cur.batch_size(batch)
                    n = 0
                    t_first = None
                    for _ in cur:
                        if t_first is None:
                            t_first = time.time() - t0
                        n += 1
                    if t_first is None:
                        t_first = time.time() - t0
                    print(f"{label} trial {trial} {name} {mode}: first {t_first * 1000:.0f} ms, all {n} in {time.time() - t0:.2f} s", flush=True)
                except Exception as e:
                    print(f"{label} trial {trial} {name} {mode}: error after {time.time() - t0:.1f} s: {str(e)[:100]}", flush=True)
        for name, fn in (("count", lambda: c.d.t.count_documents({})), ("group", lambda: sum(x["n"] for x in c.d.t.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}])))):
            for mode in ("cold", "warm"):
                if mode == "cold":
                    c.d.command("tenantAdmin", close=True)
                    c.d.command("ping")
                t0 = time.time()
                try:
                    v = fn()
                    print(f"{label} trial {trial} {name} {mode}: {v} in {time.time() - t0:.2f} s", flush=True)
                except Exception as e:
                    print(f"{label} trial {trial} {name} {mode}: error after {time.time() - t0:.1f} s: {str(e)[:100]}", flush=True)
finally:
    s.term()
