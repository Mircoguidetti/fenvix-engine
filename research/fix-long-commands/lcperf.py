"""lcperf.py BUCKET BIN LABEL [--load N]: first-batch latency and total time of a full scan (find with default batches,
drained), cold (database closed first) and warm, 3 trials each; sealed runs on (product defaults)."""
import sys, time
sys.path.insert(0, "/home/user/wt-lc/ctl-tests")
import lib
from lib import Server
lib.OUT = "/tmp/claude-0"
from pymongo import MongoClient
bucket, binary, label = sys.argv[1], sys.argv[2], sys.argv[3]
import os
BATCH = os.environ.get("LCPERF_BATCH")
s = Server(bucket, "lcperf", idle=600, binary=binary).start()
try:
    c = MongoClient(s.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=900000)
    if "--load" in sys.argv:
        n = int(sys.argv[sys.argv.index("--load") + 1])
        for b in range(0, n, 10_000):
            c.d.t.insert_many([{"_id": i, "k": i % 7, "pad": "x" * 100} for i in range(b, min(n, b + 10_000))])
        print(f"{label}: loaded {n}")
    for trial in range(3):
        for mode in ("cold", "warm"):
            if mode == "cold":
                c.d.command("tenantAdmin", close=True)
                c.d.command("ping")
            t0 = time.time()
            try:
                cur = c.d.t.find({}).batch_size(int(BATCH)) if BATCH else c.d.t.find({})
                first = next(cur)
                t_first = time.time() - t0
                n = 1 + sum(1 for _ in cur)
                print(f"{label} trial {trial} {mode}: first batch {t_first * 1000:.0f} ms, all {n} in {time.time() - t0:.1f} s", flush=True)
            except Exception as e:
                print(f"{label} trial {trial} {mode}: error after {time.time() - t0:.1f} s: {str(e)[:100]}", flush=True)
finally:
    s.term()
