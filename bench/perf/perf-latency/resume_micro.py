"""Times LangGraph's get_tuple on a target, split into its parts: find latest checkpoint, find its writes, and the
client's decoding (serde), against a ping's round trip. Pass 1 reads each sampled thread for the first time since the
server started (cold, like a long gap); pass 2 reads the same threads again (hot).
Usage: resume_micro.py URI N"""
import sys, time, random
from pymongo import MongoClient
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
uri, n = sys.argv[1], int(sys.argv[2])
c = MongoClient(uri); db = c["app_demo"]; cp = db["checkpoints"]; wr = db["checkpoint_writes"]
serde = JsonPlusSerializer()
threads = sorted(cp.distinct("thread_id"))
random.seed(7); sample = random.sample(threads, min(n, len(threads)))
def q(v, p): v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))]
def one_pass(name):
    parts = {"ping": [], "find_latest": [], "find_writes": [], "decode": [], "total_resume": [], "doc_kb": []}
    for t in sample:
        a = time.perf_counter(); c.admin.command("ping"); parts["ping"].append((time.perf_counter() - a) * 1000)
        t0 = time.perf_counter()
        d = next(iter(cp.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)), None)
        t1 = time.perf_counter()
        ws = list(wr.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": d["checkpoint_id"]}))
        t2 = time.perf_counter()
        serde.loads_typed((d["type"], d["checkpoint"])); [serde.loads_typed((w["type"], w["value"])) for w in ws]
        t3 = time.perf_counter()
        for k, v in (("find_latest", t1 - t0), ("find_writes", t2 - t1), ("decode", t3 - t2), ("total_resume", t3 - t0)):
            parts[k].append(v * 1000)
        parts["doc_kb"].append(len(d["checkpoint"]) / 1024)
    print(f"[{name}] n={len(sample)}")
    for k, v in parts.items():
        print(f"  {k:12s} p50={q(v,.5):8.2f} p90={q(v,.9):8.2f} p99={q(v,.99):8.2f}")
one_pass("pass 1: first read since start (cold)")
one_pass("pass 2: same threads again (hot)")
