"""What one operation costs in points (fair.rs), measured from the engine's own per-database accounting, and how much
RAM an open database weighs.

Part 1, the unit: every workload runs alone in a database of its own, prefilled with 200 documents of about 1 KB; the
metrics of GET /v1/metrics/databases before and after (plus 3 s for the background S3 requests the writes cause) give
points per operation and their parts (CPU, bytes written, bytes read, S3 PUT and GET).
Part 2, RAM: a fresh engine, then N small databases opened one after the other (each: one find_one; or the whole
collection read, so its blocks sit in the cache); the process RSS before and after, divided by N.

usage: units.py [--ops 1000] [--ram-dbs 300] [--out FILE]   (ISO_STORE=s3 with the S3 stand-in on :9000, or local)"""
import argparse
import json
import os
import shutil
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "round3"))
sys.path.insert(0, HERE)
from common import *  # noqa
import engine as E
import run as R
from noisy_quota import Engine, STORE, LOCAL_DIR  # noqa

PAD = "x" * 900


def metrics(eng, db):
    return eng.admin("GET", "/v1/metrics/databases")[1]["databases"].get(db, {})


def delta(a, b, keys):
    return {k: (b.get(k) or 0) - (a.get(k) or 0) for k in keys}


KEYS = ["commands", "points", "pointsCpu", "pointsWritten", "pointsRead", "pointsS3", "cpuUs", "bytesWritten", "bytesRead", "s3Put", "s3Get"]


def workload(eng, name, ops, fn, threads=1):
    c = eng.client()
    db = f"u_{name}"
    c[db].items.insert_many([{"_id": i, "k": i % 50, "pad": PAD} for i in range(200)])
    if name.startswith("find_k"):
        c[db].items.create_index("k")
    c[db].items.find_one({"_id": 0})
    time.sleep(3)
    m0 = metrics(eng, db)
    t0 = time.time()
    per = ops // threads

    def run(k):
        cc = eng.client()
        for i in range(per):
            fn(cc[db], k * per + i)
    ts = [threading.Thread(target=run, args=(k,)) for k in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    dt = time.time() - t0
    time.sleep(3)
    m1 = metrics(eng, db)
    d = delta(m0, m1, KEYS)
    n = max(1, d["commands"])
    r = {"workload": name, "threads": threads, "ops": per * threads, "commands": d["commands"], "secs": round(dt, 2)}
    for k in KEYS[1:]:
        r[k + "PerOp"] = round(d[k] / n, 3)
    print(json.dumps(r), flush=True)
    return r


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def ram(bucket_base, flags, n, read_all):
    """RSS per open database: create n databases, restart, open them one by one."""
    i = flags.index("--idle-secs")
    flags = flags[:i] + flags[i + 2:]
    tag = "ramfull" if read_all else "ramsmall"
    bucket = f"{bucket_base}-{tag}"
    if STORE == "local":
        shutil.rmtree(os.path.join(LOCAL_DIR, bucket), ignore_errors=True)
    else:
        E.make_bucket(bucket)
    eng = Engine(f"units-{tag}-fill", bucket, flags + ["--idle-secs", "3600"])
    eng.pin = True
    eng.start()
    c = eng.client()
    from concurrent.futures import ThreadPoolExecutor

    def fill(i):
        d = c[f"r{i:04d}"]
        d.items.insert_many([{"_id": j, "k": j % 50, "pad": PAD} for j in range(200)])
        d.items.create_index("k")
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(fill, range(n)))
    eng.stop()
    eng = Engine(f"units-{tag}", bucket, flags + ["--idle-secs", "3600"])
    eng.pin = True
    eng.start()
    c = eng.client()
    c.admin.command("ping")
    time.sleep(2)
    base = rss_kb(eng.pid)
    marks = []
    for i in range(n):
        d = c[f"r{i:04d}"]
        if read_all:
            list(d.items.find({}))
        else:
            d.items.find_one({"_id": 1})
        if (i + 1) % max(1, n // 5) == 0:
            time.sleep(1)
            marks.append((i + 1, rss_kb(eng.pid)))
    time.sleep(3)
    after = rss_kb(eng.pid)
    st = eng.admin("GET", "/v1/metrics/databases")[1]
    open_n = sum(1 for v in st["databases"].values() if v.get("state") in ("reader", "writer", "busy"))
    eng.stop()
    r = {"ram": tag, "databases": n, "open_reported": open_n, "rss_base_kb": base, "rss_after_kb": after,
         "kb_per_open_db": round((after - base) / n, 1), "marks": marks}
    print(json.dumps(r), flush=True)
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=1000)
    ap.add_argument("--ram-dbs", type=int, default=300)
    ap.add_argument("--skip-ram", action="store_true")
    ap.add_argument("--skip-workloads", action="store_true")
    ap.add_argument("--out", default=os.path.join(ROOT, "results-isolamento", f"units-{STORE}.json"))
    a = ap.parse_args()
    E.ENGINE_CPUS = os.environ.get("ISO_ENGINE_CPUS", "0")
    flags, _ = R.mode_flags("immediate", "units")
    out = {"store": STORE, "workloads": [], "ram": []}
    bucket = "iso-units"
    if STORE == "local":
        shutil.rmtree(os.path.join(LOCAL_DIR, bucket), ignore_errors=True)
    else:
        E.make_bucket(bucket)
    if not a.skip_workloads:
        eng = Engine("units", bucket, flags)
        eng.pin = True
        eng.start()
        n = a.ops
        w = out["workloads"]
        w.append(workload(eng, "read_id", n, lambda d, i: d.items.find_one({"_id": i % 200})))
        w.append(workload(eng, "insert_1kb", n, lambda d, i: d.ev.insert_one({"_id": i, "pad": PAD})))
        w.append(workload(eng, "insert_1kb_8conn", n, lambda d, i: d.ev.insert_one({"_id": i, "pad": PAD}), threads=8))
        w.append(workload(eng, "update_id_set", n, lambda d, i: d.items.update_one({"_id": i % 200}, {"$set": {"n": i}})))
        w.append(workload(eng, "update_id_inc_8conn", n, lambda d, i: d.items.update_one({"_id": i % 200}, {"$inc": {"c": 1}}), threads=8))
        w.append(workload(eng, "find_k_index_4docs", n // 4, lambda d, i: list(d.items.find({"k": i % 50}))))
        w.append(workload(eng, "find_scan_200docs", n // 10, lambda d, i: list(d.items.find({"pad": "nope"}))))
        w.append(workload(eng, "aggregate_group_200docs", n // 10, lambda d, i: list(d.items.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}]))))
        w.append(workload(eng, "insert_many_100x1kb", n // 20, lambda d, i: d.ev.insert_many([{"_id": i * 1000 + j, "pad": PAD} for j in range(100)])))
        w.append(workload(eng, "where_js_200docs", n // 20, lambda d, i: list(d.items.find({"$where": "this.k == 3"}))))
        eng.stop()
    dump(a.out, out)
    if not a.skip_ram:
        out["ram"].append(ram("iso-ram", flags, a.ram_dbs, False))
        out["ram"].append(ram("iso-ram", flags, a.ram_dbs, True))
    dump(a.out, out)


if __name__ == "__main__":
    main()
