"""Reviewer's hot read latency through slices (review-c2-step6). Three servers of the same binary at once, the same data
on a customer-keyed collection (--customer-key d=u): off (no sealed runs: today's path), whole (--sealed-runs,
--sealed-slices off: step 3's counter), area (--sealed-runs, slices by area, the step 6 default). Timed reads alternate
between the servers in blocks of 500. Query shapes: id = find_one({_id}) (locator, then the document); uid =
find_one({u, _id}) (the document key); page = find({u}).sort(_id, -1).limit(5) (a reverse area read).
Usage: python hot_slices.py <engine root> [rounds]"""
import json, os, random, statistics, subprocess, sys, time
ROOT = sys.argv[1]; ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 3
DOCS, HOT, READS, BLK = 30_000, 2_000, 12_000, 500
sys.path.insert(0, os.path.join(ROOT, "mongo-tests"))
from pymongo import MongoClient
SERVERS = {"off": (27041, "--customer-key d=u"),
           "whole": (27042, "--customer-key d=u --sealed-runs 60 --sealed-group-mb 4 --sealed-e1-wait-ms 0 --sealed-slices off"),
           "area": (27043, "--customer-key d=u --sealed-runs 60 --sealed-group-mb 4 --sealed-e1-wait-ms 0 --sealed-slices area")}
def start(port, extra):
    env = dict(os.environ, SERVER_EXTRA=extra); env.pop("FENVIX_SEALED_RUNS", None)
    subprocess.run([os.path.join(ROOT, "mongo-tests", "server.sh"), str(port), "600"], check=True, env=env, cwd=ROOT, stdout=subprocess.DEVNULL)
def stop(port):
    pid = open(os.path.join(ROOT, "mongo-tests", "out", f"server-{port}.pid")).read().strip()
    subprocess.run(["kill", "-TERM", pid])
    while subprocess.run(["kill", "-0", pid], stderr=subprocess.DEVNULL).returncode == 0: time.sleep(0.2)
def rnd(r):
    cols, cl = {}, {}
    for m, (port, extra) in SERVERS.items():
        start(port, extra)
        c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true")
        col = c[f"hs{r}{int(time.time())}"].d
        pad = "x" * 260
        for b in range(DOCS // 100):
            col.insert_many([{"_id": b * 100 + i, "u": (b * 100 + i) % 997, "pad": pad} for i in range(100)])
        time.sleep(0.5)
        # a few more write batches so the last publish covers every document read (inserts: an upsert on this
        # customer-keyed collection took 4 s without sealed runs and 27 s with them, see the review report)
        for i in range(70): col.insert_one({"_id": DOCS + i, "u": 0})
        cols[m], cl[m] = col, c
    ids = random.Random(r).sample(range(DOCS), HOT)
    us = sorted({i % 997 for i in ids})[:300]
    s0 = {m: cl[m].admin.command("serverStatus").get("fenvix", {}).get("sealedRuns", {}) for m in SERVERS}
    shapes = {"id": lambda col, i: col.find_one({"_id": i}),
              "uid": lambda col, i: col.find_one({"u": i % 997, "_id": i}),
              "page": lambda col, i: list(col.find({"u": i % 997}).sort("_id", -1).limit(5))}
    for m in SERVERS:
        for _ in range(2):
            for i in ids:
                for f in shapes.values(): assert f(cols[m], i)
    time.sleep(1.5)  # trims due (1 s)
    for m in SERVERS:
        for i in ids:
            for f in shapes.values(): f(cols[m], i)
    rng = random.Random(1000 + r)
    lat = {(m, s): [] for m in SERVERS for s in shapes}
    order = list(SERVERS)
    for blk in range(len(SERVERS) * READS // BLK):
        m = order[(blk + r) % len(order)]
        for _ in range(BLK):
            i = rng.choice(ids)
            for s, f in shapes.items():
                t = time.perf_counter_ns(); d = f(cols[m], i); lat[(m, s)].append((time.perf_counter_ns() - t) / 1000)
                assert d
    s1 = {m: cl[m].admin.command("serverStatus").get("fenvix", {}).get("sealedRuns", {}) for m in SERVERS}
    for c in cl.values(): c.close()
    for port, _ in SERVERS.values(): stop(port)
    res = {"round": r}
    for (m, s), l in lat.items():
        l.sort(); res[f"p50_{m}_{s}"] = l[len(l) // 2]; res[f"p99_{m}_{s}"] = l[int(len(l) * 0.99)]
    for m in ("whole", "area"):
        res[f"rows_from_runs_{m}"] = s1[m].get("rowsFromRuns", 0) - s0[m].get("rowsFromRuns", 0)
        res[f"run_gets_{m}"] = s1[m].get("runGets", 0) - s0[m].get("runGets", 0)
        res[f"counter_{m}"] = s1[m].get("counter", {})
    for s in shapes:
        res[f"ratio_area_off_{s}"] = res[f"p50_area_{s}"] / res[f"p50_off_{s}"]
        res[f"ratio_area_whole_{s}"] = res[f"p50_area_{s}"] / res[f"p50_whole_{s}"]
        res[f"ratio_whole_off_{s}"] = res[f"p50_whole_{s}"] / res[f"p50_off_{s}"]
    return res
rs = []
for r in range(ROUNDS):
    x = rnd(r); print(json.dumps(x), flush=True); rs.append(x)
summ = {k: statistics.median(x[k] for x in rs) for k in rs[0] if k.startswith("ratio") or k.startswith("p50")}
print(json.dumps({"median": summ, "rounds": ROUNDS}))
