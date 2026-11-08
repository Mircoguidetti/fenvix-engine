"""Hot point read p50, compaction 2 step 3: today's read path against the merged reader, same machine, same data shape.

For each round, two servers are started one after the other on MinIO (judge environment):
- off: no --sealed-runs (today's path: SlateDB only);
- on:  --sealed-runs N, so the server publishes sealed and delta runs while the data loads; the documents read were all
       written before the last publish, so every read goes through the merged reader and is answered by our runs (the
       server's serverStatus.sealedRuns.rowsFromRuns is checked to grow by at least the number of reads).
Each server loads DOCS documents of about 300 bytes in batches of 100, then reads HOT of them by _id, WARM rounds first
(blocks cached), then READS timed reads in random order. p50 and p99 are of the client round trip (pymongo).
Usage: python -I hot_p50.py <engine root> [rounds]
"""
import json, os, random, statistics, subprocess, sys, time

ROOT = sys.argv[1]
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 3
DOCS, HOT, WARM, READS = 30_000, 2_000, 2, 30_000
PORT = 27031
sys.path.insert(0, os.path.join(ROOT, "mongo-tests"))
from pymongo import MongoClient  # noqa: E402


def start(extra):
    env = dict(os.environ, SERVER_EXTRA=extra)
    env.pop("FENVIX_SEALED_RUNS", None)
    subprocess.run([os.path.join(ROOT, "mongo-tests", "server.sh"), str(PORT), "600"], check=True, env=env, cwd=ROOT, stdout=subprocess.DEVNULL)


def stop():
    pid = open(os.path.join(ROOT, "mongo-tests", "out", f"server-{PORT}.pid")).read().strip()
    subprocess.run(["kill", "-TERM", pid])
    while subprocess.run(["kill", "-0", pid], stderr=subprocess.DEVNULL).returncode == 0:
        time.sleep(0.2)


def one(mode, rnd):
    # T_pin 200 ms: envelopes start (rule 26) early in the load, so the publishes cover it
    extra = "--sealed-runs 60 --sealed-group-mb 4 --sealed-t-pin-ms 200" if mode == "on" else ""
    start(extra)
    c = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true")
    db = c[f"hot{mode}{rnd}{int(time.time())}"]
    col = db.d
    pad = "x" * 260
    for b in range(DOCS // 100):
        col.insert_many([{"_id": b * 100 + i, "u": (b * 100 + i) % 997, "pad": pad} for i in range(100)])
    # a few more write batches so the last publish covers every document read below
    time.sleep(0.5)
    for i in range(70):
        col.update_one({"_id": -1}, {"$inc": {"n": 1}}, upsert=True)
    ids = random.Random(rnd).sample(range(DOCS), HOT)
    s0 = c.admin.command("serverStatus").get("fenvix", {}).get("sealedRuns", {})
    for _ in range(WARM):
        for i in ids:
            assert col.find_one({"_id": i}) is not None
    rng = random.Random(1000 + rnd)
    lat = []
    for _ in range(READS):
        i = rng.choice(ids)
        t = time.perf_counter_ns()
        d = col.find_one({"_id": i})
        lat.append((time.perf_counter_ns() - t) / 1000)
        assert d is not None and d["_id"] == i
    s1 = c.admin.command("serverStatus").get("fenvix", {}).get("sealedRuns", {})
    c.close()
    stop()
    lat.sort()
    from_runs = s1.get("rowsFromRuns", 0) - s0.get("rowsFromRuns", 0)
    return {"mode": mode, "round": rnd, "p50_us": lat[len(lat) // 2], "p99_us": lat[int(len(lat) * 0.99)], "mean_us": statistics.mean(lat),
            "rows_from_runs": from_runs, "publishes": s1.get("publishes", 0), "run_gets_during_reads": s1.get("runGets", 0) - s0.get("runGets", 0)}


out = []
for r in range(ROUNDS):
    for mode in (("off", "on") if r % 2 == 0 else ("on", "off")):
        res = one(mode, r)
        print(json.dumps(res), flush=True)
        out.append(res)
p_off = statistics.median([x["p50_us"] for x in out if x["mode"] == "off"])
p_on = statistics.median([x["p50_us"] for x in out if x["mode"] == "on"])
print(json.dumps({"median_p50_off_us": p_off, "median_p50_on_us": p_on, "ratio_on_over_off": p_on / p_off, "rounds": ROUNDS, "cpu": open("/proc/cpuinfo").read().split("model name")[1].split("\n")[0].strip(": ")}))
