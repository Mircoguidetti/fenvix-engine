"""Before/after measurement of sort-with-limit paths (task N5, fix-sort-top1) on 200,000 documents (~120 bytes each),
local store (--store local), 2 cores. Usage: python3 bench.py <mongo-server binary> <label>. Prints one JSON line per case:
median / min / max time over REPS runs, and the largest RSS rise during one run (sampled every 2 ms)."""
import os, random, statistics, subprocess, sys, time, shutil, socket, threading, json
from pymongo import MongoClient, UpdateOne

BIN = sys.argv[1]
LABEL = sys.argv[2]
N = int(os.environ.get("N", "200000"))
BD = os.environ.get("BENCH_DIR", "/tmp/sort-top1")
os.makedirs(BD, exist_ok=True)
REPS = int(os.environ.get("REPS", "5"))

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

d = f"{BD}/store-{LABEL}"
shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
port = free_port()
log = open(f"{BD}/server-{LABEL}.log", "w")
p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir", d, "--no-ttl",
                      "--idle-secs", "3600", "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"], stdout=log, stderr=log)
mc = None
for _ in range(100):
    try:
        mc = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=500)
        mc.admin.command("ping"); break
    except Exception:
        time.sleep(0.2)
c = mc["bench"]["c"]
rnd = random.Random(7)
batch = []
for i in range(N):
    k = rnd.random()
    batch.append({"k": k, "j": k, "g": i % 10, "n": 0, "pad": "x" * 50})
    if len(batch) == 10000:
        c.insert_many(batch); batch = []
if batch: c.insert_many(batch)
c.create_index("j")  # the indexed sort field (same values as k)
print("avg doc bytes", mc["bench"].command("collStats", "c").get("avgObjSize"), flush=True)

def rss():
    for l in open(f"/proc/{p.pid}/status"):
        if l.startswith("VmRSS"): return int(l.split()[1]) // 1024
def peak_reset():
    try: open(f"/proc/{p.pid}/clear_refs", "w").write("5")
    except Exception: pass
def peak():
    for l in open(f"/proc/{p.pid}/status"):
        if l.startswith("VmHWM"): return int(l.split()[1]) // 1024

def measure(name, fn):
    times, peaks = [], []
    fn()  # warm
    for _ in range(REPS):
        time.sleep(0.3)
        base = rss(); top = [base]; stop = [False]
        def sample():
            while not stop[0]:
                top[0] = max(top[0], rss()); time.sleep(0.002)
        th = threading.Thread(target=sample); th.start()
        t = time.perf_counter(); fn(); dt = time.perf_counter() - t
        stop[0] = True; th.join()
        times.append(dt * 1000); peaks.append(top[0] - base)
    r = {"case": name, "ms_median": round(statistics.median(times), 1), "ms_min": round(min(times), 1), "ms_max": round(max(times), 1), "extra_mb_max": max(peaks), "rss_mb": rss()}
    print(json.dumps(r), flush=True)
    return r

def txn(op):
    def run():
        with mc.start_session() as s:
            s.start_transaction()
            c.update_one({"g": 3, "k": {"$lt": 0.5}}, {"$inc": {"n": 1}}, session=s)  # an earlier write of the transaction to the collection
            op(s)
            s.commit_transaction()
    return run

res = []
res.append(measure("updateOne_no_sort", lambda: c.update_one({}, {"$inc": {"n": 1}})))
res.append(measure("updateOne_sort_unindexed", lambda: c.update_one({}, {"$inc": {"n": 1}}, sort={"k": 1})))
res.append(measure("updateOne_sort_indexed", lambda: c.update_one({}, {"$inc": {"n": 1}}, sort={"j": 1})))
res.append(measure("updateOne_sort_id", lambda: c.update_one({}, {"$inc": {"n": 1}}, sort={"_id": 1})))
res.append(measure("updateOne_sort_id_desc", lambda: c.update_one({}, {"$inc": {"n": 1}}, sort={"_id": -1})))
res.append(measure("find_sort_id_limit1", lambda: list(c.find().sort("_id", 1).limit(1))))
res.append(measure("find_sort_unindexed_limit10", lambda: list(c.find({"g": 5}).sort("k", -1).limit(10))))
res.append(measure("findOneAndUpdate_sort_unindexed", lambda: c.find_one_and_update({}, {"$inc": {"n": 1}}, sort=[("k", 1)])))
res.append(measure("txn_updateOne_sort_indexed", txn(lambda s: c.update_one({}, {"$inc": {"n": 1}}, sort={"j": 1}, session=s))))
res.append(measure("txn_updateOne_sort_unindexed", txn(lambda s: c.update_one({}, {"$inc": {"n": 1}}, sort={"k": 1}, session=s))))
res.append(measure("txn_findOneAndUpdate_sort_id", txn(lambda s: c.find_one_and_update({}, {"$inc": {"n": 1}}, sort=[("_id", 1)], session=s))))
json.dump(res, open(f"{BD}/result-{LABEL}.json", "w"), indent=1)
p.terminate(); p.wait(30)
