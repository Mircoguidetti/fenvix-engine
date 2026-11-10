"""Compaction 2 step 4, fix round: the compactor in the server, checked and timed.

Starts mongo-server on the judge's MinIO the way the kill phase does (s3 store, leases), in one of three modes:
  off        sealed runs off
  step3      --sealed-runs 2 (step 3's offline publishes every 2 write batches)
  compactor  --sealed-runs 2 with FENVIX_SEALED_COMPACTOR=small (the first version by step 3's builder, then one
             compactor per open writer)
Then 15 s of acknowledged single-document inserts from 8 client threads, then: the documents acknowledged, every one
read back (count and a find of each id), and serverStatus' sealed-run and compactor counters.

Run: python3 server_check.py <mode> (from the engine checkout, MinIO up as the judge leaves it)
"""
import json, os, subprocess, sys, threading, time, uuid

for k, v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[k] = v
from pymongo import MongoClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "release", "mongo-server")
PORT = 28077
mode = sys.argv[1]
env = dict(os.environ)
for k in ("FENVIX_SEALED_RUNS", "FENVIX_SEALED_COMPACTOR", "FENVIX_SEALED_TPIN_MS", "FENVIX_SEALED_E1_WAIT_MS"):
    env.pop(k, None)
if mode in ("step3", "compactor"):
    env.update({"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_TPIN_MS": "1000", "FENVIX_SEALED_E1_WAIT_MS": "0"})
if mode == "compactor":
    env["FENVIX_SEALED_COMPACTOR"] = "small"
log = open(os.path.join(os.path.dirname(__file__), f"server-check-{mode}.log"), "w")
p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{PORT}", "--idle-secs", "100000", "--holder", "K", "--lease-ttl-secs", "30", "--endpoint", "http://127.0.0.1:9000", "--store", "s3"], stdout=log, stderr=subprocess.STDOUT, env=env)
try:
    c = None
    for _ in range(300):
        try:
            c = MongoClient(f"mongodb://127.0.0.1:{PORT}", serverSelectionTimeoutMS=200)
            c.admin.command("ping")
            break
        except Exception:
            time.sleep(0.1)
    dbname = "chk_" + uuid.uuid4().hex[:8]
    coll = c[dbname]["msgs"]
    acked = []
    lock = threading.Lock()
    stop = time.time() + 15

    def worker(t):
        cl = MongoClient(f"mongodb://127.0.0.1:{PORT}")
        co = cl[dbname]["msgs"]
        i = 0
        while time.time() < stop:
            _id = f"{t}-{i}"
            co.insert_one({"_id": _id, "user": f"u{i % 50}", "text": "x" * 300})
            with lock:
                acked.append(_id)
            i += 1

    th = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    time.sleep(3)
    n = coll.count_documents({})
    missing = sum(1 for _id in acked if coll.find_one({"_id": _id}, {"_id": 1}) is None)
    st = c.admin.command("serverStatus").get("fenvix", {})
    out = {"mode": mode, "acked": len(acked), "per_sec": round(len(acked) / 15.0, 1), "count": n, "missing": missing, "sealedRuns": st.get("sealedRuns"), "compactor": st.get("compactor")}
    print(json.dumps(out))
finally:
    p.terminate()
    try:
        p.wait(10)
    except Exception:
        p.kill()
