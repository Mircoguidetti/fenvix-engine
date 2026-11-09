"""Step 9 · program "log-ack": how long the 3-zone log takes to confirm a write, on the real network (log nodes in
eu-west-1a, b and c; the cook and this client in a).
Two measures:
  engine  single inserts with w: "majority" through the measured cook (port 27017): the latency a customer sees,
          our server + one round of the log (leader, then 2 of 3 copies fsynced). One writer, then CONC writers at once.
          Database s9_logack (not one of the measured databases; dropped at the end).
  raw     the log alone, without our server: `pq commit` (proto/quaderno bench) from this client to the three log nodes,
          at 1, 8 and 64 commits in flight, 20 s each. Its records belong to databases that no engine owns (random ids of
          the bench), so the cook never replays them; they stay in the log's window until it drops them.
Raw lines in OUT/log-ack.jsonl (engine), OUT/log-ack-raw.txt (pq output); summary in OUT/log-ack-summary.json.
Cost: no S3 request beyond the cook's own uploads; seconds of machine time already paid by the round.
Usage: log_ack.py OUT --uri URI --logs IP:7200,IP:7200,IP:7200 [--n 1000] [--conc 8,64] [--secs 20] [--pq ./pq]"""
import argparse
import json
import os
import subprocess
import threading
import time

from pymongo import MongoClient, WriteConcern

ap = argparse.ArgumentParser()
ap.add_argument("out")
ap.add_argument("--uri", required=True)
ap.add_argument("--logs", default="")
ap.add_argument("--n", type=int, default=1000)
ap.add_argument("--conc", default="8,64")
ap.add_argument("--secs", type=float, default=20)
ap.add_argument("--pq", default="./pq")
ap.add_argument("--doc-bytes", type=int, default=340, help="like the Atlas small documents (340 B)")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
raw = open(os.path.join(a.out, "log-ack.jsonl"), "a")
lock = threading.Lock()


def pct(v, p):
    v = sorted(v)
    return round(v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))], 3) if v else None


cli = MongoClient(a.uri, appname="app-demo", maxPoolSize=200)
coll = cli["s9_logack"].get_collection("w", write_concern=WriteConcern(w="majority", j=True))
pad = "x" * a.doc_bytes
coll.insert_one({"warm": 1, "p": pad})  # creates the database and collection before timing
summary = {"engine": {}, "raw": None}


def run(conc, label, n_each=None, secs=None):
    lat = []
    stop = time.time() + (secs or 1e9)

    def w(t):
        i = 0
        while (n_each is None or i < n_each) and time.time() < stop:
            t0 = time.perf_counter()
            coll.insert_one({"t": t, "i": i, "p": pad})
            ms = (time.perf_counter() - t0) * 1000
            with lock:
                lat.append(ms)
                raw.write(json.dumps({"t": time.time(), "label": label, "thread": t, "ms": round(ms, 3)}) + "\n")
            i += 1

    ths = [threading.Thread(target=w, args=(t,)) for t in range(conc)]
    t0 = time.time()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    el = time.time() - t0
    s = {"conc": conc, "n": len(lat), "p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99), "max": round(max(lat), 3),
         "writes_per_s": round(len(lat) / el, 1)}
    summary["engine"][label] = s
    print(f"engine {label}: {json.dumps(s)}", flush=True)


run(1, "single", n_each=a.n)
for c in [int(x) for x in a.conc.split(",") if x]:
    run(c, f"conc{c}", secs=a.secs)
cli.drop_database("s9_logack")
if a.logs:
    r = subprocess.run([a.pq, "commit", "--replicas", a.logs, "--conc", "1,8,64", "--secs", str(int(a.secs)), "--dbs", "100"],
                       capture_output=True, text=True, timeout=600)
    open(os.path.join(a.out, "log-ack-raw.txt"), "w").write(r.stdout + r.stderr)
    summary["raw"] = {"rc": r.returncode, "last_lines": (r.stdout + r.stderr).strip().splitlines()[-6:]}
    print("raw log:", "\n".join(summary["raw"]["last_lines"]), flush=True)
json.dump(summary, open(os.path.join(a.out, "log-ack-summary.json"), "w"), indent=1)
print("LOG-ACK: DONE")
