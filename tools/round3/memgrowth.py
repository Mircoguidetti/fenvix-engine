"""Memory growth under writes (the 4.6 KB per 1 KB write of results-density.md 4.2): N databases, RATE inserts/s each of ~1 KB,
RSS every 10 s. usage: memgrowth.py TAG SECS [--n 20] [--rate 10] [--bin B] [-- engine flags...]"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
from engine import Engine, make_bucket

ap = argparse.ArgumentParser()
ap.add_argument("tag")
ap.add_argument("secs", type=float)
ap.add_argument("--n", type=int, default=20)
ap.add_argument("--rate", type=float, default=10)
ap.add_argument("--bin", default=None)
ap.add_argument("--out", default=None)
ap.add_argument("flags", nargs="*")
a = ap.parse_args()
bucket = make_bucket(f"mem-{a.tag}".lower().replace("_", "-"))
eng = Engine(a.tag, bucket, ["--idle-secs", "600", "--retention-window-secs", "604800"] + a.flags, bin=a.bin).start()
dbs = [f"m{i:03d}" for i in range(a.n)]
c = eng.client()
for d in dbs:
    c[d].items.insert_one({"_id": 0})
samples = []
t0 = time.time()
load = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(__file__), "load.py"), "--port", str(eng.port), "--dbs", ",".join(dbs),
                         "--rate", str(a.rate), "--mix", "write", "--secs", str(a.secs), "--out", os.path.join(WORK, f"mem-{a.tag}-load.json"),
                         "--procs", "2", "--t-start", str(t0 + 2)])
while load.poll() is None:
    s = proc_stat(eng.pid)
    s["t"] = round(time.time() - t0, 1)
    samples.append(s)
    print(json.dumps(s), flush=True)
    time.sleep(10)
st = eng.stats()
eng.stop()
res = {"tag": a.tag, "flags": a.flags, "n": a.n, "rate": a.rate, "secs": a.secs, "samples": samples,
       "load": json.load(open(os.path.join(WORK, f"mem-{a.tag}-load.json")))}
writes = res["load"]["n_write"]
if len(samples) > 3 and writes:
    res["kb_per_write"] = round((samples[-1]["rss_mb"] - samples[1]["rss_mb"]) * 1024 / (writes * (samples[-1]["t"] - samples[1]["t"]) / a.secs), 2)
print("RESULT", json.dumps({k: res.get(k) for k in ("tag", "kb_per_write")}))
dump(a.out or os.path.join(WORK, f"mem-{a.tag}.json"), res)
