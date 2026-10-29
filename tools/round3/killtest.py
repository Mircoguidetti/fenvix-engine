"""kill -9 in each write mode: how many acknowledged writes are lost.

N databases get RATE inserts/s each (every acknowledged _id is written to a file as soon as the client has the reply);
after SECS seconds the engine is killed with SIGKILL while the load goes on, restarted with the same holder (and the same
local log directory), and every database is read back. Lost = acknowledged ids not found.
usage: killtest.py MODE [--trials 3] [--dbs 4] [--rate 20] [--secs 20]"""
import argparse
import glob
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine as E
import run as R

ap = argparse.ArgumentParser()
ap.add_argument("mode")
ap.add_argument("--trials", type=int, default=3)
ap.add_argument("--dbs", type=int, default=4)
ap.add_argument("--rate", type=float, default=20)
ap.add_argument("--secs", type=float, default=20)
ap.add_argument("--out", default=os.path.join(ROOT, "results-round3", "minio", "kill"))
a = ap.parse_args()
trials = []
for t in range(a.trials):
    name = f"kill-{a.mode}-{t}"
    flags, bin_ = R.mode_flags(a.mode, name)
    bucket = E.make_bucket(f"r3-{name}")
    eng = E.Engine(name, bucket, flags, bin=bin_, holder=f"kill-{a.mode}").start()
    dbs = [f"k{i:02d}" for i in range(a.dbs)]
    E.prefill(eng, dbs, n=10)
    acked = os.path.join(WORK, f"{name}.acked")
    for f in glob.glob(acked + "*"):
        os.remove(f)
    wc = "j0" if a.mode == "relaxed" else "default"
    t_start = time.time() + 2
    load = subprocess.Popen(R.load_cmd(eng, dbs, a.rate, "write", a.secs + 10, os.path.join(WORK, f"{name}-load.json"), wc, t_start, extra=["--acked", acked, "--socket-timeout-ms", "5000"]),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(max(0, t_start + a.secs - time.time()))
    eng.kill9()
    t_kill = time.time()
    load.wait()
    ids = set()
    for f in glob.glob(acked + ".p*"):
        ids |= {l.strip() for l in open(f) if l.strip()}
    t_restart = time.time()
    eng.start()
    t_up = time.time() - t_restart
    c = eng.client()
    have = set()
    for d in dbs:
        for _ in range(60):
            try:
                have |= {x["_id"] for x in c[d].events.find({}, {"_id": 1})}
                break
            except Exception:
                time.sleep(1)
    st = eng.stats()
    eng.stop()
    lost = sorted(ids - have)
    tr = {"trial": t, "acked": len(ids), "lost": len(lost), "lost_sample": lost[:5], "restart_to_port_open_s": round(t_up, 2),
          "replayed": st.get("localLog", {}).get("replayed") if isinstance(st, dict) else None}
    print(json.dumps(tr), flush=True)
    trials.append(tr)
res = {"mode": a.mode, "dbs": a.dbs, "rate_per_db": a.rate, "secs_before_kill": a.secs, "trials": trials,
       "acked_total": sum(t["acked"] for t in trials), "lost_total": sum(t["lost"] for t in trials)}
dump(os.path.join(a.out, f"kill-{a.mode}.json"), res)
print("RESULT", json.dumps({k: res[k] for k in ("mode", "acked_total", "lost_total")}))
