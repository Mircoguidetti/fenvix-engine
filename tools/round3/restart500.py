"""Restart with N databases (default 500): every database receives a request in the same second after a clean restart
(the wake storm of a deploy), then 1 request/s each (50% writes) for SECS seconds. Run once per --max-concurrent-wakes
value. Reports the latency of the first request of each database, the p50/p99 of the first minute, errors, open file
descriptors and RSS.
usage: restart500.py [--dbs 500] [--secs 120] [--wakes 0,32] [--reuse-bucket]"""
import argparse
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
ap.add_argument("--dbs", type=int, default=500)
ap.add_argument("--secs", type=float, default=120)
ap.add_argument("--wakes", default="0,32")
ap.add_argument("--bucket", default="r3-restart500")
ap.add_argument("--reuse-bucket", action="store_true")
ap.add_argument("--out", default=os.path.join(ROOT, "results-round3", "minio", "restart500.json"))
a = ap.parse_args()
dbs = [f"r{i:04d}" for i in range(a.dbs)]
flags, _ = R.mode_flags("immediate", "restart500")
if not a.reuse_bucket:
    E.make_bucket(a.bucket)
    eng = E.Engine("r500-setup", a.bucket, flags).start()
    t0 = time.time()
    E.prefill(eng, dbs, parallel=32)
    eng.stop()
    print(f"prefill {a.dbs} databases: {time.time() - t0:.0f} s", flush=True)
res = {"dbs": a.dbs, "secs": a.secs, "runs": []}
for w in [int(x) for x in a.wakes.split(",")]:
    eng = E.Engine(f"r500-w{w}", a.bucket, flags + ["--max-concurrent-wakes", str(w)]).start()
    out = os.path.join(WORK, f"r500-w{w}.json")
    # first request of every database within 0.3 s of each other (rate 1/s with the first gap ~0), then 1/s
    t_start = time.time() + max(10, a.dbs / 40)
    p = subprocess.Popen(R.load_cmd(eng, dbs, 1.0, "half", a.secs, out, "default", t_start) + ["--first-at-start"])
    samples = []
    while p.poll() is None:
        s = proc_stat(eng.pid)
        s["t"] = round(time.time() - t_start, 1)
        samples.append(s)
        time.sleep(2)
    st = eng.stats()
    eng.stop()
    l = json.load(open(out))
    run = {"max_concurrent_wakes": w, "client": l, "fds_max": max(s["fds"] for s in samples), "rss_mb_max": round(max(s["rss_mb"] for s in samples), 1),
           "fds_per_db": round(max(s["fds"] for s in samples) / a.dbs, 2), "samples": samples, "engine_stats_end": st}
    res["runs"].append(run)
    print(json.dumps({"wakes": w, **{k: l.get(k) for k in ("first_p50_ms", "first_p99_ms", "first_max_ms", "minute1_write_p99_ms", "minute1_read_p99_ms", "errors")},
                      "fds_max": run["fds_max"], "rss_mb_max": run["rss_mb_max"]}), flush=True)
dump(a.out, res)
