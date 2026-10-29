"""Diagnostic (NOT one of the scenarios of the brief): where the S3 requests come from. The engine runs with the same
flags MINUS --auth, because `tenantStats` (the engine's own request counters by object category) is refused to every
user under --auth. Clients still use TLS and SCRAM. Variants, each on a fresh engine, N databases, DURATION seconds,
counters taken between t=60 s and the end (after the cold wakes):
  mix        the scenario-1 mix (90% reads / 10% writes), 1 request/s
  reads      reads only (find by _id / by k), 1 request/s
  writes     updates only, 1 request/s
  idle_open  one read every 50 s (open, almost idle: the fixed cost of an open database)
usage: diag.py N DURATION"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa
import engine
from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
VARIANTS = {"mix": ["steady"], "reads": ["steady", "--mix", "read_id=0.5,read_idx=0.5"], "writes": ["steady", "--mix", "update=1"],
            "idle_open": ["period", "--period", "50"]}


def stats(c):
    t = c.admin.command("tenantStats")
    t.pop("lat", None)
    return json.loads(json.dumps(t, default=str))


def sub(a, b):
    if isinstance(a, dict):
        return {k: sub(a.get(k, 0), b[k]) for k in b}
    if isinstance(b, (int, float)) and isinstance(a, (int, float)):
        return b - a
    return b


def main():
    n, dur = int(sys.argv[1]), float(sys.argv[2])
    out = {}
    engine.FLAGS.remove("--auth")
    for name, args in VARIANTS.items():
        tag = f"diag_{name}"
        p = engine.start(tag)
        c = MongoClient("127.0.0.1", PORT, tls=True, tlsCAFile=CA, directConnection=True, serverMonitoringMode="poll")
        mode, rest = args[0], args[1:]
        lp = subprocess.Popen(["taskset", "-c", OTHER_CPUS, PY, os.path.join(HERE, "load.py"), mode, str(n), str(dur),
                               os.path.join(WORK, "runs", tag), "--procs", "1", "--warmup", "15"] + rest,
                              stdout=subprocess.PIPE, text=True)
        t_start = float(lp.stdout.readline().split()[1])
        time.sleep(max(0, t_start + 60 - time.time()))
        a, ta = stats(c), time.time()
        lp.wait()
        b, tb = stats(c), time.time()
        lat = []
        import csv, glob
        for f in glob.glob(os.path.join(WORK, "runs", f"{tag}.p*.csv")):
            lat += [r for r in csv.DictReader(open(f)) if float(r["t"]) >= 60]
        d = sub(a, b)
        out[name] = {"span_s": round(tb - ta, 1), "client_requests": len(lat), "n": n, "open_writers_end": b["openWriters"], "open_readers_end": b["openReaders"],
                     "requests": d["requests"], "putByCategory": {k: v for k, v in d["putByCategory"].items() if v},
                     "getByCategory": {k: v for k, v in d["getByCategory"].items() if v}, "walGc": d["walGc"], "sstGc": d["sstGc"],
                     "leases": d["leases"], "maintenance": d["maintenance"]}
        print(name, json.dumps(out[name]), flush=True)
        c.close()
        engine.stop(p)
    os.makedirs(os.path.join(RESULTS, "diag"), exist_ok=True)
    with open(os.path.join(RESULTS, "diag", "diag_categories.json"), "w") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
