#!/usr/bin/env python3
"""Chunked, resumable run of the full differential suites (pymongo and Node) against the final binary.
The machine restarts every 5 to 20 minutes and kills the servers, so the tests run in slices; a finished slice is kept
in out/final/<driver>_<n>.json and skipped when the script runs again. usage: run_suites.py [chunk=150]"""
import json, os, subprocess, sys, time
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MT = os.path.join(ROOT, "mongo-tests"); OUT = os.path.join(MT, "out", "final"); os.makedirs(OUT, exist_ok=True)
CH = int(sys.argv[1]) if len(sys.argv) > 1 else 150
SUB = 50
n = len(json.load(open(os.path.join(MT, "tests.json"))))
REAL, OURS, IDLE = "mongodb://localhost:27020/?directConnection=true", "mongodb://localhost:27018/?directConnection=true", "mongodb://localhost:27019/?directConnection=true"
env = dict(os.environ, SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%")
def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, cwd=MT, env=env, **kw)
for driver in ("py", "node"):
    for i in range(0, (n + CH - 1) // CH):
        f = os.path.join(OUT, f"{driver}_{i:03d}.json")
        if os.path.exists(f):
            continue
        if sh(os.path.join(ROOT, "tools/final/env_up.sh"), stdout=subprocess.DEVNULL).returncode != 0:
            sys.exit("environment not up")
        sh("./server.sh 27018 60 > /dev/null; ./server.sh 27019 3 > /dev/null")
        # a chunk runs in sub-slices of SUB tests; a finished sub-slice is kept (the machine can restart inside a long chunk)
        t = time.time(); parts = []; lo, hi = i * CH, min(n, (i + 1) * CH)
        for a in range(lo, hi, SUB):
            sl = f"{a}:{min(hi, a + SUB)}"; pf = os.path.join(OUT, f"{driver}_{i:03d}_{a}.part")
            if not os.path.exists(pf):
                sh("./server.sh 27018 60 > /dev/null; ./server.sh 27019 3 > /dev/null")
                if driver == "py":
                    sh(f".venv/bin/python run_py.py --real '{REAL}' --ours '{OURS}' --idle '{IDLE}' --slice {sl} --out {pf}.tmp > {pf}.log 2>&1")
                else:
                    sh(f"cd node && node run_node.js '--real={REAL}' '--ours={OURS}' '--idle={IDLE}' '--slice={sl}' '--out={pf}.tmp' > {pf}.log 2>&1")
                if not os.path.exists(pf + ".tmp"):
                    print(driver, i, sl, "NO RESULT (interrupted?)", flush=True)
                    sys.exit(2)
                os.rename(pf + ".tmp", pf)
            parts.append(pf)
        r = [x for pf in parts for x in json.load(open(pf))]
        json.dump(r, open(f + ".tmp", "w")); os.rename(f + ".tmp", f)
        print(driver, i, f"{lo}:{hi}", "pass", sum(x["status"] == "pass" for x in r), "/", len(r), "%.0fs" % (time.time() - t), flush=True)
print("ALL SLICES DONE")
