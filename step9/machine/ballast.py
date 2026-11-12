"""Step 9 · on the filler machine: load the BALLAST that brings the kitchen from the Atlas size (about 27 GB of BSON,
the five m30-corto databases, loaded by the unchanged client) up to 100 GB or 1 TB.
The ballast is made by the founder's script unchanged (prova_infinite.py `carica`), one database per line of the plan,
with the same document shapes as the Atlas databases (messages, events, orders, 2 MB customers, dormant share) and
seeds of its own, PARALLEL lines at once (one Python process uses about one core: PARALLEL = vCPU of the filler - 1).
Each database gets its own state file, so the extras client can run the same read phases on ballast databases
(programs/ballast_rounds.sh): our numbers only, there is nothing like it on Atlas.
Usage: ballast.py PLAN_JSON WORK OUT     (FENVIX_P1_URI in the environment)
  PLAN_JSON: {"parallel": P, "lines": [["app_data_b01", "--gb", "24.5", "--seme", "2001", ...], ...]}
Exit 0 = every line finished ("obiettivo raggiunto")."""
import json
import os
import shutil
import subprocess
import sys
import threading
import time

plan_f, work, out = sys.argv[1:4]
plan = json.load(open(plan_f))
os.makedirs(out, exist_ok=True)
HERE = os.path.dirname(os.path.abspath(__file__))
lines = list(plan["lines"])
lock = threading.Lock()
results = []


def worker():
    while True:
        with lock:
            if not lines:
                return
            args = lines.pop(0)
        db = args[0]
        d = os.path.join(work, db)
        os.makedirs(d, exist_ok=True)
        shutil.copy(os.path.join(HERE, "prova_infinite.py"), d)
        t0 = time.time()
        with open(os.path.join(out, f"{db}.txt"), "w") as t:
            rc = subprocess.run([sys.executable, "prova_infinite.py", "carica", "--file", "risultati_p1.json", "--db", db] + args[1:],
                                cwd=d, stdout=t, stderr=subprocess.STDOUT).returncode
        st = {}
        try:
            st = json.load(open(os.path.join(d, "risultati_p1.json")))
        except Exception:
            pass
        car = st.get("caricamento") or {}
        r = {"db": db, "rc": rc, "seconds": round(time.time() - t0, 1), "bson_gb": round((car.get("byte_bson") or 0) / 1e9, 3),
             "documents": car.get("documenti"), "mb_s": car.get("mb_s"), "reason": car.get("motivo_fine"),
             "ok": rc == 0 and car.get("motivo_fine") == "obiettivo raggiunto"}
        print(json.dumps(r), flush=True)
        with lock:
            results.append(r)


t0 = time.time()
ths = [threading.Thread(target=worker) for _ in range(max(1, int(plan.get("parallel", 1))))]
for t in ths:
    t.start()
for t in ths:
    t.join()
ok = all(r["ok"] for r in results) and len(results) == len(plan["lines"])
total = sum(r["bson_gb"] for r in results)
summary = {"ok": ok, "seconds": round(time.time() - t0, 1), "bson_gb": round(total, 3),
           "mb_s_total": round(total * 1000 / max(1, time.time() - t0), 3), "databases": sorted(results, key=lambda r: r["db"])}
json.dump(summary, open(os.path.join(out, "ballast-summary.json"), "w"), indent=1)
print(f"BALLAST: {'DONE' if ok else 'FAILED'}: {len(results)} databases, {total:.2f} GB of BSON in {summary['seconds']:.0f} s "
      f"({summary['mb_s_total']:.2f} MB/s together)")
sys.exit(0 if ok else 1)
