"""Step 9 · the plan of the ballast generator (`mongo-server --ballast PLAN`, mongo-server/src/ballast.rs).
The customers come from the founder's script itself, unchanged and imported (prova_infinite.py, md5 checked): for each
ballast line the same numbers `carica` computes from its arguments, n = max(50, int(gb * 1024 / mb_cliente)) customers,
their sizes and the dormant ones from `prepara_clienti(random.Random(seme), ...)`; and the script's vocabulary with its
zipf weights, its timeline (T_INIZIO, T_SPAN_S) and FINE_DORMIENTI.
Usage: ballast_plan.py SCRIPT_DIR LINES_JSON PLAN_OUT --spill-dir D --out D [--parallel P] [--spill-mb M]
         [--sample-every K --sample-max M]
  LINES_JSON: the ballast lines of the resolved command, [["app_data_b01", "--gb", "24.3", "--seme", "2001", ...], ...]"""
import argparse
import hashlib
import json
import os
import random
import sys

SCRIPT_MD5 = "8396ca455557baaa4e3a9b1f881451f9"

ap = argparse.ArgumentParser()
ap.add_argument("script_dir")
ap.add_argument("lines")
ap.add_argument("plan_out")
ap.add_argument("--spill-dir", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--parallel", type=int, default=1)
ap.add_argument("--spill-mb", type=int, default=256)
ap.add_argument("--sample-every", type=int, default=0)
ap.add_argument("--sample-max", type=int, default=0)
a = ap.parse_args()

path = os.path.join(a.script_dir, "prova_infinite.py")
md5 = hashlib.md5(open(path, "rb").read()).hexdigest()
if md5 != SCRIPT_MD5:
    sys.exit(f"prova_infinite.py md5 {md5}, expected {SCRIPT_MD5}: the script must be the founder's, unchanged")
sys.path.insert(0, a.script_dir)
import prova_infinite as p  # noqa: E402  (the script runs nothing on import: its main() is behind __name__)


def arg(line, name, default):
    return type(default)(line[line.index(name) + 1]) if name in line else default


lines = json.load(open(a.lines))
dbs = []
for k, line in enumerate(lines):
    db = line[0]
    gb = arg(line, "--gb", 30.0)
    mb = arg(line, "--mb-cliente", 2.0)
    seme = arg(line, "--seme", 42)
    quota = arg(line, "--quota-riservati", 0.30)
    # exactly what cmd_carica does before it loads: rng = random.Random(a.seme); n = ...; prepara_clienti(rng, ...)
    rng = random.Random(seme)
    n = max(50, int(gb * 1024 / mb))
    taglie, riservati = p.prepara_clienti(rng, n, mb, quota)
    dbs.append({"db": db, "seed": seme, "gb": gb, "mb_cliente": mb, "quota_riservati": quota, "taglie": taglie,
                "riservati": sorted(riservati), "tag": (k % 250) + 1, "line": line})
plan = {"databases": dbs, "parallel": a.parallel, "spill_dir": a.spill_dir, "spill_mb": a.spill_mb, "out": a.out,
        "vocab": p.VOCAB, "cum_vocab": p.CUM_VOCAB, "t_inizio": int(p.T_INIZIO.timestamp()), "t_span_s": p.T_SPAN_S,
        "fine_dormienti": p.FINE_DORMIENTI, "sample_every": a.sample_every, "sample_max": a.sample_max,
        "script": {"versione": p.VERSIONE, "md5": md5}}
json.dump(plan, open(a.plan_out, "w"))
print(f"ballast plan: {len(dbs)} databases, {sum(len(d['taglie']) for d in dbs)} customers, "
      f"{sum(d['gb'] for d in dbs):.1f} GiB of BSON")
