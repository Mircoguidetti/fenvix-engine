#!/bin/bash
# Step 9 local mode · a fake "Atlas" and a shrunk kitchen, exactly as kitchen/local-test/local-test.sh makes them (the
# code below is that file's steps 0 and 1, moved here so step 9 can use them):
#   - code-irlanda's m30-corto program and load plan, SHRUNK (rimpicciolisci.py: same steps and phases, small numbers);
#   - a fake Atlas: our mongo-server on a local directory, loaded with the founder's script unchanged like Atlas was
#     (the five databases, stopped at the fractions Atlas reached on 2026-11-05, the old one whole, the 2 KB
#     collections), then state files like the ones the Atlas round started from, and their CATENA.txt;
#   - a copy of kitchen/ with the shrunk program and plan in place and its SOURCES.md5 redone for them.
# Usage: fake_atlas.sh WORK MONGO_SERVER PYTHON   -> WORK/atlas-states/, WORK/kitchen-small/
set -u
W=$1; SRV=$2; PY=$3
HERE=$(cd "$(dirname "$0")" && pwd); K=$(cd "$HERE/../../kitchen" && pwd)
APORT=${APORT:-27754}
mkdir -p "$W"; cd "$W" || exit 1
"$PY" "$K/irlanda/prova-locale/rimpicciolisci.py" "$K/programs" small-programs > /dev/null
"$PY" - "$K/load-plans/m30-corto.txt" small-load-plan.txt <<'PY'
import re, sys
out = []
for r in open(sys.argv[1]):
    if r.strip() and not r.lstrip().startswith("#"):
        r = re.sub(r"--gb \S+", "--gb 0.1 --mb-cliente 0.5", r)
        r = re.sub(r"--thread \d+", "--thread 2", r)
        r = re.sub(r"--grandi-gib \S+", "--grandi-gib 0.004", r)
        r = re.sub(r"--grandi-clienti \d+", "--grandi-clienti 80", r)
        r = re.sub(r"--grandi-thread \d+", "--grandi-thread 2", r)
        if " giro " in r and "--grandi-gib" not in r:
            r = r.rstrip("\n") + " --grandi-gib 0.004 --grandi-clienti 80 --grandi-thread 2\n"
        r = r.rstrip("\n") + " --lotto 200\n"
    out.append(r)
open(sys.argv[2], "w").write("".join(out))
PY
rm -rf atlas-db atlas-load atlas-states
nohup "$SRV" --listen "127.0.0.1:$APORT" --store local --local-dir "$W/atlas-db" --replica-set rs0 --advertise "127.0.0.1:$APORT" > atlas-db.log 2>&1 &
ADB=$!; sleep 2
mkdir -p atlas-load
"$PY" - small-load-plan.txt "$K" "$W/atlas-load" "mongodb://127.0.0.1:$APORT/?replicaSet=rs0" <<'PY'
import os, shlex, shutil, subprocess, sys, threading
plan, K, W, uri = sys.argv[1:5]
FR = {"m10s": 0.43914, "m30s": 0.43712, "m10p": 0.29546, "m30p": 0.29672, "vecchio": 1.0}
steps = {}
for r in open(plan):
    r = r.split("#", 1)[0].strip()
    if r:
        p = shlex.split(r); steps.setdefault(p[0], []).append(p[1:])
def go(s):
    d = os.path.join(W, s); os.makedirs(d, exist_ok=True)
    shutil.copy(os.path.join(K, "irlanda", "prova_infinite.py"), d); shutil.copy(os.path.join(K, "load_until.py"), d)
    env = dict(os.environ, FENVIX_P1_URI=uri)
    with open(os.path.join(d, "load.txt"), "w") as t:
        for st in steps[s]:
            if st[0] == "carica":
                cmd = [sys.executable, "load_until.py", str(FR[s]), "carica", "--file", "risultati_p1.json"] + st[1:]
            else:
                cmd = [sys.executable, "prova_infinite.py", "giro", "--file", "risultati_p1.json", "--cartella-dati", "dati"] + st[1:]
            subprocess.run(cmd, cwd=d, env=env, stdout=t, stderr=subprocess.STDOUT)
ths = [threading.Thread(target=go, args=(s,)) for s in steps]
[t.start() for t in ths]; [t.join() for t in ths]
PY
"$PY" - "$W/atlas-load" "$W/atlas-states" <<'PY'
import json, os, random, sys
src, dst = sys.argv[1:3]
for s in os.listdir(src):
    f = os.path.join(src, s, "risultati_p1.json")
    if not os.path.exists(f):
        continue
    d = json.load(open(f))
    car = d["caricamento"]
    if s != "vecchio":
        car["motivo_fine"] = "scritture rifiutate: Replica set writes blocked, reason: InsufficientDiskSpace"
    else:
        car["motivo_fine"] = "obiettivo raggiunto"
        d["giri"] = [{"nome": "0h"}, {"nome": "2h"}]
        cl = d["clienti"]; ris = set(cl["riservati"]); rng = random.Random(7)
        att = [i for i in range(cl["n"]) if i not in ris]; dor = sorted(ris)
        rng.shuffle(att); rng.shuffle(dor)
        d["usati"] = sorted(att[: len(att) // 3] + dor[: len(dor) // 3])
        log = att[len(att) // 3: len(att) // 3 + 10]
    os.makedirs(os.path.join(dst, s), exist_ok=True)
    json.dump(d, open(os.path.join(dst, s, "risultati_p1.json"), "w"), indent=1)
    with open(os.path.join(dst, s, "risultati_p1.json.usati.log"), "w") as o:
        if s == "vecchio":
            o.write("".join(f"a {i}\n" for i in log))
    frac = car["byte_bson"] / (d["config"]["gb"] * (1 << 30))
    print(f"fake Atlas {s:8s} {d['db']}: {d['clienti']['n']} clients, {len(d['clienti']['riservati'])} dormant, loaded {100 * frac:.2f}%")
PY
( cd atlas-states && find . -type f ! -name CATENA.txt | sort | xargs md5sum ) > atlas-states/CATENA.txt
kill $ADB 2>/dev/null; wait $ADB 2>/dev/null
# the kitchen copy with the shrunk program and load plan (the real ones are checked by SOURCES.md5 on AWS)
rm -rf kitchen-small && cp -r "$K" kitchen-small
cp small-programs/m30-corto/passi.txt kitchen-small/programs/m30-corto/passi.txt
cp small-load-plan.txt kitchen-small/load-plans/m30-corto.txt
( cd kitchen-small && { echo "# local test: shrunk program"; ( cd irlanda && md5sum ./*.py | sed 's|  ./|  irlanda/|' ); md5sum programs/m30-corto/*.txt; } > SOURCES.md5 )
echo "fake Atlas ready: $(ls atlas-states | tr '\n' ' ')"
