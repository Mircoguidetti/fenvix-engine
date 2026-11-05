#!/bin/bash
# Kitchen local test (K2, redone by K2b), $0, no real AWS:
#   - fake AWS: moto (S3, EC2, IAM, SSM) and the fake `aws` (bin/aws): `ec2 run-instances` runs the user-data HERE;
#   - a fake "Atlas": our mongo-server with a local store, loaded with the founder's script like Atlas was (the five
#     databases of m30-corto, stopped at the fractions Atlas reached, the old one whole, the 2 KB collections), then
#     state files like the ones the Atlas round started from (old hot rounds, clients already read) and a CATENA.txt;
#   - code-irlanda's m30-corto program and the load plan, SHRUNK (rimpicciolisci.py: same steps, same phases, small
#     numbers, short pauses); the founder's script unchanged;
#   - run 1 (founder's A2 road 1 mechanics): new KEPT pantry, cook 0 loads and is switched off, then TWO sizes, each on a
#     new cold cook, each from the same state files; run 2: no load, one size reading the kept pantry; then the pantry
#     is dropped. Each run: permissions, Atlas states (CATENA), packages, run.sh, fetch, cleanup, secrets, final check;
#   - BLOCK_TEST=1: run 3 with a spend cap of $0.30, which must stop with BLOCCO_SPESA and leave nothing.
# At the end check_runs.py: fractions, same clients as the fake Atlas in every size, Q2 found in our data, cold minus
# warm per size (C1), SST counts (C6), S3 counters in the SPESA line (point 4).
# Usage: local-test.sh WORK_FOLDER MONGO_SERVER_BINARY
set -u
W=$1; SRV=$2
HERE=$(cd "$(dirname "$0")" && pwd); C=$(cd "$HERE/.." && pwd)
MPORT=${MPORT:-5087}; APORT=${APORT:-27754}
rm -rf "$W"; mkdir -p "$W"; cd "$W" || exit 1
export SANDBOX=$W/sandbox; mkdir -p "$SANDBOX"; cp -r "$HERE/bin" "$HERE/bin-machine" "$SANDBOX/"
export MOTO_URL=http://127.0.0.1:$MPORT AWS_ENDPOINT_URL=http://127.0.0.1:$MPORT
export AWS_DEFAULT_REGION=eu-west-1 AWS_REGION=eu-west-1 AWS_PAGER="" AWS_ACCESS_KEY_ID=fresh AWS_SECRET_ACCESS_KEY=fresh
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export C SPEND_CAP=${SPEND_CAP:-5} MACHINE_ROLE=fenvix-coldread-machine DISK_GB=20 CLIENT=c7i.2xlarge
export CM_ACCT=123456789012 CM_ROLE=arn:aws:iam::123456789012:role/fake
export CA_DIR=$W/ca BIN=$W/bin
CM_PW=$(openssl rand -hex 24); export CM_PW
# for the "machines" (the user-data runs here): no real shutdown, short pauses, the server on 127.0.0.1
export SETTLE_STEP_S=5 SETTLE_MAX_MIN=1
export LOCAL_TEST=1 FAKE_IP=127.0.0.1 FAKE_SERVER_IP=127.0.0.1 DB_PORT=${DB_PORT:-27764} PUSH_S=10 DOWNLOAD_RETRY_S=1
export MINUTE_S=${MINUTE_S:-15} MUTE_MIN=${MUTE_MIN:-40} CEILING_MIN=${CEILING_MIN:-200} LOCAL_VENDOR=1
mkdir -p "$BIN/mongo-server-x86_64" "$BIN/mongo-server-aarch64"
for a in x86_64 aarch64; do cp "$SRV" "$BIN/mongo-server-$a/mongo-server"; echo "local-test $(cd "$C" && git rev-parse HEAD)" > "$BIN/mongo-server-$a/motore-commit.txt"; done
nohup moto_server -p "$MPORT" > moto.log 2>&1 &
MOTO=$!
for _ in $(seq 1 40); do curl -fs "$MOTO_URL/moto-api/" > /dev/null 2>&1 && break; sleep 0.5; done
export PATH="$SANDBOX/bin:$PATH"
aws iam create-role --role-name fenvix-coldread-machine --assume-role-policy-document '{"Version":"2012-10-17","Statement":[]}' > /dev/null
aws iam create-instance-profile --instance-profile-name fenvix-coldread-machine > /dev/null
aws iam add-role-to-instance-profile --instance-profile-name fenvix-coldread-machine --role-name fenvix-coldread-machine
echo 16.0 > "$SANDBOX/vcpu-quota"
T0=$(date +%s)
echo "== 0. program and load plan, shrunk (same steps and phases, small numbers)"
python3 "$C/irlanda/rimpicciolisci.py" "$C/programs" small-programs > /dev/null
python3 - "$C/load-plans/m30-corto.txt" small-load-plan.txt <<'PY'
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
grep -v '^#' small-load-plan.txt
echo "== 1. the fake Atlas: loaded like Atlas (fractions of 2026-11-05), state files like before the m30-corto round"
nohup "$SRV" --listen "127.0.0.1:$APORT" --store local --local-dir "$W/atlas-db" --replica-set rs0 --advertise "127.0.0.1:$APORT" > atlas-db.log 2>&1 &
ADB=$!; sleep 2
mkdir -p atlas-load
python3 - small-load-plan.txt "$C" "$W/atlas-load" "mongodb://127.0.0.1:$APORT/?replicaSet=rs0" <<'PY'
# like the Atlas load: one process per database, stopped at the fraction Atlas reached on 2026-11-05 (MEASURED from
# its state files), the old database whole
import os, shlex, shutil, subprocess, sys, threading
plan, C, W, uri = sys.argv[1:5]
FR = {"m10s": 0.43914, "m30s": 0.43712, "m10p": 0.29546, "m30p": 0.29672, "vecchio": 1.0}
steps = {}
for r in open(plan):
    r = r.split("#", 1)[0].strip()
    if r:
        p = shlex.split(r); steps.setdefault(p[0], []).append(p[1:])
def go(s):
    d = os.path.join(W, s); os.makedirs(d, exist_ok=True)
    shutil.copy(os.path.join(C, "irlanda", "prova_infinite.py"), d); shutil.copy(os.path.join(C, "load_until.py"), d)
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
python3 - "$W/atlas-load" "$W/atlas-states" <<'PY'
# the state files as Atlas had them before m30-corto: the fake loads' reason like Atlas ("writes refused"), the old
# database with its two hot rounds of 2026-11-04 and most of its clients already read (in 'usati' and in the register)
import json, os, random, shutil, sys
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
    print(f"fake Atlas {s:8s} {d['db']}: {d['clienti']['n']} clients, {len(d['clienti']['riservati'])} dormant, loaded {100 * frac:.2f}%, already read {len(d.get('usati', []))}")
PY
( cd atlas-states && find . -type f ! -name CATENA.txt | sort | xargs md5sum ) > atlas-states/CATENA.txt
kill $ADB 2>/dev/null
mkdir -p atlas-dry && cp -r atlas-states/. atlas-dry/ && rm atlas-dry/CATENA.txt
PYTHONPATH="" python3 "$C/irlanda/simula_programma.py" atlas-dry small-programs/m30-corto/passi.txt > atlas-dry.txt 2>&1
echo "dry run of the shrunk m30-corto on the fake Atlas states: $(tail -1 atlas-dry.txt)"
export ATLAS_STATES_DIR=$W/atlas-states PROGRAM=m30-corto LOAD_PLAN=m30-corto
STATES="m10p m10s m30p m30s vecchio"
run_once() { # run_once NAME MODE PANTRY LOAD LOADER SIZES
  local name=$1
  export PANTRY_MODE=$2 PANTRY=$3 LOAD=$4 LOADER=$5 SIZES=$6 GITHUB_RUN_ID=$RANDOM
  export BUCKET=fenvix-coldread-cm-$GITHUB_RUN_ID CM_OUT=$W/out-$name GITHUB_STEP_SUMMARY=$W/summary-$name.md
  export GITHUB_ENV=$W/github-$name.env PACKAGES=$W/packages-$name CM_START
  CM_START=$(date -u -d '1 minute ago' +%FT%TZ)
  echo "== run $name: pantry $PANTRY ($PANTRY_MODE), load $LOAD (${LOADER:-no loader}), sizes $SIZES"
  local pc=0; [ "$PANTRY_MODE" != new ] && pc=1
  PANTRY_CHECK=$pc bash "$C/ci/permissions.sh" > "permissions-$name.txt" 2>&1; echo "permissions: code $? $(grep -E "quota|PERMESSI" "permissions-$name.txt" | tr '\n' ' ' | cut -c1-220)"
  # shellcheck disable=SC2086
  bash "$C/ci/atlas_states.sh" local "$W/atlas-$name.tgz" $STATES
  # the shrunk program and load plan in place of the real ones (the real ones are checked by SOURCES.md5 in packages.sh)
  rm -rf "kitchen-$name" && cp -r "$C" "kitchen-$name" && cp small-programs/m30-corto/passi.txt "kitchen-$name/programs/m30-corto/passi.txt"
  cp small-load-plan.txt "kitchen-$name/load-plans/m30-corto.txt"
  ( cd "kitchen-$name" && { echo "# local test: shrunk program"; ( cd irlanda && md5sum ./*.py | sed 's|  ./|  irlanda/|' ); md5sum programs/m30-corto/*.txt; } > SOURCES.md5 )
  local CI="$C/ci"
  C="$W/kitchen-$name" ATLAS_TGZ="$W/atlas-$name.tgz" bash "$CI/packages.sh" > "packages-$name.txt" 2>&1; echo "packages: code $?; $(grep -E "^(client|server)" "packages-$name.txt" | tr '\n' ' ' | cut -c1-260)"
  local t; t=$(date +%s)
  bash "$C/ci/run.sh" > "run-$name.log" 2>&1; echo "run.sh: code $? in $(( $(date +%s) - t )) s"
  grep -E "^(run:|pantry|cook [0-9]+:|outcome)|BLOCCO" "run-$name.log"; grep "^SPESA" "run-$name.log" | tail -1
  bash "$C/ci/fetch_results.sh"
  DROP_PANTRY=${DROP:-} bash "$C/ci/cleanup.sh" > "cleanup-$name.txt" 2>&1; echo "cleanup: code $?; $(grep -E 'KEPT|RIMASTO|ALLARME|deleted: fenvix-kitchen' "cleanup-$name.txt" | tr '\n' ' ')"
  python3 "$C/ci/check_secrets.py" "$CM_OUT"; echo "secrets: code $?"
  bash "$C/ci/final_check.sh" > "final-$name.txt" 2>&1; echo "final check: code $?; $(tr '\n' ' ' < "final-$name.txt")"
  cat "$CM_OUT/client/steps.log" | grep -E "load finished|MERGE|PLANS|size [0-9]+ \(|outcomes|finish" | cut -c1-200
}
KEEP=fenvix-kitchen-pantry-$RANDOM$RANDOM
run_once 1 keep "$KEEP" 1 c7i.xlarge '["c7i.large","c7g.large"]'
run_once 2 existing "$KEEP" 0 "" '["m7i.large"]'
echo "== mode drop-pantry (cleanup and final check only)"
DROP_PANTRY=$KEEP bash "$C/ci/cleanup.sh" > cleanup-drop.txt 2>&1; echo "cleanup: code $?; $(grep -E 'kept pantry deleted|KEPT|RIMASTO|ALLARME' cleanup-drop.txt | tr '\n' ' ')"
bash "$C/ci/final_check.sh" > final-drop.txt 2>&1; echo "final check: code $?; $(tr '\n' ' ' < final-drop.txt)"
if [ -n "${BLOCK_TEST:-}" ]; then
  SPEND_CAP=0.30 run_once block new "fenvix-coldread-cmd-$RANDOM" 1 c7i.xlarge '["c7i.large"]'
fi
kill $MOTO 2>/dev/null
pgrep -f "mongo-server --listen 0.0.0.0:$DB_PORT" > /dev/null && echo "WARNING: a mongo-server is still alive" || echo "no mongo-server left alive"
echo "== checks"
python3 "$HERE/check_runs.py" "$W" atlas-states out-1 out-2
echo "total time: $(( $(date +%s) - T0 )) s"
