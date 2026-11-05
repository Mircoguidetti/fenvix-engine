#!/bin/bash
# Kitchen (K2, corrected by K2b) · the packages for the machines and the CA of the run (on the runner, no AWS). Used by
# the workflow and by the local test. Variables: C (kitchen folder), PACKAGES (output), CA_DIR, PANTRY, LOAD (1 = this
# run loads), PROGRAM (programs/NAME), LOAD_PLAN (load-plans/NAME.txt), ATLAS_TGZ (atlas_states.sh), BIN (folder with
# mongo-server-x86_64/ and mongo-server-aarch64/, each with mongo-server and motore-commit.txt).
# LOCAL_VENDOR=1 (local test only): Python libraries for this machine's Python instead of Ubuntu 24.04.
set -euo pipefail
mkdir -p "$PACKAGES"
( cd "$C" && grep -v '^#' SOURCES.md5 | md5sum -c --quiet ) || { echo "a code-irlanda file in kitchen/ is not the original (SOURCES.md5)"; exit 1; }
vendor() { # vendor FOLDER CPU PACKAGES...: Python libraries for Ubuntu 24.04 (Python 3.12) of the machine
  local d=$1 cpu=$2; shift 2
  if [ -n "${LOCAL_VENDOR:-}" ]; then python3 -m pip install -q --target "$d/vendor" "$@"; return; fi
  /usr/bin/python3 -m pip install -q --target "$d/vendor" --only-binary=:all: --implementation cp --python-version 3.12 \
    --platform "manylinux_2_28_$cpu" --platform "manylinux_2_17_$cpu" --platform "manylinux2014_$cpu" "$@"
}
# client (x86_64): code-irlanda's files unchanged, flat (programma.py finds the script and its helpers next to itself)
d=$(mktemp -d)
vendor "$d" x86_64 'pymongo>=4.6,<5' backports.zstd boto3
cp "$C"/irlanda/prova_infinite.py "$C"/irlanda/programma.py "$C"/irlanda/simula_programma.py "$C"/irlanda/somma_massimo.py \
  "$C"/irlanda/controlla_parallelo.py "$C"/irlanda/cpu_macchina.py "$d"/
cp "$C"/client.sh "$C"/s3io.py "$C"/wait_db.py "$C"/load.py "$C"/load_until.py "$C"/merge_states.py "$C"/compare_plans.py "$C"/settle.py "$d"/
cp "$C/programs/$PROGRAM/passi.txt" "$d"/passi.txt
cp "$C/load-plans/$LOAD_PLAN.txt" "$d"/load-plan.txt
cp "$ATLAS_TGZ" "$d"/atlas-states.tar.gz
( cd "$d" && md5sum prova_infinite.py programma.py simula_programma.py somma_massimo.py controlla_parallelo.py cpu_macchina.py passi.txt ) > "$d"/vendored.md5
bash "$C"/ci/certs.sh ca "$CA_DIR"; cp "$CA_DIR"/ca.pem "$d"/
{ echo "PANTRY=$PANTRY"; echo "LOAD=$LOAD"; echo "PROGRAM=$PROGRAM"; } > "$d"/client.env
tar czf "$PACKAGES"/client.tar.gz -C "$d" .
echo "client: script md5 $(md5sum "$d"/prova_infinite.py | cut -d' ' -f1), program $PROGRAM (passi.txt md5 $(md5sum "$d"/passi.txt | cut -d' ' -f1)), load plan $LOAD_PLAN, loads in this run: $LOAD"
# server: one per compiled CPU
for cpu in x86_64 aarch64; do
  b=$BIN/mongo-server-$cpu
  [ -f "$b/mongo-server" ] || continue
  d=$(mktemp -d)
  vendor "$d" "$cpu" 'pymongo>=4.6,<5' boto3
  cp "$b"/mongo-server "$b"/motore-commit.txt "$C"/server.sh "$C"/s3io.py "$C"/create_user.py "$C"/s3_counters.py \
    "$C"/sst_count.py "$C"/irlanda/cpu_macchina.py "$C"/server-options.txt "$d"/
  chmod +x "$d"/mongo-server "$d"/server.sh
  echo "PANTRY=$PANTRY" > "$d"/server.env
  tar czf "$PACKAGES"/server-$cpu.tar.gz -C "$d" .
  echo "server $cpu: engine $(cat "$d"/motore-commit.txt), sha256 $(sha256sum "$d"/mongo-server | cut -d' ' -f1)"
done
ls -la "$PACKAGES"
