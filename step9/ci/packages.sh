#!/bin/bash
# Step 9 · the packages the machines download (on the runner, no AWS). Same packages in modes aws and local.
#   client.tar.gz        kitchen/ci/packages.sh's client package UNCHANGED in content (the founder's script, code-irlanda's
#                        programma.py and program, the load plan, the Atlas state files, the kitchen client helpers), plus
#                        the step 9 machine scripts and programs for the roles client, filler and extras, and pq (x86_64)
#                        for the raw log-ack measure. client.env gets WAIT_COOK_MIN (a 1 TB ballast load takes hours).
#   node-ARCH.tar.gz     one per CPU: mongo-server and pq built from the engine ref, the kitchen server helpers, the
#                        step 9 machine scripts; for the roles log, loader, cook, spare, compactor, generator.
# Variables: S9 (step9 folder), K (kitchen folder), PACKAGES, CA_DIR, BIN (mongo-server-ARCH/ and pq-ARCH/ folders),
# ATLAS_TGZ, PANTRY (the resolved pantry name), LOAD (1 = the client loads the Atlas databases), WAIT_COOK_MIN.
set -euo pipefail
mkdir -p "$PACKAGES"
vendor() { # vendor FOLDER CPU PACKAGES...: Python libraries for Ubuntu 24.04's Python 3.12 on that CPU
  local d=$1 cpu=$2; shift 2
  /usr/bin/python3 -m pip install -q --target "$d/vendor" --only-binary=:all: --implementation cp --python-version 3.12 \
    --platform "manylinux_2_28_$cpu" --platform "manylinux_2_17_$cpu" --platform "manylinux2014_$cpu" "$@"
}
# 1. the kitchen's client package, built by the kitchen's own script (its SOURCES.md5 check included)
kp=$(mktemp -d)
C="$K" PACKAGES="$kp" CA_DIR="$CA_DIR" PANTRY="$PANTRY" LOAD="$LOAD" PROGRAM=m30-corto LOAD_PLAN=m30-corto ATLAS_TGZ="$ATLAS_TGZ" \
  BIN="$BIN" bash "$K/ci/packages.sh" > "$PACKAGES/kitchen-packages.txt"
grep -E "^client" "$PACKAGES/kitchen-packages.txt"
d=$(mktemp -d); tar xzf "$kp/client.tar.gz" -C "$d"
cp "$S9"/machine/{node.sh,common.sh,cfg.py,filler.sh,ballast.py,extras.sh,mongo_client.sh,pantry_marks.py} "$S9"/programs/*.py "$d"/
cp "$BIN/pq-x86_64/pq" "$d"/pq; chmod +x "$d"/pq "$d"/*.sh
# the agent benchmark (bench/agent-checkpoints) and its libraries, apart from the kitchen's (its own vendor folder)
AB="$S9/../bench/agent-checkpoints"
mkdir -p "$d"/agentbench && cp "$AB"/{bench,generator,graph_runner,targets,cost,delta_hook}.py "$AB"/prices.json "$d"/agentbench/
/usr/bin/python3 -m pip install -q --target "$d/agentbench/vendor" --only-binary=:all: --implementation cp --python-version 3.12 \
  --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 -r "$AB/requirements.txt"
echo "WAIT_COOK_MIN=${WAIT_COOK_MIN:-45}" >> "$d"/client.env
( cd "$d" && md5sum -c --quiet vendored.md5 ) || { echo "client package: a code-irlanda file changed"; exit 1; }
tar czf "$PACKAGES"/client.tar.gz -C "$d" .
echo "client: $(md5sum "$d"/prova_infinite.py | cut -d' ' -f1) prova_infinite.py; step 9 roles client, filler, extras"
# 2. the node packages
for cpu in x86_64 aarch64; do
  [ -f "$BIN/mongo-server-$cpu/mongo-server" ] && [ -f "$BIN/pq-$cpu/pq" ] || { echo "node $cpu: no binaries, skipped"; continue; }
  d=$(mktemp -d)
  vendor "$d" "$cpu" 'pymongo>=4.6,<5' boto3
  cp "$BIN/mongo-server-$cpu"/mongo-server "$BIN/mongo-server-$cpu"/motore-commit.txt "$BIN/pq-$cpu"/pq "$d"/
  cp "$K"/s3io.py "$K"/create_user.py "$K"/s3_counters.py "$K"/sst_count.py "$K"/irlanda/cpu_macchina.py "$d"/
  cp "$S9"/machine/{node.sh,common.sh,cfg.py,log.sh,cook.sh,takeover_agent.py,pgbox.sh,generator.sh,ballast_states.py,mongod.sh} "$d"/
  # the MongoDB line (aws-local-round): mongod 7.0 for the x86 members, from MONGOD_BIN (local: the mongo:7.0 image's
  # binary) or MONGOD_TGZ_URL (the workflow: MongoDB's official tarball, its sha256 printed and kept in the package)
  if [ "$cpu" = x86_64 ] && [ -n "${MONGOD_BIN:-}" ]; then
    cp "$MONGOD_BIN" "$d"/mongod
  elif [ "$cpu" = x86_64 ] && [ -n "${MONGOD_TGZ_URL:-}" ]; then
    mt=$(mktemp -d); curl -fsSL --retry 3 -o "$mt/m.tgz" "$MONGOD_TGZ_URL"
    got=$(sha256sum "$mt/m.tgz" | cut -d' ' -f1); echo "mongod tarball $MONGOD_TGZ_URL sha256 $got"
    [ -z "${MONGOD_TGZ_SHA256:-}" ] || [ "$got" = "$MONGOD_TGZ_SHA256" ] || { echo "mongod tarball: sha256 $got, expected $MONGOD_TGZ_SHA256"; exit 1; }
    tar xzf "$mt/m.tgz" -C "$mt" && cp "$mt"/mongodb-linux-*/bin/mongod "$d"/mongod && echo "$got $MONGOD_TGZ_URL" > "$d"/mongod-source.txt
  fi
  [ -f "$d"/mongod ] && { chmod +x "$d"/mongod; echo "node $cpu: mongod $("$d"/mongod --version 2>/dev/null | head -1)"; }
  chmod +x "$d"/mongo-server "$d"/pq "$d"/*.sh
  tar czf "$PACKAGES/node-$cpu.tar.gz" -C "$d" .
  echo "node $cpu: engine $(cat "$d"/motore-commit.txt), mongo-server sha256 $(sha256sum "$d"/mongo-server | cut -c1-16), pq sha256 $(sha256sum "$d"/pq | cut -c1-16)"
done
ls -la "$PACKAGES"
