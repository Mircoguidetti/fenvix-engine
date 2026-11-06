#!/usr/bin/env bash
# The judge: runs every existing suite against the current commit and writes one verdict per phase.
# Runs inside any session sandbox (after judge/setup.sh), $0, no docker, no GitHub Actions minutes.
# Usage: judge/run.sh [phase ...]   phases: build unit diff-py diff-node ctl kill   (default: all, in that order)
# Output: judge/out/<commit>/<phase>.log and judge/out/<commit>/VERDICT.md (one line per phase: PASS, FAIL or NOT RUN, with counts).
# A phase FAILs when anything in it fails; there is no "almost". The author and the reviewer run the same script on the same commit.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); cd "$ROOT"
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; ENV="$JH/env"; PY="$ENV/bin/python"
[ -x "$ENV/bin/mongod" ] || { echo "run judge/setup.sh first"; exit 2; }
export PATH="$ENV/bin:$PATH"
SHA=$(git rev-parse --short HEAD); DIRTY=$(git status --porcelain -- mongo-server src Cargo.toml Cargo.lock | head -1)
OUT="$ROOT/judge/out/$SHA"; mkdir -p "$OUT"; V="$OUT/VERDICT.md"
PHASES=${*:-build unit diff-py diff-node ctl kill}
[ -f "$V" ] || { echo "# Judge verdict, commit $SHA${DIRTY:+ (with uncommitted engine changes: not a valid verdict)}"; echo; } > "$V"
verdict() { sed -i "/^- $1:/d" "$V"; echo "- $1: $2" >> "$V"; echo "JUDGE $1: $2"; }

# the reference mongod (replica set rs0, as the suites expect) and MinIO with the bucket "bench"
env_up() {
  mkdir -p "$JH/data/rs" "$JH/data/minio/bench"
  if ! (exec 3<>/dev/tcp/127.0.0.1/27020) 2>/dev/null; then
    mongod --replSet rs0 --port 27020 --bind_ip 127.0.0.1 --dbpath "$JH/data/rs" --fork --logpath "$JH/data/rs.log" > /dev/null
    "$PY" - <<'P'
import pymongo, time
c = pymongo.MongoClient("mongodb://127.0.0.1:27020/?directConnection=true")
try: c.admin.command("replSetInitiate", {"_id": "rs0", "members": [{"_id": 0, "host": "localhost:27020"}]})
except Exception: pass
for _ in range(60):
    if c.admin.command("hello").get("isWritablePrimary"): break
    time.sleep(1)
P
  fi
  if ! (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null; then
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin nohup minio server "$JH/data/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > "$JH/data/minio.log" 2>&1 &
    for _ in $(seq 1 60); do (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null && break; sleep 0.5; done
  fi
  # the test harnesses look for their Python here
  [ -e mongo-tests/.venv ] || ln -s "$ENV" mongo-tests/.venv
}
REAL="mongodb://localhost:27020/?directConnection=true"; OURS="mongodb://localhost:27018/?directConnection=true"; IDLE="mongodb://localhost:27019/?directConnection=true"
servers() { SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%" bash -c './mongo-tests/server.sh 27018 60 >/dev/null && ./mongo-tests/server.sh 27019 3 >/dev/null'; }
export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%"

for p in $PHASES; do
  log="$OUT/$p.log"
  case $p in
    build)
      if cargo build --release -p mongo-server > "$log" 2>&1; then verdict build PASS; else verdict build "FAIL (see build.log)"; exit 1; fi ;;
    unit)
      cargo test --release --workspace > "$log" 2>&1; rc=$?
      sum=$(grep -E "^test result:" "$log" | awk '{p+=$4; f+=$6} END {print p" passed, "f" failed"}')
      [ $rc -eq 0 ] && verdict unit "PASS ($sum)" || verdict unit "FAIL ($sum, see unit.log)" ;;
    diff-py|diff-node)
      env_up; servers; (cd mongo-tests && "$PY" gen_tests.py tests.json > /dev/null)
      if [ $p = diff-py ]; then
        (cd mongo-tests && "$PY" run_py.py --real "$REAL" --ours "$OURS" --idle "$IDLE" --out "$OUT/$p.json") > "$log" 2>&1
      else
        (cd mongo-tests/node && node run_node.js "--real=$REAL" "--ours=$OURS" "--idle=$IDLE" "--out=$OUT/$p.json") > "$log" 2>&1
      fi
      if [ -f "$OUT/$p.json" ]; then
        r=$("$PY" -c "import json,sys;r=json.load(open(sys.argv[1]));n=len(r);ok=sum(x['status']=='pass' for x in r);print(ok,n)" "$OUT/$p.json")
        set -- $r; [ "$1" = "$2" ] && verdict $p "PASS ($1/$2 same answers as mongod 7.0)" || verdict $p "FAIL ($1/$2 same answers as mongod 7.0, see $p.log)"
      else verdict $p "FAIL (no result file, see $p.log)"; fi ;;
    ctl)
      env_up; pass=0; fail=0; bad=""
      for f in ctl-tests/test_*.py; do
        n=$(basename "$f" .py)
        (cd ctl-tests && timeout 1500 "$PY" -m pytest -q -p no:cacheprovider --tb=short "$n.py") > "$OUT/ctl_$n.log" 2>&1 && pass=$((pass+1)) || { fail=$((fail+1)); bad="$bad $n"; }
      done
      [ $fail -eq 0 ] && verdict ctl "PASS ($pass files)" || verdict ctl "FAIL ($fail of $((pass+fail)) files:$bad)" ;;
    kill)
      # SIGKILL durability: no acknowledged write may be lost; each test has a negative control that MUST lose writes
      env_up; ok=1; : > "$log"
      (cd loadtest && "$PY" test_group_commit.py --trials 2 --secs 30) >> "$log" 2>&1 || ok=0
      (cd loadtest && "$PY" test_open_gc.py --trials 1 --secs 60) >> "$log" 2>&1 || ok=0
      [ $ok = 1 ] && verdict kill "PASS (see kill.log for counts)" || verdict kill "FAIL (see kill.log)" ;;
    *) echo "unknown phase $p"; exit 2 ;;
  esac
done
cat "$V"
