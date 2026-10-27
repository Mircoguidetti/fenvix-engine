#!/usr/bin/env bash
# Runs ON the instance (started through SSM, detached). Usage: run.sh <bucket> <tests, e.g. 1 or 1,2,3>
# Order matters for the spend limits: the guard is started and confirmed alive BEFORE anything that talks to S3.
set -u
B=${1:?bucket}; TESTS=${2:?tests}
export S3T_BUCKET=$B AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=""
ROOT=/opt/s3test; STATE=/var/lib/s3test; OUT=$STATE/out
mkdir -p "$OUT" "$STATE" "$STATE/counts"
exec >>"$OUT/run.log" 2>&1
ulimit -n 200000

die() { echo "FATAL: $*"; aws s3 cp "$OUT/run.log" "s3://$B/_s3test/out/run.log" --only-show-errors; shutdown -h now; exit 1; }

echo '{"put_tier":0,"get_tier":0}' > "$STATE/extra.json"
dnf install -y python3-pip tar xz gzip procps-ng >/dev/null 2>&1 || die "dnf failed"
python3 -m pip install --quiet pymongo || die "pip pymongo failed"
python3 -c 'import pymongo; print("pymongo", pymongo.version)'

# 1) guard first; refuse to continue unless its heartbeat shows up.
nohup python3 "$ROOT/scripts/guard.py" --bucket "$B" --ports 27018,27019 > "$OUT/guard.log" 2>&1 &
for _ in $(seq 1 30); do [ -s "$STATE/guard.json" ] && break; sleep 2; done
[ -s "$STATE/guard.json" ] || die "guard did not start"
echo "guard up"

# 2) evidence that no long-lived credentials exist on the box: only the instance profile can authenticate.
{ echo "AWS_* variables in the environment (names only):"; env | grep -o '^AWS_[A-Z_]*' ; echo "credentials files:"; ls -la ~/.aws 2>&1; } > "$OUT/credentials-sources.txt"
aws sts get-caller-identity > "$OUT/caller-identity.json" 2>"$OUT/caller-identity.err" || echo "sts failed (see err)"

add_extra() { python3 - "$@" <<'P'
import json,sys
p="/var/lib/s3test/extra.json"; d=json.load(open(p)); d["put_tier"]+=int(sys.argv[1]); d["get_tier"]+=int(sys.argv[2]); json.dump(d,open(p,"w"))
P
}
export -f add_extra

IFS=, read -ra T <<< "$TESTS"
for t in "${T[@]}"; do
  case $t in
    1) bash "$ROOT/scripts/instance_test1.sh" ;;
    2) bash "$ROOT/scripts/instance_test2.sh" ;;
    3) bash "$ROOT/scripts/instance_test3.sh" ;;
    *) echo "unknown test $t (only 1, 2, 3 are prepared)" ;;
  esac
  echo "test $t finished rc=$?"
done

pkill -TERM -x mongo-server; sleep 3
n=$(find "$OUT" -type f | wc -l)
aws s3 cp "$OUT" "s3://$B/_s3test/out/" --recursive --only-show-errors
add_extra "$n" 0
cp "$STATE/guard.json" "$OUT/guard-last.json"
aws s3 cp "$STATE/guard.json" "s3://$B/_s3test/guard/last.json" --only-show-errors
echo done > "$STATE/DONE"; aws s3 cp "$STATE/DONE" "s3://$B/_s3test/out/DONE" --only-show-errors
