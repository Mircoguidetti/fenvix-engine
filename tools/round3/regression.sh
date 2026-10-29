#!/usr/bin/env bash
# Every existing test of the repository against one frozen copy of the engine binary, resumable (a finished part is kept).
#   regression.sh LABEL BINARY [parts]     parts: any of suites ctl ctlauth kill load unit (default: all)
# Outputs in mongo-tests/out/round3/LABEL/. Needs MinIO :9000 and the reference mongod (tools/final/env_up.sh).
set -u
cd "$(dirname "$0")/../.."
ROOT=$PWD
LABEL=${1:?label}; BIN=${2:?binary}; shift 2
PARTS=${*:-suites ctl ctlauth kill load unit}
OUT=$ROOT/mongo-tests/out/round3/$LABEL; mkdir -p $OUT
PYV=/tmp/claude-0/venv/bin/python
MPY=$ROOT/mongo-tests/.venv/bin/python
export MONGO_SERVER_BIN=$BIN SERVER_BIN=$BIN BIN=$BIN
done_file() { [ -f "$1" ] && grep -Eq "[0-9]+ (passed|failed|error).* in [0-9.]+s|^DONE" "$1"; }
has() { [[ " $PARTS " == *" $1 "* ]]; }

if has suites; then
  # differential suites, pymongo and Node, sliced (as tools/final/run_suites.py, own output dir)
  n=$(python3 -c "import json;print(len(json.load(open('mongo-tests/tests.json'))))")
  REAL="mongodb://localhost:27020/?directConnection=true"; OURS="mongodb://localhost:27018/?directConnection=true"; IDLE="mongodb://localhost:27019/?directConnection=true"
  export SERVER_EXTRA="--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%"
  for drv in py node; do
    for ((a=0; a<n; a+=100)); do
      b=$((a+100 < n ? a+100 : n)); pf=$OUT/suite_${drv}_$a.json
      [ -f $pf ] && continue
      tools/final/env_up.sh > /dev/null || exit 1
      (cd mongo-tests && ./server.sh 27018 60 > /dev/null && ./server.sh 27019 3 > /dev/null)
      if [ $drv = py ]; then
        (cd mongo-tests && $MPY run_py.py --real "$REAL" --ours "$OURS" --idle "$IDLE" --slice $a:$b --out $pf.tmp > $pf.log 2>&1)
      else
        (cd mongo-tests/node && node run_node.js "--real=$REAL" "--ours=$OURS" "--idle=$IDLE" "--slice=$a:$b" "--out=$pf.tmp" > $pf.log 2>&1)
      fi
      [ -f $pf.tmp ] && mv $pf.tmp $pf || { echo "suite $drv $a: no result"; }
    done
  done
  unset SERVER_EXTRA
  pkill -f "mongo-server --listen 127.0.0.1:2701[89]" 2>/dev/null
  python3 - "$OUT" <<'P'
import json, glob, sys, collections
out = sys.argv[1]
for drv in ("py", "node"):
    c = collections.Counter(); fails = []
    for f in sorted(glob.glob(f"{out}/suite_{drv}_*.json")):
        for r in json.load(open(f)):
            c[r["status"]] += 1
            if r["status"] != "pass":
                fails.append(r.get("name") or r.get("id"))
    print(drv, dict(c), "non-pass:", fails[:20])
P
fi

if has ctl || has ctlauth; then
  for f in ctl-tests/test_*.py; do
    nm=$(basename $f .py)
    if has ctl; then
      o=$OUT/ctl_$nm.txt
      if ! done_file $o; then
        tools/final/env_up.sh > /dev/null || exit 1
        (cd ctl-tests && timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $nm.py > $o 2>&1)
        echo "ctl $nm: $(tail -1 $o)"
        tools/final/clean_buckets.sh > /dev/null
      fi
    fi
  done
  if has ctlauth; then
    for f in ctl-tests/test_0[1-8]*.py; do
      nm=$(basename $f .py); o=$OUT/ctlauth_$nm.txt
      done_file $o && continue
      tools/final/env_up.sh > /dev/null || exit 1
      (cd ctl-tests && CTL_TEST_AUTH=1 timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $nm.py > $o 2>&1)
      echo "ctl(auth) $nm: $(tail -1 $o)"
      tools/final/clean_buckets.sh > /dev/null
    done
  fi
fi

if has kill; then
  # SIGKILL durability tests with their negative controls (as tools/final/run_kill.sh)
  one() { local name=$1; shift; local o=$OUT/kill_$name.txt
    done_file $o && return
    tools/final/env_up.sh > /dev/null || exit 1
    (cd loadtest && timeout 900 $MPY "$@" > $o.tmp 2>&1) && { mv $o.tmp $o; echo DONE >> $o; echo "kill $name: $(tail -2 $o | head -1 | cut -c1-200)"; } || { mv $o.tmp $o.failed; echo "kill $name: FAILED"; }
  }
  for t in 1 2; do one wal_$t test_group_commit.py --trials 1 --secs 60 --binary $BIN; done
  one walctl_1 test_group_commit.py --trials 1 --secs 30 --control --binary $BIN
  for t in 1 2; do one sst_$t test_open_gc.py --trials 1 --secs 100; done
  one sstctl_1 test_open_gc.py --trials 1 --secs 60 --control
fi

if has load; then
  # functional tests of loadtest/ (the ones without a fixed binary path take BIN; the others read target/release)
  for t in test_lease test_durable_reads test_shared_reads test_stop test_fallback_recovery; do
    o=$OUT/load_$t.txt
    done_file $o && continue
    tools/final/env_up.sh > /dev/null || exit 1
    (cd loadtest && timeout 1800 $MPY $t.py > $o.tmp 2>&1; echo "exit=$?" >> $o.tmp; mv $o.tmp $o; echo DONE >> $o)
    echo "load $t: $(grep -c '^PASS' $o) PASS, $(grep -c '^FAIL' $o) FAIL, $(grep exit= $o)"
  done
fi

if has unit; then
  o=$OUT/unit.txt
  done_file $o || { cargo test --release --workspace > $o 2>&1; echo DONE >> $o; grep -E "^test result" $o | awk '{p+=$4; f+=$6} END {print "unit: passed", p, "failed", f}'; }
fi
echo "REGRESSION $LABEL DONE"
