#!/usr/bin/env bash
# Regression of one binary (network-limits work), resumable, one phase after the other:
#   suites  pymongo + Node differential suites (core, extra, replset)      -> mongo-tests/out/nl_<label>/<driver>_<n>.json
#   ctl     every ctl-tests/test_*.py (pass 1) and test_01..08b with CTL_TEST_AUTH=1 (pass 2) -> .../ctl_<file>.txt, ctlauth_<file>.txt
#   kill    SIGKILL durability trials and their negative controls       -> .../kill_<name>.txt
# usage: run_regression.sh <label> <binary> [phases...]   (NEW_TESTS=0 skips test_13/test_14, for the binary that predates them)
set -u
cd "$(dirname "$0")/../.."
LABEL=$1; BIN=$2; shift 2; PHASES=${*:-"suites ctl kill"}
OUT=mongo-tests/out/nl_$LABEL; mkdir -p $OUT
PYV=/tmp/claude-0/venv/bin/python
export SERVER_BIN=$BIN MONGO_SERVER_BIN=$BIN BIN
done_file() { [ -f "$1" ] && grep -Eq "[0-9]+ (passed|failed|error).* in [0-9.]+s" "$1"; }
clean() { find /tmp/minio-data -maxdepth 1 -name 'ctl-*' -mmin +1 -exec rm -rf {} + 2>/dev/null; }
for ph in $PHASES; do
  case $ph in
  suites)
    NL_OUT=nl_$LABEL python3 tools/netlimits/run_suites.py 150 || exit 1
    for port in 27018 27019; do pf=mongo-tests/out/server-$port.pid; [ -f $pf ] && kill -TERM "$(cat $pf)" 2>/dev/null; done
    # the suites' data in MinIO is not needed afterwards
    rm -rf /tmp/minio-data/bench/* 2>/dev/null; echo "PHASE suites done $(df -h / | tail -1)" ;;
  ctl)
    tools/final/env_up.sh > /dev/null || exit 1; clean
    for f in ctl-tests/test_*.py; do
      n=$(basename $f .py); o=$OUT/ctl_$n.txt
      case $n in test_13*|test_14*) [ "${NEW_TESTS:-1}" = 0 ] && continue ;; esac
      done_file $o && continue
      (cd ctl-tests && timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $n.py > ../$o 2>&1)
      echo "ctl $n: $(tail -1 $o)"; clean
    done
    for f in ctl-tests/test_0[1-8]*.py; do
      n=$(basename $f .py); o=$OUT/ctlauth_$n.txt
      done_file $o && continue
      (cd ctl-tests && CTL_TEST_AUTH=1 timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $n.py > ../$o 2>&1)
      echo "ctl(auth) $n: $(tail -1 $o)"; clean
    done
    rm -rf ctl-tests/out/*.log.old 2>/dev/null; echo "PHASE ctl done $(df -h / | tail -1)" ;;
  kill)
    cd loadtest
    one() { local name=$1; shift; local o=../$OUT/kill_$name.txt
      if [ -f "$o" ] && grep -q "^DONE" "$o"; then return; fi
      ../tools/final/env_up.sh > /dev/null || exit 1
      timeout 900 $PYV "$@" > "$o.tmp" 2>&1 && { mv "$o.tmp" "$o"; echo DONE >> "$o"; echo "kill $name: $(grep TOTAL $o | tail -1 | cut -c1-160)"; } || { echo "interrupted: $name"; exit 2; }; }
    for t in 1 2 3 4; do one wal_$t test_group_commit.py --binary $BIN --trials 1 --secs 60; done
    for t in 1 2; do one walctl_$t test_group_commit.py --binary $BIN --trials 1 --secs 30 --control; done
    for t in 1 2 3 4; do one sst_$t test_open_gc.py --trials 1 --secs 100; done
    for t in 1 2; do one sstctl_$t test_open_gc.py --trials 1 --secs 60 --control; done
    cd ..; echo "PHASE kill done $(df -h / | tail -1)" ;;
  esac
done
echo "ALL DONE $LABEL"
