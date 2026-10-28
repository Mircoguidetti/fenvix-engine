#!/usr/bin/env bash
# ctl-tests of the final binary, one test file at a time, resumable: a file is done when its pytest summary line exists
# (a file that finished with failures is also "done": the phase report counts them; delete its .txt to run it again).
#   pass 1: every ctl-tests/test_*.py as they are            -> out/final/ctl_<file>.txt
#   pass 2: test_01..test_08b with CTL_TEST_AUTH=1 (the server runs with --auth, the tests use an authenticated root client) -> out/final/ctlauth_<file>.txt
cd "$(dirname "$0")/../.."
OUT=mongo-tests/out/final; mkdir -p $OUT
PYV=/tmp/claude-0/venv/bin/python
tools/final/env_up.sh > /dev/null || exit 1
done_file() { [ -f "$1" ] && grep -Eq "[0-9]+ (passed|failed|error).* in [0-9.]+s" "$1"; }
for f in ctl-tests/test_*.py; do
  n=$(basename $f .py); o=$OUT/ctl_$n.txt
  done_file $o && continue
  tools/final/env_up.sh > /dev/null || exit 1
  (cd ctl-tests && timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $n.py > ../$o 2>&1)
  echo "ctl $n: $(tail -1 $o)"
done
for f in ctl-tests/test_0[1-8]*.py; do
  n=$(basename $f .py); o=$OUT/ctlauth_$n.txt
  done_file $o && continue
  tools/final/env_up.sh > /dev/null || exit 1
  (cd ctl-tests && CTL_TEST_AUTH=1 timeout 2400 $PYV -m pytest -q -p no:cacheprovider --tb=short $n.py > ../$o 2>&1)
  echo "ctl(auth) $n: $(tail -1 $o)"
done
