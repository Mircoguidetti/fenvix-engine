#!/usr/bin/env bash
# control-ops ctl tests, one test file at a time (resumable: a file is done when its pytest summary line exists).
cd "$(dirname "$0")/../.."
OUT=mongo-tests/out/fusion; mkdir -p $OUT
PYV=/tmp/claude-0/venv/bin/python
tools/fusion/env_up.sh > /dev/null || exit 1
for f in ctl-tests/test_*.py; do
  n=$(basename $f .py); o=$OUT/ctl_$n.txt
  if [ -f "$o" ] && grep -Eq "[0-9]+ (passed|failed|error).* in [0-9.]+s" "$o"; then continue; fi
  tools/fusion/env_up.sh > /dev/null || exit 1
  (cd ctl-tests && timeout 1500 $PYV -m pytest -q -p no:cacheprovider --tb=short $n.py > ../$o 2>&1)
  tail -1 $o
done
