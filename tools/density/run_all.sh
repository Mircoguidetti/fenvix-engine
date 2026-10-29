#!/usr/bin/env bash
# Scenarios 1-3 in sequence (each run: fresh engine, all databases asleep at start). Progress in WORK/progress.log.
set -u
cd "$(dirname "$0")"
PY=${PY:-/home/user/venv-density/bin/python}
W=${DENSITY_WORK:-/home/user/density-work}
run() { local name=$1; shift
  [ -f $W/runs/$name/meta.txt ] && grep -q engine_exit $W/runs/$name/meta.txt && { echo "$(date +%T) skip $name" >> $W/progress.log; return; }
  rm -rf $W/runs/$name; echo "$(date +%T) start $name $*" >> $W/progress.log
  $PY run_scenario.py $name "$@" >> $W/progress.log 2>&1
  $PY analyze.py $name >> $W/progress.log 2>&1
}
for n in ${S1_NS:-50 100 200 500}; do run s1_steady_n$n steady $n 600 --procs 2; done
run s2_burst_n200 burst 200 600 --procs 2
run s3_period70_n100 period 100 900 --period 70 --procs 1
run s3_period45_n100 period 100 900 --period 45 --procs 1
echo "$(date +%T) ALL DONE" >> $W/progress.log
