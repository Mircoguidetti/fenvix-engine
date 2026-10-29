#!/usr/bin/env bash
# After run_all.sh: scenario 4 (ramp), its data-size extra (bigcache) and the request-source diagnostic.
set -u
cd "$(dirname "$0")"
PY=${PY:-/home/user/venv-density/bin/python}
W=${DENSITY_WORK:-/home/user/density-work}
until grep -q "ALL DONE" $W/progress.log; do sleep 20; done
echo "$(date +%T) start s4_ramp" >> $W/progress.log
$PY ramp.py s4_ramp --step 250 --step-s 180 --period 20 --max-n 2000 --limit-mb 3072 --hold-s 180 >> $W/progress.log 2>&1
$PY analyze.py s4_ramp 0 >> $W/progress.log 2>&1
echo "$(date +%T) start bigcache" >> $W/progress.log
$PY bigcache.py 8 40 >> $W/progress.log 2>&1
echo "$(date +%T) start diag" >> $W/progress.log
$PY diag.py 50 300 >> $W/progress.log 2>&1
echo "$(date +%T) REST DONE" >> $W/progress.log
