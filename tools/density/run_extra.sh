#!/usr/bin/env bash
# After run_rest.sh: memory growth of written databases (slatedb freezes a writer's memtable at 64 MiB or after 4096
# WAL flushes; at 10 inserts/s a database passes 4096 writes in about 7 minutes).
set -u
cd "$(dirname "$0")"
PY=${PY:-/home/user/venv-density/bin/python}
W=${DENSITY_WORK:-/home/user/density-work}
until grep -q "REST DONE" $W/progress.log; do sleep 20; done
echo "$(date +%T) start s4_write_growth" >> $W/progress.log
$PY run_scenario.py s4_write_growth steady 20 900 --rate 10 --mix insert=1 --procs 1 --offset 1000 >> $W/progress.log 2>&1
$PY analyze.py s4_write_growth >> $W/progress.log 2>&1
echo "$(date +%T) EXTRA DONE" >> $W/progress.log
echo "$(date +%T) start bigcache_reopen" >> $W/progress.log
$PY bigcache_reopen.py 8 4 >> $W/progress.log 2>&1
echo "$(date +%T) EXTRA2 DONE" >> $W/progress.log
echo "$(date +%T) start wal_growth" >> $W/progress.log
$PY wal_growth.py 20000 180 >> $W/progress.log 2>&1
echo "$(date +%T) EXTRA3 DONE" >> $W/progress.log
