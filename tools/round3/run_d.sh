#!/usr/bin/env bash
# Part D of round3 on MinIO, resumable (a run whose JSON exists is skipped). Engine binary: $ROUND3_ENGINE_BIN (frozen copy);
# the "before" binary: $ROUND3_BEFORE_BIN (main before this work).
set -u
cd "$(dirname "$0")/../.."
PY=${PY:-/tmp/claude-0/venv/bin/python}
OUT=results-round3/minio
one() { local name=$1; shift; [ -f $OUT/$name.json ] && return; MINIO_ONLY=1 tools/fusion/env_up.sh > /dev/null; $PY tools/round3/run.py $name "$@" 2>&1 | tail -1; }
# single databases: rate 1 on 10 databases (per-database figures averaged over 10), rates 10 and 100 on 2 databases
for mode in before immediate adaptive relaxed local; do
  for mix in half write read; do
    [ $mix = read ] && [ $mode != before ] && [ $mode != immediate ] && continue   # without writes the write modes are the same
    one ${mode}-r1-$mix --mode $mode --dbs 10 --rate 1 --mix $mix --secs 360 --warm 60
    one ${mode}-r10-$mix --mode $mode --dbs 2 --rate 10 --mix $mix --secs 240 --warm 60
    one ${mode}-r100-$mix --mode $mode --dbs 2 --rate 100 --mix $mix --secs 240 --warm 60
  done
done
# fleets
for mode in before immediate adaptive relaxed local; do
  one fleet100-$mode --mode $mode --dbs 100 --rate 1 --mix write --secs 360 --warm 60
  one fleetmixed-$mode --mode $mode --fleet mixed --secs 660 --warm 60
done
echo "D DONE"
