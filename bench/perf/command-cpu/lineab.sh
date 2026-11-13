#!/bin/bash
# A/B on the kept big line: for each BIN, recreate cook+spare with it, then lineprobe (reads only, same threads), and
# compare every answer's hash with the first binary's.  lineab.sh TAG BIN...   (env: THREADS, SEED, PROF=1)
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
P=~/.fenvix-agentbench-venv/bin/python
export BENCH_PW=$(cat /home/user/.benchpw) FXBENCH_SERVER_FLAGS="--box-disk-mb 2048"
tag=$1; shift
first=""
for b in "$@"; do
  n=$(basename $b)
  COOK_ENV="CPU_PROF_FILE=/diag/prof.folded" COOK_DIR=/home/user/prof/line $P $HERE/recook.py $b > /dev/null 2>&1 || { echo "recook failed $b"; exit 1; }
  out=/home/user/prof/line-$tag-$n.json
  echo "== $n"
  $P $HERE/lineprobe.py --threads ${THREADS:-300} --seed ${SEED:-5} --out $out ${PROF:+--prof /home/user/prof/line} 2>&1 | tail -5
  if [ -n "${PROF:-}" ]; then mv /home/user/prof/line/prof.folded /home/user/prof/line-$tag-$n.folded; fi
  if [ -z "$first" ]; then first=$out; else
    $P -c "
import json,sys
a=json.load(open('$first'))['hashes']; b=json.load(open('$out'))['hashes']
d=[k for k in a if a.get(k)!=b.get(k)]
print('answers compared', len(a), 'different', len(d), d[:5])"
  fi
done
