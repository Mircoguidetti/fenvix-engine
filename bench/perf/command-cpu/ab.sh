#!/bin/bash
# A/B of server binaries on the dev bench, each on the same restored data (MinIO snapshot + plan), CPU per command.
#   ab.sh BIN... (env: SNAP=minio snapshot dir, PLAN=plan.pkl, ROUNDS)
set -u
H=/home/user/harness/bench/agent-checkpoints
P=~/.fenvix-agentbench-venv/bin/python
D=/tmp/claude-0/devbench
HERE=$(cd "$(dirname "$0")" && pwd)
for b in "$@"; do
  $P $H/devbench.py down >/dev/null 2>&1
  rm -rf $D/minio && mkdir -p $D && cp -r ${SNAP:-/home/user/devbench-minio-snap} $D/minio && cp ${PLAN:-/home/user/devbench-plan.pkl} $D/plan.pkl
  CPU_PROF_FILE=$D/prof.folded $P $H/devbench.py up --server $b >/dev/null 2>&1 || { echo "up failed $b"; exit 1; }
  echo "== $(basename $b)"
  $P $HERE/listcost.py ${ROUNDS:-3}
  $P $HERE/percmd.py 2000
  $P $HERE/cpuprobe.py --harness $H --rounds ${ROUNDS:-3} --ops resume,time_travel,list,list10 2>&1 | grep -v "^  " | tail -5
done
$P $H/devbench.py down >/dev/null 2>&1
