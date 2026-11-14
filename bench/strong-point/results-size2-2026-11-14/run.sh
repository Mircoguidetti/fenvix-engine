#!/bin/bash
# size-2 driver: run.sh LINE OUTNAME [extra history.py args]
S=/tmp/claude-0/-home-user-fenvix-engine/4a738104-bc29-53ed-9dfd-1df9bcce11ba/scratchpad
export BENCH_PW=$(cat $S/pw) FXBENCH_LOG_MEM=2g FXBENCH_CUSTOMER_KEY="messages=userId:ts" NO_PROXY='*' no_proxy='*'
export MINIO_BIN=$HOME/.fenvix-judge/env/bin/minio
l=$1; o=$2; shift 2
cd /home/user/fenvix-engine
exec ~/.fenvix-agentbench-venv/bin/python -u bench/strong-point/history.py run --state /home/user/sp/state --line $l \
  --out /home/user/sp/results/$o --mem 512m "$@"
