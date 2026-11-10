#!/usr/bin/env bash
# small LangGraph e2e (E2E_USERS x E2E_TURNS) on ours with one flag set; reports time, compactor and derived counters, absorb errors
# repro.sh NAME "<OURS_EXTRA>" [env assignments...]
R=/tmp/claude-0/-home-user/9129b5ca-4194-55a1-8a84-bd2fce2b2dd9/scratchpad/run; H=$R/bench/agent-adapters; A=/home/user/adapters
name=$1; export OURS_EXTRA="$2"; shift 2; for kv in "$@"; do export "$kv"; done
mkdir -p $R/repro; $H/target.sh ours > /dev/null 2>&1 || { echo start failed; exit 1; }
s=$(date +%s); E2E_USERS=${U:-10} E2E_TURNS=${T:-60} timeout ${TMO:-400} $A/venv-lg/bin/python $H/e2e_py.py langgraph $R/repro/$name.json > $R/repro/$name.out 2>&1; rc=$?
st=$(timeout 20 $A/venv-lg/bin/python -c "import pymongo;f=pymongo.MongoClient('mongodb://localhost:27017/?directConnection=true',socketTimeoutMS=15000).admin.command('serverStatus')['fenvix'];print('compactor',f['compactor'],'derived',f['derived'])" 2>&1 | tail -1)
P=$(cat ~/.fenvix-adapters/27017/pid); cp ~/.fenvix-adapters/27017/ours.log $R/repro/$name.log
echo "$name: exit $rc in $(( $(date +%s) - s )) s; EntityTooSmall lines $(grep -c EntityTooSmall $R/repro/$name.log); $st"
kill -TERM $P; for i in $(seq 1 60); do kill -0 $P 2>/dev/null || break; sleep 1; done
kill -0 $P 2>/dev/null && { echo "  $name: no clean stop in 60 s, killed"; kill -9 $P; }; rm -f ~/.fenvix-adapters/27017/pid
