#!/usr/bin/env bash
# Agent lane, done-means 4 (first half): the LangGraph MongoDB checkpointer and store suites (as in bench/agent-adapters on
# branch `agent-adapters`) against mongod 7.0 and against mongo-server with the replicated log on and EVERY collection on the
# agent lane (`--agent-lane '*.*'`, the hardest case: every write waits for its PUT), PUT latency modelled at 30 ms.
# Same results as mongod = same pass/fail per test.
#   loadtest/agent_lane/t4_langgraph.sh OUT_DIR
# Needs judge/setup.sh, the release build, proto/quaderno built, and the LangGraph suites installed by
# bench/agent-adapters/install.sh (only the venv-lg part is used).
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
OUT=$(mkdir -p "$1" && cd "$1" && pwd)
JH=${JUDGE_HOME:-$HOME/.fenvix-judge}; PY="$JH/env/bin/python"
A=${ADAPTERS_HOME:-/home/user/adapters}
# the adapters harness lives on branch agent-adapters; take it from there when it is not checked out here
if [ ! -x bench/agent-adapters/run_suites.sh ]; then
  git fetch -q origin agent-adapters
  mkdir -p bench/agent-adapters
  for f in target.sh run_suites.sh compare.py merge_junit.py fake_docker.py; do git show "origin/agent-adapters:bench/agent-adapters/$f" > "bench/agent-adapters/$f"; done
  chmod +x bench/agent-adapters/*.sh
fi
W="$OUT/work"; rm -rf "$W"; mkdir -p "$W"
export PQ_WORK="$W/log" PQ_BIN="$ROOT/proto/quaderno/target/release/pq" PQ_PORT_OFF=8000 PQ_CORSIE=1 PQ_RICICLA=1
"$PY" proto/quaderno/scripts/cluster.py up --fresh --no-proxy --window-ms 60000 --mem-cap-mb 1024 > "$OUT/log-up.txt" 2>&1
REPL="127.0.0.1:15200,127.0.0.1:15201,127.0.0.1:15202"
export FENVIX_SIM_S3_PUT="30,0.3,90"
for T in mongod ours; do
  if [ $T = ours ]; then
    export OURS_EXTRA="--quaderno $REPL --local-log-upload-secs 30 --local-log-dir $W/local --agent-lane *.*"
  fi
  # the scripts expand OURS_EXTRA unquoted: no file-name expansion of the pattern in them (noglob passed down via SHELLOPTS)
  (set -f; export SHELLOPTS; bench/agent-adapters/run_suites.sh $T lg-py-saver lg-py-store) 2>&1 | tee "$OUT/run-$T.txt"
  mkdir -p "$OUT/$T"; cp bench/agent-adapters/results/$T/lg-py-*.xml bench/agent-adapters/results/$T/lg-py-*.log "$OUT/$T/" 2>/dev/null
done
"$PY" proto/quaderno/scripts/cluster.py down > /dev/null 2>&1
for s in lg-py-saver lg-py-store; do
  echo "== $s: mongod vs ours (agent lane on every collection)"
  "$A/venv-lg/bin/python" bench/agent-adapters/compare.py "$OUT/mongod/$s.xml" "$OUT/ours/$s.xml"
done 2>&1 | tee "$OUT/compare.txt"
