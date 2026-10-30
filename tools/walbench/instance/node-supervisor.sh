#!/usr/bin/env bash
# Quorum log node supervisor: runs `walbench node`, and when it dies (SIGKILL on request, crash) restarts it after
# the delay the node wrote to <dir>/restart_delay_ms (default 1 s), with the log files it had: a stale node returning.
# usage: node-supervisor.sh <walbench binary> <listen addr> <dir>
set -u
BIN=$1; LISTEN=$2; DIR=$3
mkdir -p "$DIR"
while true; do
  "$BIN" node --listen "$LISTEN" --dir "$DIR"
  d=$(cat "$DIR/restart_delay_ms" 2>/dev/null || echo 1000); rm -f "$DIR/restart_delay_ms"
  echo "$(date +%T) node exited; restarting in ${d} ms" >&2
  sleep "$(awk "BEGIN{print $d/1000}")"
done
