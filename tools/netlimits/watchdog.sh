#!/usr/bin/env bash
# Keeps the network-limits regression going: every 30 s, if the runner is not alive and the log has no "ALL DONE base", it is
# started again (every phase is resumable); if no result file changed for 10 minutes, a STALL line is written. Events go to
# /tmp/claude-0/nl/watchdog.log (one line each).
cd "$(dirname "$0")/../.."
LOG=/tmp/claude-0/nl/regression.log; W=/tmp/claude-0/nl/watchdog.log
start() {
  rm -f mongo-tests/out/nl_*/*.tmp
  nohup bash -c "tools/netlimits/run_regression.sh new /tmp/claude-0/nl/bin/mongo-server.new; NEW_TESTS=0 tools/netlimits/run_regression.sh base /tmp/claude-0/nl/bin/mongo-server.base" >> $LOG 2>&1 &
}
last_stall=0
while true; do
  if grep -q "ALL DONE base" $LOG; then echo "$(date +%T) FINISHED" >> $W; exit 0; fi
  if ! pgrep -f "run_regression.sh" > /dev/null; then echo "$(date +%T) RESTART runner was not alive (last: $(tail -1 $LOG | cut -c1-100))" >> $W; start; sleep 20; fi
  newest=$(find mongo-tests/out/nl_new mongo-tests/out/nl_base ctl-tests/out $LOG -type f -printf '%T@\n' 2>/dev/null | sort -n | tail -1)
  newest=${newest%.*}; age=$(( $(date +%s) - ${newest:-0} ))
  if [ $age -gt 600 ] && [ $(( $(date +%s) - last_stall )) -gt 600 ]; then echo "$(date +%T) STALL no output for ${age}s (last: $(tail -1 $LOG | cut -c1-100))" >> $W; last_stall=$(date +%s); fi
  sleep 30
done
