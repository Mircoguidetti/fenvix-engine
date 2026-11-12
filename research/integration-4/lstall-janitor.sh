#!/bin/bash
# integration-4: while the outage loop runs, delete the finished runs' buckets (every ctl-lstall-* but the newest), so the
# loop does not fill the disk (it did at run 7 of the first loop). Usage: lstall-janitor.sh PID_TO_WATCH
M=/root/.fenvix-judge/data/minio
while kill -0 "$1" 2>/dev/null; do
  for b in $(ls -t "${M:?}" | grep -E '^ctl-lstall-' | tail -n +2); do rm -rf "${M:?}/${b:?}"; done
  sleep 30
done
