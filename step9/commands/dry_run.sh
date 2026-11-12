#!/bin/bash
# Step 9 · every final command of the round through decide.py, at $0 (no AWS call: decide.py touches nothing).
# Two passes per command: S9_MODE=local (every check except the gate: fields, types, 8 vCPU, the job's duration against
# 90% of its ceiling, refusal of a command that does nothing) and S9_MODE=aws (the same plus the gate; CLOSED until the
# final engine ref has its passing local benchmark report). RUN_ID 1000000001 = the run of job L (its kept pantry
# fenvix-kitchen-pantry-1000000001 is named in the later commands; replace it with the real run id of job L, and
# "engine" with the final merged ref, before committing a command to step9/command.json).
# Usage: bash step9/commands/dry_run.sh > step9/commands/DRY-RUN.txt
cd "$(dirname "$0")/../ci" || exit 1
export RUN_ID=1000000001 ENGINE_COMMIT=${ENGINE_COMMIT:-27014ff2e3e3e4ff223aa778746f3dce433c6549}
t=$(mktemp -d)
echo "decide.py dry run of step9/commands/*.json, $(date -u +%FT%TZ), engine commit for the gate: $ENGINE_COMMIT"
for f in ../commands/*.json; do
  echo; echo "=== $(basename "$f"): $(tr -d '\n' < "$f" | tr -s ' ')"
  for mode in local aws; do
    out=$(S9_MODE=$mode python3 decide.py "$f" "$t/r.json" 2>&1); rc=$?
    if [ $rc = 0 ]; then
      echo "  [$mode] ACCEPTED: $(grep -E '^(mode|vcpu_peak|vcpu_peaks|job_hours_total|job_hours)=' <<<"$out" | tr '\n' ' ')"
    else
      echo "  [$mode] $(head -1 <<<"$out")"; [ "$mode" = aws ] && sed -n '2,4p' <<<"$out" | sed 's/^/          /'
    fi
  done
done
