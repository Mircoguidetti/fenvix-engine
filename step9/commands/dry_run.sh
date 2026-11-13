#!/bin/bash
# Step 9 · every final command of the round through decide.py, at $0 (no AWS call: decide.py touches nothing).
# Two passes per command: S9_MODE=local (every check except the gate: fields, types, 8 vCPU, the job's duration against
# 90% of its ceiling, refusal of a command that does nothing) and S9_MODE=aws (the same plus the gate; CLOSED until the
# final engine ref has its passing local benchmark report). RUN_ID 1000000001 = the run of job L (its kept pantry
# fenvix-kitchen-pantry-1000000001 is named in the later commands; replace it with the real run id of job L before
# committing a command to step9/command.json). The engine: step9/ENGINE, one line for the whole round (the dry run passes
# it as ENGINE_COMMIT, as the workflow does). The order of the jobs: step9/commands/ROUND.txt.
# Usage: bash step9/commands/dry_run.sh > step9/commands/DRY-RUN.txt
cd "$(dirname "$0")/../ci" || exit 1
# review of the AWS harness (2026-11-13): the round's one engine is step9/ENGINE (the commands carry none)
export RUN_ID=1000000001 ENGINE_COMMIT=${ENGINE_COMMIT:-$(grep -v '^#' ../ENGINE | grep -m1 .)}
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
# review of the AWS harness (2026-11-13): the round in its order (ROUND.txt) through the pantry's writing marks, as the
# workflow's pantry check will see them job after job (each writing job marks the databases it writes)
echo; echo "=== the round in order (ROUND.txt), each job's pantry check against the marks of the jobs before it"
S9_MODE=local python3 - <<'PY'
import json, os
import decide
marks = []
for i, f in enumerate(l.split()[0] for l in open("../commands/ROUND.txt") if l.strip() and not l.startswith("#")):
    c = decide.resolve(json.load(open(f"../commands/{f}")))
    if c["mode"] != "run":
        print(f"  {f}: mode {c['mode']}")
        continue
    r = decide.pantry_mark_refusal(c, marks)
    m = decide.pantry_mark(c, 2000 + i)
    if m and c["pantry_mode"] == "existing":
        marks.append(m)
    print(f"  {f}: {'REFUSED: ' + r if r else 'runs'}; reads as loaded {c['pantry_reads'] or 'nothing'}"
          + (f"; marks {m['databases']} as written" if m else ""))
PY
