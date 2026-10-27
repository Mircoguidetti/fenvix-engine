#!/usr/bin/env bash
# First step of the job. Fails (before any AWS call) unless
#   * run.json asks only for tests 1, 2, 3 (4-6 are not prepared and never run from here),
#   * the ledger on s3-test-results has a total of at most 30 USD,
#   * for tests 2 or 3: results-merge.md exists on origin/main or origin/merge-engine and its git blob id equals
#     run.json "merge_results_blob" (the owner read the file, saw that ALL suites are green, and pinned that exact version).
set -eu
cd "${GITHUB_WORKSPACE:-.}"
J=s3-test/run.json
jq -e '.tests|type=="array" and length>0 and all(.[]; .==1 or .==2 or .==3)' $J >/dev/null || { echo "run.json: tests must be a non-empty list of 1, 2, 3"; exit 1; }
total=$(python3 -I s3-test/scripts/ledger.py total results/s3-test/ledger.json)
cap=$(jq -r .ledger_cap_usd s3-test/prices.json)
echo "ledger total: $total USD (cap $cap)"
awk -v t="$total" -v c="$cap" 'BEGIN{exit !(t>c)}' && { echo "LEDGER CAP REACHED: no new run may start"; exit 1; }
if jq -e '.tests|any(.==2 or .==3)' $J >/dev/null; then
  want=$(jq -r '.merge_results_blob // ""' $J); got=""
  for ref in origin/main origin/merge-engine; do
    b=$(git rev-parse "$ref:results-merge.md" 2>/dev/null || true); [ -n "$b" ] && [ "$b" = "$want" ] && got=$b && echo "results-merge.md pinned blob $b found on $ref"
  done
  [ -n "$want" ] && [ -n "$got" ] || { echo "Tests 2/3 need results-merge.md (main or merge-engine) whose blob id equals run.json merge_results_blob"; exit 1; }
fi
echo "gate ok"
