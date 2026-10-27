#!/usr/bin/env bash
# After cleanup: gross cost of this run -> ledger; results -> branch s3-test-results. Uses no AWS credentials.
set -u
cd "${GITHUB_WORKSPACE:-.}"; OUT=run-out; H=s3-test/scripts
CAP=$(jq -r .run_cost_cap_usd s3-test/prices.json)
secs=0; put=0; get=0; note=""; unknown=""
L=$(grep '^LAUNCH_EPOCH=' $OUT/state.env 2>/dev/null | cut -d= -f2)
if [ -z "$L" ]; then note="no instance was launched"   # only S3 calls by the runner: far below a cent
else
  secs=$(( $(date +%s) - L ))
  f=""; for c in "$OUT/instance/guard/final.json" "$OUT/instance/guard/last.json" "$OUT/instance/guard/counters.json" "$OUT/guard-latest.json"; do [ -s "$c" ] && f=$c && break; done
  if [ -n "$f" ]; then put=$(jq -r .put_tier "$f"); get=$(jq -r .get_tier "$f"); note="counters from $f"
  else unknown=1; note="NO COUNTERS RECOVERED: booked at the cap"; fi
fi
reads=$(cat $OUT/watchdog.reads 2>/dev/null || echo 0)
get=$((get + reads + 60))     # watchdog reads + the runner's own S3 calls (generous)
put=$((put + 40))
gross=$(python3 -I $H/cost.py "$put" "$get" "$secs")
[ -n "$unknown" ] && gross=$(python3 -I -c "print($CAP + $secs/3600*0.097)")
echo "gross cost of this run: $gross USD ($note)"
echo "$gross" > $OUT/gross_usd.txt
python3 -I $H/ledger.py add results/s3-test/ledger.json "$GITHUB_RUN_ID" "$(jq -r '.tests|map(tostring)|join(",")' s3-test/run.json)" "$gross" "$put" "$get" "$secs" "$note" $unknown
mkdir -p results/s3-test/runs/$GITHUB_RUN_ID; cp -r $OUT/*.jsonl $OUT/*.log $OUT/*.txt $OUT/*.cost $OUT/*.reason $OUT/instance results/s3-test/runs/$GITHUB_RUN_ID/ 2>/dev/null
touch $OUT/errors.jsonl
jq -s '.' $OUT/errors.jsonl > results/s3-test/runs/$GITHUB_RUN_ID/errors.json 2>/dev/null
jq -r -s 'if length==0 then "No permission or step errors recorded." else ("| step | IAM action needed | resource | AWS says |\n|---|---|---|---|\n" + (map("| \(.step) | \(.iam_action) | \(.resource) | \(.reported_by_aws // "") |") | join("\n"))) end' $OUT/errors.jsonl > results/s3-test/runs/$GITHUB_RUN_ID/missing-permissions.md
cd results && git add -A && git -c user.name=github-actions -c user.email=actions@users.noreply.github.com commit -qm "s3-test run $GITHUB_RUN_ID: gross $gross USD" && git push origin HEAD:s3-test-results
