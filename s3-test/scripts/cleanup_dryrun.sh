#!/usr/bin/env bash
# Dry run of the clean-up after more than one hour. NO instance is launched. Phase 1 (this script): create the run's
# bucket (tagged, one object), then wait WAIT_MIN minutes (default 65) WITHOUT any AWS call and WITHOUT refreshing, so the
# session of the "AWS credentials" step expires; then prove the old credentials are expired. The workflow then does its
# normal clean-up (fresh OIDC login + cleanup.sh), and cleanup_dryrun_verify.sh checks that it worked.
set -u
cd "${GITHUB_WORKSPACE:-.}"; OUT=run-out; mkdir -p "$OUT"; R=eu-central-1
B=fenvix-test-${GITHUB_RUN_ID:?}; WAIT_MIN=$(jq -r '.wait_minutes // 65' s3-test/run.json)
: > "$OUT/errors.jsonl"; echo "BUCKET=$B" > "$OUT/state.env"; RES=$OUT/dryrun_result.txt; : > "$RES"
say() { echo "$*" | tee -a "$RES"; }
aws s3api create-bucket --bucket "$B" --region $R --create-bucket-configuration LocationConstraint=$R >/dev/null && \
aws s3api put-bucket-tagging --bucket "$B" --tagging 'TagSet=[{Key=project,Value=fenvix-tests}]' && \
echo dryrun > "$OUT/obj.txt" && aws s3api put-object --bucket "$B" --key preflight/obj.txt --body "$OUT/obj.txt" >/dev/null \
  && say "phase1: bucket $B created, tagged, one object written" || { say "phase1 FAILED: could not create the test bucket"; exit 1; }
START=$(date +%s)
say "waiting $WAIT_MIN minutes with no AWS call and no refresh (started $(date -u +%H:%M:%S) UTC)"
while [ $(( $(date +%s) - START )) -lt $((WAIT_MIN*60)) ]; do sleep 60; [ $(( ($(date +%s)-START)/60 % 10 )) -eq 0 ] && echo "  ...$(( ($(date +%s)-START)/60 )) min"; done
if e=$(aws s3api head-bucket --bucket "$B" 2>&1); then say "PROBLEM: the old credentials still work after $WAIT_MIN min (the session did not expire, the test proves nothing)"; echo "stale-credentials-still-valid" > "$OUT/dryrun_unproven"
else say "phase1: old credentials rejected after $WAIT_MIN min, as expected: $(echo "$e" | tr '\n' ' ' | cut -c1-160)"; fi
