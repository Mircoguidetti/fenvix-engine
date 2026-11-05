#!/bin/bash
# Kitchen (K2, from P1 Ireland recupera.sh) · copy the results again with the fresh credentials of the cleanup, BEFORE
# the run bucket is deleted (M31 reviewer, C3). Never fails the job.
set -u
OUT=${CM_OUT:-/tmp/cm-out}; mkdir -p "$OUT"
if aws s3 cp "s3://$BUCKET/results/" "$OUT/" --recursive --only-show-errors; then
  echo "FETCH: $(find "$OUT" -type f | wc -l) files in $OUT"
else
  echo "FETCH: nothing to copy from s3://$BUCKET/results/ (bucket never created or empty)"
fi
[ -n "${CM_ACCT:-}" ] && grep -rl "$CM_ACCT" "$OUT" 2>/dev/null | xargs -r sed -i "s/$CM_ACCT/<ACCOUNT_ID>/g"
exit 0
