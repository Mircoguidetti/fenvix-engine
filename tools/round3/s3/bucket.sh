#!/usr/bin/env bash
# create | delete the round3 test bucket (fenvix-test-r3-<run id>, new for every run). create: eu-west-1 if the role allows
# it, otherwise eu-central-1 (the region of aws-test3), tag project=fenvix-tests, lifecycle rule expiring every object after
# 1 day. delete: abort multipart uploads, remove every object, delete the bucket, check it is gone. AWS error messages are
# reduced to their error code (never an account id or ARN). Writes results-round3/real-s3/bucket_<action>.json and, on
# create, the region chosen to $GITHUB_ENV (ROUND3_REGION).
set -u
B=fenvix-test-r3-${GITHUB_RUN_ID:?}; OUT=results-round3/real-s3; mkdir -p $OUT; act=${1:?create|delete}
code() { grep -o 'An error occurred ([A-Za-z]*)' <<<"$1" | head -1; }
res=$OUT/bucket_$act.json; steps=()
step() { steps+=("{\"step\":\"$1\",\"ok\":$2,\"error\":\"$3\"}"); }
if [ "$act" = create ]; then
  R=""
  for r in eu-west-1 eu-central-1; do
    o=$(aws s3api create-bucket --bucket "$B" --region $r --create-bucket-configuration LocationConstraint=$r 2>&1) && { step "create-bucket-$r" true ""; R=$r; break; } || step "create-bucket-$r" false "$(code "$o")"
  done
  if [ -n "$R" ]; then
    echo "ROUND3_REGION=$R" >> "$GITHUB_ENV"
    o=$(aws s3api put-bucket-tagging --region $R --bucket "$B" --tagging 'TagSet=[{Key=project,Value=fenvix-tests},{Key=run,Value=round3}]' 2>&1) && step tag true "" || { step tag false "$(code "$o")"; fail=1; }
    o=$(aws s3api put-bucket-lifecycle-configuration --region $R --bucket "$B" --lifecycle-configuration '{"Rules":[{"ID":"expire-1-day","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}' 2>&1) \
      && step lifecycle-1-day true "" || { step lifecycle-1-day false "$(code "$o")"; fail=1; }
  else fail=1; fi
else
  R=${ROUND3_REGION:-eu-west-1}
  if aws s3api head-bucket --region $R --bucket "$B" >/dev/null 2>&1; then
    for u in $(aws s3api list-multipart-uploads --region $R --bucket "$B" --query 'Uploads[].[Key,UploadId]' --output text 2>/dev/null | awk '{print $1"|"$2}'); do
      aws s3api abort-multipart-upload --region $R --bucket "$B" --key "${u%%|*}" --upload-id "${u##*|}" >/dev/null 2>&1; done
    o=$(aws s3 rm "s3://$B" --region $R --recursive --only-show-errors 2>&1) && step empty true "" || step empty false "$(code "$o")"
    left=$(aws s3api list-objects-v2 --region $R --bucket "$B" --query 'KeyCount' --output text 2>/dev/null); step "objects-left-after-empty:${left:-?}" true ""
    o=$(aws s3api delete-bucket --bucket "$B" --region $R 2>&1) && step delete-bucket true "" || step delete-bucket false "$(code "$o")"
  else step "bucket-not-found" true ""; fi
  if aws s3api head-bucket --region $R --bucket "$B" >/dev/null 2>&1; then step verify-gone false "bucket still exists"; fail=1; else step verify-gone true ""; fi
fi
( IFS=,; echo "{\"bucket\":\"$B\",\"action\":\"$act\",\"region\":\"${R:-}\",\"steps\":[${steps[*]}]}" ) > $res
cat $res
[ -z "${fail:-}" ]
