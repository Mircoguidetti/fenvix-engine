#!/usr/bin/env bash
# create | delete the test bucket (name fenvix-test-t3-<run id>, new for every run). create: tag project=fenvix-tests and a
# lifecycle rule that expires every object after 1 day (safety net). delete: abort open multipart uploads, remove every
# object, delete the bucket, then check it is gone. Never prints account ids or ARNs: AWS error messages are reduced
# to their error code. Writes results-aws-test3/bucket_<action>.json.
set -u
R=eu-central-1; B=fenvix-test-t3-${GITHUB_RUN_ID:?}; OUT=results-aws-test3; mkdir -p $OUT; act=${1:?create|delete}
code() { grep -o 'An error occurred ([A-Za-z]*)' <<<"$1" | head -1; }
res=$OUT/bucket_$act.json; steps=()
step() { steps+=("{\"step\":\"$1\",\"ok\":$2,\"error\":\"$3\"}"); }
if [ "$act" = create ]; then
  o=$(aws s3api create-bucket --bucket "$B" --region $R --create-bucket-configuration LocationConstraint=$R 2>&1) && step create-bucket true "" || { step create-bucket false "$(code "$o")"; fail=1; }
  if [ -z "${fail:-}" ]; then
    o=$(aws s3api put-bucket-tagging --bucket "$B" --tagging 'TagSet=[{Key=project,Value=fenvix-tests}]' 2>&1) && step tag true "" || { step tag false "$(code "$o")"; fail=1; }
    o=$(aws s3api put-bucket-lifecycle-configuration --bucket "$B" --lifecycle-configuration '{"Rules":[{"ID":"expire-1-day","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}' 2>&1) \
      && step lifecycle-1-day true "" || { step lifecycle-1-day false "$(code "$o") (needs s3:PutLifecycleConfiguration on the test bucket)"; fail=1; }
  fi
else
  if aws s3api head-bucket --bucket "$B" >/dev/null 2>&1; then
    for u in $(aws s3api list-multipart-uploads --bucket "$B" --query 'Uploads[].[Key,UploadId]' --output text 2>/dev/null | awk '{print $1"|"$2}'); do
      aws s3api abort-multipart-upload --bucket "$B" --key "${u%%|*}" --upload-id "${u##*|}" >/dev/null 2>&1; done
    o=$(aws s3 rm "s3://$B" --recursive --only-show-errors 2>&1) && step empty true "" || step empty false "$(code "$o")"
    left=$(aws s3api list-objects-v2 --bucket "$B" --query 'KeyCount' --output text 2>/dev/null); step "objects-left-after-empty:${left:-?}" true ""
    o=$(aws s3api delete-bucket --bucket "$B" --region $R 2>&1) && step delete-bucket true "" || step delete-bucket false "$(code "$o")"
  else step "bucket-not-found" true ""; fi
  if aws s3api head-bucket --bucket "$B" >/dev/null 2>&1; then step verify-gone false "bucket still exists"; fail=1; else step verify-gone true ""; fi
fi
( IFS=,; echo "{\"bucket\":\"$B\",\"action\":\"$act\",\"steps\":[${steps[*]}]}" ) > $res
cat $res
[ -z "${fail:-}" ]
