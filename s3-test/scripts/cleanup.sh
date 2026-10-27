#!/usr/bin/env bash
# Workflow step with `if: always()`: terminate every instance tagged project=fenvix-tests that is pending or running,
# then empty and delete the run's bucket (and any other bucket fenvix-test-<this run>). Never fails the job; prints
# what it could not do so it is visible in the log. Needs: ec2:DescribeInstances, ec2:TerminateInstances (tag condition),
# s3:ListBucket, s3:DeleteObject, s3:DeleteBucket.
set -u
R=eu-central-1; OUT=${1:-run-out}; B=fenvix-test-${GITHUB_RUN_ID:?}
ids=$(aws ec2 describe-instances --region $R --filters Name=tag:project,Values=fenvix-tests Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>&1)
if [ -n "$ids" ] && ! printf '%s' "$ids" | grep -qiE 'error|denied|unauthorized'; then
  echo "terminating: $ids"; t=$(aws ec2 terminate-instances --region $R --instance-ids $ids 2>&1); echo "$t" | tail -3
  printf '%s' "$t" | grep -qiE 'denied|unauthorized|not authorized' && jq -cn --arg m "$t" '{step:"ec2 TerminateInstances",access_denied:true,iam_action:"ec2:TerminateInstances",resource:"arn:aws:ec2:eu-central-1:*:instance/* (condition aws:ResourceTag/project=fenvix-tests)",reported_by_aws:"",message:$m}' >> "$OUT/errors.jsonl"
  aws ec2 wait instance-terminated --region $R --instance-ids $ids 2>&1 | tail -2
else echo "describe-instances: ${ids:-none}"; fi
if aws s3api head-bucket --bucket "$B" 2>/dev/null; then
  r1=$(aws s3 rm "s3://$B" --recursive --only-show-errors 2>&1); echo "$r1" | tail -3
  r2=$(aws s3api delete-bucket --bucket "$B" --region $R 2>&1); [ -z "$r2" ] && echo "bucket $B deleted" || echo "$r2" | tail -3
  printf '%s%s' "$r1" "$r2" | grep -qiE 'denied|unauthorized|not authorized' && jq -cn --arg m "$r1 $r2" '{step:"s3 empty+delete bucket",access_denied:true,iam_action:"s3:ListBucket, s3:DeleteObject, s3:DeleteBucket",resource:"arn:aws:s3:::fenvix-test-*",reported_by_aws:"",message:$m}' >> "$OUT/errors.jsonl"
else echo "bucket $B does not exist (or not reachable)"; fi
left=$(aws ec2 describe-instances --region $R --filters Name=tag:project,Values=fenvix-tests Name=instance-state-name,Values=pending,running --query 'Reservations[].Instances[].InstanceId' --output text 2>&1)
echo "instances still pending/running after cleanup: ${left:-none}" | tee "$OUT/cleanup.txt"
exit 0
