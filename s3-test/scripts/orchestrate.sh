#!/usr/bin/env bash
# Runner side of the S3 tests (GitHub Actions). One run = at most one c6i.large. Everything created here carries the
# tag project=fenvix-tests and is removed by cleanup.sh (workflow step with `if: always()`).
#
# Every AWS action is tried through `attempt`, which records a failure with the exact IAM action and resource it
# needed and CONTINUES, so one run lists all missing permissions. Nothing is worked around.
# Steps that depend on a failed one are marked SKIPPED (not counted as missing permissions).
set -u
cd "${GITHUB_WORKSPACE:-.}"
R=eu-central-1
. "$(dirname "$0")/aws_refresh.sh"; export AWS_CREDS_AT=$(date +%s)   # the workflow logged in just before this step
OUT=run-out; mkdir -p "$OUT"
ERR=$OUT/errors.jsonl; : > "$ERR"
STEPS=$OUT/steps.jsonl; : > "$STEPS"
TESTS=$(jq -r '.tests|map(tostring)|join(",")' s3-test/run.json)
BUCKET=fenvix-test-${GITHUB_RUN_ID:?}
STATE=$OUT/state.env
ACCT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo unknown)
echo "BUCKET=$BUCKET" > "$STATE"
LAST=""

jstep() { jq -cn --arg n "$1" --arg s "$2" --arg d "${3:-}" '{step:$n,status:$s,detail:$d}' >> "$STEPS"; }

# attempt <name> <iam action(s)> <resource> <command...>   -> $LAST holds stdout on success
attempt() {
  local name=$1 act=$2 res=$3; shift 3
  local o
  refresh_aws_creds || true
  if o=$("$@" 2>&1); then LAST=$o; echo "OK      $name"; jstep "$name" ok; return 0; fi
  echo "FAILED  $name: $o"
  local reported; reported=$(printf '%s' "$o" | grep -oiE 'not authorized to perform[^"]{0,200}' | head -1)
  local denied=false; printf '%s' "$o" | grep -qiE 'AccessDenied|UnauthorizedOperation|not authorized|AuthFailure' && denied=true
  jq -cn --arg n "$name" --arg a "$act" --arg r "$res" --arg m "$o" --arg rep "$reported" --argjson d "$denied" \
    '{step:$n,access_denied:$d,iam_action:$a,resource:$r,reported_by_aws:$rep,message:$m}' >> "$ERR"
  jstep "$name" failed "$o"
  LAST=""; return 1
}
skipped() { echo "SKIPPED $1 ($2)"; jstep "$1" skipped "$2"; }

echo "== account $ACCT, run $GITHUB_RUN_ID, tests $TESTS, bucket $BUCKET"
LEDGER_OK=1

# ---- S3 (independent of EC2) -------------------------------------------------------------------------------------
BUCKET_OK=0; INST_OK=1
attempt "s3 CreateBucket" "s3:CreateBucket" "arn:aws:s3:::$BUCKET" aws s3api create-bucket --bucket "$BUCKET" --region $R --create-bucket-configuration LocationConstraint=$R && BUCKET_OK=1
if [ $BUCKET_OK = 1 ]; then
  attempt "s3 PutBucketTagging" "s3:PutBucketTagging" "arn:aws:s3:::$BUCKET" aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging 'TagSet=[{Key=project,Value=fenvix-tests}]'
  echo "hello $GITHUB_RUN_ID" > "$OUT/obj.txt"
  attempt "s3 PutObject" "s3:PutObject" "arn:aws:s3:::$BUCKET/preflight/obj.txt" aws s3api put-object --bucket "$BUCKET" --key preflight/obj.txt --body "$OUT/obj.txt"
  attempt "s3 GetObject" "s3:GetObject" "arn:aws:s3:::$BUCKET/preflight/obj.txt" aws s3api get-object --bucket "$BUCKET" --key preflight/obj.txt "$OUT/obj.back"
  attempt "s3 ListBucket" "s3:ListBucket" "arn:aws:s3:::$BUCKET" aws s3api list-objects-v2 --bucket "$BUCKET" --prefix preflight/
  attempt "s3 DeleteObject" "s3:DeleteObject" "arn:aws:s3:::$BUCKET/preflight/obj.txt" aws s3api delete-object --bucket "$BUCKET" --key preflight/obj.txt
else
  for s in "s3 PutBucketTagging" "s3 PutObject" "s3 GetObject" "s3 ListBucket" "s3 DeleteObject"; do skipped "$s" "bucket not created"; done
fi

# ---- EC2 pre-conditions -------------------------------------------------------------------------------------------
AMI=""
attempt "ec2 DescribeImages (AL2023)" "ec2:DescribeImages" "*" aws ec2 describe-images --region $R --owners amazon \
  --filters 'Name=name,Values=al2023-ami-2023.*-x86_64' 'Name=state,Values=available' 'Name=architecture,Values=x86_64' \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text && AMI=$LAST || INST_OK=0
[ "$AMI" = None ] && { AMI=""; INST_OK=0; }
attempt "ec2 DescribeVpcs (default VPC exists)" "ec2:DescribeVpcs" "*" aws ec2 describe-vpcs --region $R --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text || true
RUNNING=""
if attempt "ec2 DescribeInstances (one at a time)" "ec2:DescribeInstances" "*" aws ec2 describe-instances --region $R \
     --filters Name=tag:project,Values=fenvix-tests Name=instance-state-name,Values=pending,running --query 'Reservations[].Instances[].InstanceId' --output text; then
  RUNNING=$LAST
  if [ -n "$RUNNING" ] && [ "$RUNNING" != None ]; then
    echo "REFUSED: instance(s) already pending/running with tag project=fenvix-tests: $RUNNING"
    jstep "one instance at a time" failed "already running: $RUNNING"; INST_OK=0
  fi
else INST_OK=0; fi

UD=$OUT/userdata.sh
cat > "$UD" <<'U'
#!/bin/bash
# Absolute timer first: whatever happens afterwards, the box powers off (and, with shutdown behaviour = terminate,
# is terminated) 90 minutes after boot.
shutdown -h +90
U
RUN_ARGS=(--region $R --image-id "$AMI" --instance-type c6i.large --count 1 --iam-instance-profile Name=fenvix-test-instance
  --instance-initiated-shutdown-behavior terminate --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled
  --user-data "file://$UD" --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=8,VolumeType=gp3,DeleteOnTermination=true}'
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-tests},{Key=Name,Value=fenvix-test-$GITHUB_RUN_ID}]" \
                       "ResourceType=volume,Tags=[{Key=project,Value=fenvix-tests}]")
if [ -n "$AMI" ]; then
  o=$(aws ec2 run-instances "${RUN_ARGS[@]}" --dry-run 2>&1)
  if printf '%s' "$o" | grep -q DryRunOperation; then echo "OK      ec2 RunInstances (dry run)"; jstep "ec2 RunInstances (dry run)" ok
  else
    echo "FAILED  ec2 RunInstances dry run: $o"; INST_OK=0
    rep=$(printf '%s' "$o" | grep -oiE 'not authorized to perform[^"]{0,200}' | head -1)
    jq -cn --arg m "$o" --arg rep "$rep" '{step:"ec2 RunInstances (dry run)",access_denied:true,iam_action:"ec2:RunInstances (instance, image, subnet, security-group, network-interface, volume resources) + ec2:CreateTags on them",resource:"arn:aws:ec2:eu-central-1:*:instance/*, image/*, subnet/*, security-group/*, network-interface/*, volume/*",reported_by_aws:$rep,message:$m}' >> "$ERR"
    jstep "ec2 RunInstances (dry run)" failed "$o"
  fi
fi
# iam:PassRole is not always checked by a dry run; a real launch is the proof. Listed as REQUIRED either way.
echo "REQUIRED (not provable by dry run): iam:PassRole on the role inside instance profile fenvix-test-instance"

if [ "$INST_OK" != 1 ] || [ $BUCKET_OK != 1 ]; then
  for s in "ec2 RunInstances" "ec2 wait running" "ssm instance registered" "ssm SendCommand proof" "s3 proof file downloaded" "engine tests on instance" "ec2 TerminateInstances"; do skipped "$s" "a prerequisite failed (see errors above)"; done
  echo "NOINSTANCE=1" >> "$STATE"
else
  # ---- build bundle (made earlier by build.sh into run-out/bundle.tgz) and upload -------------------------------------
  attempt "s3 PutObject (bundle)" "s3:PutObject" "arn:aws:s3:::$BUCKET/_s3test/bundle.tgz" aws s3 cp "$OUT/bundle.tgz" "s3://$BUCKET/_s3test/bundle.tgz" --only-show-errors || INST_OK=0
  # ---- launch ------------------------------------------------------------------------------------------------------
  ID=""
  if [ "$INST_OK" = 1 ]; then
    # re-check right before launching: still nothing pending/running
    again=$(aws ec2 describe-instances --region $R --filters Name=tag:project,Values=fenvix-tests Name=instance-state-name,Values=pending,running --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$again" ] && [ "$again" != None ]; then echo "REFUSED at launch: $again already running"; jstep "ec2 RunInstances" failed "already running $again"; INST_OK=0
    else
      attempt "ec2 RunInstances" "ec2:RunInstances, ec2:CreateTags, iam:PassRole" "instance/*, image/$AMI, role of instance profile fenvix-test-instance" \
        aws ec2 run-instances "${RUN_ARGS[@]}" --query 'Instances[0].InstanceId' --output text && ID=$LAST || INST_OK=0
    fi
  fi
  if [ -n "$ID" ]; then
    echo "INSTANCE=$ID" >> "$STATE"; echo "LAUNCH_EPOCH=$(date +%s)" >> "$STATE"
    attempt "ec2 wait instance-running" "ec2:DescribeInstances" "*" aws ec2 wait instance-running --region $R --instance-ids "$ID" || INST_OK=0
  else
    for s in "ec2 wait running" "ssm instance registered" "ssm SendCommand proof" "s3 proof file downloaded" "engine tests on instance"; do skipped "$s" "no instance"; done
  fi
  # ---- SSM sees the instance ---------------------------------------------------------------------------------------
  if [ -n "$ID" ] && [ "$INST_OK" = 1 ]; then
    seen=0
    for _ in $(seq 1 40); do
      o=$(aws ssm describe-instance-information --region $R --filters "Key=InstanceIds,Values=$ID" --query 'InstanceInformationList[0].PingStatus' --output text 2>&1)
      if [ "$o" = Online ]; then seen=1; break; fi
      if printf '%s' "$o" | grep -qi 'AccessDenied\|not authorized'; then break; fi
      sleep 15
    done
    if [ $seen = 1 ]; then echo "OK      ssm instance registered"; jstep "ssm instance registered" ok
    else
      echo "FAILED  ssm instance registered: last answer: $o"
      jq -cn --arg m "$o" '{step:"ssm instance registered",access_denied:false,iam_action:"ssm:DescribeInstanceInformation (workflow role); the INSTANCE role also needs AmazonSSMManagedInstanceCore-equivalent permissions and a route to the SSM endpoints",resource:"*",reported_by_aws:"",message:$m}' >> "$ERR"
      jstep "ssm instance registered" failed "$o"; INST_OK=0
    fi
  fi
  # ---- SSM command 1: writes a file to S3 from the instance ---------------------------------------------------------
  ssm_run() { # <name> <commands-json-array> <timeout-secs>  -> sets LAST to the command id; waits for completion
    local name=$1 cmds=$2 to=${3:-600} cid st
    cid=$(aws ssm send-command --region $R --instance-ids "$ID" --document-name AWS-RunShellScript --timeout-seconds "$to" \
      --parameters "commands=$cmds" --query Command.CommandId --output text 2>&1) || {
        echo "FAILED  $name: $cid"
        jq -cn --arg n "$name" --arg m "$cid" '{step:$n,access_denied:true,iam_action:"ssm:SendCommand",resource:"arn:aws:ssm:eu-central-1::document/AWS-RunShellScript and arn:aws:ec2:eu-central-1:*:instance/* (tag project=fenvix-tests)",reported_by_aws:"",message:$m}' >> "$ERR"; jstep "$name" failed "$cid"; return 1; }
    for _ in $(seq 1 $((to/5+6))); do
      st=$(aws ssm get-command-invocation --region $R --command-id "$cid" --instance-id "$ID" --query Status --output text 2>&1)
      case $st in Success) echo "OK      $name"; jstep "$name" ok; LAST=$cid; return 0;;
        Failed|Cancelled|TimedOut) echo "FAILED  $name: $st"; jstep "$name" failed "$st"; return 1;;
        *AccessDenied*|*not\ authorized*) echo "FAILED $name: $st"
          jq -cn --arg n "$name" --arg m "$st" '{step:$n,access_denied:true,iam_action:"ssm:GetCommandInvocation",resource:"arn:aws:ssm:eu-central-1:*:*",reported_by_aws:"",message:$m}' >> "$ERR"; jstep "$name" failed "$st"; return 1;; esac
      sleep 5; refresh_aws_creds || true
    done
    echo "FAILED  $name: still $st"; jstep "$name" failed "timeout"; return 1
  }
  NONCE="n$GITHUB_RUN_ID-$RANDOM"
  if [ -n "$ID" ] && [ "$INST_OK" = 1 ]; then
    ssm_run "ssm SendCommand proof" "[\"echo $NONCE-\$(cat /var/lib/cloud/data/instance-id) | aws s3 cp - s3://$BUCKET/_s3test/ssm-proof.txt --region $R --only-show-errors\"]" 300 || INST_OK=0
    if [ "$INST_OK" = 1 ]; then
      if attempt "s3 proof file downloaded" "s3:GetObject" "arn:aws:s3:::$BUCKET/_s3test/ssm-proof.txt" aws s3 cp "s3://$BUCKET/_s3test/ssm-proof.txt" "$OUT/ssm-proof.txt" --only-show-errors \
         && grep -q "^$NONCE-" "$OUT/ssm-proof.txt"; then echo "        proof content: $(cat "$OUT/ssm-proof.txt")"
      else echo "proof file missing or wrong"; INST_OK=0; fi
    fi
  fi
  # ---- start the tests on the instance, watched by watchdog.sh ---------------------------------------------------------
  if [ -n "$ID" ] && [ "$INST_OK" = 1 ]; then
    bash s3-test/scripts/watchdog.sh "$ID" "$BUCKET" "$OUT" & WD=$!
    START="aws s3 cp s3://$BUCKET/_s3test/bundle.tgz /tmp/b.tgz --region $R --only-show-errors && mkdir -p /opt/s3test && tar xzf /tmp/b.tgz -C /opt/s3test && chmod +x /opt/s3test/scripts/*.sh && (setsid nohup bash /opt/s3test/scripts/run.sh $BUCKET $TESTS >/dev/null 2>&1 < /dev/null &) ; echo started"
    if ssm_run "start run.sh on instance" "$(jq -cn --arg c "$START" '[$c]')" 300; then
      done_=0
      for _ in $(seq 1 600); do   # up to 100 minutes; the instance's own timer ends at 90
        refresh_aws_creds || true
        if aws s3api head-object --bucket "$BUCKET" --key _s3test/out/DONE >/dev/null 2>&1; then done_=1; break; fi
        [ -f "$OUT/watchdog.stopped" ] && break
        sleep 10
      done
      if [ $done_ = 1 ]; then echo "OK      engine tests on instance finished"; jstep "engine tests on instance" ok
      else echo "FAILED  engine tests on instance: no DONE marker (stopped by guard/watchdog or timer)"; jstep "engine tests on instance" failed "no DONE marker"; fi
    else INST_OK=0; fi
    kill $WD 2>/dev/null; wait $WD 2>/dev/null
  fi
  refresh_aws_creds 0 || true   # fresh credentials for the final downloads
  mkdir -p "$OUT/instance"
  aws s3 cp "s3://$BUCKET/_s3test/" "$OUT/instance/" --recursive --exclude "bundle.tgz" --only-show-errors 2>/dev/null || true
fi
echo "ERRORS: $(wc -l < "$ERR") recorded"
