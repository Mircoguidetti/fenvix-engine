#!/bin/bash
# Kitchen (K2, corrected by K2b; from P1 Ireland permessi.sh) · permission check, $0: reads, --dry-run, an empty bucket
# created and deleted at once, security groups created and deleted (they are free), the vCPU quota.
# Prints "OK" or "MANCA" (missing) for every permission, and at the end "PERMESSI: tutti presenti" or the list.
# Variables: SIZES (JSON list), LOADER (type of cook 0, empty if this run does not load), CLIENT (from the program),
# MACHINE_ROLE, DISK_GB, PANTRY_CHECK (1 = also the kept-pantry bucket fenvix-kitchen-pantry-*, founder's road 1 of
# A2: it needs the new IAM lines of the K2b report). If GITHUB_ENV is set, writes NEEDED_VCPU there.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
missing=()
try() { local name=$1; shift; if out=$("$@" 2>&1); then echo "OK    $name"; else
  if grep -q DryRunOperation <<<"$out"; then echo "OK    $name (dry-run)"; else echo "MANCA $name: $(head -c 300 <<<"$out")"; missing+=("$name"); fi; fi; }
try "sts:GetCallerIdentity" aws sts get-caller-identity --query Arn --output text
try "ssm:GetParameter (AMI Ubuntu 24.04 amd64)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
try "ssm:GetParameter (AMI Ubuntu 24.04 arm64, Graviton)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
try "ec2:DescribeVpcs" aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
[ "$VPC" != "None" ] && [ -n "$VPC" ] || { echo "MANCA default VPC in eu-west-1"; missing+=("default VPC"); }
try "ec2:DescribeSubnets" aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text
try "ec2:DescribeInstances" aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --query 'length(Reservations)'
try "ec2:DescribeVolumes (final check)" aws ec2 describe-volumes --filters Name=tag:project,Values=fenvix-coldread --query 'length(Volumes)'
try "ec2:DescribeAddresses (final check)" aws ec2 describe-addresses --filters Name=tag:project,Values=fenvix-coldread --query 'length(Addresses)'
try "ec2:DescribeSecurityGroups (cleanup and final check)" aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query 'length(SecurityGroups)'
G="fenvix-coldread-cm-perm-${GITHUB_RUN_ID:-0}"
try "ec2:CreateSecurityGroup with the tag (client)" aws ec2 create-security-group --group-name "$G-client" --description "kitchen permission check" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
try "ec2:CreateSecurityGroup with the tag (server)" aws ec2 create-security-group --group-name "$G-server" --description "kitchen permission check" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
gid() { aws ec2 describe-security-groups --filters Name=group-name,Values="$1" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null; }
GC=$(gid "$G-client"); GS=$(gid "$G-server")
try "ec2:AuthorizeSecurityGroupIngress (27017 only from the client group)" aws ec2 authorize-security-group-ingress --group-id "$GS" \
  --ip-permissions "IpProtocol=tcp,FromPort=27017,ToPort=27017,UserIdGroupPairs=[{GroupId=$GC}]"
try "ec2:DeleteSecurityGroup (server)" aws ec2 delete-security-group --group-id "$GS"
try "ec2:DeleteSecurityGroup (client)" aws ec2 delete-security-group --group-id "$GC"
try "iam:GetInstanceProfile ($MACHINE_ROLE)" aws iam get-instance-profile --instance-profile-name "$MACHINE_ROLE" --query 'InstanceProfile.Roles[0].RoleName' --output text
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text 2>/dev/null)
declare -A AMI
AMI[x86_64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
AMI[aarch64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
# every allowed cook and every client machine: dry launch, with tag, role and disk (iam:PassRole included)
for t in c7i.large c7i.xlarge m7i.large c7g.medium c7g.large m7g.large t4g.small c7i.2xlarge c7i.4xlarge c7i.8xlarge; do
  try "ec2:RunInstances $t ($(python3 "$HERE/machine_types.py" arch "$t")) with tag, role and disk" aws ec2 run-instances --dry-run \
    --image-id "${AMI[$(python3 "$HERE/machine_types.py" arch "$t")]}" --instance-type "$t" --subnet-id "$SUBNET" \
    --associate-public-ip-address --iam-instance-profile Name="$MACHINE_ROLE" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${DISK_GB:-20},VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-cm-perm}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]"
done
# the on-demand vCPU quota (standard families, Graviton included: code L-1216C47A, DECLARED by AWS). RunInstances
# --dry-run does NOT check it. During a run the client machine and ONE cook are on together (cooks go one at a time).
# The client is the one of the program (code-irlanda macchina.txt: c7i.2xlarge with 5 copies for m30-corto; reviewer's
# A5): with a smaller client the maximum load would not be one to one, so there is no automatic smaller choice.
if q=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>&1); then
  if r=$(python3 "$HERE/machine_types.py" client "$q" "${SIZES:-[]}" "${LOADER:-}" "$CLIENT"); then
    read -r _ needed <<<"$r"
    echo "OK    servicequotas:GetServiceQuota: on-demand standard vCPU quota $q; client $CLIENT plus the biggest cook need $needed vCPU together"
    [ -n "${GITHUB_ENV:-}" ] && echo "NEEDED_VCPU=$needed" >> "$GITHUB_ENV"
  else
    echo "MANCA vCPU quota: $r. Ask for the increase (Service Quotas, L-1216C47A)"; missing+=("vCPU quota $q")
  fi
else
  echo "MANCA servicequotas:GetServiceQuota: $(head -c 300 <<<"$q")"; missing+=("servicequotas:GetServiceQuota")
fi
# ec2:TerminateInstances is not tried here: with the tag condition, a --dry-run on a fake machine says nothing.
bucket_try() { # bucket_try NAME LABEL
  local P=$1 L=$2
  try "s3:CreateBucket ($L)" aws s3api create-bucket --bucket "$P" --create-bucket-configuration LocationConstraint=eu-west-1
  try "s3:PutBucketPublicAccessBlock ($L)" aws s3api put-public-access-block --bucket "$P" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  try "s3:PutBucketTagging ($L)" aws s3api put-bucket-tagging --bucket "$P" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
  try "s3:PutLifecycleConfiguration ($L)" aws s3api put-bucket-lifecycle-configuration --bucket "$P" --lifecycle-configuration '{"Rules":[{"ID":"expire","Status":"Enabled","Filter":{},"Expiration":{"Days":1}}]}'
  try "s3:PutObject ($L)" bash -c "echo ok | aws s3 cp - s3://$P/check.txt"
  try "s3:GetObject ($L)" aws s3 cp "s3://$P/check.txt" -
  try "s3:ListBucket ($L)" aws s3 ls "s3://$P/"
  try "s3:DeleteObject ($L)" aws s3 rm "s3://$P/check.txt"
  try "s3:DeleteBucket ($L)" aws s3api delete-bucket --bucket "$P"
}
bucket_try "fenvix-coldread-cm-perm-${GITHUB_RUN_ID:-0}" "run bucket"
[ "${PANTRY_CHECK:-0}" = 1 ] && bucket_try "fenvix-kitchen-pantry-${GITHUB_RUN_ID:-0}0" "kept pantry, needs the new IAM lines"
try "s3:ListAllMyBuckets" aws s3api list-buckets --query 'length(Buckets)'
if [ ${#missing[@]} -eq 0 ]; then echo "PERMESSI: tutti presenti" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  { echo "PERMESSI MANCANTI:"; printf -- '- %s\n' "${missing[@]}"; } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
