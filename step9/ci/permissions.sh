#!/bin/bash
# Step 9 · permission check, $0 (mode "permissions", and before every run): reads, --dry-run launches of every type and
# zone the round uses, an empty bucket created and deleted, security groups created and deleted (free), the vCPU quota.
# Prints "OK" or "MANCA" (missing) for every permission and at the end "PERMESSI: tutti presenti" or the list.
# Variables: RESOLVED (the resolved command of decide.py), MACHINE_ROLE, DISK_GB. Writes VCPU_QUOTA to GITHUB_ENV.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
missing=()
try() { local name=$1; shift; local out; if out=$("$@" 2>&1); then echo "OK    $name"; else
  if grep -q DryRunOperation <<<"$out"; then echo "OK    $name (dry-run)"; else echo "MANCA $name: $(head -c 300 <<<"$out")"; missing+=("$name"); fi; fi; }
r() { python3 "$HERE/../machine/cfg.py" "$RESOLVED" "$1"; }
try "sts:GetCallerIdentity" aws sts get-caller-identity --query Arn --output text
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
[ "$VPC" != None ] && [ -n "$VPC" ] && echo "OK    default VPC" || { echo "MANCA default VPC in eu-west-1"; missing+=("default VPC"); }
declare -A SUB
for z in eu-west-1a eu-west-1b eu-west-1c; do
  SUB[$z]=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true Name=availability-zone,Values="$z" --query 'Subnets[0].SubnetId' --output text 2>/dev/null)
  [[ "${SUB[$z]}" == subnet-* ]] && echo "OK    default subnet in $z" || { echo "MANCA default subnet in $z"; missing+=("subnet $z"); }
done
try "ec2:DescribeInstances" aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --query 'length(Reservations)'
try "ec2:DescribeVolumes" aws ec2 describe-volumes --filters Name=tag:project,Values=fenvix-coldread --query 'length(Volumes)'
try "ec2:DescribeAddresses" aws ec2 describe-addresses --filters Name=tag:project,Values=fenvix-coldread --query 'length(Addresses)'
try "ec2:DescribeSecurityGroups" aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query 'length(SecurityGroups)'
G="fenvix-coldread-s9-perm-${GITHUB_RUN_ID:-0}"
try "ec2:CreateSecurityGroup with the tag (client)" aws ec2 create-security-group --group-name "$G-client" --description "step9 permission check" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
try "ec2:CreateSecurityGroup with the tag (kitchen)" aws ec2 create-security-group --group-name "$G-kitchen" --description "step9 permission check" \
  --vpc-id "$VPC" --tag-specifications "ResourceType=security-group,Tags=[{Key=project,Value=fenvix-coldread}]"
gid() { aws ec2 describe-security-groups --filters Name=group-name,Values="$1" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null; }
GC=$(gid "$G-client"); GK=$(gid "$G-kitchen")
try "ec2:AuthorizeSecurityGroupIngress (27017-27018, 7200 from client and kitchen; 7100 inside the kitchen)" aws ec2 authorize-security-group-ingress --group-id "$GK" \
  --ip-permissions "IpProtocol=tcp,FromPort=27017,ToPort=27018,UserIdGroupPairs=[{GroupId=$GC},{GroupId=$GK}]" \
  "IpProtocol=tcp,FromPort=7200,ToPort=7200,UserIdGroupPairs=[{GroupId=$GC},{GroupId=$GK}]" "IpProtocol=tcp,FromPort=7100,ToPort=7100,UserIdGroupPairs=[{GroupId=$GK}]"
try "ec2:RevokeSecurityGroupIngress (teardown)" aws ec2 revoke-security-group-ingress --group-id "$GK" --ip-permissions "IpProtocol=tcp,FromPort=7100,ToPort=7100,UserIdGroupPairs=[{GroupId=$GK}]"
try "ec2:DeleteSecurityGroup (kitchen)" aws ec2 delete-security-group --group-id "$GK"
try "ec2:DeleteSecurityGroup (client)" aws ec2 delete-security-group --group-id "$GC"
try "iam:GetInstanceProfile ($MACHINE_ROLE)" aws iam get-instance-profile --instance-profile-name "$MACHINE_ROLE" --query 'InstanceProfile.Roles[0].RoleName' --output text
declare -A AMI
AMI[x86_64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
AMI[aarch64]=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
[ -n "${AMI[x86_64]}" ] && [ -n "${AMI[aarch64]}" ] && echo "OK    ssm:GetParameter (Ubuntu 24.04 amd64 and arm64)" || { echo "MANCA ssm:GetParameter"; missing+=("ssm:GetParameter"); }
# every machine of this round, in its zone: dry launch with tag, role and disk (iam:PassRole included)
for pair in "cook eu-west-1a" "spare eu-west-1b" "log eu-west-1a" "log eu-west-1b" "log eu-west-1c" "compactor eu-west-1c" \
            "loader eu-west-1a" "client eu-west-1a" "filler eu-west-1a"; do
  read -r role z <<<"$pair"; t=$(r "$role"); [ -n "$t" ] || continue
  a=$(python3 "$HERE/machine_types.py" arch "$t")
  try "ec2:RunInstances $role $t ($a) in $z" aws ec2 run-instances --dry-run --image-id "${AMI[$a]}" --instance-type "$t" \
    --network-interfaces "DeviceIndex=0,SubnetId=${SUB[$z]},AssociatePublicIpAddress=true" \
    --iam-instance-profile Name="$MACHINE_ROLE" --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${DISK_GB:-20},VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread},{Key=Name,Value=fenvix-coldread-s9-perm}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]"
done
if q=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>&1); then
  need=$(r vcpu_peak)
  if python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)" "$need" "$q"; then
    echo "OK    vCPU quota (on-demand standard, L-1216C47A) $q; this round needs $need at once"
  else echo "MANCA vCPU quota: $q, this round needs $need at once (Service Quotas, L-1216C47A)"; missing+=("vCPU quota $q < $need"); fi
  [ -n "${GITHUB_ENV:-}" ] && echo "VCPU_QUOTA=$q" >> "$GITHUB_ENV"
else echo "MANCA servicequotas:GetServiceQuota: $(head -c 300 <<<"$q")"; missing+=("servicequotas:GetServiceQuota"); fi
B="fenvix-coldread-s9-perm-${GITHUB_RUN_ID:-0}"
try "s3:CreateBucket" aws s3api create-bucket --bucket "$B" --create-bucket-configuration LocationConstraint=eu-west-1
try "s3:PutBucketPublicAccessBlock" aws s3api put-public-access-block --bucket "$B" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
try "s3:PutBucketTagging" aws s3api put-bucket-tagging --bucket "$B" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
try "s3:PutLifecycleConfiguration" aws s3api put-bucket-lifecycle-configuration --bucket "$B" --lifecycle-configuration '{"Rules":[{"ID":"expire","Status":"Enabled","Filter":{},"Expiration":{"Days":1}}]}'
try "s3:PutObject" bash -c "echo ok | aws s3 cp - s3://$B/check.txt"
try "s3:GetObject" aws s3 cp "s3://$B/check.txt" -
try "s3:ListBucket" aws s3 ls "s3://$B/"
try "s3:DeleteObject" aws s3 rm "s3://$B/check.txt"
try "s3:DeleteBucket" aws s3api delete-bucket --bucket "$B"
[ "$(r pantry_mode)" != new ] && try "s3:ListBucket on the kept pantry prefix (fenvix-kitchen-pantry-*)" aws s3api list-buckets --query 'length(Buckets)'
if [ ${#missing[@]} -eq 0 ]; then echo "PERMESSI: tutti presenti" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  { echo "PERMESSI MANCANTI:"; printf -- '- %s\n' "${missing[@]}"; } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
