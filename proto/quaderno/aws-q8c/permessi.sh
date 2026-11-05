#!/bin/bash
# Q8c (copiato da Q8 e M31; prova i due tipi di macchina TIPO_REPLICA e TIPO_GEN) · controllo dei permessi, 0 $: letture, --dry-run e un bucket vuoto creato e cancellato subito.
# Scrive per ogni permesso "OK" o "MANCA", e alla fine l'elenco di quello che manca.
set -u
manca=()
prova() { local nome=$1; shift; if out=$("$@" 2>&1); then echo "OK    $nome"; else
  if grep -q DryRunOperation <<<"$out"; then echo "OK    $nome (dry-run)"; else echo "MANCA $nome: $(head -c 300 <<<"$out")"; manca+=("$nome"); fi; fi; }
prova "sts:GetCallerIdentity" aws sts get-caller-identity --query Arn --output text
prova "ssm:GetParameter (AMI Ubuntu 24.04 pubblica)" aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
prova "ec2:DescribeVpcs" aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo None)
[ "$VPC" != "None" ] && [ -n "$VPC" ] || { echo "MANCA VPC di serie in eu-west-1"; manca+=("VPC di serie"); }
prova "ec2:DescribeSubnets" aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text
prova "ec2:DescribeInstances" aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread --query 'length(Reservations)'
prova "ec2:DescribeVolumes (controllo finale)" aws ec2 describe-volumes --filters Name=tag:project,Values=fenvix-coldread --query 'length(Volumes)'
prova "ec2:DescribeAddresses (controllo finale)" aws ec2 describe-addresses --filters Name=tag:project,Values=fenvix-coldread --query 'length(Addresses)'
prova "iam:GetInstanceProfile ($RUOLO_MACCHINA, creato a mano dal fondatore)" aws iam get-instance-profile --instance-profile-name "$RUOLO_MACCHINA" --query 'InstanceProfile.Roles[0].RoleName' --output text
SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=default-for-az,Values=true --query 'sort_by(Subnets,&AvailabilityZone)[0].SubnetId' --output text 2>/dev/null)
AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text 2>/dev/null)
for TIPO in $(echo "$TIPO_REPLICA $TIPO_GEN" | tr ' ' '\n' | sort -u); do
prova "ec2:RunInstances $TIPO con tag, ruolo e disco (iam:PassRole compreso)" aws ec2 run-instances --dry-run --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" \
  --associate-public-ip-address --iam-instance-profile Name="$RUOLO_MACCHINA" --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}' \
  --tag-specifications "ResourceType=instance,Tags=[{Key=project,Value=fenvix-coldread}]" "ResourceType=volume,Tags=[{Key=project,Value=fenvix-coldread}]"
done
# Q8c: vCPU quota (Running On-Demand Standard instances) against what the day needs: 3 x TIPO_REPLICA + 1 x TIPO_GEN
Q=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>/dev/null | cut -d. -f1)
VR=$(aws ec2 describe-instance-types --instance-types "$TIPO_REPLICA" --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' --output text 2>/dev/null)
VG=$(aws ec2 describe-instance-types --instance-types "$TIPO_GEN" --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' --output text 2>/dev/null)
if [[ "$Q" =~ ^[0-9]+$ && "$VR" =~ ^[0-9]+$ && "$VG" =~ ^[0-9]+$ ]]; then
  SERVE=$((3 * VR + VG)); echo "QUOTA vCPU: $Q, serve $SERVE (3 x $TIPO_REPLICA + $TIPO_GEN)"
  [ "$SERVE" -le "$Q" ] || { echo "MANCA quota vCPU: $SERVE > $Q"; manca+=("quota vCPU $SERVE > $Q"); }
else echo "NON SO la quota vCPU (servicequotas:GetServiceQuota o ec2:DescribeInstanceTypes non permessi): controllare a mano"; fi
# ec2:TerminateInstances non si prova qui: con la condizione sul tag un --dry-run su una macchina finta non dice niente.
P=fenvix-coldread-permessi-${GITHUB_RUN_ID:-0}
prova "s3:CreateBucket" aws s3api create-bucket --bucket "$P" --create-bucket-configuration LocationConstraint=eu-west-1
prova "s3:PutBucketPublicAccessBlock" aws s3api put-public-access-block --bucket "$P" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
prova "s3:PutBucketTagging" aws s3api put-bucket-tagging --bucket "$P" --tagging "TagSet=[{Key=project,Value=fenvix-coldread}]"
prova "s3:PutLifecycleConfiguration" aws s3api put-bucket-lifecycle-configuration --bucket "$P" --lifecycle-configuration '{"Rules":[{"ID":"scade","Status":"Enabled","Filter":{},"Expiration":{"Days":1}}]}'
prova "s3:PutObject" bash -c "echo ok | aws s3 cp - s3://$P/prova.txt"
prova "s3:GetObject" aws s3 cp "s3://$P/prova.txt" -
prova "s3:ListBucket" aws s3 ls "s3://$P/"
prova "s3:DeleteObject" aws s3 rm "s3://$P/prova.txt"
prova "s3:DeleteBucket" aws s3api delete-bucket --bucket "$P"
prova "s3:ListAllMyBuckets" aws s3api list-buckets --query 'length(Buckets)'
if [ ${#manca[@]} -eq 0 ]; then echo "PERMESSI: tutti presenti" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  { echo "PERMESSI MANCANTI:"; printf -- '- %s\n' "${manca[@]}"; } | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
