#!/bin/bash
# Kitchen (K2, corrected by K2b; from P1 Ireland pulizia.sh, same tag and same buckets, plus the security groups) ·
# cleanup: terminates the machines tagged project=fenvix-coldread, deletes the buckets fenvix-coldread-* (the run
# bucket and a pantry that is not kept, with everything inside) and the tagged security groups; then checks.
# A KEPT pantry (fenvix-kitchen-pantry-*, founder's choice A2 "road 1") is NOT deleted here: it has another prefix
# exactly so that the cleanup of the other tests (M31, Ireland: they delete every fenvix-coldread-*) cannot delete it
# either. It goes away with mode "drop-pantry" (DROP_PANTRY=name) or, at the latest, by its lifecycle rule (3 days).
# Prints "RIMASTO: niente" (RULES.md, the words the founder reads) or "ALLARME" with what is left; "NON SO" if it
# cannot read.
set -u
ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null) || { echo "NON SO: cannot read the machines"; exit 1; }
# shellcheck disable=SC2086
if [ -n "$ids" ]; then aws ec2 terminate-instances --instance-ids $ids >/dev/null && echo "machines terminated: $ids"; aws ec2 wait instance-terminated --instance-ids $ids || true; fi
# fenvix-coldread-* (run bucket, pantry not kept, the per-size copies of K3) and the permission check's test bucket
# fenvix-kitchen-pantry-check-* (K7); never a kept pantry fenvix-kitchen-pantry-<digits>
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -E '^fenvix-coldread-|^fenvix-kitchen-pantry-check-'); do
  aws s3 rb "s3://$b" --force >/dev/null && echo "bucket deleted: $b"; done
if [ -n "${DROP_PANTRY:-}" ]; then
  [[ "$DROP_PANTRY" =~ ^fenvix-kitchen-pantry-[0-9]+$ ]] || { echo "ALLARME: drop-pantry name not valid: $DROP_PANTRY"; exit 1; }
  aws s3 rb "s3://$DROP_PANTRY" --force >/dev/null && echo "kept pantry deleted: $DROP_PANTRY"
fi
# security groups: first those that refer to another one (server), then the others; a machine just terminated keeps
# its group for a few seconds (network interface), so retry for up to 3 minutes
groups() { aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query "$1" --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$'; }
for _ in $(seq 1 18); do
  all=$(groups 'SecurityGroups[].GroupId') || true
  [ -z "$all" ] && break
  for g in $(groups 'SecurityGroups[?length(IpPermissions[].UserIdGroupPairs[]) > `0`].GroupId') $all; do
    aws ec2 delete-security-group --group-id "$g" >/dev/null 2>&1 && echo "security group deleted: $g"; done
  [ -z "$(groups 'SecurityGroups[].GroupId')" ] && break
  sleep 10
done
m=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo "?")
b=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -cE '^fenvix-coldread-|^fenvix-kitchen-pantry-check-' || true)
g=$(aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query 'length(SecurityGroups)' --output text 2>/dev/null || echo "?")
for k in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -E '^fenvix-kitchen-pantry-[0-9]+$'); do
  echo "KEPT: pantry $k (founder's choice; its objects expire after 3 days)"; done
if [ "$m" = "0" ] && [ "$b" = "0" ] && [ "$g" = "0" ]; then echo "RIMASTO: niente" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  echo "ALLARME: machines $m, buckets $b, security groups $g" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
