#!/bin/bash
# Q8: pulizia (come M31): cancella le macchine con il tag project=fenvix-coldread e i bucket fenvix-coldread-*; i dischi
# si cancellano con le macchine (DeleteOnTermination). La verifica vera la fa controllo-finale.sh, sempre, dopo.
set -u
ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null) || { echo "NON SO: non posso leggere le macchine"; exit 1; }
if [ -n "$ids" ]; then aws ec2 terminate-instances --instance-ids $ids >/dev/null && echo "macchine cancellate: $ids"; aws ec2 wait instance-terminated --instance-ids $ids || true; fi
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep '^fenvix-coldread-'); do
  aws s3 rb "s3://$b" --force >/dev/null && echo "bucket cancellato: $b"; done
