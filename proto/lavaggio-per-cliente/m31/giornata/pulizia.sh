#!/bin/bash
# M31 · pulizia: cancella le macchine con il tag progetto=fenvix-m31 e i bucket fenvix-m31-*; poi verifica.
# Scrive "RIMASTO: niente" oppure "ALLARME" con cosa resta. Se non può leggere, scrive "NON SO".
set -u
ids=$(aws ec2 describe-instances --filters Name=tag:progetto,Values=fenvix-m31 Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null) || { echo "NON SO: non posso leggere le macchine"; exit 1; }
if [ -n "$ids" ]; then aws ec2 terminate-instances --instance-ids $ids >/dev/null && echo "macchine cancellate: $ids"; aws ec2 wait instance-terminated --instance-ids $ids || true; fi
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep '^fenvix-m31-'); do
  aws s3 rb "s3://$b" --force >/dev/null && echo "bucket cancellato: $b"; done
m=$(aws ec2 describe-instances --filters Name=tag:progetto,Values=fenvix-m31 Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo "?")
b=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -c '^fenvix-m31-' || true)
if [ "$m" = "0" ] && [ "$b" = "0" ]; then echo "RIMASTO: niente" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  echo "ALLARME: macchine $m, bucket $b" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
