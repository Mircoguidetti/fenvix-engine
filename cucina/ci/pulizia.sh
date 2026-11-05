#!/bin/bash
# Cucina minima (K2; da P1 Irlanda pulizia.sh, stesso tag e stessi bucket, in piu' i gruppi di sicurezza) · pulizia:
# cancella le macchine con il tag project=fenvix-coldread, i bucket fenvix-coldread-* (anche quello dei dati, con tutto
# dentro) e i gruppi di sicurezza con il tag; poi verifica. Scrive "RIMASTO: niente" oppure "ALLARME" con cosa resta.
# Se non puo' leggere, scrive "NON SO".
set -u
ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null) || { echo "NON SO: non posso leggere le macchine"; exit 1; }
# shellcheck disable=SC2086
if [ -n "$ids" ]; then aws ec2 terminate-instances --instance-ids $ids >/dev/null && echo "macchine cancellate: $ids"; aws ec2 wait instance-terminated --instance-ids $ids || true; fi
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep '^fenvix-coldread-'); do
  aws s3 rb "s3://$b" --force >/dev/null && echo "bucket cancellato: $b"; done
# gruppi di sicurezza: prima quelli che ne citano un altro (server), poi gli altri; una macchina appena cancellata
# tiene il gruppo ancora qualche secondo (interfaccia di rete), quindi si riprova fino a 3 minuti
gruppi() { aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query "$1" --output text 2>/dev/null | tr '\t' '\n' | grep -v '^$'; }
for _ in $(seq 1 18); do
  tutti=$(gruppi 'SecurityGroups[].GroupId') || true
  [ -z "$tutti" ] && break
  for g in $(gruppi 'SecurityGroups[?length(IpPermissions[].UserIdGroupPairs[]) > `0`].GroupId') $tutti; do
    aws ec2 delete-security-group --group-id "$g" >/dev/null 2>&1 && echo "gruppo di sicurezza cancellato: $g"; done
  [ -z "$(gruppi 'SecurityGroups[].GroupId')" ] && break
  sleep 10
done
m=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo "?")
b=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -c '^fenvix-coldread-' || true)
g=$(aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-coldread --query 'length(SecurityGroups)' --output text 2>/dev/null || echo "?")
if [ "$m" = "0" ] && [ "$b" = "0" ] && [ "$g" = "0" ]; then echo "RIMASTO: niente" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; else
  echo "ALLARME: macchine $m, bucket $b, gruppi di sicurezza $g" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"; exit 1; fi
