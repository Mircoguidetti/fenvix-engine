#!/bin/bash
# Q8d (reviewer C2): cleanup of THIS test only. Other AWS tests use the same tag project=fenvix-coldread and buckets
# fenvix-coldread-* (Ireland, minimal kitchen with its kept pantry fenvix-coldread-cmd-*): they are never touched.
#   - normal run: machines named fenvix-q8-<this run>-* and this run's buckets ($BUCKET, fenvix-coldread-permessi-<run>);
#   - Q8_MODO=smonta (cleanup of a previous Q8 run): every machine named fenvix-q8-* and every bucket fenvix-coldread-q8-*.
# Disks go with the machines (DeleteOnTermination). The real check is controllo-finale.sh, always, afterwards.
set -u
RUN=${GITHUB_RUN_ID:-0}
if [ "${Q8_MODO:-}" = smonta ]; then NOMI="fenvix-q8-*"; BRE='^fenvix-coldread-q8-'; else NOMI="fenvix-q8-$RUN-*"; BRE="^(${BUCKET:-fenvix-coldread-q8-$RUN}|fenvix-coldread-permessi-$RUN)\$"; fi
ids=$(aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-coldread "Name=tag:Name,Values=$NOMI" Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null) || { echo "NON SO: non posso leggere le macchine"; exit 1; }
if [ -n "$ids" ]; then aws ec2 terminate-instances --instance-ids $ids >/dev/null && echo "macchine cancellate: $ids"; aws ec2 wait instance-terminated --instance-ids $ids || true; fi
for b in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null | tr '\t' '\n' | grep -E "$BRE"); do
  aws s3 rb "s3://$b" --force >/dev/null && echo "bucket cancellato: $b"; done
