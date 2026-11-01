#!/usr/bin/env bash
# Pulizia e verifica di TUTTE le risorse fenvix-prova-efs-* in eu-west-1. Si può ripetere quante volte si vuole.
# Ordine: istanze -> mount target -> file system -> security group -> (bucket dei risultati: svuotato e cancellato
# solo se PULIZIA_BUCKET=1, dopo che i risultati sono stati copiati sul runner).
# Uscita: $OUT/pulizia.txt; ultima riga "RIMASTO: niente" oppure "ALLARME: ...".
set -uo pipefail
OUT=${OUT:-risultati-efs}; mkdir -p "$OUT"; export AWS_DEFAULT_REGION=eu-west-1 AWS_PAGER=""
L="$OUT/pulizia.txt"; echo "== pulizia $(date -u +%FT%TZ)" >> "$L"
log() { echo "$*" | tee -a "$L"; }
ids_ist() { aws ec2 describe-instances --filters Name=tag:project,Values=fenvix-prova-efs Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null; }
fs_ids()  { aws efs describe-file-systems --query "FileSystems[?starts_with(Name||'',\`fenvix-prova-efs\`)].FileSystemId" --output text 2>/dev/null; }
sg_ids()  { aws ec2 describe-security-groups --filters Name=tag:project,Values=fenvix-prova-efs --query 'SecurityGroups[].GroupId' --output text 2>/dev/null; }

I=$(ids_ist); [ -n "$I" ] && { log "termino istanze: $I"; aws ec2 terminate-instances --instance-ids $I >/dev/null 2>>"$L"; }
for F in $(fs_ids); do
  for M in $(aws efs describe-mount-targets --file-system-id "$F" --query 'MountTargets[].MountTargetId' --output text 2>/dev/null); do
    log "cancello mount target $M"; aws efs delete-mount-target --mount-target-id "$M" 2>>"$L"; done
done
for F in $(fs_ids); do
  for t in $(seq 1 40); do
    n=$(aws efs describe-mount-targets --file-system-id "$F" --query 'length(MountTargets)' --output text 2>/dev/null || echo x)
    [ "$n" = 0 ] && break; sleep 10; done
  log "cancello file system $F"; aws efs delete-file-system --file-system-id "$F" 2>>"$L"
done
# le istanze devono essere terminate prima di cancellare i security group
for t in $(seq 1 40); do [ -z "$(ids_ist)" ] && break; sleep 10; done
for t in $(seq 1 3); do
  for G in $(sg_ids); do aws ec2 delete-security-group --group-id "$G" 2>/dev/null && log "cancellato security group $G"; done
  [ -z "$(sg_ids)" ] && break; sleep 30
done
if [ "${PULIZIA_BUCKET:-0}" = 1 ]; then
  for B in $(aws s3api list-buckets --query "Buckets[?starts_with(Name,\`fenvix-prova-efs-\`)].Name" --output text 2>/dev/null); do
    aws s3 rb "s3://$B" --force >/dev/null 2>>"$L" && log "cancellato bucket $B"; done
fi
sleep 20
I=$(ids_ist); F=$(fs_ids); G=$(sg_ids)
MT=""; for f in $F; do MT="$MT $(aws efs describe-mount-targets --file-system-id "$f" --query 'MountTargets[].MountTargetId' --output text 2>/dev/null)"; done
B=$(aws s3api list-buckets --query "Buckets[?starts_with(Name,\`fenvix-prova-efs-\`)].Name" --output text 2>/dev/null)
log "verifica: istanze accese [${I}] file system [${F}] mount target [$(echo $MT)] security group [${G}] bucket [${B}]"
# se una delle letture è negata, la verifica non può dire "niente"
leggo=1; aws ec2 describe-instances --max-items 1 >/dev/null 2>&1 || leggo=0; aws efs describe-file-systems --max-items 1 >/dev/null 2>&1 || leggo=0
aws ec2 describe-security-groups --max-items 1 >/dev/null 2>&1 || leggo=0
if [ $leggo = 0 ]; then log "NON SO: il ruolo non può leggere istanze, file system o security group; controllare a mano"; exit 3; fi
if [ -z "$I$F$G$(echo $MT)" ]; then log "RIMASTO: niente (istanze, file system, mount target, security group)"
else log "ALLARME: resta acceso qualcosa: istanze [$I] file system [$F] mount target [$(echo $MT)] security group [$G]"; exit 2; fi
