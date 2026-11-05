#!/bin/bash
# Kitchen (K2, corrected by K2b; copy of P1 Ireland controllo-finale.sh, plus the security groups) · final check
# (founder's rule 2026-11-04): last step of the job, always, with fresh credentials, AFTER cleanup.sh. Deletes nothing:
# it only looks, in eu-west-1, whether something that costs money is left:
#   - EC2 machines tagged project=fenvix-coldread in any state but terminated;
#   - EBS disks with that tag (any state), or unattached (available) and created after CM_START;
#   - Elastic IPs with that tag;
#   - buckets starting with fenvix-coldread-;
#   - security groups with that tag.
# Nothing: "RIMASTO: niente" (the words of RULES.md), exit 0. Something (or a read that fails): "ALLARME: rimasto ..."
# with the list, an ::error:: annotation, the step summary starts with the ALLARME, exit 1 (red job).
# A kept pantry (fenvix-kitchen-pantry-<digits>, founder's choice A2) is not something left behind: it is listed as KEPT,
# with its creation date, and does not make the job red.
# CM_START: start of the run in UTC (YYYY-MM-DDTHH:MM:SSZ), set by the first step of the workflow. Missing: last 24 hours.
set -u
export START=${CM_START:-$(date -u -d '24 hours ago' +%FT%TZ)}   # jq reads it as $ENV.START
TAGF="Name=tag:project,Values=fenvix-coldread"
left=()
unknown=()
ERR=$(mktemp); trap 'rm -f "$ERR"' EXIT
short() { tr '\n' ' ' < "$ERR" | sed 's/  */ /g; s/^ //' | head -c 200; }  # the AWS error on one line
# look WHAT COMMAND... : runs the AWS command (JSON on stdout, errors apart) and passes it to jq with the filter in
# $FILTER; every line jq prints is something left. If AWS or jq fail (broken JSON too) it is a NON SO, never "nothing".
look() {
  local what=$1; shift; local j lines
  if ! j=$("$@" --output json 2>"$ERR"); then unknown+=("$what: $(short)"); return; fi
  if ! lines=$(jq -r "$FILTER" <<<"$j" 2>"$ERR"); then unknown+=("$what: answer not readable ($(short))"); return; fi
  while IFS= read -r r; do [ -n "$r" ] && left+=("$r"); done <<<"$lines"
}

# 1. machines: every state but terminated
FILTER='.Reservations[].Instances[] | "machine \(.InstanceId) (\(.State.Name), \(.InstanceType))"'
look "EC2 machines" aws ec2 describe-instances --filters "$TAGF" Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped

# 2. disks: tagged (any state), or unattached and created during the run (an untagged disk the termination did not
# remove); times compared without the fractions of a second
FILTER='.Volumes[] | "disk \(.VolumeId) (\(.State), \(.Size) GB, tagged)"'
look "tagged EBS disks" aws ec2 describe-volumes --filters "$TAGF"
FILTER='.Volumes[] | select(([.Tags[]? | select(.Key == "project" and .Value == "fenvix-coldread")] | length) == 0)
  | select((.CreateTime | sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z")) >= $ENV.START)
  | "disk \(.VolumeId) (\(.State), \(.Size) GB, unattached, created during the run)"'
look "unattached EBS disks" aws ec2 describe-volumes --filters Name=status,Values=available

# 3. tagged Elastic IPs
FILTER='.Addresses[] | "address \(.PublicIp) (\(.AllocationId // "?"))"'
look "Elastic IPs" aws ec2 describe-addresses --filters "$TAGF"

# 4. buckets fenvix-coldread-*, and fenvix-kitchen-pantry-* that is not a kept pantry (K7: for example the empty bucket
# of the permission check, fenvix-kitchen-pantry-check-<run>, if its deletion failed)
FILTER='.Buckets[].Name | select(startswith("fenvix-coldread-") or (startswith("fenvix-kitchen-pantry-") and (test("^fenvix-kitchen-pantry-[0-9]+$") | not))) | "bucket \(.)"'
look "S3 buckets" aws s3api list-buckets

# 5. tagged security groups
FILTER='.SecurityGroups[] | "security group \(.GroupId) (\(.GroupName))"'
look "security groups" aws ec2 describe-security-groups --filters "$TAGF"

S=${GITHUB_STEP_SUMMARY:-/dev/null}
# kept pantries (founder's choice A2): listed, not an alarm
kept=$(aws s3api list-buckets --output json 2>/dev/null | jq -r '.Buckets[] | select(.Name | test("^fenvix-kitchen-pantry-[0-9]+$")) | "\(.Name) (created \(.CreationDate))"' 2>/dev/null)
[ -n "$kept" ] && while IFS= read -r k; do echo "KEPT: pantry $k; its objects expire after 3 days, mode drop-pantry deletes it now" | tee -a "$S"; done <<<"$kept"
if [ ${#left[@]} -eq 0 ] && [ ${#unknown[@]} -eq 0 ]; then
  echo "RIMASTO: niente (final check: machines, disks, IP addresses, buckets, security groups in eu-west-1, since $START)" | tee -a "$S"
  exit 0
fi
list=$( { for r in "${left[@]}"; do echo "$r"; done; for r in "${unknown[@]}"; do echo "NON SO, read failed: $r"; done; } )
one=$(paste -sd ';' <<<"$list" | sed 's/;/; /g')
echo "ALLARME: rimasto $one"
echo "::error title=ALLARME final check::left $one"
{ echo "# ALLARME: something is left on AWS"; echo; sed 's/^/- /' <<<"$list"; echo
  echo 'What to do: commit {"mode": "teardown"} in kitchen/command.json (see kitchen/README.md), then look at this check again.'; } >> "$S"
exit 1
