#!/usr/bin/env bash
# After the normal clean-up (fresh login + cleanup.sh): the bucket must be gone and no tagged instance may exist.
set -u
cd "${GITHUB_WORKSPACE:-.}"; OUT=run-out; R=eu-central-1; B=fenvix-test-${GITHUB_RUN_ID:?}; RES=$OUT/dryrun_result.txt
say() { echo "$*" | tee -a "$RES"; }
ok=1
if aws s3api head-bucket --bucket "$B" >/dev/null 2>&1; then say "FAIL: bucket $B still exists after the clean-up"; ok=0; else say "ok: bucket $B is gone"; fi
left=$(aws ec2 describe-instances --region $R --filters Name=tag:project,Values=fenvix-tests Name=instance-state-name,Values=pending,running --query 'Reservations[].Instances[].InstanceId' --output text 2>&1)
if [ -z "$left" ] || [ "$left" = None ]; then say "ok: no tagged instance pending/running"; else say "FAIL: instances: $left"; ok=0; fi
[ -e "$OUT/dryrun_unproven" ] && { say "NOT PROVEN: the old credentials had not expired"; ok=0; }
[ $ok = 1 ] && say "DRYRUN RESULT: PASS (clean-up works after more than one hour, no instance launched)" || say "DRYRUN RESULT: FAIL"
[ $ok = 1 ]
