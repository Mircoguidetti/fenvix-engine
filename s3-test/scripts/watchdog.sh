#!/usr/bin/env bash
# Second spend control, run by the workflow every 30 s while the instance works. Independent of the on-instance guard:
# it reads the guard's counters from S3 and computes the gross cost itself (cost.py, same prices). It terminates the
# instance when
#   * the cost exceeds the cap (5 USD),
#   * the guard's heartbeat is older than 90 s (guard dead => nothing is watching), or
#   * no heartbeat ever appeared 8 minutes after launch.
# Fail closed. Its own reads (one GET per 30 s) are negligible and added to the ledger by finalize.
set -u
ID=$1; B=$2; OUT=$3; R=eu-central-1
HERE=$(dirname "$0"); . "$HERE/aws_refresh.sh"; export AWS_CREDS_AT=$(date +%s)
CAP=$(jq -r .run_cost_cap_usd "$HERE/../prices.json")
LAUNCH=$(grep '^LAUNCH_EPOCH=' "$OUT/state.env" | cut -d= -f2); : "${LAUNCH:=$(date +%s)}"
reads=0
while true; do
  sleep 30
  # this process outlives the 1 h session: it renews its own credentials; if that fails twice it stops the instance
  # with the credentials it still has (fail closed) and ends.
  if ! refresh_aws_creds; then sleep 5; refresh_aws_creds || {
    echo "$(date +%s) WATCHDOG: cannot renew the AWS credentials: terminating $ID with the current ones and stopping" | tee -a "$OUT/watchdog.log"
    echo "credential renewal failed" > "$OUT/watchdog.reason"
    aws ec2 terminate-instances --region $R --instance-ids "$ID" >> "$OUT/watchdog.log" 2>&1
    touch "$OUT/watchdog.stopped"; exit 0; }; fi
  now=$(date +%s); el=$((now-LAUNCH)); reads=$((reads+1)); echo $reads > "$OUT/watchdog.reads"
  reason=""
  if aws s3 cp "s3://$B/_s3test/guard/counters.json" "$OUT/guard-latest.json" --only-show-errors 2>/dev/null; then
    ts=$(jq -r .ts "$OUT/guard-latest.json"); put=$(jq -r .put_tier "$OUT/guard-latest.json"); get=$(jq -r .get_tier "$OUT/guard-latest.json")
    cost=$(python3 -I "$HERE/cost.py" "$put" "$get" $((el+60)))
    echo "$now cost=$cost put=$put get=$get age=$((now-ts))s" >> "$OUT/watchdog.log"
    echo "$cost" > "$OUT/watchdog.cost"
    awk -v c="$cost" -v cap="$CAP" 'BEGIN{exit !(c>cap)}' && reason="cost $cost > cap $CAP"
    [ $((now-ts)) -gt 90 ] && [ -z "$reason" ] && reason="guard heartbeat stale ($((now-ts)) s)"
    jq -e .stopped "$OUT/guard-latest.json" >/dev/null 2>&1 && [ "$(jq -r .stopped "$OUT/guard-latest.json")" != null ] && { echo "guard stopped the run itself" >> "$OUT/watchdog.log"; touch "$OUT/watchdog.stopped"; exit 0; }
  else
    echo "$now no guard counters yet (el=${el}s)" >> "$OUT/watchdog.log"
    [ $el -gt 480 ] && reason="no guard heartbeat 8 minutes after launch"
  fi
  # the instance may already be gone (own timer, guard shutdown)
  st=$(aws ec2 describe-instances --region $R --instance-ids "$ID" --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
  case $st in terminated|shutting-down|stopped|stopping) echo "instance $st" >> "$OUT/watchdog.log"; touch "$OUT/watchdog.stopped"; exit 0;; esac
  if [ -n "$reason" ]; then
    echo "$now WATCHDOG TERMINATES $ID: $reason" | tee -a "$OUT/watchdog.log"
    echo "$reason" > "$OUT/watchdog.reason"
    aws ec2 terminate-instances --region $R --instance-ids "$ID" >> "$OUT/watchdog.log" 2>&1
    touch "$OUT/watchdog.stopped"; exit 0
  fi
done
