#!/bin/bash
# takeovers log on (50) on the new binary, and the full ctl set sealed2+derive, in parallel
cd /home/user/fenvix-engine
W=/tmp/claude-0/tk
rm -rf $W-on $W-on.head.sh $W-on.sh
( SRV=/home/user/fenvix-engine/target/release/mongo-server PQ=/home/user/fenvix-engine/proto/quaderno/target/release/pq \
  bash research/integration-3/takeover-run.sh /home/user/s9 $W-on on 50 > /tmp/claude-0/runs/takeovers-on.txt 2>&1; echo "EXIT $?" >> /tmp/claude-0/runs/takeovers-on.txt ) &
T=$!
( JUDGE_SEALED_DERIVE=1 bash research/integration-3/ctl-rerun.sh sealed2 /tmp/claude-0/runs/ctl-full-s2d $(cd ctl-tests && ls test_*.py | sed 's/\.py$//') > /tmp/claude-0/runs/ctl-full-s2d.txt 2>&1; echo "EXIT $?" >> /tmp/claude-0/runs/ctl-full-s2d.txt ) &
C=$!
wait $T; wait $C
echo done
