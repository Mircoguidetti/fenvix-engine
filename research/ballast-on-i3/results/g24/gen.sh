#!/bin/bash
W=/home/user/work/g24
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
OPTS="--envelopes write --customer-key messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"
echo "$OPTS" > $W/generator-options.txt
( while sleep 5; do ps -o rss=,pcpu= -C mongo-server | awk -v t=$(date +%s) '{print t, $1, $2}'; done ) > $W/rss.log &
M=$!
T0=$(date +%s); /home/user/fenvix-engine/target/release/mongo-server --store s3 --bucket ballast24 --region us-east-1 --endpoint http://127.0.0.1:9000 $OPTS --ballast $W/plan.json > $W/generator.log 2> >(tee -a $W/generator.log >&2)
echo "EXIT $? WALL $(( $(date +%s) - T0 )) s" >> $W/generator.log
kill $M
