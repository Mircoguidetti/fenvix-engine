#!/bin/bash
# after the write phase of run $1: cgroup throttling and per-thread CPU of the cook, into $1-throttle.txt
R=$1
until [ -s $R/ops.jsonl ] && [ $(tail -1 $R/ops.jsonl | grep -c '"phase": "write"') = 0 ]; do sleep 10; done
ID=$(docker inspect -f "{{.Id}}" cook)
grep -E "nr_periods|nr_throttled|throttled_time" /sys/fs/cgroup/cpu/docker/$ID/cpu.stat | tr "\n" " " > $R-throttle.txt
P=$(pgrep -f "^/opt/fx/mongo-server.*advertise 172.31.77.43" | head -1)
for t in /proc/$P/task/*; do echo $(cat $t/comm) $(awk '{print ($14+$15)/100}' $t/stat); done | awk '{s[$1]+=$2} END{for(k in s) print k, s[k]}' >> $R-throttle.txt
date >> $R-throttle.txt
