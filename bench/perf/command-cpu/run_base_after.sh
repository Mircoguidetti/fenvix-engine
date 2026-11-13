#!/bin/bash
H=/home/user/harness/bench/agent-checkpoints; R=$H/results/big-2026-11-13-cpu
until grep -q "firstnext exit" $R/driver.log; do sleep 20; done
export BENCH_PW=$(cat /home/user/.benchpw)
rm -f /home/user/harness/target/release/mongo-server; cp /home/user/bins/mongo-server.base-13d531b /home/user/harness/target/release/mongo-server
echo "$(date -u +%FT%TZ) fenvix-log BASE 13d531b -> $R/fenvix-base" >> $R/driver.log
~/.fenvix-agentbench-venv/bin/python $H/lines.py run fenvix-log --big --mem 1g --profile big --seed 42 --workers 3 --mix-write 1000 --mix-read 1200 --delay-ms 20 --min-free-gib 6 --server-flags "--box-disk-mb 2048" --out $R/fenvix-base > $R/fenvix-base.runner.txt 2>&1
echo "$(date -u +%FT%TZ) fenvix-log BASE: exit $? $(tail -1 $R/fenvix-base/terminal.txt | cut -c1-300)" >> $R/driver.log
rm -f /home/user/harness/target/release/mongo-server; cp /home/user/bins/mongo-server.final-b60a55c /home/user/harness/target/release/mongo-server
