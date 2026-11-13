#!/bin/bash
# restart only the cook of the kept fenvix-log line with the engine binary of /home/user/E, with extra env
cd /home/user/H/bench/agent-checkpoints
docker stop -t 60 cook > /dev/null 2>&1; docker rm cook > /dev/null 2>&1
cp /home/user/E/target/release/mongo-server /home/user/H/target/release/mongo-server
export BENCH_PW=$(cat /tmp/claude-0/bench_pw)
~/.fenvix-agentbench-venv/bin/python -c "
import lines
lines.run_ours('cook', lines.server_cmd('cook', True))
lines.wait_until('cook answers', lambda: lines.ours_ready('fenvix-log'), 240)
print('cook up')"
