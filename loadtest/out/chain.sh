#!/usr/bin/env bash
cd /home/user/test5
python3 loadtest/run_load.py s3 --lease-ttl 60 --s3-secs 300 > loadtest/out/s3.stdout 2>&1
python3 loadtest/analyze.py loadtest/out/s3 > /dev/null
mkdir -p loadtest/results/s3 && cp loadtest/out/s3/{summary.json,analysis.json,samples.jsonl} loadtest/results/s3/
python3 loadtest/run_load.py s1 --lease-ttl 60 --s1-secs 300 > loadtest/out/s1b.stdout 2>&1
python3 loadtest/analyze.py loadtest/out/s1 > /dev/null
mkdir -p loadtest/results/s1b && cp loadtest/out/s1/{summary.json,analysis.json,samples.jsonl} loadtest/results/s1b/
echo done > loadtest/out/chain.done
