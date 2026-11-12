#!/bin/bash
export BENCH_PW=localbenchpw
H=/home/user/bench-h/bench/agent-checkpoints; OUT=/tmp/claude-0/runs/m7-erase-grace
rm -rf $OUT
cd $H && unset FXBENCH_EXTRA_FLAGS && ~/.fenvix-agentbench-venv/bin/python lines.py run fenvix-log --out $OUT --profile medium --read-sample 100 --erase-users 15 --cold-threads 50 --min-free-gib 3 --keep > /tmp/claude-0/runs/m7.log 2>&1
echo "$(date -u +%T) run exit $?"
FXB=/home/user/fenvix-engine/target/release/fx_bytes
for i in 0 1 2 3 4 5 6 7 8; do
  AWS_ACCESS_KEY_ID=minio-local AWS_SECRET_ACCESS_KEY=minio-local-secret $FXB --endpoint http://172.31.77.10:9000 --bucket fxbench --db app_demo > /tmp/claude-0/runs/m7-bytes-$i.json 2>/dev/null
  python3 -c "import json;d=json.load(open('/tmp/claude-0/runs/m7-bytes-$i.json'));f=d['fx'];print('$(date -u +%T)', d['total'], d['by_kind'].get('fx'), f['live_runs'], f['retained_runs_older_versions_only'], f['unreferenced_runs_grace'], f['versions'])"
  sleep 120
done
echo "$(date -u +%T) done"
