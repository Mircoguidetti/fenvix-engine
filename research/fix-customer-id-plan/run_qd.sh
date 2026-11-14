#!/usr/bin/env bash
# perf.py and range_ab.py of the query-defects review (copied as qd_perf.py, qd_range_ab.py), base and head
set -u
cd "$(dirname "$0")"; export RUN_DB=app_perf PATH=/root/.fenvix-judge/env/bin:$PATH
B=/home/user/bin/mongo-server-base-0b143c4; H=/home/user/bin/mongo-server-head3
python run_perf.py new $B qbase1 qd_perf.py --load --out results/qd-perf-base1.json > results/qd-perf-base1.txt 2>&1
BK=$(grep -o "ctl-cperf-[0-9a-f]*" results/qd-perf-base1.txt | head -1)
sleep 10; python run_perf.py $BK $H qhead1 qd_perf.py --out results/qd-perf-head1.json > results/qd-perf-head1.txt 2>&1
sleep 10; python run_perf.py $BK $B qbase2 qd_perf.py --out results/qd-perf-base2.json > results/qd-perf-base2.txt 2>&1
sleep 10; python run_perf.py $BK $H qhead2 qd_perf.py --out results/qd-perf-head2.json > results/qd-perf-head2.txt 2>&1
for r in 1 2; do for b in base head; do
  bin=$B; [ $b = head ] && bin=$H
  python - "$bin" "$b$r" <<'P' >> results/qd-range_ab.txt 2>&1
import os, subprocess, sys
sys.path.insert(0, "../../ctl-tests")
from lib import Server, make_bucket
s = Server(make_bucket("qrange"), "qrange", idle=600, binary=sys.argv[1]).start()
try:
    r = subprocess.run([sys.executable, "qd_range_ab.py", s.uri, "app_range", "200000"], capture_output=True, text=True, timeout=3600)
    print("\n".join(f"{sys.argv[2]:6} {l}" for l in (r.stdout + r.stderr[-600:]).splitlines()), flush=True)
finally:
    s.term()
P
done; done
echo ALLDONE
