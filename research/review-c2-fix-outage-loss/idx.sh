export PATH=/root/.fenvix-judge/env/bin:$PATH
cd /home/user/rv/ctl-tests
for b in "$@"; do
  echo "== $b"
  MONGO_SERVER_BIN=/home/user/bin-$b timeout 1500 python -m pytest -q -s test_52r_review_c2_fix_outage_loss.py -k "index_build" 2>&1 | grep -E "^lease losses|attempt|^acknowledged|^background|^E  |passed|failed" | cut -c1-300
done
