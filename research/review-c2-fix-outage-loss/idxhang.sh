export PATH=/root/.fenvix-judge/env/bin:$PATH
cd /home/user/rv/ctl-tests
for b in "$@"; do
  echo "== $b"
  MONGO_SERVER_BIN=/home/user/bin-$b timeout 900 python -m pytest -q -s test_52r_review_c2_fix_outage_loss.py -k "keep_answering" 2>&1 | grep -E "^cycles|^hung|^server log|^E  |passed|failed" | cut -c1-1600
done
