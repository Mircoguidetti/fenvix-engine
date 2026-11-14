"""run_perf.py BUCKET BINARY LABEL SCRIPT [script args...]: our server on BUCKET (product defaults), then SCRIPT --ours URI."""
import os, subprocess, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server, make_bucket
bucket, binary, label, script = sys.argv[1:5]
if bucket == "new":
    bucket = make_bucket("cperf"); print("bucket", bucket, flush=True)
s = Server(bucket, f"perf{label}", idle=900, binary=binary).start()
import time
from pymongo import MongoClient
for _ in range(120):   # an earlier process's lease on the database must lapse first
    try:
        MongoClient(s.uri)[os.environ.get("RUN_DB", "app_cperf")].command("count", "docs"); break
    except Exception:
        time.sleep(1)
try:
    r = subprocess.run([sys.executable, script, "--ours", s.uri, "--label", label] + sys.argv[5:], text=True, timeout=10800)
finally:
    s.term()
