import sys, time
sys.path.insert(0, "/home/user/wt-lc/ctl-tests")
import lib
from lib import Server
lib.OUT = "/tmp/claude-0"
from pymongo import MongoClient
bucket, binary, label = sys.argv[1:4]
s = Server(bucket, "cagg", idle=600, binary=binary).start()
try:
    c = MongoClient(s.uri, socketTimeoutMS=900000)
    for t in range(3):
        t0 = time.time(); n = c.d.t.count_documents({}); t1 = time.time()
        g = list(c.d.t.aggregate([{"$group": {"_id": "$k", "n": {"$sum": 1}}}])); t2 = time.time()
        print(f"{label} trial {t}: count {n} in {t1-t0:.1f} s, group in {t2-t1:.1f} s", flush=True)
finally:
    s.term()
