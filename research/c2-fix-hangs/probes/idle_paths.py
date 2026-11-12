# c2-fix-hangs measurement; run from ctl-tests/ with the judge environment (FENVIX_SEALED_* as research/integration-3/ctl-rerun.sh sealed2 sets them)
import os, sys, time, json, collections, re
sys.path.insert(0, ".")
import proxy
from lib import Server, make_bucket
from pymongo import MongoClient
paths = collections.Counter()
orig = proxy.classify
def cl(method, path, query, headers):
    k = orig(method, path, query, headers)
    p = re.sub(r"[0-9A-Z]{26}", "<ULID>", re.sub(r"\d{6,}", "<N>", path))
    paths[(k, p + ("?" + query[:60] if k == "LIST" else ""))] += 1
    return k
proxy.classify = cl
px = proxy.CountingProxy()
srv = Server(make_bucket("hang"), "hang", idle=600, endpoint=px.endpoint, extra=["--db-quota-queue", "4"]).start()
c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(200)])
time.sleep(3); paths.clear(); time.sleep(12)
for k, v in paths.most_common(40): print(v, k)
srv.kill9()
