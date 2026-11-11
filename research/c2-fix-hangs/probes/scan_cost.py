# c2-fix-hangs measurement; run from ctl-tests/ with the judge environment (FENVIX_SEALED_* as research/integration-3/ctl-rerun.sh sealed2 sets them)
import sys, time; sys.path.insert(0, ".")
from lib import Server, make_bucket
from pymongo import MongoClient
srv = Server(make_bucket("cost"), "cost", idle=600).start()
c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(3000)])
time.sleep(1.5)
def m():
    d = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]; return d["points"], d["s3Get"]
for k in range(3):
    p0, g0 = m(); n = len(list(c.q.t.find({}))); p1, g1 = m()
    print(f"find {k+1}: {n} docs, {p1-p0:.0f} pt, {g1-g0} GETs counted", flush=True)
p0, g0 = m(); c.q.t.find_one({"_id": 1500}); p1, g1 = m(); print(f"find_one: {p1-p0:.0f} pt, {g1-g0} GETs counted")
srv.kill9()
