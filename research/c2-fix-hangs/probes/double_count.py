# c2-fix-hangs measurement; run from ctl-tests/ with the judge environment (FENVIX_SEALED_* as research/integration-3/ctl-rerun.sh sealed2 sets them)
import sys, time; sys.path.insert(0, ".")
import proxy
from lib import Server, make_bucket
from pymongo import MongoClient
px = proxy.CountingProxy()
srv = Server(make_bucket("dbl"), "dbl", idle=600, endpoint=px.endpoint).start()
c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(3000)])
time.sleep(3)
def m():
    d = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]; return d["s3Get"]
g0 = m(); px.reset(); n = len(list(c.q.t.find({}))); g1 = m(); s = px.snapshot()
print("metric s3Get", g1 - g0, "proxy GET-class under tenants/q", sum(v for k, v in s["by_prefix"].get("tenants/q", {}).items() if k in ("GET", "GET_RANGE", "HEAD")))
srv.kill9()
