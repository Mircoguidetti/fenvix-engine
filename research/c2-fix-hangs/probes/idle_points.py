# c2-fix-hangs measurement; run from ctl-tests/ with the judge environment (FENVIX_SEALED_* as research/integration-3/ctl-rerun.sh sealed2 sets them)
import os, sys, time, json
sys.path.insert(0, ".")
from lib import Server, make_bucket
from pymongo import MongoClient
srv = Server(make_bucket("hang"), "hang", idle=600, extra=["--db-quota-queue", "4"]).start()
c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000)
c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(200)])
def m():
    d = srv.admin.get("/v1/metrics/databases")[1]["databases"]["q"]
    return {k: d.get(k) for k in ("points","pointsS3","s3Put","s3Get","quotaWaits","quotaBalance","balance")}
for i in range(21):
    print(i, m(), flush=True); time.sleep(1)
print("status", c.admin.command("serverStatus").get("fenvix", {}).get("compactor"), flush=True)
srv.kill9()
