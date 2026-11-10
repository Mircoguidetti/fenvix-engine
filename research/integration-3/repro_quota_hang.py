# integration-3: reads of a database with a quota hang once its compactor runs (sealed runs on, T_pin 1 s).
# Run from ctl-tests/ with MINIO_ENDPOINT and the FENVIX_SEALED_* variables; NOQ=1 drops the quota (then it passes).
import os, sys, time, threading, json
from lib import Server, make_bucket
from pymongo import MongoClient
srv = Server(make_bucket("hang"), "hang", idle=600, extra=["--db-quota-queue", "4"]).start()
c = MongoClient(f"mongodb://127.0.0.1:{srv.port}/?directConnection=true", serverSelectionTimeoutMS=5000)
c.q.t.insert_many([{"_id": i, "pad": "x" * 1000} for i in range(200)])
if os.environ.get("NOQ") != "1": print("quota", srv.admin.call("PUT", "/v1/databases/q/quota", {"pts_per_sec": 2000, "burst_pts": 2000})[0])
print("quota state", srv.admin.get("/v1/databases/q/quota"))
list(c.q.t.find({})); print("after one find", srv.admin.get("/v1/databases/q/quota"))
done = [0]; errs = [0]
def w():
    cc = MongoClient(f"mongodb://127.0.0.1:{srv.port}/?directConnection=true", socketTimeoutMS=60000)
    for i in range(20):
        try:
            list(cc.q.t.find({})); done[0] += 1
        except Exception as e:
            errs[0] += 1
ts = [threading.Thread(target=w, daemon=True) for _ in range(16)]; [t.start() for t in ts]
for k in range(4):
    time.sleep(5)
    print(f"t={5*(k+1)}s done {done[0]} errors {errs[0]}")
m = srv.admin.get("/v1/databases/q/metrics")
print("metrics", json.dumps(m[1])[:800] if m[0] == 200 else m)
st = c.admin.command("serverStatus").get("fenvix", {})
print("compactor", st.get("compactor"))
srv.kill9()
