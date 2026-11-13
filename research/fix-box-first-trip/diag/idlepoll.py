"""GETs a writer database sends while idle, after its first writes (sealed mode): counted by serverStatus, by second."""
import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
import test_79_big_speed as T
from lib import Server, make_bucket
bucket = make_bucket("idlepoll")
s = Server(bucket, "idlepoll", idle=600, env=T.SIM).start()
c = s.client(); db = c.app
for i in range(20):
    db.checkpoints.update_one({"thread_id": f"t{i}", "checkpoint_ns": "", "checkpoint_id": "x"}, {"$set": {"v": i}}, upsert=True)
time.sleep(3)
g0 = T.gets(c); l0 = T.fx(c)["s3"].get("list", 0)
per = []
for _ in range(10):
    a = T.gets(c); time.sleep(1); per.append(T.gets(c) - a)
print("idle writer GETs per second:", per, "total", T.gets(c) - g0, "LIST", T.fx(c)["s3"].get("list", 0) - l0)
c.close(); s.term()
