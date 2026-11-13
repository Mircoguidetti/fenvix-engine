import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
import test_79_big_speed as T
from lib import Server, make_bucket
b = make_bucket("termt")
for k in range(3):
    s = Server(b, "termt", idle=600, env=T.SIM).start(); c = s.client()
    for i in range(30):
        c.app.checkpoints.update_one({"thread_id": f"t{i}", "checkpoint_ns": "", "checkpoint_id": f"x{k}"}, {"$set": {"v": i}}, upsert=True)
    time.sleep(0.5)
    t0 = time.time(); s.term(); print("term s %.2f" % (time.time() - t0))
    print("\n".join(l[:160] for l in s.log_text().splitlines()[-1:]))
