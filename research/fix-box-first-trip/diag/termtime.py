import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
import test_79_big_speed as T
from lib import Server, make_bucket
N = int(sys.argv[1]) if len(sys.argv) > 1 else 12
b = make_bucket("termt")
times = []
for k in range(N):
    s = Server(b, "termt", idle=600, env=T.SIM).start(); c = s.client()
    for i in range(30):
        c.app.checkpoints.update_one({"thread_id": f"t{i}", "checkpoint_ns": "", "checkpoint_id": f"x{k}"}, {"$set": {"v": i}}, upsert=True)
    time.sleep(0.5)
    t0 = time.time(); s.term(); d = time.time() - t0; times.append(round(d, 2))
    if d > 4:
        lines = s.log_text().splitlines()
        i0 = max(i for i, l in enumerate(lines) if "shutting down" in l)
        print(f"--- slow stop {d:.2f} s, run {k}"); print("\n".join(l[:200] for l in lines[i0 - 3:]))
print("stop seconds:", times)
