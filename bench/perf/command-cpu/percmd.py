"""Server CPU per wire command for a few fixed shapes (dev bench): ping, a find of a missing _id, the saver's pending
writes lookup and its newest-checkpoint lookup on one warm thread. MEASURED as /proc/<pid>/stat utime+stime deltas."""
import os, sys, time, pickle
from pymongo import MongoClient
D = os.environ.get("DEVBENCH_DIR", "/tmp/claude-0/devbench")
pid = int(open(os.path.join(D, "server.pid")).read())
TCK = os.sysconf("SC_CLK_TCK")
def cpu():
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / TCK
N = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
c = MongoClient("mongodb://127.0.0.1:27317/?directConnection=true")
db = c.app_demo
st = pickle.load(open(os.path.join(D, "plan.pkl"), "rb"))
t = st["threads"][0]
newest = next(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1))
cid = newest["checkpoint_id"]
shapes = {
    "ping": lambda: c.admin.command("ping"),
    "find_missing_id": lambda: list(db.checkpoints.find({"_id": "nope"})),
    "writes_lookup": lambda: list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": cid})),
    "newest_ckpt": lambda: list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)], limit=1)),
}
for k, f in shapes.items():
    for _ in range(50):
        f()
    c0, t0 = cpu(), time.perf_counter()
    for _ in range(N):
        f()
    dt, dc = time.perf_counter() - t0, cpu() - c0
    print(f"{k:16s} cpu {1e6*dc/N:7.1f} us/cmd  wall {1e6*dt/N:7.1f} us/cmd", flush=True)
