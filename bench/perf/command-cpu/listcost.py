import os, pickle, time, bson, sys
from pymongo import MongoClient
D = "/tmp/claude-0/devbench"
pid = int(open(os.path.join(D, "server.pid")).read())
TCK = os.sysconf("SC_CLK_TCK")
def cpu():
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / TCK
c = MongoClient("mongodb://127.0.0.1:27317/?directConnection=true")
db = c.app_demo
st = pickle.load(open(os.path.join(D, "plan.pkl"), "rb"))
tot_docs = tot_bytes = 0
c0 = cpu(); t0 = time.time()
R = int(sys.argv[1]) if len(sys.argv) > 1 else 3
for _ in range(R):
    for t in st["threads"]:
        docs = list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)]))
        tot_docs += len(docs); tot_bytes += sum(len(bson.encode(d)) for d in docs)
dc = cpu() - c0
n = R * len(st["threads"])
print(f"main list query: {n} calls, {tot_docs/n:.1f} docs/call, {tot_bytes/n/1e6:.2f} MB/call, server cpu {1000*dc/n:.2f} ms/call, {1e9*dc/tot_bytes:.2f} ns/byte ({1e3*dc/ (tot_bytes/1e6):.2f} ms/MB)")
