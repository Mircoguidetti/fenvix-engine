"""CPU of a whole-history list on the big line, split: the checkpoints query (one find, its getMores) and the
pending-writes lookups (one find per checkpoint), as the MongoDB saver sends them. Cook cgroup CPU, MEASURED."""
import sys, time, subprocess, random
sys.path.insert(0, "/home/user/harness/bench/agent-checkpoints")
import lines, generator
from pymongo import MongoClient
cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", "cook"], capture_output=True, text=True).stdout.strip()
CG = f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage"
cpu = lambda: int(open(CG).read()) / 1e6
c = MongoClient(lines.uri_of("fenvix-log")); db = c.app_demo
threads, _ = generator.plan(42, "big"); tids = sorted(t.thread_id for t in threads); random.Random(int(sys.argv[1]) if len(sys.argv) > 1 else 9).shuffle(tids)
tids = tids[:150]
for t in tids:  # FIRST (warm the box)
    db.checkpoints.find_one({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)])
main = wr = 0.0; nw = nd = 0; wl = []
for t in tids:
    a = cpu(); docs = list(db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, sort=[("checkpoint_id", -1)])); main += cpu() - a
    nd += len(docs)
    for d in docs:
        a = cpu(); t0 = time.perf_counter()
        list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": d["checkpoint_id"]}))
        wr += cpu() - a; nw += 1; wl.append((time.perf_counter() - t0) * 1000)
wl.sort()
print(f"threads {len(tids)} docs/thread {nd/len(tids):.1f}: main query {main/len(tids):.2f} ms/list, writes lookups {wr/len(tids):.2f} ms/list ({1000*wr/nw:.0f} us each, wall p50 {wl[len(wl)//2]:.3f} ms)")
