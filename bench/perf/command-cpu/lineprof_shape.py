"""Profile one command shape on the big line (cpu-prof cook): the saver's pending-writes lookup, repeated over the
checkpoints of N warm threads. Writes /home/user/prof/line/prof.folded (moved to ARG2)."""
import os, sys, time, subprocess, random, signal, shutil
sys.path.insert(0, "/home/user/harness/bench/agent-checkpoints")
import lines, generator
from pymongo import MongoClient
tini = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", "cook"], capture_output=True, text=True).stdout.strip()
srv = int(subprocess.run(["pgrep", "-P", tini], capture_output=True, text=True).stdout.split()[0])
c = MongoClient(lines.uri_of("fenvix-log")); db = c.app_demo
threads, _ = generator.plan(42, "big"); tids = sorted(t.thread_id for t in threads); random.Random(11).shuffle(tids)
work = []
for t in tids[:60]:
    for d in db.checkpoints.find({"thread_id": t, "checkpoint_ns": ""}, {"checkpoint_id": 1}):
        work.append((t, d["checkpoint_id"]))
shape = sys.argv[1]
os.kill(srv, signal.SIGUSR2); time.sleep(0.3)
for _ in range(int(sys.argv[3]) if len(sys.argv) > 3 else 3):
    for t, ck in work:
        if shape == "writes":
            list(db.checkpoint_writes.find({"thread_id": t, "checkpoint_ns": "", "checkpoint_id": ck}))
        elif shape == "ping":
            c.admin.command("ping")
time.sleep(0.3); os.kill(srv, signal.SIGUSR2); time.sleep(3)
d = "/home/user/prof/line"; f = max((x for x in os.listdir(d) if x.startswith("prof.folded")), key=lambda x: os.path.getmtime(os.path.join(d, x))); shutil.move(os.path.join(d, f), sys.argv[2])
print(len(work), "calls per pass")
