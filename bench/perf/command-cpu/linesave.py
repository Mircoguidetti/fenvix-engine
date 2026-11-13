"""Saves on the kept big line: the write task's two turns (bigmix.py) on N threads of the plan, with the cook's CPU per
save step (cgroup, MEASURED) and the save step's latency. Threads from the end of a seeded shuffle (not the ones
lineprobe.py reads). Optional --prof: SIGUSR2 around the NEXT turn's saves."""
import argparse, json, os, random, signal, subprocess, sys, time
sys.path.insert(0, "/home/user/harness/bench/agent-checkpoints")
import generator, graph_runner, lines
from langgraph.checkpoint.mongodb import MongoDBSaver
from pymongo import MongoClient
ap = argparse.ArgumentParser(); ap.add_argument("--threads", type=int, default=100); ap.add_argument("--seed", type=int, default=77)
ap.add_argument("--extra", type=int, default=0, help="turns already added to these threads by earlier runs"); ap.add_argument("--out", required=True)
a = ap.parse_args()
cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", "cook"], capture_output=True, text=True).stdout.strip()
CG = f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage"
cpu = lambda: int(open(CG).read()) / 1e6
threads, _ = generator.plan(42, "big"); by = {t.thread_id: t for t in threads}
tids = sorted(by); random.Random(a.seed).shuffle(tids); tids = tids[-a.threads:]
c = MongoClient(lines.uri_of("fenvix-log")); saver = MongoDBSaver(c, db_name="app_demo")
rows = []
class Rec:
    def __call__(self, r): rows.append(r)
ts = graph_runner.TimedSaver(saver, Rec(), expected=None)
script = graph_runner.Script(); graph = graph_runner.build_graph(ts, script)
cpu_by = {"long": 0.0, "none": 0.0}
for t in tids:
    th = generator.extend_thread(seed=42, th=by[t], extra=a.extra + 2)
    for j, label in ((0, "long"), (1, "none")):
        ts.context = {"gap": label}
        c0 = cpu(); n0 = len(rows)
        graph_runner.run_turn(graph, script, 42, th, by[t].turns + a.extra + j)
        cpu_by[label] += cpu() - c0
        for r in rows[n0:]:
            r["gap"] = label
def pct(xs, q):
    xs = sorted(xs); return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2)
out = {}
for label in ("long", "none"):
    s = [r["ms"] for r in rows if r["op"] == "save_step" and r.get("gap") == label]
    turn_reads = [r["ms"] for r in rows if r["op"] == "resume" and r.get("gap") == label]
    out[label] = {"save_steps": len(s), "p50_ms": pct(s, .5), "p99_ms": pct(s, .99), "cook_cpu_ms_per_save_step_incl_turn_resume": round(cpu_by[label] / max(1, len(s)), 3)}
    print(label, json.dumps(out[label]), flush=True)
json.dump({"summary": out, "rows": rows}, open(a.out, "w"), default=str)
