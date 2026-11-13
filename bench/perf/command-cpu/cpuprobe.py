"""P-command-cpu: server CPU per NEXT request, on the dev bench (devbench.py up + load).

For every measured op (resume, time travel, whole-history list, last-10 list, save one step) it runs the op on the
loaded threads after a warm-up visit of each thread (so every request is a NEXT request: the customer's box is in
memory), and reports per op:
  - the server process's CPU per call (utime + stime of /proc/<pid>/stat, all threads), MEASURED;
  - the client latency p50 / p99;
  - the number of wire commands per call (pymongo command listener).
With --prof DIR, the server (a `--features cpu-prof` build started with CPU_PROF_FILE) is sent SIGUSR2 around each op
so DIR gets one folded-stack profile per op.

  python cpuprobe.py --harness /path/to/bench/agent-checkpoints [--rounds 3] [--prof DIR] [--ops resume,list,...]
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import random
import signal
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--harness", required=True)
ap.add_argument("--dir", default=os.environ.get("DEVBENCH_DIR", "/tmp/claude-0/devbench"))
ap.add_argument("--rounds", type=int, default=3)
ap.add_argument("--prof", default=None)
ap.add_argument("--ops", default="resume,time_travel,list,list10,save")
ap.add_argument("--saves", type=int, default=120, help="save steps per round (graph turns until at least this many)")
ap.add_argument("--out", default=None)
a = ap.parse_args()
sys.path.insert(0, a.harness)
import generator  # noqa: E402
import graph_runner  # noqa: E402
from langgraph.checkpoint.mongodb import MongoDBSaver  # noqa: E402
from pymongo import MongoClient, monitoring  # noqa: E402

URI = "mongodb://127.0.0.1:27317/?directConnection=true"
SEED = 42
pid = int(open(os.path.join(a.dir, "server.pid")).read())
TCK = os.sysconf("SC_CLK_TCK")


def cpu_s() -> float:
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(f[11]) + int(f[12])) / TCK


class Count(monitoring.CommandListener):
    n = 0
    names: dict = {}

    def started(self, e):
        if e.command_name in ("hello", "isMaster", "ismaster", "endSessions"):
            return
        Count.n += 1
        Count.names[e.command_name] = Count.names.get(e.command_name, 0) + 1

    def succeeded(self, e):
        pass

    def failed(self, e):
        pass


st = pickle.load(open(os.path.join(a.dir, "plan.pkl"), "rb"))
exp = graph_runner.Expected()
exp.merge_state(st["expected"])
threads_all, _ = generator.plan(SEED, st["profile"])
threads = {t.thread_id: t for t in threads_all if t.thread_id in set(st["threads"])}
tids = sorted(threads)
client = MongoClient(URI, event_listeners=[Count()])
saver = MongoDBSaver(client, db_name="app_demo")
rows = []


class Rec:
    def __call__(self, row):
        rows.append(row)


ts = graph_runner.TimedSaver(saver, Rec(), expected=exp)
script = graph_runner.Script()
graph = graph_runner.build_graph(ts, script)
extra = st.get("extra", {})


def cfg(t, cid=None):
    c = {"configurable": {"thread_id": t, "checkpoint_ns": ""}}
    if cid:
        c["configurable"]["checkpoint_id"] = cid
    return c


def warm():
    for t in tids:
        ts.get_tuple(cfg(t))


def one(op, t, r):
    if op == "resume":
        ts.get_tuple(cfg(t))
    elif op == "time_travel":
        ids = sorted(cid for ns, cid in exp.written_ids(t) if ns == "")
        ts.get_tuple(cfg(t, r.choice(ids)))
    elif op == "list":
        list(ts.list(cfg(t)))
    elif op == "list10":
        list(ts.list(cfg(t), limit=10))


def save_round(r):
    """Graph turns on random threads until a.saves save steps ran; returns the number of save steps."""
    n0 = sum(1 for x in rows if x["op"] == "save_step")
    while sum(1 for x in rows if x["op"] == "save_step") - n0 < a.saves:
        t = r.choice(tids)
        k = extra.get(t, 0)
        th = generator.extend_thread(seed=SEED, th=threads[t], extra=k + 1)
        ts.get_tuple(cfg(t))  # the customer's box warm, as in the bench's second turn
        ts.context = {"phase": "save"}
        graph_runner.run_turn(graph, script, SEED, th, threads[t].turns + k)
        extra[t] = k + 1
    return sum(1 for x in rows if x["op"] == "save_step") - n0


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


warm()
res = {}
for op in a.ops.split(","):
    r = random.Random(7)
    cpu_tot, calls, cmds, lat = 0.0, 0, 0, []
    if a.prof:
        os.kill(pid, signal.SIGUSR2)
        time.sleep(0.3)
    for rnd in range(a.rounds):
        n_rows = len(rows)
        c0, k0 = cpu_s(), Count.n
        if op == "save":
            ts.context = {"phase": "save"}
            n = save_round(r)
            c1, k1 = cpu_s(), Count.n
            # the save step's own latency; the turn's resume (get_tuple) and the checks are not in it
            lat += [x["ms"] for x in rows[n_rows:] if x["op"] == "save_step"]
            # CPU of the whole turn (its warm resume included) divided over its save steps: an upper bound per save
        else:
            n = 0
            for t in tids:
                ts.context = {"phase": op}
                one(op, t, r)
                n += 1
            c1, k1 = cpu_s(), Count.n
            lat += [x["ms"] for x in rows[n_rows:] if x["op"] in ("resume", "time_travel", "list", "list_limit10")]
        cpu_tot += c1 - c0
        calls += n
        cmds += k1 - k0
    if a.prof:
        time.sleep(0.3)
        os.kill(pid, signal.SIGUSR2)
        time.sleep(1.0)
        src = os.environ.get("CPU_PROF_FILE", os.path.join(a.dir, "prof.folded"))
        # the server names the n-th profile path.n; find the newest
        cands = [p for p in os.listdir(os.path.dirname(src)) if p.startswith(os.path.basename(src))]
        newest = max(cands, key=lambda p: os.path.getmtime(os.path.join(os.path.dirname(src), p)))
        os.makedirs(a.prof, exist_ok=True)
        os.replace(os.path.join(os.path.dirname(src), newest), os.path.join(a.prof, f"{op}.folded"))
    res[op] = {"calls": calls, "cpu_ms_per_call": round(1000 * cpu_tot / calls, 3), "cmds_per_call": round(cmds / calls, 2),
               "p50_ms": round(pct(lat, 0.5), 2), "p99_ms": round(pct(lat, 0.99), 2), "n_lat": len(lat)}
    print(op, json.dumps(res[op]), flush=True)
print("wrong reads", sum(exp.wrong.values()), dict(exp.wrong), "reads checked", exp.reads_checked)
st["extra"], st["expected"] = extra, exp.export_state()
pickle.dump(st, open(os.path.join(a.dir, "plan.pkl"), "wb"))
if a.out:
    json.dump({"ops": res, "wrong": sum(exp.wrong.values()), "commands": Count.names}, open(a.out, "w"), indent=1)
