"""P-command-cpu: FIRST and NEXT requests on the kept big line, with the cook's CPU per call (its cgroup's
cpuacct.usage, ns) and a hash of every answer (for A/B: two binaries on the same data must give the same hashes).

Per thread (K threads of the big plan, seed S): FIRST resume, then the NEXT requests resume, time travel, whole
history, last 10 (the saver's own calls, as bigmix.py's read task makes them). Reads only: the data does not change, so
two runs on two binaries see the same data.

  BENCH_PW=... python lineprobe.py --threads 300 --seed 5 --out FILE.json [--prof]
"""
import argparse, hashlib, json, os, random, signal, subprocess, sys, time
H = os.environ.get("HARNESS", "/home/user/harness/bench/agent-checkpoints")
sys.path.insert(0, H)
import generator  # noqa: E402
import lines  # noqa: E402
from langgraph.checkpoint.mongodb import MongoDBSaver  # noqa: E402
from pymongo import MongoClient  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--threads", type=int, default=300)
ap.add_argument("--seed", type=int, default=5)
ap.add_argument("--out", required=True)
ap.add_argument("--prof", default=None, help="dir: SIGUSR2 around the NEXT requests (cpu-prof cook)")
a = ap.parse_args()
cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", "cook"], capture_output=True, text=True).stdout.strip()
tini = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", "cook"], capture_output=True, text=True).stdout.strip()
srv = subprocess.run(["pgrep", "-P", tini], capture_output=True, text=True).stdout.split()[0]
CG = f"/sys/fs/cgroup/cpuacct/docker/{cid}/cpuacct.usage"


def cpu_ns():
    return int(open(CG).read())


threads, _ = generator.plan(42, "big")
tids = sorted(t.thread_id for t in threads)
random.Random(a.seed).shuffle(tids)
tids = tids[: a.threads]
c = MongoClient(lines.uri_of("fenvix-log"))
saver = MongoDBSaver(c, db_name="app_demo")


def h(x):
    return hashlib.sha1(repr(x).encode()).hexdigest()[:16]


def cfg(t, ck=None):
    d = {"configurable": {"thread_id": t, "checkpoint_ns": ""}}
    if ck:
        d["configurable"]["checkpoint_id"] = ck
    return d


def tup(x):
    if x is None:
        return None
    return (x.config, x.checkpoint, x.metadata, x.parent_config, x.pending_writes)


rows, hashes = [], {}
if a.prof:
    os.kill(int(srv), signal.SIGUSR2)
for i, t in enumerate(tids):
    seq = [("resume:first", lambda: tup(saver.get_tuple(cfg(t))))]
    for op, f in seq + [("resume", lambda: tup(saver.get_tuple(cfg(t)))),
                        ("list", lambda: [tup(x) for x in saver.list(cfg(t))]),
                        ("list_limit10", lambda: [tup(x) for x in saver.list(cfg(t), limit=10)])]:
        c0, t0 = cpu_ns(), time.perf_counter()
        out = f()
        dt, dc = time.perf_counter() - t0, cpu_ns() - c0
        rows.append({"op": op, "t": t, "ms": dt * 1000, "cpu_ms": dc / 1e6})
        hashes[f"{op}:{t}"] = h(out)
        if op == "list":
            ids = sorted(x[0]["configurable"]["checkpoint_id"] for x in out)
            ck = random.Random(f"{a.seed}:{t}").choice(ids) if ids else None
            if ck:
                c0, t0 = cpu_ns(), time.perf_counter()
                o2 = tup(saver.get_tuple(cfg(t, ck)))
                dt, dc = time.perf_counter() - t0, cpu_ns() - c0
                rows.append({"op": "time_travel", "t": t, "ms": dt * 1000, "cpu_ms": dc / 1e6})
                hashes[f"time_travel:{t}"] = h(o2)
if a.prof:
    os.kill(int(srv), signal.SIGUSR2)
    time.sleep(3)


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 2)


summ = {}
for op in ("resume:first", "resume", "time_travel", "list", "list_limit10"):
    r = [x for x in rows if x["op"] == op]
    if r:
        summ[op] = {"n": len(r), "p50_ms": pct([x["ms"] for x in r], .5), "p99_ms": pct([x["ms"] for x in r], .99),
                    "cpu_ms_mean": round(sum(x["cpu_ms"] for x in r) / len(r), 3), "cpu_ms_p50": pct([x["cpu_ms"] for x in r], .5)}
        print(op, json.dumps(summ[op]), flush=True)
json.dump({"summary": summ, "rows": rows, "hashes": hashes, "threads": tids}, open(a.out, "w"))
