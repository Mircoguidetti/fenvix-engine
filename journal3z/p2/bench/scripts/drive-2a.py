#!/usr/bin/env python3
"""§2a driver (docs/journal3z-phase2-plan.md §2a): long chunks of 115 minutes until BOTH at least 12 hours of faults
and every minimum fault count are reached, then the 50 short runs of 15 minutes. Stops at the first failing run
(§2d). After every run: journal3z/results/phase2/2a/progress.json (totals so far, raw counts from each summary.json).
Local only (MinIO on this machine)."""
import json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(HERE, "../../../results/phase2/2a")
MIN = {  # §2a minimum counts
    "node kill -9 (NodeKill)": (["NodeKill"], 300),
    "writer kill -9 (WriterKill)": (["WriterKill"], 300),
    "disk lost (DiskLoss)": (["DiskLoss"], 50),
    "network cuts (CutNode+CutOneWay+CutZone)": (["CutNode", "CutOneWay", "CutZone"], 300),
    "  of which one direction (CutOneWay)": (["CutOneWay"], 75),
    "  of which whole zone (CutZone)": (["CutZone"], 50),
    "clock jumps (ClockJump)": (["ClockJump"], 150),
    "two writers (TwoWriters)": (["TwoWriters"], 150),
    "fsync errors (FsyncError)": (["FsyncError"], 50),
    "disk full (DiskFull)": (["DiskFull"], 50),
}

def totals():
    t = {"runs": [], "fault_minutes": 0.0, "faults": {}, "acked": 0}
    for name in sorted(os.listdir(R)):
        f = os.path.join(R, name, "summary.json")
        if not os.path.exists(f):
            continue
        d = json.load(open(f))
        kind = "chunk" if name.startswith("chunk") else "short"
        t["runs"].append({"name": name, "minutes": round(d["minutes_run"], 1), "failures": d["failures"], "acked": d["writes_acked"]})
        if kind == "chunk":
            t["fault_minutes"] += d["minutes_run"]
        t["acked"] += d["writes_acked"]
        for k, v in d["fault_counts"].items():
            t["faults"][k] = t["faults"].get(k, 0) + v
    t["minimums"] = {k: {"have": sum(t["faults"].get(x, 0) for x in ks), "need": n} for k, (ks, n) in MIN.items()}
    t["acked_need"] = 2_000_000
    t["long_done"] = t["fault_minutes"] >= 720 and all(v["have"] >= v["need"] for v in t["minimums"].values())
    json.dump(t, open(os.path.join(R, "progress.json"), "w"), indent=1)
    return t

def run(args):
    rc = subprocess.call(["bash", os.path.join(HERE, "run-2a.sh")] + args)
    t = totals()
    if rc != 0:
        print("FAILED", args, file=sys.stderr)
        sys.exit(1)
    return t

os.makedirs(R, exist_ok=True)
t = totals()
n = sum(1 for r in t["runs"] if r["name"].startswith("chunk"))
while not t["long_done"] and n < 12:
    n += 1
    t = run(["chunk", str(n), "115"])
done_short = sum(1 for r in t["runs"] if r["name"].startswith("short"))
for i in range(done_short + 1, 51):
    t = run(["short", str(i), str(i)])
print("ALL DONE", file=sys.stderr)
