"""Step 9 · the gate before any AWS run (founder, RULES.md 8-9, 2026-11-09). decide.py calls it for mode "run";
mode "permissions" ($0) is not gated. It refuses while either is true, and prints every blocker:
  1. the latest local takeover test (step9/local/results-*/takeovers-summary-*.json, newest file) shows any acknowledged
     write missing, or did not finish (rule 8: zero loss is absolute);
  2. in the latest local agent benchmark (bench/agent-checkpoints/results/local-*/), our server ("fenvix") is worse than
     the PostgreSQL or the mongod saver on any operation: p50 or p99 latency, or stored bytes per logical byte (rule 9).
The local results must be committed with the engine ref the round would use; the gate reads the files in this checkout.
Usage: gate.py [REPO_ROOT]   -> prints "GATE: OPEN" (exit 0) or "GATE: CLOSED" and the blockers (exit 1)"""
import glob
import json
import os
import sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
OPS = ["put", "put_writes", "resume[read]", "resume[write/first]", "resume[write/long]", "list", "list_limit10", "erase_user"]
blockers = []


def newest(pattern):
    files = sorted(glob.glob(os.path.join(ROOT, pattern)), key=os.path.getmtime)
    return files[-1] if files else None


t = newest("step9/local/results-*/takeovers-summary-*.json")
if not t:
    blockers.append("no local takeover result (step9/local/results-*/takeovers-summary-*.json)")
else:
    d = json.load(open(t))
    lost = d.get("lost")
    if d.get("verdict") != "PASS" or lost not in (0,) or d.get("done") != d.get("n"):
        blockers.append(f"takeovers ({os.path.relpath(t, ROOT)}): verdict {d.get('verdict')}, {d.get('done')}/{d.get('n')} done, "
                        f"lost {lost} of {d.get('acknowledged')} acknowledged writes (rule 8: must be 0)")

run = newest("bench/agent-checkpoints/results/local-*/fenvix/summary.json")
if not run:
    blockers.append("no local agent benchmark (bench/agent-checkpoints/results/local-*/fenvix/summary.json)")
else:
    base = os.path.dirname(os.path.dirname(run))
    ours = json.load(open(run))
    for other in ("postgres", "mongo"):
        f = os.path.join(base, other, "summary.json")
        if not os.path.exists(f):
            blockers.append(f"agent benchmark: no {other} result next to ours ({os.path.relpath(base, ROOT)})")
            continue
        them = json.load(open(f))
        for op in OPS:
            a, b = ours["latency_ms"].get(op), them["latency_ms"].get(op)
            if not a or not b:
                continue
            for q in ("p50", "p99"):
                if a[q] > b[q]:
                    blockers.append(f"agent benchmark {op} {q}: ours {a[q]:.2f} ms > {other} {b[q]:.2f} ms")
        sa, sb = ours["bytes"].get("stored_per_logical"), them["bytes"].get("stored_per_logical")
        if sa and sb and sa > sb:
            blockers.append(f"agent benchmark stored bytes per logical byte: ours {sa:.3f} > {other} {sb:.3f}")

if blockers:
    print(f"GATE: CLOSED ({len(blockers)} blockers; RULES.md 8-9: no AWS run, not even a pilot)")
    for b in blockers:
        print(f"  - {b}")
    sys.exit(1)
print("GATE: OPEN (local takeovers 0 lost; our agent benchmark not worse than PostgreSQL and mongod on any operation)")
