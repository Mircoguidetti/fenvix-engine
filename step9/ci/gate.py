"""Step 9 · the gate before any AWS run (founder, RULES.md 8-9, 2026-11-09). decide.py calls it for mode "run";
mode "permissions" ($0) is not gated. It stays CLOSED unless the LOCAL BENCHMARK REPORT of the engine ref the round
names exists and passes:
  0. the report: step9/local/benchmark-report-<commit>.json (written by step9/local/benchmark_report.py), made on the
     exact commit the round's "engine" ref names (ENGINE_COMMIT, resolved by the workflow before decide.py). No report
     for that commit, or no commit at all: CLOSED. A report made on another commit does not count (the engine changed).
  1. the takeover test the report names shows no acknowledged write missing and finished (rule 8: zero loss is absolute);
  2. in the agent benchmark the report names, our server ("fenvix") is not worse than the PostgreSQL or the mongod saver
     on any operation: p50 or p99 latency, or stored bytes per logical byte (rule 9).
The report and the result files it names must be committed on the branch the workflow runs from; the gate reads this
checkout. With no report for the commit, the gate also lists what the newest local results show (information only).
Usage: gate.py [REPO_ROOT]   (ENGINE_REF and ENGINE_COMMIT in the environment)
  -> prints "GATE: OPEN" (exit 0) or "GATE: CLOSED" and the blockers (exit 1)"""
import glob
import json
import os
import sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
OPS = ["put", "put_writes", "resume[read]", "resume[write/first]", "resume[write/long]", "list", "list_limit10", "erase_user",
       # P-bench-local (2026-11-11): added, nothing removed or loosened
       "save_step", "time_travel", "resume[cold]"]


def rel(p):
    return os.path.relpath(p, ROOT)


def check_takeovers(path):
    """Blockers of one takeover summary (step9/programs/takeovers.py)."""
    if not path or not os.path.exists(path):
        return [f"takeovers: no result ({path})"]
    d = json.load(open(path))
    lost = d.get("lost")
    if d.get("verdict") != "PASS" or lost not in (0,) or d.get("done") != d.get("n"):
        return [f"takeovers ({rel(path)}): verdict {d.get('verdict')}, {d.get('done')}/{d.get('n')} done, "
                f"lost {lost} of {d.get('acknowledged')} acknowledged writes (rule 8: must be 0)"]
    return []


def check_agent(fenvix_summary):
    """Blockers of one local agent benchmark (bench/agent-checkpoints/results/local-*/{fenvix,postgres,mongo}/)."""
    if not fenvix_summary or not os.path.exists(fenvix_summary):
        return [f"agent benchmark: no result ({fenvix_summary})"]
    out = []
    base = os.path.dirname(os.path.dirname(fenvix_summary))
    ours = json.load(open(fenvix_summary))
    for other in ("postgres", "mongo"):
        f = os.path.join(base, other, "summary.json")
        if not os.path.exists(f):
            out.append(f"agent benchmark: no {other} result next to ours ({rel(base)})")
            continue
        them = json.load(open(f))
        for op in OPS:
            a, b = ours["latency_ms"].get(op), them["latency_ms"].get(op)
            if not a or not b:
                continue
            for q in ("p50", "p99"):
                if a[q] > b[q]:
                    out.append(f"agent benchmark {op} {q}: ours {a[q]:.2f} ms > {other} {b[q]:.2f} ms")
        sa, sb = ours["bytes"].get("stored_per_logical"), them["bytes"].get("stored_per_logical")
        if sa and sb and sa > sb:
            out.append(f"agent benchmark stored bytes per logical byte: ours {sa:.3f} > {other} {sb:.3f}")
    return out


def newest(pattern):
    files = sorted(glob.glob(os.path.join(ROOT, pattern)), key=os.path.getmtime)
    return files[-1] if files else None


def find_report(commit):
    """The local benchmark report made on this commit (a full or abbreviated hex id, at least 7 characters)."""
    if not commit or len(commit) < 7:
        return None
    for f in sorted(glob.glob(os.path.join(ROOT, "step9/local/benchmark-report-*.json"))):
        try:
            r = json.load(open(f))
        except Exception:
            continue
        rc = str(r.get("engine_commit") or "")
        if len(rc) >= 7 and (rc.startswith(commit) or commit.startswith(rc)):
            return f, r
    return None


def main():
    ref = os.environ.get("ENGINE_REF", "")
    commit = os.environ.get("ENGINE_COMMIT", "")
    blockers = []
    found = find_report(commit)
    if not commit:
        blockers.append(f"the engine ref {ref or '(none)'} names no commit the workflow could resolve: no local benchmark report can match it")
    if not found:
        blockers.append(f"no local benchmark report for engine {ref or '(none)'} at commit {commit or '?'} "
                        f"(step9/local/benchmark-report-<commit>.json, made by step9/local/benchmark_report.py on that commit)")
        info = check_takeovers(newest("step9/local/results-*/takeovers-summary-*.json")) + \
            check_agent(newest("bench/agent-checkpoints/results/local-*/fenvix/summary.json"))
        blockers += [f"(newest local results, another commit) {b}" for b in info]
    else:
        f, r = found
        if r.get("verdict") != "PASS":
            blockers.append(f"{rel(f)}: verdict {r.get('verdict')}")
        blockers += check_takeovers(os.path.join(ROOT, r.get("takeovers") or "-"))
        blockers += check_agent(os.path.join(ROOT, r.get("agent_bench") or "-"))
    if blockers:
        print(f"GATE: CLOSED ({len(blockers)} blockers; RULES.md 8-9: no AWS run, not even a pilot)")
        for b in blockers:
            print(f"  - {b}")
        sys.exit(1)
    print(f"GATE: OPEN (local benchmark report {rel(found[0])} for {ref} at {commit}: takeovers 0 lost; our agent benchmark "
          f"not worse than PostgreSQL and mongod on any operation)")


if __name__ == "__main__":
    main()
