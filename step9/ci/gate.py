"""Step 9 · the gate before any AWS run (founder, RULES.md 8-9, 2026-11-09). decide.py calls it for mode "run";
mode "permissions" ($0) is not gated. It stays CLOSED unless the LOCAL BENCHMARK REPORT of the engine ref the round
names exists and passes:
  0. the report: step9/local/benchmark-report-<commit>.json (written by step9/local/benchmark_report.py), made on the
     exact commit the round's "engine" ref names (ENGINE_COMMIT, resolved by the workflow before decide.py). No report
     for that commit, or no commit at all: CLOSED. A report made on another commit does not count (the engine changed).
  1. the takeover test the report names shows no acknowledged write missing and finished (rule 8: zero loss is absolute);
  2. in the agent benchmark the report names, our server ("fenvix") is not worse than the PostgreSQL or the mongod saver
     on any operation: p50 or p99 latency, or stored bytes per logical byte (rule 9);
  3. (aws-fixes, B1 of the total review, 2026-11-11) that benchmark is complete and correct on every side: the summary's
     own "ok" is true for ours, PostgreSQL and mongod (no write error, no wrong read, no erased thread readable); every
     operation of OPS is present on every side with at least MIN_SAMPLES[op] samples; stored bytes per logical byte is
     known on every side; the three sides ran the same plan (digest). A missing operation, a missing number or a failed
     run is a blocker, never skipped;
  4. the same for the --workers 8 line, when the report names one ("agent_bench_w8");
  5. (bench-local fix round, review of 2026-11-11) the two commonest resumes, resume[write/none] and
     resume[write/short], are compared like every other operation (F1); resume[cold] must be present with its minimum of
     samples but is reported, not compared (F4, DECISIONS 2026-11-11); our side's bytes count only if its settle ended
     idle (not capped) after compaction 2 published at least once (F9); our side fails if the 3-zone log refused any
     batch (quaderno.failed > 0: those writes were acknowledged by the S3-WAL fallback, not by 2 of 3 log copies, F5);
     any side whose client was the limit (summary "client_limited", F7) is not valid.
The report and the result files it names must be committed on the branch the workflow runs from; the gate reads this
checkout. With no report for the commit, the gate also lists what the newest local results show (information only).
Usage: gate.py [REPO_ROOT]   (ENGINE_REF and ENGINE_COMMIT in the environment)
  -> prints "GATE: OPEN" (exit 0) or "GATE: CLOSED" and the blockers (exit 1)"""
import glob
import json
import os
import sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
OPS = ["put", "put_writes", "save_step", "resume[read]", "resume[write/first]", "resume[write/none]", "resume[write/short]",
       "resume[write/long]", "resume[cold]", "time_travel", "list", "list_limit10", "erase_user"]
# reported (present, with its minimum of samples) but never compared: DECISIONS 2026-11-11, locally the store is MinIO on
# the same machine, so the cold number says nothing about S3 (F4 of the bench-local review)
NOT_COMPARED = {"resume[cold]"}
# The written minimum of samples per operation (B1). Fixed BEFORE any result is read: the counts bench.py gives with
# the standard profile (seed 42: 2,453 threads, 25,398 turns, 1,000 users), --read-sample 30 (30 threads read, 3 time
# travels each), --erase-users 0 (10% of users = 100) and the cold phase of P-bench-local (200 threads). The small
# profile (41 threads, 8 long resumes, 2 erased users) does not pass: a p99 of 8 samples is the slowest of 8.
# resume[write/none] and resume[write/short] (F1, written before any result is read): 21,173 and 1,083 in the standard
# plan; minimums 1,000 and 300.
MIN_SAMPLES = {"put": 1000, "put_writes": 1000, "save_step": 1000, "resume[write/first]": 300, "resume[write/long]": 300,
               "resume[write/none]": 1000, "resume[write/short]": 300,
               "resume[read]": 30, "resume[cold]": 200, "time_travel": 90, "list": 30, "list_limit10": 30,
               "erase_user": 20}
SIDES = ("fenvix", "postgres", "mongo")


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


def check_agent(fenvix_summary, label="agent benchmark"):
    """Blockers of one local agent benchmark (bench/agent-checkpoints/results/local-*/{fenvix,postgres,mongo}/).
    Nothing is skipped: a side, an operation, a sample count or a byte figure that is missing is a blocker."""
    if not fenvix_summary or not os.path.exists(fenvix_summary):
        return [f"{label}: no result ({fenvix_summary})"]
    out = []
    base = os.path.dirname(os.path.dirname(fenvix_summary))
    sums = {}
    for side in SIDES:
        f = os.path.join(base, side, "summary.json")
        if not os.path.exists(f):
            out.append(f"{label}: no {side} result ({rel(f)})")
            continue
        try:
            sums[side] = json.load(open(f))
        except Exception as e:  # noqa: BLE001 - an unreadable summary is a blocker
            out.append(f"{label}: {side} summary unreadable ({type(e).__name__})")
    for side, d in sums.items():
        if d.get("ok") is not True:
            w, r, e = d.get("write") or {}, d.get("read") or {}, d.get("erase") or {}
            out.append(f"{label} {side}: ok is {d.get('ok')} (write errors {len(w.get('errors') or [])}, wrong reads "
                       f"{r.get('inconsistencies')}, wrong_reads {d.get('wrong_reads', r.get('wrong_reads'))}, erased threads still readable "
                       f"{e.get('threads_left_readable')})")
        if d.get("client_limited"):
            out.append(f"{label} {side}: the client was the limit ({d.get('client_cpu')}): not valid (RULES: the sender must "
                       f"not be the limit)")
        lat = d.get("latency_ms") or {}
        for op in OPS:
            a = lat.get(op)
            if not a or a.get("p50") is None or a.get("p99") is None:
                out.append(f"{label} {side}: operation {op} missing")
            elif (a.get("n") or 0) < MIN_SAMPLES[op]:
                out.append(f"{label} {side}: operation {op} has {a.get('n') if a.get('n') is not None else 'no count of'} samples, fewer than the written minimum {MIN_SAMPLES[op]}")
        if not (d.get("bytes") or {}).get("stored_per_logical"):
            out.append(f"{label} {side}: stored bytes per logical byte unknown")
    if "fenvix" in sums:
        out += check_ours(sums["fenvix"], label)
    digests = {side: (d.get("plan") or {}).get("digest") for side, d in sums.items()}
    if len(set(digests.values())) > 1 or None in digests.values():
        out.append(f"{label}: the sides did not run the same plan (digests {digests})")
    ours = sums.get("fenvix")
    if ours:
        for other in ("postgres", "mongo"):
            them = sums.get(other)
            if not them:
                continue
            for op in OPS:
                if op in NOT_COMPARED:
                    continue
                a, b = (ours.get("latency_ms") or {}).get(op), (them.get("latency_ms") or {}).get(op)
                if not a or not b:
                    continue   # already a blocker above
                for q in ("p50", "p99"):
                    if a.get(q) is not None and b.get(q) is not None and a[q] > b[q]:
                        out.append(f"{label} {op} {q}: ours {a[q]:.2f} ms > {other} {b[q]:.2f} ms")
            sa, sb = (ours.get("bytes") or {}).get("stored_per_logical"), (them.get("bytes") or {}).get("stored_per_logical")
            if sa and sb and sa > sb:
                out.append(f"{label} stored bytes per logical byte: ours {sa:.3f} > {other} {sb:.3f}")
    return out


def check_ours(d, label):
    """Our side only: the bytes were measured after compaction 2, settled (F9); the 3-zone log refused nothing (F5)."""
    out = []
    st = d.get("settle")
    if not st:
        out.append(f"{label} fenvix: no settle record: the bytes were not measured after compaction (--settle)")
    else:
        if st.get("capped") or not st.get("idle"):
            out.append(f"{label} fenvix: the settle hit its cap after {st.get('seconds')} s: bytes not settled")
        if not st.get("compaction2_published"):
            out.append(f"{label} fenvix: compaction 2 never published (compactor.publishes 0): the bytes are not after compaction 2")
    for k in ("server_status_after_write", "server_status_end"):
        q = ((d.get(k) or {}).get("quaderno") or {})
        if (q.get("failed") or 0) > 0:
            out.append(f"{label} fenvix: the 3-zone log refused {q['failed']} batches ({k}): writes acknowledged by the "
                       f"S3-WAL fallback, not at equal durability")
    if not d.get("server_status_end"):
        out.append(f"{label} fenvix: no server status at the end of the run (quaderno.failed unknown)")
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
        if r.get("agent_bench_w8"):
            blockers += check_agent(os.path.join(ROOT, r["agent_bench_w8"]), label="agent benchmark --workers 8")
    if blockers:
        print(f"GATE: CLOSED ({len(blockers)} blockers; RULES.md 8-9: no AWS run, not even a pilot)")
        for b in blockers:
            print(f"  - {b}")
        sys.exit(1)
    print(f"GATE: OPEN (local benchmark report {rel(found[0])} for {ref} at {commit}: takeovers 0 lost; our agent benchmark "
          f"ok, complete ({len(OPS)} operations, written minimum samples) and not worse than PostgreSQL and mongod on any "
          f"operation" + (", also with --workers 8" if found[1].get("agent_bench_w8") else "") + ")")


if __name__ == "__main__":
    main()
