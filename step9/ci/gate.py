"""Step 9 · the gate before any AWS run (founder, RULES.md 8-9, 2026-11-09). decide.py calls it for mode "run";
mode "permissions" ($0) is not gated. It stays CLOSED unless the LOCAL BENCHMARK REPORT of the engine ref the round
names exists and passes:
  0. the report: step9/local/benchmark-report-<commit>.json (written by step9/local/benchmark_report.py), made on the
     exact commit the round's "engine" ref names (ENGINE_COMMIT, resolved by the workflow before decide.py). No report
     for that commit, or no commit at all: CLOSED. A report made on another commit does not count (the engine changed).
  1. the takeover test the report names shows no acknowledged write missing and finished (rule 8: zero loss is absolute);
  2. in the agent benchmark the report names, on every operation our server's ("fenvix") p50 and our p99 are each at
     most the speed tolerance above the best of the PostgreSQL and the mongod saver for that percentile: by default
     SPEED_TOLERANCE_MS (20 ms) at p50 and p99 for every operation (DECISIONS 2026-11-12, "Speed tolerance for the AWS
     round", which replaces rule 9's "not worse" for latency). The tolerance is configurable per operation and per
     percentile in step9/ci/speed_tolerance.json (`ms` above the best and/or `ratio` of the best; with both, the larger
     allowance); a missing file means the DECISIONS default, an invalid one closes the gate. Changing it is the
     founder's decision, written in DECISIONS.md like the default; stored
     bytes per logical byte stays not worse than either (rule 9);
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
# DECISIONS 2026-11-12 (founder): on every operation, our p50 and our p99 may each be at most this much above the best
# of PostgreSQL and mongod (the same percentile). Exactly 20 ms above passes; more fails.
SPEED_TOLERANCE_MS = 20.0
SPEED_DEFAULT_SOURCE = "DECISIONS 2026-11-12"
TOLERANCE_FILE = "step9/ci/speed_tolerance.json"


def speed_tolerance():
    """(tolerance, problems): tolerance[op or "default"][q] = {"ms": float|None, "ratio": float|None, "source": str},
    from TOLERANCE_FILE when present; without it the DECISIONS default (20 ms at p50 and p99, no ratio)."""
    dflt = {q: {"ms": SPEED_TOLERANCE_MS, "ratio": None, "source": SPEED_DEFAULT_SOURCE} for q in ("p50", "p99")}
    tol, problems = {"default": dflt}, []
    path = os.path.join(ROOT, TOLERANCE_FILE)
    if not os.path.exists(path):
        return tol, problems
    try:
        cfg = json.load(open(path))
    except Exception as e:
        return tol, [f"{TOLERANCE_FILE}: not readable JSON ({e})"]
    source = cfg.get("source") or "?"

    def entry(where, d):
        out = {}
        for q in ("p50", "p99"):
            e = (d or {}).get(q)
            if e is None:
                continue
            ms, ratio = e.get("ms"), e.get("ratio")
            if ms is None and ratio is None:
                problems.append(f"{TOLERANCE_FILE}: {where} {q} has neither ms nor ratio")
                continue
            if (ms is not None and (not isinstance(ms, (int, float)) or ms < 0)) or \
               (ratio is not None and (not isinstance(ratio, (int, float)) or ratio < 1)):
                problems.append(f"{TOLERANCE_FILE}: {where} {q}: ms must be >= 0 and ratio >= 1 (got ms {ms}, ratio {ratio})")
                continue
            out[q] = {"ms": None if ms is None else float(ms), "ratio": None if ratio is None else float(ratio),
                      "source": e.get("source") or source}
        return out

    tol["default"].update(entry("default", cfg.get("default")))
    for op, d in (cfg.get("ops") or {}).items():
        if op not in OPS:
            problems.append(f"{TOLERANCE_FILE}: unknown operation {op!r}")
            continue
        got = entry(op, d)
        if got:
            tol[op] = got
    return tol, problems


def tolerance_for(tol, op, q):
    return (tol.get(op) or {}).get(q) or tol["default"][q]


def describe(t):
    """'20 ms', 'x1.10' or '20 ms or x1.10 (the larger)'."""
    parts = ([f"{t['ms']:g} ms"] if t["ms"] is not None else []) + ([f"x{t['ratio']:g}"] if t["ratio"] is not None else [])
    return " or ".join(parts) + (" (the larger)" if len(parts) == 2 else "")


def allowed_above(t, best):
    """How far above the best ours may be under tolerance t (the larger of the two allowances when both are set)."""
    a = [t["ms"]] if t["ms"] is not None else []
    if t["ratio"] is not None:
        a.append((t["ratio"] - 1.0) * best)
    return max(a) if a else 0.0


def speed_blockers(label, op, ours, others, tol=None):
    """Blockers of one operation under the speed tolerance. `ours`/`others[side]` are latency dicts with p50 and p99."""
    tol = tol or speed_tolerance()[0]
    out = []
    for q in ("p50", "p99"):
        if ours.get(q) is None:
            continue
        theirs = {side: d.get(q) for side, d in others.items() if d and d.get(q) is not None}
        if not theirs:
            continue
        best_side = min(theirs, key=theirs.get)
        best = theirs[best_side]
        t = tolerance_for(tol, op, q)
        if ours[q] - best > allowed_above(t, best) + 1e-9:
            out.append(f"{label} {op} {q}: ours {ours[q]:.2f} ms is {ours[q] - best:.2f} ms above the best ({best_side} "
                       f"{best:.2f} ms), more than the tolerance of {describe(t)} ({t['source']})")
    return out


TOL, TOL_PROBLEMS = speed_tolerance()


def rel(p):
    return os.path.relpath(p, ROOT)


# total review R3 hole 3 (2026-11-12): one takeover instead of 50 opened the gate; the written minimum (B6: 50 takeovers)
TAKEOVERS_MIN = 50


def check_takeovers(path, log_on=False):
    """Blockers of one takeover summary (step9/programs/takeovers.py). With the log on (total review R3 hole 4), a write
    batch that took the S3-WAL fallback was acknowledged without 2 of 3 log copies, which is not equal durability
    (DECISIONS 2026-11-11): more than 0, or a count not recorded, closes the gate."""
    if not path or not os.path.exists(path):
        return [f"takeovers: no result ({path})"]
    d = json.load(open(path))
    lost = d.get("lost")
    out = []
    if d.get("verdict") != "PASS" or lost not in (0,) or d.get("done") != d.get("n"):
        out.append(f"takeovers ({rel(path)}): verdict {d.get('verdict')}, {d.get('done')}/{d.get('n')} done, "
                   f"lost {lost} of {d.get('acknowledged')} acknowledged writes (rule 8: must be 0)")
    if not isinstance(d.get("n"), int) or d.get("n") < TAKEOVERS_MIN:
        out.append(f"takeovers ({rel(path)}): {d.get('n')} takeovers, fewer than the written minimum {TAKEOVERS_MIN}")
    if log_on:
        fb = (d.get("log_fallback") or {}).get("batches")
        if fb is None:
            out.append(f"takeovers ({rel(path)}): the write batches that took the S3-WAL fallback are not recorded "
                       f"(log_fallback.batches): equal durability cannot be shown")
        elif fb > 0:
            out.append(f"takeovers ({rel(path)}): {fb} write batches took the S3-WAL fallback with the log on (not 2 of 3 "
                       f"log copies: not equal durability, DECISIONS 2026-11-11)")
    return out


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
        for op in OPS:
            if op in NOT_COMPARED:
                continue
            a = (ours.get("latency_ms") or {}).get(op)
            others = {o: (sums[o].get("latency_ms") or {}).get(op) for o in ("postgres", "mongo") if o in sums}
            if not a or not all(others.values()):
                continue   # already a blocker above
            out += speed_blockers(label, op, a, others, TOL)
        for other in ("postgres", "mongo"):
            them = sums.get(other)
            if not them:
                continue
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
        if "compaction2_published" not in st:
            out.append(f"{label} fenvix: whether compaction 2 published is not recorded (a summary older than the fix "
                       f"round): the bytes are not shown to be after compaction 2")
        elif not st["compaction2_published"]:
            out.append(f"{label} fenvix: compaction 2 never published (compactor.publishes 0): the bytes are not after compaction 2")
    for k in ("server_status_after_write", "server_status_before_cold", "server_status_end"):
        if k == "server_status_before_cold" and k not in d:
            continue   # summaries older than the fix round
        if k == "server_status_end" and not d.get(k):
            continue   # its own blocker below
        q = ((d.get(k) or {}).get("quaderno") or {})
        f = q.get("failed")
        if not isinstance(f, int):
            out.append(f"{label} fenvix: quaderno.failed UNKNOWN ({k} could not be read): equal durability not shown")
        elif f > 0:
            out.append(f"{label} fenvix: the 3-zone log refused {f} batches ({k}): writes acknowledged by the "
                       f"S3-WAL fallback, not at equal durability")
    if not d.get("server_status_end"):
        out.append(f"{label} fenvix: no server status at the end of the run (quaderno.failed unknown)")
    return out


def binary_of(path):
    """The sha256 of the mongo-server binary a result names: the summary's own "engine_binary_sha256", or a sidecar
    file <summary>.binary-sha256 written next to it by the run that made it."""
    try:
        v = json.load(open(path)).get("engine_binary_sha256")
    except Exception:  # noqa: BLE001
        v = None
    side = path + ".binary-sha256"
    if not v and os.path.exists(side):
        v = open(side).read().split()[0]
    return v


def check_report_results(r):
    """Every result a report names (total review N3: the takeovers with the log on AND off; and the risk under N5: every
    result made by the binary of the report's commit)."""
    out = []
    want = r.get("engine_sha256")
    if not want:
        out.append("the report names no engine binary (sha256 of mongo-server built from the commit): its results cannot "
                   "be tied to the commit")
    for key, label in (("takeovers", "takeovers log on"), ("takeovers_off", "takeovers log off")):
        if not r.get(key):
            out.append(f"{label}: no result named in the report (RULES 8: every takeover test counts)")
            continue
        p = os.path.join(ROOT, r[key])
        out += [f"{label}: {b}" for b in check_takeovers(p, log_on=(key == "takeovers"))]
        got = binary_of(p) if os.path.exists(p) else None
        if want and got != want:
            out.append(f"{label}: made by binary {str(got)[:16]}, not the report's {want[:16]}")
    for key, label in (("agent_bench", "agent benchmark"), ("agent_bench_w8", "agent benchmark --workers 8")):
        # total review R3 hole 1 (2026-11-12): DECISIONS 2026-11-12 names "repeat 1 and the --workers 8 line"; a report
        # without the 8-worker line opened the gate, so leaving a slow line out opened it. Now it is required.
        if not r.get(key):
            out.append(f"{label}: no result named in the report (DECISIONS 2026-11-12: repeat 1 and the --workers 8 line)")
            continue
        p = os.path.join(ROOT, r.get(key) or "-")
        out += check_agent(p, label=label)
        got = binary_of(p) if os.path.exists(p) else None
        if want and got != want:
            out.append(f"{label}: our summary was made by binary {str(got)[:16]}, not the report's {want[:16]}")
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


def tolerance_text(tol):
    d = tol["default"]
    txt = (f"{describe(d['p50'])} ({d['p50']['source']})" if d["p50"] == d["p99"]
           else f"p50 {describe(d['p50'])}, p99 {describe(d['p99'])} ({d['p50']['source']})")
    extra = [f"{op} {q} {describe(t)}" for op in OPS if op in tol for q, t in tol[op].items()]
    return txt + (f"; per operation: {', '.join(extra)}" if extra else "")


def main():
    ref = os.environ.get("ENGINE_REF", "")
    commit = os.environ.get("ENGINE_COMMIT", "")
    blockers = list(TOL_PROBLEMS)
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
        blockers += check_report_results(r)
    if blockers:
        print(f"GATE: CLOSED ({len(blockers)} blockers; RULES.md 8-9: no AWS run, not even a pilot)")
        for b in blockers:
            print(f"  - {b}")
        sys.exit(1)
    print(f"GATE: OPEN (local benchmark report {rel(found[0])} for {ref} at {commit}: takeovers 0 lost; our agent benchmark "
          f"ok, complete ({len(OPS)} operations, written minimum samples); on every operation our p50 and p99 within the "
          f"speed tolerance of {tolerance_text(TOL)} above the best of PostgreSQL and mongod, "
          f"stored bytes per logical byte not worse, also with --workers 8; takeovers at least {TAKEOVERS_MIN} per log mode, "
          f"0 lost, 0 write batches on the S3-WAL fallback with the log on)")


if __name__ == "__main__":
    main()
