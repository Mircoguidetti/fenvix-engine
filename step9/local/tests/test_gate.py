"""Step 9 · tests of the gate (step9/ci/gate.py) and of the report it reads (step9/local/benchmark_report.py).
aws-fixes, B1 of the total review (fenvix-brain sessions/2026-11-11-review-total/INTERMEDIATE-1.md): the gate opened for
a benchmark that failed. Every case builds a result tree in a temporary folder, the way the real files are laid out, and
asks gate.py as decide.py asks it (a subprocess, ENGINE_REF and ENGINE_COMMIT in the environment).
  - the reviewer's script, unchanged but for "|| true": must say GATE: CLOSED;
  - a report forged to say PASS over the same failed results: CLOSED (the gate re-reads the results, not the verdict);
  - each defect alone, on a result set that is otherwise clean: CLOSED, with the reason named;
  - the clean result set: OPEN (the control: without it, "always CLOSED" would pass every test).
$0, no network. Usage: python3 step9/local/tests/test_gate.py   (or python -m pytest -q step9/local/tests/test_gate.py)"""
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "step9", "ci"))
import gate  # noqa: E402

COMMIT = "b" * 40
BASE = "bench/agent-checkpoints/results/local-t"


def clean_summary(scale=1.0):
    """A summary as bench.py writes it, every operation present with the written minimum of samples (and more)."""
    lat = {op: {"n": gate.MIN_SAMPLES[op] * 2, "p50": 2.0 * scale, "p90": 4.0 * scale, "p99": 8.0 * scale}
           for op in gate.OPS}
    return {"ok": True, "plan": {"digest": "d1"}, "write": {"errors": []}, "read": {"inconsistencies": 0, "wrong_reads": 0},
            "erase": {"threads_left_readable": 0}, "latency_ms": lat, "bytes": {"stored_per_logical": 0.30 * scale}}


def tree(ours, pg, mongo, takeovers=None, w8=None, verdict=None):
    d = tempfile.mkdtemp()
    os.makedirs(os.path.join(d, "step9", "ci"))
    os.makedirs(os.path.join(d, "step9", "local", "results-t"))
    shutil.copy(os.path.join(REPO, "step9", "ci", "gate.py"), os.path.join(d, "step9", "ci"))
    for base, sums in ((BASE, (ours, pg, mongo)),) + (((BASE + "-w8", w8),) if w8 else ()):
        for side, s in zip(("fenvix", "postgres", "mongo"), sums):
            if s is None:
                continue
            os.makedirs(os.path.join(d, base, side))
            json.dump(s, open(os.path.join(d, base, side, "summary.json"), "w"))
    tk = "step9/local/results-t/takeovers-summary-on.json"
    json.dump(takeovers or {"verdict": "PASS", "lost": 0, "done": 50, "n": 50, "acknowledged": 500000},
              open(os.path.join(d, tk), "w"))
    rep = {"engine_ref": "final", "engine_commit": COMMIT, "takeovers": tk, "agent_bench": BASE + "/fenvix/summary.json",
           "verdict": verdict or "PASS", "blockers": []}
    if w8:
        rep["agent_bench_w8"] = BASE + "-w8/fenvix/summary.json"
    json.dump(rep, open(os.path.join(d, "step9", "local", f"benchmark-report-{COMMIT[:12]}.json"), "w"))
    return d


def ask(d):
    g = subprocess.run([sys.executable, os.path.join(d, "step9", "ci", "gate.py"), d], capture_output=True, text=True,
                       env={**os.environ, "ENGINE_REF": "final", "ENGINE_COMMIT": COMMIT})
    shutil.rmtree(d, ignore_errors=True)
    return g.returncode, g.stdout


def closed(d, why):
    rc, out = ask(d)
    assert rc == 1 and out.startswith("GATE: CLOSED"), out
    assert why in out, f"expected {why!r} in:\n{out}"
    return out


def test_reviewer_script_says_closed():
    t = tempfile.mkdtemp()
    r = subprocess.run(["bash", os.path.join(HERE, "gate_opens_on_failed_result.sh"), REPO], capture_output=True, text=True, cwd=t)
    shutil.rmtree(t, ignore_errors=True)
    out = r.stdout
    assert "GATE: CLOSED" in out and "GATE: OPEN" not in out, out + r.stderr
    for why in ("fenvix: ok is False", "operation resume[read] missing", "stored bytes per logical byte unknown",
                "save_step p50", "time_travel p50"):
        assert why in out, f"{why!r} not in:\n{out}"


def test_forged_pass_report_over_failed_results():
    bad = clean_summary()
    bad["ok"] = False
    bad["read"]["inconsistencies"] = 37
    closed(tree(bad, clean_summary(2), clean_summary(2), verdict="PASS"), "fenvix: ok is False")


def test_ok_false_on_a_competitor_closes():
    pg = clean_summary(2)
    pg["ok"] = False
    closed(tree(clean_summary(), pg, clean_summary(2)), "postgres: ok is False")


def test_operation_missing_on_their_side_closes():
    pg = clean_summary(2)
    del pg["latency_ms"]["time_travel"]
    closed(tree(clean_summary(), pg, clean_summary(2)), "postgres: operation time_travel missing")


def test_operation_missing_on_our_side_closes():
    ours = clean_summary()
    del ours["latency_ms"]["resume[cold]"]
    closed(tree(ours, clean_summary(2), clean_summary(2)), "fenvix: operation resume[cold] missing")


def test_too_few_samples_closes():
    ours = clean_summary()
    ours["latency_ms"]["resume[write/long]"]["n"] = 8   # the small profile
    closed(tree(ours, clean_summary(2), clean_summary(2)), "resume[write/long] has 8 samples")


def test_new_operations_are_compared():
    for op in ("save_step", "time_travel", "resume[cold]"):
        ours = clean_summary()
        ours["latency_ms"][op]["p99"] = 100.0
        closed(tree(ours, clean_summary(2), clean_summary(2)), f"{op} p99: ours 100.00 ms > postgres")


def test_stored_bytes_unknown_closes():
    ours = clean_summary()
    ours["bytes"]["stored_per_logical"] = None
    closed(tree(ours, clean_summary(2), clean_summary(2)), "fenvix: stored bytes per logical byte unknown")


def test_different_plans_close():
    mg = clean_summary(2)
    mg["plan"]["digest"] = "other"
    closed(tree(clean_summary(), clean_summary(2), mg), "did not run the same plan")


def test_missing_competitor_closes():
    closed(tree(clean_summary(), clean_summary(2), None), "no mongo result")


def test_workers8_line_checked_when_named():
    w8_ours = clean_summary()
    w8_ours["latency_ms"]["save_step"]["p50"] = 50.0
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2), w8=(w8_ours, clean_summary(2), clean_summary(2))),
           "agent benchmark --workers 8 save_step p50")


def test_takeover_loss_still_closes():
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2),
                takeovers={"verdict": "FAIL", "lost": 1, "done": 50, "n": 50, "acknowledged": 500000}), "lost 1")


def test_clean_results_open():
    rc, out = ask(tree(clean_summary(), clean_summary(2), clean_summary(2),
                       w8=(clean_summary(), clean_summary(2), clean_summary(2))))
    assert rc == 0 and out.startswith("GATE: OPEN"), out


def test_benchmark_report_says_fail_on_failed_results():
    """benchmark_report.py writes the verdict from the same checks: FAIL, exit 1."""
    bad = clean_summary()
    bad["erase"]["threads_left_readable"] = 5
    bad["ok"] = False
    d = tree(bad, clean_summary(2), clean_summary(2))
    os.makedirs(os.path.join(d, "step9", "local"), exist_ok=True)
    shutil.copy(os.path.join(REPO, "step9", "local", "benchmark_report.py"), os.path.join(d, "step9", "local"))
    r = subprocess.run([sys.executable, os.path.join(d, "step9", "local", "benchmark_report.py"), "final", COMMIT,
                        "step9/local/results-t/takeovers-summary-on.json", BASE + "/fenvix/summary.json"],
                       capture_output=True, text=True)
    shutil.rmtree(d, ignore_errors=True)
    assert r.returncode == 1 and ": FAIL" in r.stdout and "erased threads still readable 5" in r.stdout, r.stdout + r.stderr


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as e:
            fails += 1
            print(f"FAIL {name}\n{e}")
    print(f"{'ALL PASS' if not fails else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)
