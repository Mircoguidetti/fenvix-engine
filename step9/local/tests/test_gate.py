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
SHA = "a" * 64   # the binary built from COMMIT
BASE = "bench/agent-checkpoints/results/local-t"


def clean_summary(scale=1.0):
    """A summary as bench.py writes it, every operation present with the written minimum of samples (and more)."""
    lat = {op: {"n": gate.MIN_SAMPLES[op] * 2, "p50": 2.0 * scale, "p90": 4.0 * scale, "p99": 8.0 * scale}
           for op in gate.OPS}
    return {"ok": True, "plan": {"digest": "d1"}, "write": {"errors": []}, "read": {"inconsistencies": 0, "wrong_reads": 0},
            "erase": {"threads_left_readable": 0}, "latency_ms": lat, "bytes": {"stored_per_logical": 0.30 * scale},
            # our side's settle and log counters (bench-local fix round, F5 and F9); ignored on the other sides
            "settle": {"idle": True, "capped": False, "seconds": 750.0, "compaction2_published": True},
            "server_status_after_write": {"quaderno": {"sent": 1000, "failed": 0}},
            "server_status_end": {"quaderno": {"sent": 1200, "failed": 0}},
            "engine_binary_sha256": SHA}


def tree(ours, pg, mongo, takeovers=None, w8=None, verdict=None, takeovers_off=None, report_extra=None):
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
    tk_off = "step9/local/results-t/takeovers-summary-off.json"
    clean_tk = {"verdict": "PASS", "lost": 0, "done": 50, "n": 50, "acknowledged": 500000, "engine_binary_sha256": SHA}
    json.dump(takeovers or clean_tk, open(os.path.join(d, tk), "w"))
    json.dump(takeovers_off or clean_tk, open(os.path.join(d, tk_off), "w"))
    rep = {"engine_ref": "final", "engine_commit": COMMIT, "engine_sha256": SHA, "takeovers": tk, "takeovers_off": tk_off,
           "agent_bench": BASE + "/fenvix/summary.json", "verdict": verdict or "PASS", "blockers": []}
    rep.update(report_extra or {})
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
    # bench-local F1: resume[write/none] and resume[write/short] are compared too
    for op in ("save_step", "time_travel", "resume[write/none]", "resume[write/short]"):
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
                        "step9/local/results-t/takeovers-summary-on.json", BASE + "/fenvix/summary.json",
                        "--takeovers-off", "step9/local/results-t/takeovers-summary-off.json", "--engine-sha256", SHA],
                       capture_output=True, text=True)
    shutil.rmtree(d, ignore_errors=True)
    assert r.returncode == 1 and ": FAIL" in r.stdout and "erased threads still readable 5" in r.stdout, r.stdout + r.stderr



# ---- bench-local fix round (review of 2026-11-11, fenvix-brain sessions/2026-11-11-review-bench-local) ----

def _gate_skips_cases():
    """The reviewer's script (evidence/gate_skips.sh), unchanged: cases A, B, C on this checkout."""
    t = tempfile.mkdtemp()
    r = subprocess.run(["bash", os.path.join(HERE, "review_bench_local_gate_skips.sh"), REPO], capture_output=True, text=True, cwd=t)
    shutil.rmtree(t, ignore_errors=True)
    parts = r.stdout.split("== case ")[1:]
    return {p[0]: p for p in parts}


def test_review_case_a_failed_run_closes():
    c = _gate_skips_cases()["A"]
    assert "GATE: CLOSED" in c and "GATE: OPEN" not in c, c


def test_review_case_b_none_and_short_close():
    """F1: ours 10x slower only on resume[write/none] and resume[write/short] (87.6% of the plan's resumes)."""
    c = _gate_skips_cases()["B"]
    assert "GATE: CLOSED" in c and "resume[write/none] p50" in c and "resume[write/short] p99" in c, c


def test_review_case_c_cold_only_slower_is_not_a_blocker():
    """F4: cold is reported, not compared (DECISIONS 2026-11-11). The reviewer's fakes carry no settle record, which is
    now a blocker of its own (F9); every other blocker must be absent, and no cold comparison."""
    c = _gate_skips_cases()["C"]
    assert "resume[cold] p50" not in c and "resume[cold] p99" not in c, c
    lines = [l for l in c.splitlines() if l.strip().startswith("- ") and "settle record" not in l and "server status at the end" not in l and "verdict FAIL" not in l
             and "takeovers log off" not in l and "names no engine binary" not in l and "made by binary" not in l]
    assert not lines, c


def test_cold_slower_alone_opens():
    ours = clean_summary()
    ours["latency_ms"]["resume[cold]"].update(p50=200.0, p99=900.0)
    rc, out = ask(tree(ours, clean_summary(2), clean_summary(2)))
    assert rc == 0 and out.startswith("GATE: OPEN"), out


def test_cold_missing_still_closes():
    ours = clean_summary()
    del ours["latency_ms"]["resume[cold]"]
    closed(tree(ours, clean_summary(2), clean_summary(2)), "fenvix: operation resume[cold] missing")


def test_none_and_short_need_their_minimum():
    ours = clean_summary()
    ours["latency_ms"]["resume[write/none]"]["n"] = 79   # the tiny profile
    closed(tree(ours, clean_summary(2), clean_summary(2)), "resume[write/none] has 79 samples")


def test_capped_settle_closes():
    """F9: a settle that hit its cap leaves the bytes not settled."""
    ours = clean_summary()
    ours["settle"].update(idle=False, capped=True, seconds=2700.0)
    closed(tree(ours, clean_summary(2), clean_summary(2)), "bytes not settled")


def test_no_compaction2_publish_closes():
    """F9, the reviewer's tiny run: idle, but compaction 2 never published (fx/ held a 20-byte marker)."""
    ours = clean_summary()
    ours["settle"]["compaction2_published"] = False
    closed(tree(ours, clean_summary(2), clean_summary(2)), "compaction 2 never published")


def test_no_settle_record_closes():
    ours = clean_summary()
    del ours["settle"]
    closed(tree(ours, clean_summary(2), clean_summary(2)), "no settle record")


def test_log_fallback_closes():
    """F5: with two log copies stopped, the engine acknowledged through the S3-WAL fallback; quaderno.failed grew."""
    ours = clean_summary()
    ours["server_status_end"]["quaderno"]["failed"] = 3
    closed(tree(ours, clean_summary(2), clean_summary(2)), "the 3-zone log refused 3 batches")


def test_client_limited_closes():
    """F7: a line whose client was the limit is not valid."""
    w8 = clean_summary(2)
    w8["client_limited"] = True
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2), w8=(clean_summary(), w8, clean_summary(2))),
           "the client was the limit")


# ---- total review intermediate 2: N3 and the binary every result names ----

def test_review_n3_log_off_loss_closes():
    """N3: 37 acknowledged writes lost in the log-off takeovers, everything else clean: the old gate said OPEN."""
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2),
                takeovers_off={"verdict": "FAIL", "lost": 37, "done": 50, "n": 50, "acknowledged": 250000,
                               "engine_binary_sha256": SHA}), "takeovers log off")


def test_report_without_log_off_closes():
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2), report_extra={"takeovers_off": None}),
           "takeovers log off: no result named")


def test_report_without_binary_closes():
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2), report_extra={"engine_sha256": None}),
           "names no engine binary")


def test_summary_from_another_binary_closes():
    ours = clean_summary()
    ours["engine_binary_sha256"] = "c" * 64
    closed(tree(ours, clean_summary(2), clean_summary(2)), "made by binary cccccccccccccccc")


def test_takeovers_from_another_binary_closes():
    closed(tree(clean_summary(), clean_summary(2), clean_summary(2),
                takeovers={"verdict": "PASS", "lost": 0, "done": 50, "n": 50, "acknowledged": 500000,
                           "engine_binary_sha256": "d" * 64}), "takeovers log on: made by binary dddd")


def test_takeovers_binary_from_sidecar():
    d = tree(clean_summary(), clean_summary(2), clean_summary(2))
    for m in ("on", "off"):
        f = os.path.join(d, f"step9/local/results-t/takeovers-summary-{m}.json")
        s = json.load(open(f))
        del s["engine_binary_sha256"]
        json.dump(s, open(f, "w"))
        open(f + ".binary-sha256", "w").write(SHA + "  mongo-server\n")
    rc, out = ask(d)
    assert rc == 0 and out.startswith("GATE: OPEN"), out


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

