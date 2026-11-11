"""bench-local fix round, F3 of the review: bench_local.sh skipped any line whose summary.json existed, also a STOPped
(exit 3, wrong reads) or failed (ok false) one, and carried it to the verdict. The reviewer put three summaries with
ok false and wrong_reads 37 in an equal-r1 folder and ran STEPS=equal1: all three were skipped, exit 0.
Here the line runner is a stub (no Docker): it records each call and exits with STUB_RC. Checks:
  1. the reviewer's case: the three failed lines are run again (the old folders kept aside), not skipped;
  2. a line that exits 3 writes the STOP file and the script exits 3;
  3. with the STOP file present, equal1 and verdict refuse (exit 3) and run nothing;
  4. a line whose runner said exit 0 and whose summary says ok true is skipped (the control).
$0. Usage: python tests/test_rerun_never_skips_failed.py (from bench/agent-checkpoints)."""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.abspath(os.path.join(BENCH, "..", ".."))
STUB = """#!/bin/bash
# stands in for the bench python: a call to lines.py is recorded, everything else is the real python
if [ "${1##*/}" = "lines.py" ]; then echo "$3" >> "$STUB_LOG"; exit ${STUB_RC:-0}; fi
exec "%s" "$@"
""" % sys.executable


def setup():
    d = tempfile.mkdtemp()
    shutil.copytree(BENCH, os.path.join(d, "bench", "agent-checkpoints"), ignore=shutil.ignore_patterns("results", "tests"))
    os.makedirs(os.path.join(d, "bench", "agent-checkpoints", "results"))
    # the copy's line runner refuses to run: this test must never reach Docker (a first version of the stub matched
    # the wrong argument, ran the real lines.py, and its down() removed a running line's containers)
    open(os.path.join(d, "bench", "agent-checkpoints", "lines.py"), "w").write(
        'raise SystemExit("test_rerun_never_skips_failed: the real lines.py must not run here")\n')
    stub = os.path.join(d, "py")
    open(stub, "w").write(STUB)
    os.chmod(stub, 0o755)
    return d, stub


def run(d, stub, steps, rc=0):
    log = os.path.join(d, "calls")
    env = dict(os.environ, BENCH_PY=stub, STUB_LOG=log, STUB_RC=str(rc), ENGINE_DIR=d, ENGINE_COMMIT="c" * 40,
               ENGINE_REF="test", BENCH_PW="x", TAG="t", STEPS=steps, WORK=os.path.join(d, "work"))
    r = subprocess.run(["bash", os.path.join(d, "bench", "agent-checkpoints", "bench_local.sh")], env=env,
                       capture_output=True, text=True)
    calls = open(log).read().split() if os.path.exists(log) else []
    return r.returncode, calls, r.stdout + r.stderr


def put(d, side, ok, bench_rc, wrong=0):
    f = os.path.join(d, "bench", "agent-checkpoints", "results", "local-t-equal-r1", side)
    os.makedirs(f, exist_ok=True)
    json.dump({"ok": ok, "wrong_reads": wrong}, open(os.path.join(f, "summary.json"), "w"))
    if bench_rc is not None:
        json.dump({"bench_rc": bench_rc}, open(os.path.join(f, "line_result.json"), "w"))


def test_reviewer_case_failed_lines_run_again():
    d, stub = setup()
    for side in ("postgres", "mongo", "fenvix"):
        put(d, side, False, 3, wrong=37)
    rc, calls, out = run(d, stub, "equal1")
    assert calls == ["postgres-sync", "mongo-rs3", "fenvix-log"], out
    assert rc == 0, out
    kept = [x for x in os.listdir(os.path.join(d, "bench", "agent-checkpoints", "results", "local-t-equal-r1")) if ".failed-" in x]
    assert len(kept) == 3, kept
    shutil.rmtree(d)


def test_summary_without_line_result_runs_again():
    d, stub = setup()
    put(d, "postgres", True, None)
    rc, calls, out = run(d, stub, "equal1")
    assert calls[0] == "postgres-sync", out
    shutil.rmtree(d)


def test_exit_3_writes_stop_and_blocks_everything():
    d, stub = setup()
    rc, calls, out = run(d, stub, "equal1", rc=3)
    assert rc == 3 and calls == ["postgres-sync"], out
    assert os.path.exists(os.path.join(d, "bench", "agent-checkpoints", "results", "local-t-STOP")), out
    for steps in ("equal1", "single", "verdict"):
        rc2, calls2, out2 = run(d, stub, steps)
        assert rc2 == 3 and calls2 == ["postgres-sync"], (steps, out2)   # nothing new ran
    shutil.rmtree(d)


def test_done_and_ok_line_is_skipped():
    d, stub = setup()
    put(d, "postgres", True, 0)
    rc, calls, out = run(d, stub, "equal1")
    assert calls == ["mongo-rs3", "fenvix-log"], out
    shutil.rmtree(d)


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")
