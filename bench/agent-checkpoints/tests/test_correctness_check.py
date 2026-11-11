"""bench-local fix round, F6 of the review (fenvix-brain sessions/2026-11-11-review-bench-local): the reviewer's own
script (review_miss_wrong_answers.py, unchanged) runs the harness's write, read, erase and cold phases on the tiny plan
against LangGraph's in-memory saver with one planted fault at a time. Every fault must be CAUGHT (ok False) and the
control must stay clean. Before the fix, list_wrong_order, list_duplicates, limit10_oldest_one and erase_keeps_old_ids
were MISSED. $0, no database. Usage: python tests/test_correctness_check.py (from bench/agent-checkpoints)."""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)


def test_every_planted_fault_is_caught():
    r = subprocess.run([sys.executable, os.path.join(HERE, "review_miss_wrong_answers.py"), BENCH], capture_output=True,
                       text=True, cwd=BENCH)
    out = r.stdout
    assert r.returncode == 0, out + r.stderr
    lines = {l.split()[0]: l for l in out.splitlines() if l and not l.startswith(" ")}
    assert "-> clean" in lines["none"], out
    for fault in ("stale_resume", "missing_pending_write", "list_wrong_order", "list_duplicates", "limit10_oldest_one",
                  "erase_keeps_old_ids"):
        assert "-> CAUGHT" in lines[fault], f"{fault} not caught:\n{out}"


if __name__ == "__main__":
    test_every_planted_fault_is_caught()
    print("ALL PASS")
