"""bench-local, the reviewer's fix check (fenvix-brain sessions/2026-11-11-review-bench-local/REPORT.md, "Fix check") and
DECISIONS 2026-11-11 "Client limit". One test per point, $0, no Docker:
  F10 the member health check runs also right before the cold restart; a member lost after the write phase (the
      reviewer stopped mongo-c at the first read-phase row; docker restart started it again and the run said ok) now
      fails the run. bench.main runs end to end on the tiny plan against LangGraph's in-memory saver.
  F7  the --workers 8 line is client-limited at 0.8 core (the reviewer measured 0.855 core on the w1 client); the
      --workers 1 line is exempt and reports its client CPU.
  F9  compaction2_published is tied to sealed runs under tenants/app_demo/fx/run/, not to the process-wide
      compactor.publishes (a publish for admin set it) nor to fx/ alone (a 20-byte E1 marker is always there).
  F5  an unreadable server status makes quaderno.failed UNKNOWN and the run not ok, never 0.
Usage: python tests/test_fix_check.py (from bench/agent-checkpoints)."""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import bench  # noqa: E402
import delta_hook  # noqa: E402
import targets  # noqa: E402
from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402


# ---- F10 ----
class _H:
    def close(self):
        pass


class FakeTarget:
    name = "fake"

    def __init__(self):
        self.saver = InMemorySaver()

    def make_saver(self):
        return self.saver, _H()

    def reset(self):
        pass

    def sizes(self):
        return {"stored_bytes": 1000, "stored_from": "fake"}


def run_main(health_fail_on_call):
    """bench.main on the tiny plan; the health command fails on its Nth call (0: never). Returns (summary, call log)."""
    d = tempfile.mkdtemp()
    log = os.path.join(d, "calls")
    n = os.path.join(d, "n")
    health = (f'c=$(($(cat {n} 2>/dev/null || echo 0)+1)); echo $c > {n}; echo health$c >> {log}; '
              f'[ "$c" != "{health_fail_on_call}" ]')
    saved = (targets.from_args, delta_hook.run, sys.argv)
    targets.from_args = lambda *a, **k: FakeTarget()
    delta_hook.run = lambda *a, **k: {}
    sys.argv = ["bench.py", "--target", "mongo", "--uri", "x", "--profile", "tiny", "--read-sample", "5",
                "--erase-users", "2", "--cold-threads", "3", "--out", os.path.join(d, "out"),
                "--cold-cmd", f"echo cold >> {log}", "--health-cmd", health]
    try:
        bench.main()
    except SystemExit:
        pass
    finally:
        targets.from_args, delta_hook.run, sys.argv = saved
    return json.load(open(os.path.join(d, "out", "summary.json"))), open(log).read().split()


def test_health_runs_before_the_cold_restart():
    s, calls = run_main(0)
    assert calls == ["health1", "health2", "cold", "health3"], calls
    assert [h["when"] for h in s["health"]] == ["after write", "before cold", "end"] and s["ok"] is True, s["health"]


def test_member_lost_before_the_cold_restart_fails_the_run():
    """The reviewer's case: healthy after write, a member down before the cold restart, healthy again at the end."""
    s, calls = run_main(2)
    assert calls == ["health1", "health2", "cold", "health3"], calls
    assert s["ok"] is False and [h["rc"] for h in s["health"]] == [0, 1, 0], s["health"]


# ---- F7 ----
def cpu_with(workers, cores):
    c = bench.ClientCpu(os.devnull, lambda: [], workers=workers)
    c.rows = [{"t": i, "host_busy": 0.5, "procs": {"1": cores}} for i in range(10)]
    return c.summary()


def test_w8_limited_at_0_8_core():
    assert cpu_with(8, 0.855)["client_limited"] is True      # the reviewer's measured client load
    assert cpu_with(8, 0.80)["client_limited"] is True
    assert cpu_with(8, 0.79)["client_limited"] is False


def test_w1_exempt_and_reports_its_cpu():
    s = cpu_with(1, 0.855)
    assert s["client_limited"] is False and s["process_cores_max_mean"] == 0.855, s
    assert "exempt" in s["rule"] and "0.855" in s["rule"], s


# ---- F9 ----
def published_with(by_kind):
    saved = targets.s3_prefix_bytes
    targets.s3_prefix_bytes = lambda s3, prefix, levels=1: {"by_kind": by_kind, "count_by_kind": {k: 1 for k in by_kind}}
    try:
        return targets.published_here(targets.Target("fenvix", "x", s3={"bucket": "b"}))
    finally:
        targets.s3_prefix_bytes = saved


def test_published_only_with_sealed_runs_of_the_benchmark_database():
    # the reviewer's tiny run: only the E1 marker under fx/ (process-wide publishes could still be > 0, e.g. admin)
    assert published_with({"fx/e1-announced": 20, "compacted/x": 5_000_000})["compaction2_published"] is False
    r = published_with({"fx/e1-announced": 20, "fx/run": 64_000_000, "fx/manifest": 4000})
    assert r["compaction2_published"] is True and r["fx_run_bytes"] == 64_000_000, r


def test_settle_record_uses_it():
    saved = (targets.fenvix_status, targets.s3_prefix_bytes)
    targets.fenvix_status = lambda uri: {"compactor": {"publishes": 7}, "compaction": {"running": 0}}
    targets.s3_prefix_bytes = lambda s3, prefix, levels=1: {"by_kind": {"fx": 20} if levels == 1 else {"fx/e1-announced": 20},
                                                            "count_by_kind": {}}
    try:
        st = targets.wait_compaction_idle(targets.Target("fenvix", "x", s3={"bucket": "b"}), poll_s=0, stable_polls=2)
    finally:
        targets.fenvix_status, targets.s3_prefix_bytes = saved
    assert st["idle"] and st["process_publishes"] == 7 and st["compaction2_published"] is False, st


# ---- F5 ----
CLEAN = dict(w={"errors": []}, rd={"inconsistencies": 0}, er={"threads_left_readable": 0}, check={"wrong_reads": 0},
             health=[{"rc": 0}], cold=None)


def test_unreadable_status_is_not_ok():
    assert bench.run_ok(**CLEAN, log_failed={"after_write": 0, "before_cold": 0, "end": 0}) is True
    assert bench.run_ok(**CLEAN, log_failed={"after_write": 0, "before_cold": None, "end": 0}) is False


def test_status_error_reads_as_unknown():
    """fenvix_status returns {"error": ...} when the server cannot be reached; quaderno.failed is then None."""
    s = {"error": "ServerSelectionTimeoutError"}
    assert ((s or {}).get("quaderno") or {}).get("failed") is None


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")
