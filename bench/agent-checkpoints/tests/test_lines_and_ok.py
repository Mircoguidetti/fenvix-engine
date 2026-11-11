"""bench-local fix round: F5, F8 and F10 of the review (fenvix-brain sessions/2026-11-11-review-bench-local), without
Docker: the docker and database calls of lines.py are replaced by recorders.
  F5  the reviewer stopped two log copies and our server acknowledged a checkpoint through the S3-WAL fallback after
      10.5 s, with tenantStats.quaderno.failed growing; the runner only checked that quaderno.sent grew. Now a run with
      quaderno.failed > 0 (after write, before the cold restart or at the end) is not ok.
  F8  MinIO had no memory cap (its page cache held all of our data) and Postgres kept the stock 128 MB shared_buffers;
      now MinIO runs with --memory 3g (CPU uncapped) and every Postgres member with shared_buffers 768MB (25% of 3 GiB).
  F10 nothing checked the members during the run; now `lines.py health` fails when a member is not running or was
      OOM-killed, or when the line's durability check fails.
$0. Usage: python tests/test_lines_and_ok.py (from bench/agent-checkpoints)."""
import json
import os
import sys
import types
from contextlib import contextmanager

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
os.environ.setdefault("BENCH_PW", "test-only")
import bench  # noqa: E402
import lines  # noqa: E402


class R:
    def __init__(self, out="", rc=0):
        self.stdout, self.stderr, self.returncode = out, "", rc


def record(monkey):
    calls = []

    def sh(cmd, check=True, capture=True, env=None, timeout=None):
        calls.append(list(cmd))
        return monkey(cmd) if monkey else R()
    lines.sh = sh
    lines.wait_until = lambda what, fn, timeout=300, every=1.0: True
    return calls


def docker_run(calls, name):
    return next(c for c in calls if c[:2] == ["docker", "run"] and name in c)


# ---- F8 ----
def test_minio_memory_capped_cpu_not():
    calls = record(None)
    fake = types.SimpleNamespace(client=lambda *a, **k: types.SimpleNamespace(list_buckets=lambda: {}, create_bucket=lambda **k: None))
    sys.modules["boto3"] = fake
    lines.up_minio()
    c = docker_run(calls, "minio")
    i = c.index("--memory")
    assert c[i + 1] == "3g" and "--cpus" not in c, c


def test_postgres_shared_buffers_25_percent_on_every_member():
    calls = record(None)

    class Cur:
        def fetchone(self):
            return ["x"]

    @contextmanager
    def conn(host, db="postgres"):
        yield types.SimpleNamespace(execute=lambda *a: Cur())
    lines.pg_conn = conn
    lines.pg_replication = lambda: [{"application_name": f"standby{i}", "state": "streaming", "sync_state": "quorum"} for i in (1, 2)]
    lines.up_postgres_sync()
    lines.up_pg("pg-single", [])
    for n in ("pg-primary", "pg-single"):
        c = docker_run(calls, n)
        assert "shared_buffers=768MB" in c, (n, c)
    for n in ("pg-standby1", "pg-standby2"):
        c = docker_run(calls, n)
        assert "shared_buffers=768MB" in c[-1], (n, c[-1])


# ---- F10 ----
def inspect(oom_for=None, stopped=None):
    def m(cmd):
        if cmd[:2] == ["docker", "inspect"]:
            n = cmd[-1]
            st = {"Running": n != stopped, "OOMKilled": n == oom_for, "ExitCode": 137 if n == oom_for else 0,
                  "Status": "running" if n != stopped else "exited"}
            return R(json.dumps(st) + "|0")
        return R()
    return m


def test_health_fails_on_an_oom_killed_member():
    record(inspect(oom_for="mongo-b"))
    lines.check_line = lambda line: {"ok": True}
    h = lines.health("mongo-rs3")
    assert h["ok"] is False and h["not_healthy"] == ["mongo-b"], h


def test_health_fails_on_a_stopped_member():
    record(inspect(stopped="log-1"))
    lines.check_line = lambda line: {"ok": True}
    h = lines.health("fenvix-log")
    assert h["ok"] is False and "log-1" in h["not_healthy"], h


def test_health_fails_when_the_durability_check_fails():
    record(inspect())
    lines.check_line = lambda line: {"ok": False}
    assert lines.health("postgres-sync")["ok"] is False


def test_health_ok_control():
    record(inspect())
    lines.check_line = lambda line: {"ok": True}
    h = lines.health("fenvix-log")
    assert h["ok"] is True and len(h["members"]) == 6, h


# ---- F5 (and the F6 / F10 parts of ok) ----
CLEAN = dict(w={"errors": []}, rd={"inconsistencies": 0}, er={"threads_left_readable": 0, "checkpoints_left_readable_by_id": 0},
             check={"wrong_reads": 0}, log_failed={"after_write": 0, "before_cold": 0, "end": 0},
             health=[{"rc": 0}, {"rc": 0}], cold={"restart": {"rc": 0}, "threads": 200, "n_errors": 0})


def test_ok_control():
    assert bench.run_ok(**CLEAN) is True


def test_log_fallback_makes_the_run_not_ok():
    for k in ("after_write", "before_cold", "end"):
        bad = dict(CLEAN, log_failed=dict(CLEAN["log_failed"], **{k: 1}))
        assert bench.run_ok(**bad) is False, k


def test_erased_checkpoint_readable_by_id_is_not_ok():
    assert bench.run_ok(**dict(CLEAN, er=dict(CLEAN["er"], checkpoints_left_readable_by_id=3))) is False


def test_unhealthy_member_is_not_ok():
    assert bench.run_ok(**dict(CLEAN, health=[{"rc": 0}, {"rc": 1}])) is False


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")
