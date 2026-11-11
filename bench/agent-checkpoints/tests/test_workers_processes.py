"""bench-local fix round, F7 of the review: with --workers 8 the eight workers were threads of one Python process, so
the GIL serialized them and the client was the limit (the reviewer measured 0.95 core, 91% of seconds at 0.8 or more,
no gain in write time). Now each worker is its own process. This test runs bench.write_phase with 8 workers on the
small plan (20 users) against a fake target (LangGraph's in-memory saver, one per process) and checks: 8 distinct worker processes,
none of them the parent; every checkpoint and row merged back (the same counts as one worker); the client's CPU
recorded. $0, no database. Usage: python tests/test_workers_processes.py (from bench/agent-checkpoints)."""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import bench  # noqa: E402
import generator  # noqa: E402
import graph_runner  # noqa: E402
from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402


class H:
    def close(self):
        pass


class PidSaver(InMemorySaver):
    def put(self, config, checkpoint, metadata, new_versions):
        with open(os.environ["PID_LOG"], "a") as f:
            f.write(f"{os.getpid()}\n")
        return super().put(config, checkpoint, metadata, new_versions)


class FakeTarget:
    name = "fake"

    def make_saver(self):
        return PidSaver(), H()


def run(workers):
    threads_list, events = generator.plan(42, "small")
    threads = {t.thread_id: t for t in threads_list}
    out = tempfile.mkdtemp()
    os.environ["PID_LOG"] = os.path.join(out, "pids")
    rec = bench.Recorder(os.path.join(out, "ops.jsonl"))
    exp = graph_runner.Expected()
    w = bench.write_phase(FakeTarget(), 42, threads, events, rec, workers, exp, out)
    rec.close()
    pids = {int(x) for x in open(os.environ["PID_LOG"]).read().split()}
    return w, rec.rows, exp, pids


def test_eight_workers_are_eight_processes():
    w1, rows1, exp1, pids1 = run(1)
    w8, rows8, exp8, pids8 = run(8)
    assert not w1["errors"] and not w8["errors"], (w1["errors"], w8["errors"])
    assert pids1 == {os.getpid()}, pids1
    assert len(pids8) == 8 and os.getpid() not in pids8, pids8
    assert w8["processes"] == 8
    assert len(exp8.ck) == len(exp1.ck) > 0, (len(exp8.ck), len(exp1.ck))
    assert sorted(r["op"] for r in rows8) == sorted(r["op"] for r in rows1)
    assert exp8.report()["wrong_reads"] == 0
    cc = w8["client_cpu"]
    assert "client_limited" in cc and "process_cores_mean" in cc, cc


if __name__ == "__main__":
    test_eight_workers_are_eight_processes()
    print("ALL PASS")
