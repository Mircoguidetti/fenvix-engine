"""Total review R1 point 3 (2026-11-12): in the agent benchmark, right after each cold resume, the next reads of that
same thread are made and reported apart ("<op>:next"), so "the next requests must be as fast as the others" is measured.
A stub saver stands in for the targets ($0, no server)."""
import os
import sys
import tempfile
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import bench  # noqa: E402


class StubSaver:
    serde = None

    def __init__(self):
        self.calls = []

    def get_tuple(self, config):
        self.calls.append(("get_tuple", config["configurable"].get("checkpoint_id")))
        return SimpleNamespace(config=config)

    def list(self, config, *, filter=None, before=None, limit=None):  # noqa: A002
        self.calls.append(("list", limit))
        return iter([1, 2, 3][: limit or 3])


class StubExpected:
    def check_tuple(self, *a, **k):
        pass

    def check_list(self, *a, **k):
        pass

    def written_ids(self, tid):
        return [("", f"{tid}-c1"), ("", f"{tid}-c2"), ("sub", "x")]


class StubTarget:
    def __init__(self):
        self.saver = StubSaver()

    def make_saver(self):
        return self.saver, SimpleNamespace(close=lambda: None)


def _run(next_ops):
    import graph_runner
    orig = graph_runner.BaseCheckpointSaver.__init__
    graph_runner.BaseCheckpointSaver.__init__ = lambda self, serde=None: None
    try:
        rec = bench.Recorder(os.path.join(tempfile.mkdtemp(), "ops.jsonl"))
        threads = {f"t{i}": SimpleNamespace(kind="chat", turns=3) for i in range(5)}
        t = StubTarget()
        out = bench.cold_phase(t, threads, rec, 3, 7, "true", set(), StubExpected(), next_ops=next_ops)
        rec.close()
        return out, rec.rows, t.saver.calls
    finally:
        graph_runner.BaseCheckpointSaver.__init__ = orig


def test_each_cold_resume_is_followed_by_the_next_reads_of_that_thread():
    out, rows, calls = _run(True)
    assert out["threads"] == 3 and out["n_errors"] == 0 and out["next_ops"] is True, out
    s = bench.summarize(rows)
    for k in ("resume[cold]", "resume[cold]:next", "list_limit10:next", "list:next", "time_travel:next"):
        assert s[k]["n"] == 3, (k, s.get(k))
    # per thread: the cold resume first, then its four next reads, in that order, on the same thread
    by_thread = {}
    for r in rows:
        by_thread.setdefault(r["thread_id"], []).append((r["op"], r.get("which")))
    for seq in by_thread.values():
        assert seq == [("resume", None), ("resume", "next"), ("list_limit10", "next"), ("list", "next"), ("time_travel", "next")], seq
    # the time travel picks a checkpoint of the root namespace written for that thread
    assert all(cid and cid.split("-c")[1] in ("1", "2") for op, cid in calls if op == "get_tuple" and cid)


def test_no_cold_next_keeps_the_old_phase():
    out, rows, _ = _run(False)
    s = bench.summarize(rows)
    assert s["resume[cold]"]["n"] == 3 and not any(k.endswith(":next") for k in s), s.keys()
