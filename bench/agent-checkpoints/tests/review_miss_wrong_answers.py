"""Review of bench-local (2026-11-11): can the correctness check of bench.py (graph_runner.Expected, read_phase,
erase_phase, cold_phase) miss a wrong answer? Runs the harness's OWN functions (bench.write_phase, read_phase,
erase_phase, cold_phase) on the tiny plan against LangGraph's InMemorySaver wrapped by a saver that plants one fault.
Usage: python miss_wrong_answers.py BENCH_DIR   (bench/agent-checkpoints of the branch under review)
Prints, per fault: wrong_reads, read inconsistencies, erased threads left readable, and the run's `ok` as bench.py
computes it. A fault is CAUGHT when ok is False."""
import os, sys, random, tempfile
sys.path.insert(0, sys.argv[1])
import bench, generator, graph_runner
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import InMemorySaver


class Faulty(BaseCheckpointSaver):
    def __init__(self, inner, fault):
        super().__init__(serde=inner.serde)
        self.inner, self.fault, self.hidden, self.dropped = inner, fault, set(), 0

    def put(self, *a, **k): return self.inner.put(*a, **k)
    def get_next_version(self, c, ch): return self.inner.get_next_version(c, ch)

    def put_writes(self, config, writes, task_id, task_path=""):
        if self.fault == "missing_pending_write" and len(writes) > 0 and random.Random(task_id).random() < 0.2:
            writes = list(writes)[:-1]   # one pending write silently not stored (1 call in 5)
            self.dropped += 1
        return self.inner.put_writes(config, writes, task_id, task_path)

    def get_tuple(self, config):
        c = config["configurable"]
        if c["thread_id"] in self.hidden and not c.get("checkpoint_id"):
            return None
        if self.fault == "stale_resume" and not c.get("checkpoint_id"):
            items = list(self.inner.list(config, limit=2))
            if len(items) == 2 and hash(c["thread_id"]) % 7 == 0:
                return items[1]   # the checkpoint before the newest
        return self.inner.get_tuple(config)

    def list(self, config, *, filter=None, before=None, limit=None):
        tid = (config or {}).get("configurable", {}).get("thread_id")
        if tid in self.hidden:
            return iter([])
        items = list(self.inner.list(config, filter=filter, before=before, limit=None if limit else limit))
        if limit is None:
            if self.fault == "list_wrong_order" and len(items) > 2:
                items = items[:1] + items[1:][::-1]      # newest first, then the rest oldest first
            if self.fault == "list_duplicates" and len(items) > 1:
                items = items[:1] + items                  # the newest twice
            return iter(items)
        if self.fault == "limit10_oldest_one":
            return iter(items[-1:])                         # "last 10" answers with the single OLDEST checkpoint
        return iter(items[:limit])

    def delete_thread(self, thread_id):
        if self.fault == "erase_keeps_old_ids":
            self.hidden.add(thread_id)                     # latest and list say "gone"; get by checkpoint id still answers
            return
        return self.inner.delete_thread(thread_id)


class H:
    def close(self): pass


class FakeTarget:
    name = "fake"
    def __init__(self, fault): self.saver = Faulty(InMemorySaver(), fault)
    def make_saver(self): return self.saver, H()


def run(fault):
    seed, profile = 42, "tiny"
    thread_list, events = generator.plan(seed, profile)
    threads = {t.thread_id: t for t in thread_list}
    out = tempfile.mkdtemp()
    rec = bench.Recorder(os.path.join(out, "ops.jsonl"))
    exp = graph_runner.Expected()
    t = FakeTarget(fault)
    w = bench.write_phase(t, seed, threads, events, rec, 1, exp)
    rd = bench.read_phase(t, threads, rec, 10, seed, exp)
    chosen = rd.pop("chosen")
    er = bench.erase_phase(t, threads, rec, 2, seed, exp)
    erased = er.pop("erased")
    cold = bench.cold_phase(t, threads, rec, 5, seed, "true", set(chosen) | set(erased), exp)
    rec.close()
    chk = exp.report()
    ok = (not w["errors"] and rd["inconsistencies"] == 0 and er["threads_left_readable"] == 0 and chk["wrong_reads"] == 0
          and cold["restart"]["rc"] == 0 and not cold.get("n_errors") and cold.get("threads", 0) > 0)
    n10 = [r["n"] for r in rec.rows if r["op"] == "list_limit10"]
    print(f"{fault:24s} ok={ok!s:5s} -> {'CAUGHT' if not ok else 'MISSED' if fault != 'none (control)' else 'clean'}; "
          f"wrong_reads={chk['wrong_reads']} {chk['by_kind']} inconsistencies={rd['inconsistencies']} "
          f"left_readable={er['threads_left_readable']} reads_checked={chk['reads_checked']} "
          f"list_limit10 items: {sorted(set(n10))}" + (f" dropped={t.saver.dropped}" if t.saver.dropped else ""))
    if fault == "erase_keeps_old_ids":
        sv = t.saver   # extra probe, not in bench.py: read an erased thread's checkpoint by its id
        tid = erased[0]
        any_id = next(iter(sv.inner.list({"configurable": {"thread_id": tid, "checkpoint_ns": ""}})), None)
        got = sv.get_tuple(any_id.config) if any_id else None
        print(f"{'':24s} erased thread {tid}: get_tuple by checkpoint id after the erase -> "
              f"{'STILL READABLE' if got else 'gone'}")


for f in ("none (control)", "stale_resume", "missing_pending_write", "list_wrong_order", "list_duplicates",
          "limit10_oldest_one", "erase_keeps_old_ids"):
    run(f)
