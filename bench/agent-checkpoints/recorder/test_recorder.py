"""Part C · tests of the recorder and the replay (aws-fixes, B11 of the total review, 2026-11-11). Fake model, $0.
  1. a failing conversation (more model calls in one user turn than --max-steps: LangGraph's GraphRecursionError, the
     reviewer's case) is recorded as failed WITH its spend, its partial file is not a complete recording, the summary
     is written, the run goes on and stops after two failures in a row;
  2. the run resumes: the same command again pays nothing (no model call), and --retry-failed records only the failed;
  3. the cap is checked at every model call: a run capped at half a conversation stops inside that conversation
     (stopped_by_cap), within one call of the cap; the old recorder checked only after a whole user turn;
  4. the replay refuses incomplete recordings;
  5. (with RECORDER_TEST_PG_URI, a PostgreSQL the test may create schemas in) the replay renumbers channel versions with
     the Postgres saver's own function: the same conversations replayed and run directly store the same row bytes within
     1% (sum of pg_column_size), where the recorded integer versions store about 9% fewer row bytes and about 13% fewer
     relation bytes (the reviewer measured 12.4%). Relation sizes are also printed; at this tiny size they move by whole
     8 KB index pages, so they are information, not the assertion.
Usage: python -m pytest -q bench/agent-checkpoints/recorder/test_recorder.py   (langgraph, langchain-core installed)"""
import glob
import json
import os
import subprocess
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REC = os.path.join(HERE, "record.py")
REP = os.path.join(HERE, "replay.py")


def record(out, *args):
    r = subprocess.run([sys.executable, REC, out, "--model", "fake", *args], capture_output=True, text=True, timeout=600)
    s = json.load(open(os.path.join(out, "summary.json"))) if os.path.exists(os.path.join(out, "summary.json")) else None
    return r, s


def by_id(s):
    return {c["task_id"]: c for c in s["conversations"]}


def test_failure_is_recorded_with_its_spend_and_the_summary_is_written():
    out = tempfile.mkdtemp()
    r, s = record(out, "--support", "3", "--coding", "2", "--max-steps", "2", "--fake-price", "claude-sonnet-5-5")
    assert r.returncode == 1, r.stdout + r.stderr
    assert s is not None, "summary.json must be written"
    failed = [c for c in s["conversations"] if c["status"] == "failed"]
    assert failed and all(c["dollars"] > 0 and "GraphRecursionError" in c["error"] for c in failed), s["conversations"]
    assert abs(s["spent_dollars"] - sum(c["dollars"] for c in s["conversations"])) < 1e-4
    for c in failed:
        assert not os.path.exists(os.path.join(out, f"{c['task_id']}.jsonl.gz"))
        assert os.path.exists(os.path.join(out, f"{c['task_id']}.failed.incomplete.gz"))
    assert s["stop"] and "failed in a row" in s["stop"]


def test_resume_pays_nothing_again_and_retries_only_the_failed():
    out = tempfile.mkdtemp()
    _, s1 = record(out, "--support", "3", "--coding", "2", "--max-steps", "2", "--fake-price", "claude-sonnet-5-5",
                   "--max-consecutive-failures", "99")
    failed = {c["task_id"] for c in s1["conversations"] if c["status"] == "failed"}
    done = {c["task_id"] for c in s1["conversations"] if c["status"] == "done"}
    assert failed and done, s1["status_counts"]
    mtimes = {t: os.path.getmtime(os.path.join(out, f"{t}.jsonl.gz")) for t in done}
    # the same command again: nothing paid, nothing rewritten
    r2, s2 = record(out, "--support", "3", "--coding", "2", "--max-steps", "2", "--fake-price", "claude-sonnet-5-5",
                    "--max-consecutive-failures", "99")
    assert s2["spent_dollars"] == s1["spent_dollars"], (s1["spent_dollars"], s2["spent_dollars"])
    assert "already recorded, skipped (not paid again)" in r2.stdout
    # --retry-failed with enough steps: only the failed are recorded again; the earlier spend stays counted
    r3, s3 = record(out, "--support", "3", "--coding", "2", "--fake-price", "claude-sonnet-5-5", "--retry-failed")
    assert r3.returncode == 0, r3.stdout + r3.stderr
    c3 = by_id(s3)
    assert all(c3[t]["status"] == "done" and c3[t]["earlier_attempts"][0]["status"] == "failed" for t in failed)
    assert all(os.path.getmtime(os.path.join(out, f"{t}.jsonl.gz")) == mtimes[t] for t in done)
    new = sum(c3[t]["dollars"] for t in failed)
    assert abs(s3["spent_dollars"] - (s1["spent_dollars"] + new)) < 1e-4


def test_cap_is_checked_at_every_model_call():
    full = tempfile.mkdtemp()
    _, sf = record(full, "--support", "1", "--fake-price", "claude-sonnet-5-5")
    conv = sf["conversations"][0]
    assert conv["status"] == "done" and conv["model_calls"] >= 6
    cap = conv["dollars"] / 2
    out = tempfile.mkdtemp()
    r, s = record(out, "--support", "1", "--fake-price", "claude-sonnet-5-5", "--max-dollars", f"{cap:.6f}")
    c = s["conversations"][0]
    assert r.returncode == 1 and c["status"] == "stopped_by_cap", (r.stdout, c)
    assert c["model_calls"] < conv["model_calls"]
    calls = [json.loads(l)["usage"] for l in __import__("gzip").open(glob.glob(f"{out}/*.incomplete.gz")[0], "rt")
             if '"model_call"' in l]
    sys.path.insert(0, HERE)
    import claude_cost
    biggest = max(claude_cost.call_dollars("claude-sonnet-5-5", u) for u in calls)
    assert s["spent_dollars"] < cap + biggest, (s["spent_dollars"], cap, biggest)
    assert s["spent_dollars"] <= cap or c["model_calls"] == 1


def test_replay_refuses_incomplete_recordings():
    out = tempfile.mkdtemp()
    record(out, "--support", "3", "--coding", "1", "--max-steps", "2", "--max-consecutive-failures", "99")
    # a fake "partial" file with the complete name and no end event (what the old recorder left after a crash)
    import gzip
    src = glob.glob(f"{out}/*.incomplete.gz")[0]
    lines = gzip.open(src, "rt").read().splitlines()[:-1]
    with gzip.open(os.path.join(out, "support-7-99.jsonl.gz"), "wt") as f:
        f.write("\n".join(lines) + "\n")
    rep = tempfile.mkdtemp()
    r = subprocess.run([sys.executable, REP, out, rep, "--target", "mongo", "--uri", "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1"],
                       capture_output=True, text=True, timeout=120)
    assert "refused (not a complete recording): ['support-7-99.jsonl.gz']" in r.stdout, r.stdout + r.stderr


PG = os.environ.get("RECORDER_TEST_PG_URI")


@pytest.mark.skipif(not PG, reason="RECORDER_TEST_PG_URI not set (a PostgreSQL the test may create schemas in)")
def test_replay_renumbers_versions_like_the_postgres_saver():
    import psycopg
    rec = tempfile.mkdtemp()
    r, s = record(rec, "--support", "3", "--coding", "3")
    assert r.returncode == 0, r.stdout + r.stderr
    sizes = {}
    with psycopg.connect(PG, autocommit=True) as c:
        for sch in ("direct", "replay_new", "replay_old"):
            c.execute(f"drop schema if exists {sch} cascade")
            c.execute(f"create schema {sch}")
    uri = lambda sch: PG + ("&" if "?" in PG else "?") + f"options=-csearch_path%3D{sch}"
    # the same conversations, run directly on the Postgres saver (same tasks, same fake model)
    code = f"""
import sys; sys.path.insert(0, {HERE!r}); sys.argv = ['x']
import record, tasks
from langchain_core.messages import HumanMessage
sys.path.insert(0, {os.path.dirname(HERE)!r}); import targets
saver, conn = targets.postgres_saver({uri('direct')!r})
for kind, n in (("support", 3), ("coding", 3)):
    for i in range(n):
        t = tasks.make_task(kind, i, 7)
        g = record.build_graph(t, record.FakeModel(t, 7 * 100 + i), saver, lambda ev: None)
        for turn in t.user_turns:
            g.invoke({{"messages": [HumanMessage(content=turn)]}}, {{"configurable": {{"thread_id": t.task_id + "-r0"}}, "recursion_limit": 62}})
"""
    d = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600)
    assert d.returncode == 0, d.stderr[-2000:]
    for sch, extra in (("replay_new", []), ("replay_old", ["--keep-recorded-versions"])):
        rr = subprocess.run([sys.executable, REP, rec, tempfile.mkdtemp(), "--target", "postgres", "--uri", uri(sch), *extra],
                            capture_output=True, text=True, timeout=600)
        assert rr.returncode == 0, rr.stderr[-2000:]
    with psycopg.connect(PG, autocommit=True) as c:
        for sch in ("direct", "replay_new", "replay_old"):
            c.execute(f"set search_path = {sch}")
            rows = {t: c.execute(f"select count(*) from {t}").fetchone()[0] for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes")}
            total = sum(c.execute(f"select pg_total_relation_size('{sch}.{t}')").fetchone()[0]
                        for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"))
            ver = c.execute("select avg(length(version)) from checkpoint_blobs").fetchone()[0]
            row_bytes = sum(c.execute(f"select coalesce(sum(pg_column_size(x.*)), 0) from {t} x").fetchone()[0]
                            for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes"))
            sizes[sch] = {"rows": rows, "row_bytes": row_bytes, "total_bytes": total, "mean_version_length": float(ver)}
    print(json.dumps(sizes))
    json.dump(sizes, open(os.environ.get("RECORDER_TEST_SIZES_OUT", os.devnull), "w"), indent=1)
    assert sizes["direct"]["rows"] == sizes["replay_new"]["rows"] == sizes["replay_old"]["rows"]
    assert abs(sizes["replay_new"]["mean_version_length"] - sizes["direct"]["mean_version_length"]) < 1.0
    assert abs(sizes["replay_new"]["row_bytes"] / sizes["direct"]["row_bytes"] - 1) < 0.01, sizes
    assert sizes["replay_old"]["row_bytes"] < 0.95 * sizes["direct"]["row_bytes"], sizes
