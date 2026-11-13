"""aws-local-round (2026-11-13): the four corrections of the review of the AWS plan's harness code
(fenvix-brain sessions/2026-11-13-review-aws-plan/REPORT.md, NOT APPROVED), and first-next-ops' hours. Each test failed
on the plan's code (claude/sweet-sagan-w0vkgy 2b2f0af2) and passes after (the before run: the session's evidence).
  1. reads_after_writes.py stops the server-side read: maxTimeMS on the explain, ExecutionTimeout (code 50) counted as a
     cut, killOp and a wait until currentOp shows none of it before the next stage;
  2. first_next_ops.py leaves out of FIRST and NEXT every request during which the server's background counters moved
     (the compactor's, the old pass's, the GC's, the background reads), and says how many;
  3. a "pantry written" mark: run.py writes it before a writing program runs; decide.py --pantry-check refuses a job
     that reads the pantry as loaded after it (B1 after W1); extras.sh skips such a program;
  4. the order of the NEXT reads rotates per customer, and NEXT is reported by position;
  and decide.py's hours of first-next-ops double when reads-after-writes follows it."""
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(S9, "programs"))
sys.path.insert(0, os.path.join(S9, "ci"))
sys.path.insert(0, HERE)
import first_next_ops as ops  # noqa: E402
import reads_after_writes as raw  # noqa: E402
from test_first_next_ops import STATE, Server  # noqa: E402


# ---------- 1. the cut read is stopped on the server ----------
class ExecutionTimeout(Exception):
    code = 50


class CutServer(Server):
    """explain: the first `slow` reads time out; the server keeps 'running' a cut read for `busy_polls` currentOp polls
    unless killOp stops it (killable) ; every call is logged in order."""
    def __init__(self, slow=1, busy_polls=3, killable=False, error=TimeoutError("timed out"), current_op=True):
        super().__init__()
        self.slow, self.busy, self.killable, self.err, self.has_current_op = slow, 0, killable, error, current_op
        self.busy_polls, self.calls, self.max_times = busy_polls, [], []

    def explain(self, q, hinted):
        self.calls.append("explain")
        if self.slow > 0:
            self.slow -= 1
            self.busy = self.busy_polls
            raise self.err
        return super().explain(q, hinted)

    def command(self, name, *a, **kw):
        if name == "currentOp":
            if not self.has_current_op:
                raise RuntimeError("no such command: currentOp")
            self.calls.append("currentOp")
            if self.busy > 0:
                self.busy -= 1
                return {"inprog": [{"opid": 77, "command": {"explain": {"find": "ordini", "comment": raw.TAG}}}]}
            return {"inprog": [{"opid": 5, "command": {"find": "other"}}]}
        if name == "killOp":
            self.calls.append(f"killOp {kw.get('op')}")
            if self.killable:
                self.busy = 0
            return {"ok": 1}
        return super().command(name)


class CheapReadServer(CutServer):
    def __getitem__(self, db):
        srv = self
        base = Server.__getitem__(self, db)

        class DB:
            def __getitem__(self, coll):
                c = base[coll]
                c.find_one = lambda q, proj=None: srv.calls.append("cheap read") or {"_id": 1}
                return c
        return DB()


def _cursor_max_time(srv):
    """The fake cursor records maxTimeMS in srv.max_ms (test_first_next_ops.Cursor.max_time_ms)."""
    return srv.max_ms


def test_1_the_explain_carries_maxtimems_of_the_cut():
    srv = Server()
    rows = []
    raw.stage(srv, srv, "d", raw.the_reads(["cli-000001"], 2, 5), "before", rows, 300.0, 180.0)
    assert _cursor_max_time(srv) == [180000, 180000]


def test_1_execution_timeout_is_a_cut_not_an_error():
    srv = CutServer(error=ExecutionTimeout("operation exceeded time limit"))
    rows = []
    raw.stage(srv, srv, "d", raw.the_reads(["cli-000001"], 3, 5), "before", rows, 300.0, 180.0, sleep=lambda s: None)
    assert len(rows) == 1 and rows[0]["timed_out"] and rows[0]["error"] is None


def test_1_after_a_cut_the_read_is_killed_and_the_next_stage_waits_for_the_server_to_be_idle():
    clock = {"t": 0.0}
    sleep = lambda s: clock.__setitem__("t", clock["t"] + s)   # noqa: E731
    srv = CutServer(busy_polls=3, killable=False)
    rows = []
    reads = raw.the_reads(["cli-000001"], 2, 5)
    raw.stage(srv, srv, "d", reads, "before", rows, 300.0, 180.0, sleep=sleep, now=lambda: clock["t"])
    cut = rows[0]["after_cut"]
    assert cut["killed"] == [77] and cut["idle"] is True and cut["waited_s"] >= 2
    # nothing else is sent to the server before currentOp shows the cut read gone
    i_explain2 = len(srv.calls)
    raw.stage(srv, srv, "d", reads, "after-writes", rows, 300.0, 180.0, sleep=sleep, now=lambda: clock["t"])
    before_next = srv.calls[:i_explain2]
    assert before_next[0] == "explain" and before_next[1] == "currentOp" and before_next[2] == "killOp 77"
    assert before_next[-1] == "currentOp" and "explain" not in before_next[1:]
    # a server that never stops: the wait ends at its cap and says so
    srv = CutServer(busy_polls=10 ** 9)
    rows = []
    raw.stage(srv, srv, "d", reads, "before", rows, 300.0, 180.0, idle_max_s=30, sleep=sleep, now=lambda: clock["t"])
    assert rows[0]["after_cut"]["idle"] is False and rows[0]["after_cut"]["still_running"] == 1
    assert "STILL BUSY" in raw.text({"db": "d", "reads_per_stage": 2, "writes": {}, "settle": {}, "stages": raw.summarize(rows)})


def test_1_without_currentop_it_waits_for_a_cheap_read():
    srv = CheapReadServer(current_op=False)
    rows = []
    raw.stage(srv, srv, "d", raw.the_reads(["cli-000001"], 2, 5), "before", rows, 300.0, 180.0, sleep=lambda s: None)
    assert rows[0]["after_cut"]["how"] == "cheap read" and rows[0]["after_cut"]["idle"] is True and "cheap read" in srv.calls


# ---------- 2. requests during background work are left out ----------
class BackgroundServer(Server):
    """A cook whose compactor makes GETs (counted by the process-wide counter) while chosen requests run."""
    def __init__(self, during=(), **kw):
        super().__init__(box=True, **kw)
        self.during, self.n = set(during), 0

    def request(self, coll, q, n, write=False):
        super().request(coll, q, n, write)
        self.n += 1
        if self.n in self.during:
            self.get += 4                      # the compactor's GETs, inside the timed request
            self.bytes += 4_000_000
            self.compactor["absorbs"] += 1


def _run(srv, per_op=6):
    g = ops.groups(STATE, per_op, 9, "before-writes")
    rows = []
    for i in range(per_op):
        for op in ops.OPS:
            ops.measure_customer(srv, "app_data_b01", g[op][i], op, 9, 120000, ops.pick_counters(srv, "auto")[1], rows, i)
    return rows, ops.summarize(rows, True)


def test_2_a_next_request_during_background_gets_is_left_out_not_a_no():
    rows, s = _run(BackgroundServer(during={7, 40, 77}))     # three requests overlap the compactor's work
    assert s["dropped_background"] == 3
    for op in ops.READS:
        assert s["ops"][op]["next_gets_zero"] is True, op       # before the fix: "NO, it still reads from outside"
    assert sum(s["ops"][op]["dropped_background"] for op in ops.OPS) == 3
    assert "3 requests left out because the server's background work moved" in ops.text({**s, "db": "d", "label": "l", "counters": "fenvix"})


def test_2_a_warm_first_request_during_background_gets_is_not_counted_as_cold():
    g = ops.groups(STATE, 6, 9, "before-writes")
    warm = {g["resume"][0]}
    srv = BackgroundServer(during={1}, warm=warm)              # request 1 = resume's first customer's FIRST
    rows, s = _run(srv)
    assert rows[0]["cliente"] == g["resume"][0] and rows[0]["background_moved"] is True
    assert s["ops"]["resume"]["first"]["n"] == 5 and s["dropped_background_first"] == 1
    assert s["ops"]["resume"]["first"]["gets_p50"] == 3      # only truly cold firsts


def test_2_a_running_pass_counts_as_background_even_without_a_counter_change():
    a = {"background": {"maintenance": 0, "compaction": {"passes": 1, "running": 1}}}
    assert ops.background_moved(a, a) is True
    b = {"background": {"maintenance": 0, "compaction": {"passes": 1, "running": 0}}}
    assert ops.background_moved(b, b) is False
    assert ops.background_moved({}, {}) is False                # mongod: no such counters


# ---------- 3. the pantry's writing mark ----------
def _resolve(name):
    import decide
    os.environ.setdefault("RUN_ID", "1000000001")
    os.environ["S9_MODE"] = "local"
    return decide.resolve(json.load(open(os.path.join(S9, "commands", name))))


def test_3_w1_marks_the_pantry_and_b1_is_refused_after_it_m1_after_m2_too():
    import decide
    w1, b1 = _resolve("10-W1-first-next-ops-reads-after-writes-1tb.json"), _resolve("09-B1-read-rounds-1tb.json")
    m1, m2 = _resolve("02-M1-m30.json"), _resolve("03-M2-extras-write-peak.json")
    mark = decide.pantry_mark(w1, 42)
    assert mark["programs"] == ["first-next-ops", "reads-after-writes"] and mark["databases"] == ["app_data_b01"]
    assert decide.pantry_mark(b1, 43) is None and decide.pantry_mark(m1, 44) is None
    assert decide.pantry_mark_refusal(b1, []) is None
    r = decide.pantry_mark_refusal(b1, [mark])
    assert r and "first-next" in r and "ballast-rounds" in r and "no longer as loaded" in r
    assert decide.pantry_mark_refusal(m1, [decide.pantry_mark(m2, 45)]) is not None
    # a job that only writes, or only measures what it writes itself, is not refused (W1 itself, M3)
    assert decide.pantry_mark_refusal(w1, [mark]) is None
    assert decide.pantry_mark_refusal(_resolve("04-M3-agent-bench.json"), [mark]) is None


def test_3_the_pilot_reads_only_and_does_not_mark():
    import decide
    assert decide.pantry_mark(_resolve("pilot-P-small.json"), 1) is None


@pytest.fixture
def s3():
    from moto import mock_aws
    with mock_aws():
        import boto3
        c = boto3.client("s3", region_name="eu-west-1")
        c.create_bucket(Bucket="fenvix-kitchen-pantry-1000000001", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
        yield c


def test_3_pantry_check_reads_the_marks_from_s3(s3, tmp_path, capsys):
    import decide
    b1 = _resolve("09-B1-read-rounds-1tb.json")
    assert decide.read_pantry_marks(b1["pantry_name"], s3) == []
    w1 = _resolve("10-W1-first-next-ops-reads-after-writes-1tb.json")
    s3.put_object(Bucket=b1["pantry_name"], Key=f"{decide.PANTRY_MARK_PREFIX}42.json", Body=json.dumps(decide.pantry_mark(w1, 42)))
    f = tmp_path / "b1.json"
    f.write_text(json.dumps(b1))
    sys.argv = ["decide.py", "--pantry-check", str(f)]
    with pytest.raises(SystemExit) as e:
        decide.main()
    assert e.value.code == 1 and "REFUSED" in capsys.readouterr().out


def test_3_run_py_writes_the_mark_and_the_workflow_and_extras_check_it():
    run = open(os.path.join(S9, "ci", "run.py")).read()
    assert "decide.pantry_mark(cmd, RUN)" in run and "PANTRY_MARK_PREFIX" in run
    i_mark, i_launch = run.index("decide.pantry_mark(cmd, RUN)"), run.index('launch("extras", "extras"')
    assert i_mark < i_launch                                   # marked before any writing program can start
    wf = open(os.path.join(S9, "..", ".github", "workflows", "step9-kitchen.yml")).read()
    assert wf.index("decide.py --pantry-check") < wf.index("python3 step9/ci/run.py")
    sh = open(os.path.join(S9, "machine", "extras.sh")).read()
    assert "if has first-next && reads_as_loaded_ok first-next; then" in sh
    assert "if has ballast-rounds && reads_as_loaded_ok ballast-rounds; then" in sh
    assert "decide.py --pantry-check" in open(os.path.join(S9, "local", "local-run.sh")).read()


# ---------- 4. the NEXT order rotates; NEXT by position ----------
def test_4_every_next_read_takes_every_position_and_is_reported_by_it():
    rows, s = _run(Server(box=True), per_op=6)
    for op in ops.READS:
        pos = {r["pos"] for r in rows if r["op"] == op and r["role"] == "next"}
        assert pos == {1, 2, 3, 4}, (op, pos)        # 1-3 in the read groups, 1-4 in the save group
        bp = s["ops"][op]["next_by_position"]
        assert set(bp) == {"1", "2", "3", "4"} and sum(x["n"] for x in bp.values()) == s["ops"][op]["next"]["n"]
    # time travel is no longer always right after a whole-history read of the same customer
    after_history = 0
    for cli in {r["cliente"] for r in rows}:
        seq = [r["op"] for r in rows if r["cliente"] == cli]
        after_history += any(a == "history" and b == "time_travel" for a, b in zip(seq, seq[1:]))
    assert after_history < 0.5 * len({r["cliente"] for r in rows if r["group"] != "time_travel"})
    # the save stays last in every read group
    for cli in {r["cliente"] for r in rows if r["group"] != "save"}:
        assert [r["op"] for r in rows if r["cliente"] == cli][-1] == "save"
    assert "by position" in ops.text({**s, "db": "d", "label": "l", "counters": "fenvix"})


# ---------- hours ----------
def test_hours_of_first_next_ops_scale_with_per_op_and_double_after_writes():
    w1 = _resolve("10-W1-first-next-ops-reads-after-writes-1tb.json")
    import decide
    base = decide.PROGRAM_HOURS["first-next-ops"]
    assert abs(w1["job_hours"]["first_next_ops"] - round(base * 100 / 30 * 2, 2)) < 0.011
    m2 = _resolve("03-M2-extras-write-peak.json")
    assert abs(m2["job_hours"]["first_next_ops"] - round(base, 2)) < 0.011     # 30 per operation, once


def test_dry_run_still_accepts_every_command():
    out = subprocess.run(["bash", os.path.join(S9, "commands", "dry_run.sh")], capture_output=True, text=True).stdout
    assert out.count("[local] ACCEPTED") == len([f for f in os.listdir(os.path.join(S9, "commands")) if f.endswith(".json")])


# ---------- found by the local docker round ----------
def test_after_writes_against_before_writes_is_the_same_plan_across_labels():
    import first_next_compare as cmp_
    line = {"n": 5, "p50_ms": 1.0, "p99_ms": 2.0}
    a = {"db": "d", "label": "after-writes", "seed": 9, "per_op": 30, "ops": {"resume": {"first": line, "next": line}}}
    b = {**a, "label": "before-writes"}
    assert cmp_.compare(a, b)["same_plan"] is False                 # ours against mongod: the labels must match
    assert cmp_.compare(a, b, across_labels=True)["same_plan"] is True
    for f in ("extras.sh", "mongo_client.sh"):
        assert '--their-name "before the writes" --across-labels' in open(os.path.join(S9, "machine", f)).read()


def test_the_mongod_set_is_initiated_through_localhost_and_a_stopped_member_ends_the_wait():
    m = open(os.path.join(S9, "machine", "mongod.sh")).read()
    i = m.index('"replSetInitiate"')
    assert "mongodb://127.0.0.1:{port}/" in m[m.rindex("loc = ", 0, i):i]   # before: over the private IP, refused
    run = open(os.path.join(S9, "ci", "run.py")).read()
    assert "MONGOD_MEMBER_STOPPED" in run                            # before: 25 minutes of waiting after a member died


def test_a_comparison_of_other_customers_is_not_a_pair():
    """Found by the local round: ours and mongod's summaries had the same label, seed and size, but the state files
    behind them had 2,048 and 512 customers: the comparison said nothing. Now the customers' fingerprint must match."""
    import first_next_compare as cmp_
    line = {"n": 5, "p50_ms": 1.0, "p99_ms": 2.0}
    a = {"db": "d", "label": "before-writes", "seed": 9, "per_op": 12, "customers_fingerprint": "aaaa",
         "ops": {"resume": {"first": line, "next": line}}}
    assert cmp_.compare(a, dict(a))["same_plan"] is True
    c = cmp_.compare(a, {**a, "customers_fingerprint": "bbbb"}, )
    assert c["same_plan"] is False and "NOT THE SAME PLAN" in cmp_.text(c, "mongod")
    # first_next_ops writes the fingerprint
    assert "customers_fingerprint" in open(os.path.join(S9, "programs", "first_next_ops.py")).read()
