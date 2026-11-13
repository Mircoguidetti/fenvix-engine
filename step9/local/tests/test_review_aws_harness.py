"""Review of the AWS harness (2026-11-13, fenvix-brain prompts/R-aws-harness.md), on aws-local-round 2eb81f56. Each test
failed there (the run: step9/local/review-aws-harness/tests-before-on-2eb81f56.txt) and passes after.
  1. BLOCKER: the pantry's writing mark was per pantry. In the round's order job M2 writes into an Atlas-shaped
     database (first-next-ops saves), so B1 (the read rounds on the 1 TB database, which nothing had written) was
     refused by `decide.py --pantry-check`, and extras.sh would have skipped its two programs. Now per database.
  2. The decision of 2026-11-13 (100 GB AND 1 TB): both sizes wrote app_data_b01, so a pantry held one of them; the
     100 GB database is now app_data_h01, both share the one kept pantry, and first-next reads the command's own one.
  3. The engine is ONE input of the round (step9/ENGINE); the commands carry none; a command naming another one is
     refused in mode aws.
  4. reads-after-writes: a first read cut at --max-read-s is a result (the expected one at 1 TB on an amplifying
     engine), not FAILED.
  5. The pair with the mongod line: M2 runs reads-after-writes too, as MG does, so both lines have the same three
     measures at the Atlas size.
  6. estimate.py prices every job of the round from decide.py's own hours (W1 was priced at 3.0 h, decide says 4.0)."""
import json
import os
import subprocess
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
CI = os.path.join(S9, "ci")
CMDS = os.path.join(S9, "commands")
sys.path.insert(0, os.path.join(S9, "programs"))
sys.path.insert(0, CI)
sys.path.insert(0, os.path.join(S9, "machine"))
sys.path.insert(0, HERE)

# the round's order; ROUND.txt must list the same (test_round_txt_is_this_order)
ORDER = ["00-permissions.json", "01-L-load-atlas.json", "pilot-P-small.json", "02-M1-m30.json",
         "03-M2-extras-write-peak.json", "11-MG-mongod-line-atlas.json", "04-M3-agent-bench.json",
         "12-A1-atlas-ops-atlas-size.json", "13-A2-atlas-load-100gb.json", "14-A3-atlas-read-rounds-100gb.json",
         "05-G-generate-100gb.json", "06-BW-read-rounds-ops-raw-100gb.json", "08-G1-generate-1tb.json",
         "09-B1-read-rounds-1tb.json", "10-W1-first-next-ops-reads-after-writes-1tb.json", "07-drop-pantry.json"]


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("S9_MODE", "local")
    monkeypatch.setenv("RUN_ID", "1000000001")
    monkeypatch.delenv("ENGINE_COMMIT", raising=False)


def _resolve(name, **over):
    import decide
    raw = json.load(open(os.path.join(CMDS, name)))
    raw.update(over)
    return decide.resolve(raw)


def _walk(order):
    """Every job of `order` through decide.py, as the workflow runs them on the one kept pantry: the pantry check with
    the marks of the earlier jobs, then the job's own mark. Returns {job: refusal or None}."""
    import decide
    marks, out = [], {}
    for i, f in enumerate(order):
        c = _resolve(f)
        if c["mode"] != "run":
            out[f] = None
            continue
        # job L's own run made the pantry; later commands name it (1000000001 here): only those are "existing"
        out[f] = decide.pantry_mark_refusal(c, marks)
        m = decide.pantry_mark(c, 2000 + i)
        if m and c["pantry_mode"] == "existing":
            marks.append(m)
    return out


# ---------- 1. the writing mark, per database ----------
def test_1_every_job_of_the_round_in_its_order_passes_the_pantry_check():
    refused = {f: r for f, r in _walk(ORDER).items() if r}
    assert not refused, refused


def test_1_b1_after_m2_runs_but_b1_after_w1_and_m1_after_m2_are_still_refused():
    import decide
    m2, w1, w100 = _resolve(ORDER[4]), _resolve(ORDER[14]), _resolve(ORDER[11])
    b1, m1 = _resolve(ORDER[13]), _resolve(ORDER[3])
    mark_m2, mark_w1, mark_w100 = decide.pantry_mark(m2, 11), decide.pantry_mark(w1, 12), decide.pantry_mark(w100, 13)
    assert decide.pantry_mark_refusal(b1, [mark_m2]) is None           # M2 wrote an Atlas-shaped database only
    assert decide.pantry_mark_refusal(b1, [mark_w100]) is None         # the 100 GB writes are another database
    assert decide.pantry_mark_refusal(b1, [mark_w1]) is not None       # W1 wrote the 1 TB database B1 reads
    assert decide.pantry_mark_refusal(m1, [mark_m2]) is not None       # M2 wrote what M1's founder's script reads
    assert decide.pantry_mark_refusal(_resolve(ORDER[11]), [mark_w1]) is None   # 100 GB after 1 TB: still fine


def test_1_marks_name_their_database_and_an_unreadable_mark_blocks_every_reader():
    import pantry_marks as pm
    assert pm.written({"databases": ["app_data_b01"]}) == {"app_data_b01"}
    assert pm.written({"databases": ["the largest Atlas-shaped database"]}) == {"atlas"}   # aws-local-round's text
    assert pm.written({}) == {pm.ALL} and pm.written({"databases": "x"}) == {pm.ALL}
    marks = [{"run": "5", "databases": ["atlas"]}, {"run": "6", "databases": ["app_data_b01"]}]
    assert [m["run"] for m in pm.blocking(marks, ["app_data_b01"])] == ["6"]
    assert pm.blocking(marks, ["app_data_b01"], me="6") == []          # the job's own mark never blocks it
    assert [m["run"] for m in pm.blocking([{"run": "7"}], ["app_data_h01"])] == ["7"]


def test_1_the_extras_client_checks_the_databases_each_program_reads(monkeypatch, tmp_path):
    """pantry_marks.py check, as extras.sh calls it, against an S3 pantry (moto) holding M2's mark."""
    from moto import mock_aws
    import decide
    import pantry_marks as pm
    with mock_aws():
        import boto3
        s3 = boto3.client("s3", region_name="eu-west-1")
        s3.create_bucket(Bucket="pantry-test", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
        s3.put_object(Bucket="pantry-test", Key=f"{pm.PREFIX}11.json", Body=json.dumps(decide.pantry_mark(_resolve(ORDER[4]), 11)))
        monkeypatch.setitem(sys.modules, "s3io", types.SimpleNamespace(client=lambda: s3))
        b1 = _resolve(ORDER[13])
        assert pm.main(["x", "check", "pantry-test", "99", json.dumps(b1["pantry_reads"]["first-next"])]) == 0
        assert pm.main(["x", "check", "pantry-test", "99", json.dumps(b1["pantry_reads"]["ballast-rounds"])]) == 0
        assert pm.main(["x", "check", "pantry-test", "99", json.dumps(["atlas"])]) == 1
        assert pm.main(["x", "check", "pantry-test", "99", ""]) == 1             # a reader that does not say what it reads
        assert pm.main(["x", "check", "pantry-test", "11", ""]) == 0             # ... but never its own job's mark


def test_1_the_reads_reach_the_extras_client():
    assert _resolve(ORDER[13])["pantry_reads"] == {"first-next": ["app_data_b01"], "ballast-rounds": ["app_data_b01"]}
    assert _resolve(ORDER[3])["pantry_reads"] == {"m30": ["atlas"]}
    run = open(os.path.join(CI, "run.py")).read()
    assert '"pantry_reads": cmd.get("pantry_reads") or {}' in run
    sh = open(os.path.join(S9, "machine", "extras.sh")).read()
    assert '$PY pantry_marks.py check "$PANTRY" "$RUN_ID_SELF" "$(cfg "pantry_reads.$1")"' in sh
    assert "pantry_marks.py" in open(os.path.join(CI, "packages.sh")).read()


# ---------- 2. 100 GB and 1 TB in one pantry ----------
def test_2_the_100gb_and_1tb_databases_have_their_own_names_in_the_one_pantry():
    g100, g1 = _resolve(ORDER[10]), _resolve(ORDER[12])
    assert g100["pantry_name"] == g1["pantry_name"]
    assert g100["ballast_db"] == "app_data_h01" and g1["ballast_db"] == "app_data_b01"
    assert g100["ballast"]["lines"][0][0] == "app_data_h01" and g1["ballast"]["lines"][0][0] == "app_data_b01"
    bw, b1, w1 = _resolve(ORDER[11]), _resolve(ORDER[13]), _resolve(ORDER[14])
    assert bw["ballast_db"] == "app_data_h01" and b1["ballast_db"] == w1["ballast_db"] == "app_data_b01"
    # the 100 GB measure job: reads first, then the two writing programs (extras.sh's order), in one job, no spare
    assert bw["programs"] == ["idle-cost", "first-next", "ballast-rounds", "first-next-ops", "reads-after-writes"] and bw["spare"] is None


def test_2_first_next_reads_only_the_given_database():
    import first_next as fn

    class Srv:
        admin = None

        def __init__(self):
            self.dbs = []

        def list_database_names(self):
            return ["app_data_b01", "app_data_h01", "admin"]

        def __getitem__(self, d):
            self.dbs.append(d)
            raise RuntimeError("stop after the choice")
    srv = Srv()
    fake = types.ModuleType("pymongo")
    fake.MongoClient = lambda *a, **k: srv
    import tempfile
    d = tempfile.mkdtemp()
    for name, size in (("app_data_b01", 10 ** 12), ("app_data_h01", 10 ** 11)):
        os.makedirs(os.path.join(d, "s", name))
        json.dump({"db": name, "clienti": {"n": 10, "nomi_da": "cli-%06d"}, "caricamento": {"byte_bson": size}},
                  open(os.path.join(d, "s", name, "risultati_p1.json"), "w"))
    saved = sys.modules.get("pymongo")
    sys.modules["pymongo"] = fake
    try:
        with pytest.raises(RuntimeError):
            fn.main([os.path.join(d, "o"), "--uri", "x", "--states", os.path.join(d, "s"), "--db", "app_data_h01"])
    finally:
        sys.modules["pymongo"] = saved
    assert srv.dbs == ["app_data_h01"]


# ---------- 3. the engine: one input ----------
C40, OTHER = "a" * 40, "b" * 40


def _engine_file(tmp_path, monkeypatch, text):
    f = tmp_path / "ENGINE"
    f.write_text(text)
    monkeypatch.setenv("S9_ENGINE_FILE", str(f))
    import decide
    return decide


def test_3_the_commands_carry_no_engine_and_every_job_takes_step9_engine(tmp_path, monkeypatch):
    for f in os.listdir(CMDS):
        if f.endswith(".json"):
            assert "engine" not in json.load(open(os.path.join(CMDS, f))), f
    decide = _engine_file(tmp_path, monkeypatch, f"# comment\n{C40}\n")
    for f in ORDER:
        raw = json.load(open(os.path.join(CMDS, f)))
        c = decide.resolve(raw)
        if c["mode"] == "run":
            assert c["engine"] == c["engine_commit"] == C40 and c["engine_source"] == "step9/ENGINE", f
    # the real file holds one full commit id
    lines = [l.strip() for l in open(os.path.join(S9, "ENGINE")) if l.strip() and not l.startswith("#")]
    assert len(lines) == 1 and len(lines[0]) == 40 and all(ch in "0123456789abcdef" for ch in lines[0])


def test_3_a_command_naming_another_engine_is_refused_on_aws_and_no_engine_is_refused(tmp_path, monkeypatch, capsys):
    decide = _engine_file(tmp_path, monkeypatch, C40 + "\n")
    raw = json.load(open(os.path.join(CMDS, ORDER[3])))
    monkeypatch.setenv("S9_MODE", "aws")
    with pytest.raises(SystemExit):
        decide.resolve({**raw, "engine": OTHER})
    assert "one engine per round" in capsys.readouterr().out
    monkeypatch.setenv("S9_MODE", "local")
    assert decide.resolve({**raw, "engine": OTHER})["engine"] == OTHER   # the local round builds its own commit
    decide = _engine_file(tmp_path, monkeypatch, "# nothing yet\n")
    with pytest.raises(SystemExit):
        decide.resolve(raw)
    assert "step9/ENGINE" in capsys.readouterr().out


def test_3_the_workflow_builds_the_engine_decide_resolves(tmp_path, monkeypatch):
    wf = open(os.path.join(S9, "..", ".github", "workflows", "step9-kitchen.yml")).read()
    assert "python3 decide.py --engine ../command.json" in wf
    assert "json.load(open('step9/command.json')).get('engine')" not in wf
    f = tmp_path / "e"
    f.write_text(C40 + "\n")
    cmd = os.path.join(CMDS, ORDER[3])
    r = subprocess.run([sys.executable, "decide.py", "--engine", cmd], cwd=CI, capture_output=True, text=True,
                       env={**os.environ, "S9_ENGINE_FILE": str(f)})
    assert r.returncode == 0 and r.stdout.strip() == C40


# ---------- 4. a cut first read is a result ----------
def test_4_a_first_read_cut_at_the_limit_is_a_result_not_a_failure(tmp_path, monkeypatch):
    import reads_after_writes as raw
    from test_aws_local_round import CutServer
    from test_first_next_ops import STATE
    srv = CutServer(slow=1, busy_polls=0)
    fake = types.ModuleType("pymongo")
    fake.MongoClient = lambda *a, **k: srv
    wc = types.ModuleType("pymongo.write_concern")
    wc.WriteConcern = lambda w=None: types.SimpleNamespace(document={"w": w})
    monkeypatch.setitem(sys.modules, "pymongo", fake)
    monkeypatch.setitem(sys.modules, "pymongo.write_concern", wc)
    os.makedirs(tmp_path / "s" / "app_data_b01")
    json.dump(STATE, open(tmp_path / "s" / "app_data_b01" / "risultati_p1.json", "w"))
    rc = raw.main([str(tmp_path / "o"), "--uri", "x", "--states", str(tmp_path / "s"), "--reads", "3", "--updates", "20",
                   "--write-customers", "5", "--settle-min", "0"])
    s = json.load(open(tmp_path / "o" / "reads-after-writes-summary.json"))
    assert s["stages"]["before"]["timed_out"] == 1 and s["stages"]["before"]["answered"] == 0
    assert rc == 0 and s["ok"] is True
    # a stage that errors is still a failure
    srv2 = CutServer(slow=1, busy_polls=0, error=RuntimeError("boom"))
    fake.MongoClient = lambda *a, **k: srv2
    assert raw.main([str(tmp_path / "o2"), "--uri", "x", "--states", str(tmp_path / "s"), "--reads", "2", "--updates", "5",
                     "--write-customers", "2", "--settle-min", "0"]) == 1


# ---------- 5. M2 and MG measure the same three things ----------
def test_5_m2_and_the_mongod_line_run_the_same_programs_on_the_same_customers():
    m2, mg = _resolve(ORDER[4]), _resolve(ORDER[5])
    w = [p for p in m2["programs"] if p in ("first-next-ops", "reads-after-writes")]
    assert w == mg["programs"] == ["first-next-ops", "reads-after-writes"]
    a = m2["first_next_ops_args"].split()
    b = mg["first_next_ops_args"].split()
    assert a[a.index("--per-op") + 1] == b[b.index("--per-op") + 1]
    assert m2["reads_after_writes_args"] == mg["reads_after_writes_args"]
    assert m2["job_hours_total"] <= 0.9 * m2["ceiling_min"] / 60


# ---------- 6. the estimate prices the jobs decide.py resolves ----------
def test_6_the_round_estimate_takes_every_job_and_its_hours_from_decide(capsys):
    import estimate
    jobs = estimate.round_plan()
    names = [j["file"] for j in jobs]
    assert names == [f for f in ORDER if f != "00-permissions.json" and f != "07-drop-pantry.json"]
    for j in jobs:
        c = _resolve(j["file"])
        if c.get("generator"):   # a generation longer than one job: its own hours plus one boot per job
            assert j["jobs"] == c["generator_jobs_estimated"]
            assert abs(j["hours"] - (c["generator_hours_estimated"] + 0.3 * j["jobs"])) < 0.06, j
        else:
            assert j["jobs"] == 1 and abs(j["hours"] - c["job_hours_total"]) < 0.06, j
        assert 0 < j["dollars"] / j["jobs"] < 5, j                      # each job under the default cap
    w1 = [j for j in jobs if j["file"].startswith("10-W1")][0]
    assert w1["hours"] >= 4.0
    assert "TOTAL" in capsys.readouterr().out


def test_round_txt_is_this_order():
    lines = [l.split()[0] for l in open(os.path.join(CMDS, "ROUND.txt")) if l.strip() and not l.startswith("#")]
    assert lines == ORDER
    assert sorted(ORDER) == sorted(f for f in os.listdir(CMDS) if f.endswith(".json"))


def test_1_extras_sh_function_skips_only_a_reader_of_a_written_database(tmp_path):
    """extras.sh's own reads_as_loaded_ok (cut out of the file, run by bash) with the real cfg.py and pantry_marks.py; S3
    is a folder (a fake s3io on PYTHONPATH): M2's mark is there; B1's first-next runs; a reader of "atlas" is skipped."""
    import decide
    sh = open(os.path.join(S9, "machine", "extras.sh")).read()
    fn = sh[sh.index("reads_as_loaded_ok() {"):]
    fn = fn[:fn.index("\n}\n") + 3]
    marks = tmp_path / "pantry" / "step9-pantry-written"
    marks.mkdir(parents=True)
    (marks / "11.json").write_text(json.dumps(decide.pantry_mark(_resolve(ORDER[4]), 11)))
    (tmp_path / "fake").mkdir()
    (tmp_path / "fake" / "s3io.py").write_text(
        "import json, os\n"
        "class _C:\n"
        "    def get_paginator(self, _):\n"
        "        return self\n"
        "    def paginate(self, Bucket, Prefix):\n"
        "        d = os.path.join(os.environ['FAKE_S3'], Bucket, Prefix)\n"
        "        return [{'Contents': [{'Key': Prefix + f} for f in sorted(os.listdir(d))]}] if os.path.isdir(d) else [{}]\n"
        "    def get_object(self, Bucket, Key):\n"
        "        return {'Body': open(os.path.join(os.environ['FAKE_S3'], Bucket, Key), 'rb')}\n"
        "def client():\n"
        "    return _C()\n")
    b1 = _resolve(ORDER[13])
    cfgf = tmp_path / "config.json"
    cfgf.write_text(json.dumps({"pantry_reads": {**b1["pantry_reads"], "m30": ["atlas"]}}))
    script = (f'set -u\nPY={sys.executable}\nPANTRY=pantry\nRUN_ID_SELF=99\noutcomes=()\n'
              f'cfg() {{ $PY {os.path.join(S9, "machine", "cfg.py")} {cfgf} "$1"; }}\nstep() {{ echo "STEP $*"; }}\n'
              f'cd {os.path.join(S9, "machine")}\n{fn}\n'
              'reads_as_loaded_ok first-next && echo RUN-first-next\nreads_as_loaded_ok ballast-rounds && echo RUN-ballast-rounds\n'
              'reads_as_loaded_ok m30 && echo RUN-m30\nreads_as_loaded_ok unknown && echo RUN-unknown\necho "OUT ${outcomes[*]}"\n')
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                       env={**os.environ, "PYTHONPATH": str(tmp_path / "fake"), "FAKE_S3": str(tmp_path)})
    out = r.stdout
    assert "RUN-first-next" in out and "RUN-ballast-rounds" in out, out + r.stderr
    assert "RUN-m30" not in out and "RUN-unknown" not in out, out
    assert "OUT m30:PANTRY_WRITTEN unknown:PANTRY_WRITTEN" in out, out
