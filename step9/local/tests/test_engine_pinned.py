"""Total review R5 (2026-11-12): the commands name a full 40-character engine commit, never a branch or tag that moves
between the jobs of one round; decide.py refuses anything else, and refuses a workflow commit that differs. $0."""
import glob
import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CI = os.path.abspath(os.path.join(HERE, "..", "..", "ci"))
CMDS = os.path.abspath(os.path.join(HERE, "..", "..", "commands"))
C40 = "27014ff2e3e3e4ff223aa778746f3dce433c6549"


def decide(engine, env_commit=None):
    base = json.load(open(os.path.join(CMDS, "02-M1-m30.json")))
    base["engine"] = engine
    d = tempfile.mkdtemp()
    f = os.path.join(d, "c.json")
    json.dump(base, open(f, "w"))
    env = {k: v for k, v in os.environ.items() if k != "ENGINE_COMMIT"}
    env.update({"S9_MODE": "local", "RUN_ID": "1000000001"})
    if env_commit:
        env["ENGINE_COMMIT"] = env_commit
    r = subprocess.run([sys.executable, "decide.py", f, os.path.join(d, "r.json")], cwd=CI, capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


def test_every_run_command_names_a_full_commit():
    for f in glob.glob(os.path.join(CMDS, "*.json")):
        c = json.load(open(f))
        if "engine" in c:
            assert re.fullmatch(r"[0-9a-f]{40}", c["engine"]), (f, c["engine"])


def test_a_branch_tag_or_short_id_is_refused():
    for bad in ("integration-4", "aws-round-1", C40[:12], C40.upper()):
        rc, out = decide(bad)
        assert rc != 0 and "the full 40-character commit id" in out, (bad, out)


def test_a_full_commit_is_accepted_and_is_the_engine_commit():
    rc, out = decide(C40)
    assert rc == 0, out


def test_a_different_workflow_commit_is_refused():
    rc, out = decide(C40, env_commit="1" * 40)
    assert rc != 0 and "the workflow resolved" in out, out
    rc, out = decide(C40, env_commit=C40)
    assert rc == 0, out


def test_the_workflow_takes_the_commit_as_given():
    wf = open(os.path.join(HERE, "..", "..", "..", ".github", "workflows", "step9-kitchen.yml")).read()
    assert "export ENGINE_COMMIT=$ENGINE" in wf and "git ls-remote origin" not in wf
