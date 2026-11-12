"""Step 9 · tests of kitchen/settle.py (aws-fixes, B5 of the total review): the settle check must see compaction 2's
work (the server's own counters and the `fx/` objects), and must never give up silently.
S3 is moto (an offline imitation, $0); the server's counters are scripted poll by poll (no engine needed: the test is
about the decision, the counters' names are the ones integration-3's serverStatus has, mongo-server/src/diag.rs).
The control: the K2c check (git show origin/aws-ready:kitchen/settle.py, its `count`) says "calm" on the same pantry
while sealed runs are still being written, which is the reviewer's proof.
Usage: python -m pytest -q step9/local/tests/test_settle.py   (needs boto3 and moto)"""
import json
import os
import subprocess
import sys
import tempfile

import boto3
import pytest
from moto import mock_aws

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "kitchen"))
os.environ.update(AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing", AWS_DEFAULT_REGION="eu-west-1",
                  AWS_REGION="eu-west-1")
import settle  # noqa: E402

B = "fenvix-kitchen-pantry-1"
IDLE = {"compactor.running": 0, "compactor.publishes": 10, "compactor.absorbs": 4, "compactor.merges": 2,
        "compactor.reseals": 1, "compactor.errors": 0, "compaction.running": 0, "compaction.passes": 7,
        "sealedRuns.publishes": 10, "sealedRuns.publishErrors": 0, "derived.encoded": 0, "compactor.enabled": True}


@pytest.fixture
def s3():
    with mock_aws():
        c = boto3.client("s3", region_name="eu-west-1")
        c.create_bucket(Bucket=B, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
        c.put_object(Bucket=B, Key="tenants/app_data_1/compacted/0001.sst", Body=b"x" * 100)
        yield c


def run(s3, polls, most_min=0.05, least=0, on_poll=None):
    """Run settle.main() with scripted counters (one entry per poll; the last repeats) and step 0."""
    i = [0]

    def fake_counters(_client):
        if on_poll:
            on_poll(i[0])
        c = polls(i[0]) if callable(polls) else polls[min(i[0], len(polls) - 1)]
        i[0] += 1
        return (dict(c), None) if isinstance(c, dict) else (None, c)

    out = tempfile.mktemp(suffix=".json")
    old = settle.counters
    settle.counters = fake_counters
    sys.argv = ["settle.py", B, str(most_min), "0", out, str(least)]
    try:
        with pytest.raises(SystemExit) as e:
            settle.main()
    finally:
        settle.counters = old
    rec = json.load(open(out))
    os.remove(out)
    return e.value.code, rec


def test_calm_when_idle(s3):
    code, rec = run(s3, [IDLE])
    assert code == 0 and rec["calm"] and len(rec["polls"]) == 3


def test_fx_objects_still_written_is_not_calm_and_the_old_check_was_blind(s3):
    """Sealed runs keep arriving under fx/ (no .sst): not calm; the K2c count saw nothing move."""
    def write_run(n):
        s3.put_object(Bucket=B, Key=f"tenants/app_data_1/fx/run/{n:06d}", Body=b"r" * 1000)
    old_src = subprocess.run(["git", "-C", REPO, "show", "origin/aws-ready:kitchen/settle.py"], capture_output=True, text=True).stdout
    ns = {}
    exec(compile(old_src.replace('if __name__ == "__main__":', "if False:"), "old_settle", "exec"), ns)
    old_counts = []
    for n in range(4):
        write_run(n)
        old_counts.append(ns["count"](s3, B))
    assert len(set(old_counts)) == 1, old_counts   # the old check: three equal counts in a row = "calm"
    code, rec = run(s3, [IDLE], most_min=0.02, on_poll=lambda n: write_run(100 + n))
    assert code == 3 and not rec["calm"] and rec["reason"] == "pantry objects still changing", rec["reason"]


def test_compactor_running_is_not_calm(s3):
    code, rec = run(s3, [dict(IDLE, **{"compactor.running": 1})], most_min=0.02)
    assert code == 3 and rec["reason"].startswith("running: compactor.running 1")


def test_counters_moving_is_not_calm(s3):
    code, rec = run(s3, lambda k: dict(IDLE, **{"compactor.reseals": k}), most_min=0.02)
    assert code == 3 and rec["reason"] == "compaction counters still moving"


def test_calm_after_the_compactor_finishes(s3):
    polls = [dict(IDLE, **{"compactor.running": 1, "compactor.publishes": 9})] * 2 + [IDLE]
    code, rec = run(s3, polls)
    assert code == 0 and len(rec["polls"]) == 5   # 2 busy, then 3 idle and equal


def test_error_counters_growing_is_not_calm(s3):
    polls = [IDLE, dict(IDLE, **{"compactor.errors": 1})]
    code, rec = run(s3, polls, most_min=0.02)
    assert code == 3 and "error counters grew" in rec["reason"]


def test_no_counters_is_not_calm(s3):
    code, rec = run(s3, ["serverStatus has no fenvix.compactor / fenvix.compaction section"], most_min=0.02)
    assert code == 3 and rec["reason"].startswith("no server counters")


def test_minimum_wait(s3):
    code, rec = run(s3, [IDLE], most_min=0.01, least=3600)
    assert code == 3 and "minimum wait" in rec["reason"]


def test_no_silent_give_up_prints_stop(s3, capsys):
    run(s3, [dict(IDLE, **{"compaction.running": 2})], most_min=0.01)
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert out.startswith("SETTLE: NOT CALM") and out.endswith("STOP (no measure on an unsettled pantry)"), out


def test_counters_reads_the_engine_section():
    class C:
        class admin:  # noqa: N801
            @staticmethod
            def command(_):
                return {"fenvix": {"compactor": {"enabled": True, "running": 0, "publishes": 3, "absorbs": 1, "merges": 0,
                                                 "reseals": 0, "errors": 0},
                                   "compaction": {"passes": 5, "running": 0},
                                   "sealedRuns": {"publishes": 3, "publishErrors": 0}, "derived": {"encoded": 0}}}
    c, err = settle.counters(C)
    assert err is None and c["compactor.publishes"] == 3 and c["compaction.passes"] == 5 and c["compactor.enabled"]
    assert settle.counters(None)[0] is None
