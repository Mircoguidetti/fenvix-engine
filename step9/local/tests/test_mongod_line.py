"""aws-local-round (2026-11-13): the MongoDB line (phase "mongod"): 3 mongod 7.0 members in zones a, b, c and a client
that loads through the founder's script, restarts the three cold and runs first-next-ops and reads-after-writes as on our
cook. The real run is the local docker round (local-run.sh JOBS=mongod); these tests hold the checks and the wiring."""
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(S9, "ci"))
import decide  # noqa: E402

MG = json.load(open(os.path.join(S9, "commands", "11-MG-mongod-line-atlas.json")))


def resolve(raw, mode="local"):
    os.environ["RUN_ID"] = "1000000001"
    os.environ["S9_MODE"] = mode
    return decide.resolve(dict(raw))


def refused(raw, capsys):
    with pytest.raises(SystemExit):
        resolve(raw)
    return capsys.readouterr().out


def test_the_mg_command_is_accepted_at_8_vcpu_and_within_its_ceiling():
    c = resolve(MG)
    assert c["vcpu_peaks"] == {"mongod": 8} and c["vcpu_peak"] == 8
    assert c["mongod_ballast_line"] == [] and c["mongod_disk_gb"] >= 40
    assert c["job_hours_total"] <= decide.CEILING_USE * 330 / 60
    assert set(c["job_hours"]) == {"boot", "mongod_load", "mongod_cold_restart", "first_next_ops", "reads_after_writes"}


def test_the_mongod_phase_runs_alone_with_the_two_programs_and_no_pantry(capsys):
    assert "alone" in refused({**MG, "phases": ["mongod", "measure"]}, capsys)
    assert "first-next-ops" in refused({**MG, "programs": ["m30"]}, capsys)
    no_programs = {k: v for k, v in MG.items() if k != "programs"}
    assert "programs" in refused(no_programs, capsys)
    assert "pantry new" in refused({**MG, "pantry": "keep"}, capsys)
    assert "mongod_states" in refused({**MG, "mongod_states": ["nope"]}, capsys)
    assert "190 hours" in refused({**MG, "mongod_ballast_gib": 931.4}, capsys)        # 1 TB: refused, with the reason


def test_the_member_type_is_checked():
    from machine_types import ROLES
    assert ROLES["mongod"] == ["m7i.large"]          # an M30 node's size, the cook's type


def test_a_ballast_through_the_script_is_decides_own_line():
    c = resolve({**MG, "mongod_ballast_gib": 1, "ceiling_min": 340})
    assert c["mongod_ballast_line"][:5] == ["app_data_b01", "--gb", "1", "--seme", "2001"]
    assert c["mongod_fn_db"] == "app_data_b01"


def test_run_py_launches_three_members_in_three_zones_and_the_client_and_stops_them():
    run = open(os.path.join(S9, "ci", "run.py")).read()
    assert 'launch(f"mongo-{i}", "mongod", cmd["mongod"], infra.ZONES[i]' in run
    assert 'launch("mongoclient", "mongoclient", cmd["extras_client"], CLIENT_ZONE)' in run
    assert 'done="MONGOD_LINE_DONE"' in run and "stop(name, mid, wait_s=120)" in run
    node = open(os.path.join(S9, "machine", "node.sh")).read()
    assert "mongod)" in node and "mongoclient)" in node
    pk = open(os.path.join(S9, "ci", "packages.sh")).read()
    assert "mongod.sh" in pk and "mongo_client.sh" in pk and "MONGOD_TGZ_URL" in pk
    wf = open(os.path.join(S9, "..", ".github", "workflows", "step9-kitchen.yml")).read()
    assert "mongodb-linux-x86_64-ubuntu2204-7.0.48.tgz" in wf


def test_the_member_and_client_scripts_parse_and_keep_the_durability_of_atlas():
    for f in ("mongod.sh", "mongo_client.sh"):
        assert subprocess.run(["bash", "-n", os.path.join(S9, "machine", f)]).returncode == 0
    m = open(os.path.join(S9, "machine", "mongod.sh")).read()
    assert "--replSet rsm" in m and "--tlsMode requireTLS" in m and "--keyFile" in m and '"priority": 2' in m
    assert "drop" in open(os.path.join(S9, "machine", "common.sh")).read()          # serve_cold_restart drops the cache
    c = open(os.path.join(S9, "machine", "mongo_client.sh")).read()
    assert "--counters mongod" in c and "load.py" in c and "restart-request" in c
    # the same request labels as extras.sh, so the customers are the same as on our cook
    assert c.count("--label before-writes") == 1 and c.count("--label after-writes") == 1


def test_programs_blocker_takes_the_mongod_lines_outcome():
    import outcomes
    end = {"outcome": "MONGOD_LINE_DONE", "programs": "first-next-ops:rc=0 reads-after-writes:rc=0 first-next-ops-after-writes:rc=0"}
    assert outcomes.programs_blocker("mongoclient", end, ["first-next-ops", "reads-after-writes"], done="MONGOD_LINE_DONE") is None
    assert outcomes.programs_blocker("mongoclient", {**end, "outcome": "LOAD_FAILED"}, ["first-next-ops"], done="MONGOD_LINE_DONE")


def test_the_estimate_prints_the_line_and_the_1_tb_numbers(capsys):
    import estimate
    cost, h = estimate.mongod_line()
    out = capsys.readouterr().out
    assert 1.0 < cost < 5.0 and h < 5.0
    assert "mongod at 1 TB, Atlas's best" in out and "keeps no machine or disk between jobs" in out
