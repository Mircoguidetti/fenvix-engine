"""generator-1tb (2026-11-12): the ballast is ONE database of the size the command asks (ballast_gib), not several of a
fixed 24.3 GiB; a generation longer than one job is accepted only on a kept pantry, which the next job resumes. $0."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CI = os.path.abspath(os.path.join(HERE, "..", "..", "ci"))
CMDS = os.path.abspath(os.path.join(HERE, "..", "..", "commands"))
sys.path.insert(0, CI)


def decide(**over):
    base = json.load(open(os.path.join(CMDS, "05-G-generate-100gb.json")))
    base.update(over)
    d = tempfile.mkdtemp()
    f, out = os.path.join(d, "c.json"), os.path.join(d, "r.json")
    json.dump(base, open(f, "w"))
    env = dict(os.environ, S9_MODE="local", RUN_ID="1000000001")
    r = subprocess.run([sys.executable, "decide.py", f, out, "8"], cwd=CI, capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, (json.load(open(out)) if os.path.exists(out) else None)


def test_one_database_of_the_asked_size():
    rc, out, r = decide(ballast_gib=130)
    assert r is not None, out
    assert r["ballast_gib"] == 130.0 and r["ballast_db"] == "app_data_b01"
    lines = r["ballast"]["lines"]
    assert len(lines) == 1 and lines[0][0] == "app_data_b01" and lines[0][lines[0].index("--gb") + 1] == "130"
    assert r["ballast"]["parallel"] == 8 and r["generator"] == "c7i.2xlarge"


def test_defaults_per_data():
    for data, gib in (("100gb", 93.2), ("1tb", 931.4)):
        rc, out, r = decide(data=data, pantry="keep")
        assert r is not None, out
        assert r["ballast_gib"] == gib and len(r["ballast"]["lines"]) == 1


def test_size_bounds():
    for bad in (0.5, 2000, "x"):
        rc, out, r = decide(ballast_gib=bad)
        assert rc == 1 and "ballast_gib" in out, out


def test_a_generation_longer_than_a_job_needs_a_kept_pantry():
    rc, out, r = decide(data="1tb", pantry="new")
    assert rc == 1 and "kept pantry" in out, out
    rc, out, r = decide(data="1tb", pantry="fenvix-kitchen-pantry-1000000001", ballast_gib=1024)
    assert r is not None, out
    # only the generator in this job: it runs until the job's ceiling, the next job resumes it
    assert r["generator_jobs_estimated"] >= 2 and r["job_hours"]["generator"] <= 0.9 * 330 / 60


def test_the_measure_jobs_name_the_ballast_database():
    rc, out, r = decide(data="100gb", phases=["measure", "extras"], programs=["ballast-rounds"], pantry="fenvix-kitchen-pantry-1000000001")
    assert r is not None, out
    assert r["ballast_db"] == "app_data_b01" and r["ballast_gib"] == 93.2


def test_generator_hours_scale_with_the_size_and_the_writers():
    import estimate
    h130 = estimate.generator_hours(130, "c7i.2xlarge")
    assert estimate.generator_hours(1024, "c7i.2xlarge") > 7 * h130
    assert estimate.generator_hours(130, "c7i.xlarge") > 1.9 * h130
