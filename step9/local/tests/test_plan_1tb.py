"""2026-11-13 (coordinator: the 1 TB plan): the commands of the 1 TB part of the round (08 G1, 09 B1, 10 W1), the two
programs that write into the measured database (first-next-ops, reads-after-writes) in decide.py, and their place in
extras.sh. $0, nothing touches AWS."""
import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
S9 = os.path.abspath(os.path.join(HERE, "..", ".."))
CI, CMDS = os.path.join(S9, "ci"), os.path.join(S9, "commands")
sys.path.insert(0, CI)


def decide(name=None, **over):
    base = json.load(open(os.path.join(CMDS, name))) if name else {}
    base.update(over)
    d = tempfile.mkdtemp()
    f, out = os.path.join(d, "c.json"), os.path.join(d, "r.json")
    json.dump(base, open(f, "w"))
    env = dict(os.environ, S9_MODE="local", RUN_ID="1000000001")
    env.pop("ENGINE_COMMIT", None)
    r = subprocess.run([sys.executable, "decide.py", f, out, "8"], cwd=CI, capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr, (json.load(open(out)) if r.returncode == 0 and os.path.exists(out) else None)


def test_g1_generates_one_database_of_1_tb_on_the_kept_pantry_resumable_across_jobs():
    rc, out, r = decide("08-G1-generate-1tb.json")
    assert r is not None, out
    assert r["ballast_db"] == "app_data_b01" and r["ballast_gib"] == 931.4 and r["generator"] == "c7i.2xlarge"
    assert r["pantry_mode"] == "existing" and r["atlas_load"] is False       # the Atlas databases are already there
    assert r["vcpu_peak"] == 8 and len(r["ballast"]["lines"]) == 1
    assert r["generator_jobs_estimated"] in (1, 2) and 3.0 < r["generator_hours_estimated"] < 6.5
    # the same job on a NEW pantry could not resume: refused
    rc, out, r = decide("08-G1-generate-1tb.json", pantry="new", phases=["load", "measure"], programs=["m30"])
    assert rc == 1 and "REFUSED" in out


def test_b1_reads_only_and_w1_writes_last_no_spare_within_8_vcpu_and_the_ceiling():
    rc, out, b = decide("09-B1-read-rounds-1tb.json")
    assert b is not None, out
    assert b["programs"] == ["first-next", "ballast-rounds"] and b["spare"] is None and b["vcpu_peak"] <= 8
    rc, out, w = decide("10-W1-first-next-ops-reads-after-writes-1tb.json")
    assert w is not None, out
    assert w["programs"] == ["first-next-ops", "reads-after-writes"] and w["spare"] is None and w["vcpu_peak"] <= 8
    assert w["ballast_db"] == "app_data_b01" and w["job_hours"]["reads_after_writes"] == 1.5
    assert w["job_hours_total"] <= 0.9 * 5.5 and b["job_hours_total"] <= 0.9 * 5.5
    for r in (b, w):                                                          # both read the 1 TB database of the command
        assert r["data"] == "1tb" and r["ballast_gib"] == 931.4 and r["ballast"]["lines"] == []


def test_the_programs_that_write_are_never_in_the_default_list():
    import decide as d
    assert set(d.WRITING_PROGRAMS) == {"first-next-ops", "reads-after-writes"}
    assert not set(d.WRITING_PROGRAMS) & set(d.DEFAULTS["programs"])
    assert set(d.WRITING_PROGRAMS) <= set(d.PROGRAMS) and set(d.WRITING_PROGRAMS) <= set(d.EXTRA_PROGRAMS)
    # a command that names no programs gets none of them
    rc, out, r = decide(mode="permissions")
    assert r is not None and r["programs"] and not set(d.WRITING_PROGRAMS) & set(r["programs"]), out


def test_the_pilot_runs_first_next_ops_read_only_and_fits_the_ceiling():
    rc, out, r = decide("pilot-P-small.json")
    assert r is not None, out
    assert "first-next-ops" in r["programs"] and "--no-save" in r["first_next_ops_args"]
    assert "reads-after-writes" not in r["programs"]         # the pilot's pantry is read as loaded by job M1 afterwards
    assert r["job_hours"]["first_next_ops"] < 0.1 and r["job_hours_total"] <= 0.9 * 5.5


def test_extras_runs_the_writing_programs_after_the_scripts_read_rounds():
    sh = open(os.path.join(S9, "machine", "extras.sh")).read()
    pos = {k: re.search(rf"if has {k}( && reads_as_loaded_ok {k})?; then", sh).start() for k in ("first-next", "ballast-rounds", "first-next-ops", "reads-after-writes", "takeovers")}
    assert pos["first-next"] < pos["ballast-rounds"] < pos["first-next-ops"] < pos["reads-after-writes"] < pos["takeovers"]
    # every program named in extras.sh's calls exists, and every argument field it reads is a decide.py default
    import decide as d
    for prog in re.findall(r"\$PY (\w+\.py)", sh):
        assert os.path.exists(os.path.join(S9, "programs", prog)) or os.path.exists(os.path.join(S9, "machine", prog)) or \
            os.path.exists(os.path.join(S9, "..", "kitchen", prog)) or os.path.exists(os.path.join(S9, "..", "kitchen", "irlanda", prog)), prog
    for field in re.findall(r"cfg (\w+_args)", sh):
        assert field in d.DEFAULTS, field
    # "has first-next" must not match "first-next-ops" (the test is on the quoted name)
    assert 'grep -q "\\"$1\\""' in sh


def test_the_arguments_extras_passes_are_accepted_by_the_programs():
    sys.path.insert(0, os.path.join(S9, "programs"))
    import decide as d
    import first_next_ops as ops
    import pymongo
    import reads_after_writes as raw

    def no_server(*a, **k):                     # past the argument parser, stop at once (no server here)
        raise RuntimeError("no server in this test")
    real, pymongo.MongoClient = pymongo.MongoClient, no_server
    try:
        _check_arguments(d, ops, raw)
    finally:
        pymongo.MongoClient = real


def _check_arguments(d, ops, raw):
    for mod, field, extra in ((ops, "first_next_ops_args", ["--states", "s", "--db", "app_data_b01", "--label", "after-writes"]),
                              (raw, "reads_after_writes_args", ["--states", "s", "--db", "app_data_b01"])):
        argv = ["out", "--uri", "mongodb://x"] + extra + d.DEFAULTS[field].split()
        try:
            mod.main(argv)
        except SystemExit as e:                 # argparse refuses an unknown argument with exit code 2
            assert e.code != 2, (field, argv)
        except Exception:                       # noqa: BLE001 - past the arguments (no server here): fine
            pass
    pilot = json.load(open(os.path.join(CMDS, "pilot-P-small.json")))["first_next_ops_args"].split()
    try:
        ops.main(["out", "--uri", "mongodb://x", "--states", "s"] + pilot)
    except SystemExit as e:
        assert e.code != 2
    except Exception:                           # noqa: BLE001
        pass


def test_the_estimate_prints_the_1_tb_plan_with_both_ends_of_the_generator():
    r = subprocess.run([sys.executable, "estimate.py"], cwd=CI, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    t = r.stdout[r.stdout.index("THE 1 TB ADDITION"):]
    assert "G1, generator" in t and "in 1 job(s)" in t and "in 2 job(s)" in t and "B1, read rounds" in t and "W1, first-next-ops" in t
    m = re.search(r"THE ROUND WITH 1 TB, ESTIMATED: ([0-9.]+) to ([0-9.]+) \$", t)
    assert m and 10 < float(m.group(1)) < float(m.group(2)) < 20


if __name__ == "__main__":
    for k, v in list(globals().items()):
        if k.startswith("test_"):
            v()
    print("ALL PASS")
