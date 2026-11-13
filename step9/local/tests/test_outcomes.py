"""Step 9 · total review intermediate 2: the load job ended green when client/end.json said PLANS: DIFFERENT. Now
run.py stops red with that reason (ci/outcomes.py). $0. Usage: python3 step9/local/tests/test_outcomes.py"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CI = os.path.join(HERE, "..", "..", "ci")
sys.path.insert(0, CI)
import outcomes  # noqa: E402


def test_plans_different_is_red():
    assert outcomes.client_end_blocker({"finished": True, "outcome": "PLANS_DIFFERENT"}) == "PLANS_DIFFERENT"
    assert outcomes.client_end_blocker({"outcome": "STATES_DIFFERENT"}) == "STATES_DIFFERENT"
    assert outcomes.client_end_blocker({"outcome": "X", "last": "PLANS: DIFFERENT"}) == "X"


def test_a_good_or_missing_end_is_not_red():
    assert outcomes.client_end_blocker({"finished": True, "outcome": "STOPPED"}) is None
    assert outcomes.client_end_blocker(None) is None


def test_run_py_stops_on_it():
    src = open(os.path.join(CI, "run.py")).read()
    i = src.index('outcomes["client"] = end_client.get("outcome", "NO END")')
    assert "outcomes_rules.client_end_blocker(end_client)" in src[i:i + 600] and "raise Stop(red)" in src[i:i + 700]



def test_cross_zone_allowance_in_the_cost_guard():
    """N5: SPESA now counts the cross-zone traffic it missed (about $1 in job M2), spread over the job's estimate."""
    m2 = {"phases": ["measure", "extras"], "programs": ["s3-first-byte", "sealed-run", "log-ack", "takeovers", "write-peak"],
          "job_hours_total": 2.7}
    assert outcomes.cross_zone_allowance(m2, 0) == 0
    assert abs(outcomes.cross_zone_allowance(m2, 2.7) - 0.99) < 1e-9
    assert outcomes.cross_zone_allowance(m2, 10) == outcomes.cross_zone_allowance(m2, 2.7)
    load_other = {"phases": ["load"], "programs": [], "atlas_load": True, "client_zone": "other", "job_hours_total": 3.35}
    load_same = dict(load_other, client_zone="same")
    assert outcomes.cross_zone_allowance(load_other, 3.35) == 0.53 and outcomes.cross_zone_allowance(load_same, 3.35) == 0


def test_run_py_counts_it():
    src = open(os.path.join(CI, "run.py")).read()
    assert "spend_now = mach + s3d + aud + xz" in src and "outcomes_rules.cross_zone_allowance(cmd" in src


if __name__ == "__main__":
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        fn()
        print(f"PASS {name}")
    print("ALL PASS")


# total review R6 (2026-11-12): every job is red when one of its programs fails, is skipped or does not run
def test_programs_all_zero_is_not_red():
    end = {"finished": True, "outcome": "EXTRAS_DONE", "programs": "s3-first-byte:rc=0 log-ack:rc=0 takeovers:rc=0 takeovers-logoff:rc=0"}
    assert outcomes.programs_blocker("extras", end, ["s3-first-byte", "log-ack", "takeovers"]) is None


def test_a_failed_program_is_red():
    end = {"finished": True, "outcome": "EXTRAS_DONE", "programs": "s3-first-byte:rc=0 takeovers:rc=1"}
    assert outcomes.programs_blocker("extras", end, ["s3-first-byte", "takeovers"]) == "extras: failed: takeovers rc=1"


def test_a_skipped_or_missing_program_is_red():
    end = {"finished": True, "outcome": "EXTRAS_DONE", "programs": "ballast-rounds:SKIPPED"}
    assert outcomes.programs_blocker("extras", end, ["ballast-rounds"]) == "extras: failed: ballast-rounds SKIPPED"
    end = {"finished": True, "outcome": "EXTRAS_DONE", "programs": "s3-first-byte:rc=0"}
    assert outcomes.programs_blocker("extras", end, ["s3-first-byte", "log-ack"]) == "extras: did not run: log-ack"


def test_no_end_or_an_early_end_is_red():
    assert outcomes.programs_blocker("agent-bench", None, ["agent-bench"]) == "agent-bench: no end file"
    assert outcomes.programs_blocker("write-peak", {"finished": True, "outcome": "COOK_UNREACHABLE", "programs": ""}, ["write-peak"]) == "write-peak: COOK_UNREACHABLE"


def test_run_py_turns_the_job_red_and_takeovers_exits_non_zero_on_fail():
    src = open(os.path.join(CI, "run.py")).read()
    for name in ('"extras", end_ex, extras', '"write-peak", end_pk, ["write-peak"]', '"agent-bench", end_ag, ["agent-bench"]'):
        assert f"red_programs.append(outcomes_rules.programs_blocker({name}))" in src, name
    i = src.index('outcome = "FINITO"\n    # total review R6')
    assert 'outcome = "PROGRAM_FAILED: "' in src[i:i + 400]
    assert src.rstrip().endswith('sys.exit(0 if outcome == "FINITO" else 1)')
    tk = open(os.path.join(HERE, "..", "..", "programs", "takeovers.py")).read()
    assert tk.rstrip().endswith("sys.exit(0 if passed else 1)")
    ex = open(os.path.join(HERE, "..", "..", "machine", "extras.sh")).read()
    assert 'outcomes+=("$name:rc=$rc")' in ex and "finish EXTRAS_DONE" in ex
