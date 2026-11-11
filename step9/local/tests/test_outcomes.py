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
