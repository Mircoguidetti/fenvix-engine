"""Step 9 · rules on a machine's end file that make a job red (run.py imports these).
client_end_blocker: the client found that its plan is not the Atlas plan (kitchen/client.sh: compare_plans.py prints
"PLANS: DIFFERENT" and the client finishes PLANS_DIFFERENT; merge_states.py: STATES_DIFFERENT). Before (total review
intermediate 2), the load job ended green with that end file, and the next job would measure a plan that is not
Atlas's. Now the job stops red with that reason."""
import json

RED_CLIENT_OUTCOMES = ("PLANS_DIFFERENT", "STATES_DIFFERENT", "PLANS_FERMO")


def client_end_blocker(end):
    """The reason the job must be red, or None. `end` is client/end.json as a dict (or None when it is missing)."""
    if not end:
        return None
    out = str(end.get("outcome", ""))
    if out in RED_CLIENT_OUTCOMES or "PLANS: DIFFERENT" in json.dumps(end):
        return out or "PLANS_DIFFERENT"
    return None


# total review N5: the SPESA line counted machines, S3 and Aurora but no cross-zone traffic (about $1 in job M2).
# The allowance per program, from estimate.py's job plan (ESTIMATED, $0.01 out + $0.01 in per GB, DECLARED by AWS):
# write-peak 13.2 GB x 3 crossings = 0.79; log-ack and takeovers 0.20; m30 0.05; the agent benchmark 0.10; a load whose
# client is in another zone 26.6 GB x 0.02 = 0.53 (job L now runs with client_zone "same": 0).
XZ_ALLOWANCE = {"write-peak": 0.79, "log-ack": 0.10, "takeovers": 0.10, "m30": 0.05, "agent-bench": 0.10}
XZ_LOAD_OTHER_ZONE = 0.53


def cross_zone_allowance(cmd, elapsed_h):
    """The cross-zone dollars counted in SPESA so far: the job's allowance spread over its estimated hours, reaching the
    whole allowance at the estimated end (and staying there)."""
    total = sum(XZ_ALLOWANCE.get(p, 0.0) for p in cmd.get("programs", []) if p in XZ_ALLOWANCE and
                (p != "m30" or "measure" in cmd.get("phases", [])))
    if "load" in cmd.get("phases", []) and cmd.get("atlas_load") and cmd.get("client_zone", "other") != "same":
        total += XZ_LOAD_OTHER_ZONE
    est = float(cmd.get("job_hours_total") or 0) or 1.0
    return round(total * min(1.0, max(0.0, elapsed_h) / est), 4)
