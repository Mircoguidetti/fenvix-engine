#!/usr/bin/env python3
"""Short CI: every negative control must be caught. Reads the simulator's `controls` output and the lease_mc lines.
Exit 1 with the list of what was not caught."""
import json, sys

controls = json.load(open(sys.argv[1]))
missing = []
protocol = ["no_fencing", "ack_gap", "keep_diverged", "forget_fence", "ack_on_one", "recovery_longest", "recovery_single",
            "trim_before_upload", "ack_before_fsync", "ignore_fsync_error", "skip_store_fence", "wipe_same_id"]
for m in protocol + ["lease_holder_only", "lease_wall_clock"]:
    x = [c for c in controls if c["mutation"] == m and c["checker"] == "all invariants"]
    if not x or not x[0]["caught"]:
        missing.append(f"simulator: {m}")
# lease_no_margin is caught by the targeted simulator run and the exhaustive lease check (below)
focus = json.load(open(sys.argv[2]))["summary"]
if focus["seeds_failed"] == 0:
    missing.append("simulator (lease focus): lease_no_margin")
correct_focus = json.load(open(sys.argv[3]))["summary"]
if correct_focus["seeds_failed"] != 0:
    missing.append("simulator (lease focus): the CORRECT rule failed")
for line in open(sys.argv[4]):
    r = json.loads(line)
    ok = r["verdict"].startswith("complete") if r["bug"] == "none" else r["verdict"].startswith("OVERLAP")
    if not ok:
        missing.append(f"lease_mc {r['bug']}: {r['verdict']}")
print("\n".join(missing) if missing else "every negative control caught; correct versions clean")
sys.exit(1 if missing else 0)
