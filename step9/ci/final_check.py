"""Step 9 · the final check (founder's rule 2026-11-04): last step of the job, always, after the teardown. Deletes
nothing, only looks: machines in any state but terminated, tagged disks and disks left unattached since the start of
the run, tagged Elastic IPs, buckets fenvix-coldread-*, tagged security groups (local mode: containers, networks,
MinIO buckets); with S9_AURORA=1 also Aurora clusters, RDS instances, DB subnet groups and cluster snapshots named
fenvix-coldread-s9-*. Nothing: prints "RIMASTO: niente" and exits 0. Something, or a read that fails: "ALLARME: rimasto ..."
with the list, an ::error:: annotation, exit 1 (a red job).
Usage: final_check.py MODE [START_ISO]"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import infra  # noqa: E402

summary = os.environ.get("GITHUB_STEP_SUMMARY")
try:
    left = infra.make(sys.argv[1]).leftovers(sys.argv[2] if len(sys.argv) > 2 else None)
    if sys.argv[1] == "aws" and os.environ.get("S9_AURORA") == "1":
        import aurora  # noqa: E402
        left += aurora.leftovers()
except Exception as e:
    left = [f"NON SO, read failed: {type(e).__name__}: {e}"]
if not left:
    msg = "RIMASTO: niente (final check: machines, disks, IP addresses, buckets, security groups or containers" + (", Aurora" if os.environ.get("S9_AURORA") == "1" else "") + ")"
    print(msg)
    if summary:
        open(summary, "a").write(msg + "\n")
    sys.exit(0)
one = "; ".join(left)
print(f"ALLARME: rimasto {one}")
print(f"::error title=ALLARME final check::left {one}")
if summary:
    open(summary, "a").write("# ALLARME: something is left\n\n" + "".join(f"- {x}\n" for x in left)
                             + '\nWhat to do: commit {"mode": "teardown"} in step9/command.json, then look at this check again.\n')
sys.exit(1)
