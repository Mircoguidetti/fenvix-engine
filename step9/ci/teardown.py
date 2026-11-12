"""Step 9 · teardown (always runs, after the round or on its own with mode "teardown"): terminates every machine
tagged project=fenvix-coldread, deletes every bucket fenvix-coldread-* with its content and the tagged security groups
(local mode: the labelled containers and networks, the MinIO buckets); with S9_AURORA=1 also the Aurora cluster,
its writer and its subnet group (ci/aurora.py). Then the final check (final_check.py) looks.
Note: the tag and the bucket prefix are those of M31, Ireland and the minimal kitchen, whose IAM policies the founder
already has; those workflows share the concurrency group fenvix-m31 with this one, so they never run at the same time.
Usage: teardown.py MODE   (MODE aws or local)"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import infra  # noqa: E402

infra.make(sys.argv[1]).cleanup()
if sys.argv[1] == "aws" and os.environ.get("S9_AURORA") == "1":
    # the Aurora cluster of the agent benchmark (only in rounds that asked for it: the RDS policy is optional)
    import aurora  # noqa: E402
    print(f"teardown: Aurora clusters of runs {aurora.teardown() or 'none'}")
print("teardown: done")
