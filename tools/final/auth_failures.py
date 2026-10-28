#!/usr/bin/env python3
"""Classify the failures of the ctl-tests run with --auth (ctlauth_*.txt): how many are the `tenantAdmin` command being refused to root
(the engine refuses commands it has no authorization entry for, and tenantAdmin has none) and how many are anything else."""
import glob, os, re, sys
out = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "mongo-tests", "out", "final")
tot_t = tot_o = 0
for f in sorted(glob.glob(os.path.join(out, "ctlauth_*.txt"))):
    s = open(f).read()
    failed = re.findall(r"^FAILED (\S+)", s, re.M)
    ta = min(len(failed), len(re.findall(r"^E +\S*OperationFailure: not authorized on \S+ to execute command \{ tenantAdmin", s, re.M)))
    # a failure inside a helper thread (test_03c) only shows up in the warnings summary, not as an E line
    if len(failed) == 1 and ta == 0 and re.search(r"not authorized on \S+ to execute command \{ tenantAdmin", s):
        ta = 1
    other = len(failed) - ta
    tot_t += ta; tot_o += other
    print(f"{os.path.basename(f)[8:-4]:36s} failed={len(failed):3d} tenantAdmin_refused={ta:3d} other={other:3d}")
print(f"TOTAL tenantAdmin_refused={tot_t} other={tot_o}")
sys.exit(0)
