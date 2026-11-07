#!/usr/bin/env python3
"""Review round 2 of the compaction 2 plan: two numbers the round-2 answer leaves out. ESTIMATED: a model, not code.

Imports the plan's plan_cost.py unchanged (which imports the reference cost.py unchanged). Run from anywhere:
    python3 -I review_r2.py <fenvix-brain checkout>        (default /home/user/fenvix-brain)
Output: review_r2.txt next to this file.

1. The fold's own rewrite (fix 2). Every drop job rewrites SlateDB's newest sorted run: the absorbed L0 SSTs plus every
   row still above the floor. A row stays above the floor for one lease after its version is published (longer while a
   pin holds the floor down), so each fold until then rewrites it: about 1 + lease / (time between folds) writes per
   inserted byte, on top of the plan's 1 + delta tiers + sealed. Probe 4 FOLD measures the same mechanism locally.
   Time between folds = one absorption unit (4 L0 SSTs of 8 MiB) / bytes inserted per second.
2. How long an ordinary delete stays in a delta run (fix 5). A reseal does not rewrite delta runs; a dead row leaves a
   delta run only when that run's tier merges. Tier k of the delta (tiered, T = 4, 32 MiB units) merges every
   4^(k+1) units; the bottom tier is the plan's delta-tier count minus one.
"""
import contextlib, importlib.util, io, os, sys

brain = sys.argv[1] if len(sys.argv) > 1 else "/home/user/fenvix-brain"
spec = importlib.util.spec_from_file_location("plan", os.path.join(brain, "sessions/2026-11-07-compaction-2-plan/plan_cost.py"))
p = importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()):
    spec.loader.exec_module(p)
m = p.m

UNIT_GB = 32 / 1024
T = 4
SIZES = ((1024, "1 TB"), (10240, "10 TB"))
LEASES = (60, 10)        # the plan's T_pin 30 s + margin 30 s; and T_pin 5 s + margin 5 s
PIN_LIFE = 120


def money(x):
    return f"{x:,.2f} $"


def main():
    out = []
    say = out.append
    say("== 1. Rewrites by the fold into SlateDB's newest sorted run (ESTIMATED) ==")
    for gb, label in SIZES:
        for pname in ("medium", "busy"):
            prof = m.PROFILES[pname]
            plan = p.ours(gb, prof, 0.25, 12.0)
            ins_month = plan["ins"]
            ins_s = ins_month / 30.4 / 86400
            dt = UNIT_GB / ins_s
            per_gb = (plan["put"] + plan["get"] + plan["spot"]) / plan["rewritten"]
            cores = plan["core_h"] / (30.4 * 24)
            parts = []
            for lease in LEASES + (PIN_LIFE + 60,):
                extra = 1 + lease / dt
                gb_m = ins_month * extra
                add_cores = gb_m * 12.0 / 3600 / (30.4 * 24)
                tag = f"lease {lease} s" if lease != PIN_LIFE + 60 else "a 120 s pin held all the time"
                parts.append(f"{tag}: +{extra:.1f} (amplification {plan['amp']:.0f} -> {plan['amp'] + extra:.0f}), +{money(gb_m * per_gb)} a month, "
                             f"+{add_cores:.2f} cores ({cores:.2f} -> {cores + add_cores:.2f}, {4 * 0.7 / (cores + add_cores):.0f} databases per c8g.xlarge at 70%)")
            say(f"{label} {pname}: inserted {ins_month / 30.4:.1f} GB/day, one fold every {dt:.0f} s; " + "; ".join(parts))
    say("")
    say("== 2. Time an ordinary delete's dead row can stay in the bottom delta tier (ESTIMATED) ==")
    for gb, label in SIZES:
        for pname in ("medium", "busy"):
            prof = m.PROFILES[pname]
            plan = p.ours(gb, prof, 0.25, 12.0)
            ins_day = plan["ins"] / 30.4
            unit_days = UNIT_GB / ins_day
            nd = plan["nd"]
            bottom = nd - 1
            merge_days = T ** (bottom + 1) * unit_days
            reseal = 0.25 * gb / ins_day
            say(f"{label} {pname}: reseal period {reseal:.1f} days; delta tiers {nd}; the bottom tier (k = {bottom}) merges every {merge_days:.0f} days, "
                f"the tier above every {merge_days / T:.1f} days: a dead row that reached the bottom tier stays up to about {merge_days:.0f} days after its group's reseal")
    text = "\n".join(out)
    print(text)
    open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "review_r2.txt"), "w").write(text + "\n")


if __name__ == "__main__":
    main()
