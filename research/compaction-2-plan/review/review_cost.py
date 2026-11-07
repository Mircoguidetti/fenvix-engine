#!/usr/bin/env python3
"""Review of the compaction 2 plan, questions 3, 4 and 7 (ESTIMATED unless labelled). Imports the plan's plan_cost.py
(which imports the reference cost.py) unchanged. Run from anywhere: python3 -I review_cost.py <fenvix-brain checkout>.
1. Reseal period of a group = theta * D / inserted-per-day: how long a customer's rows stay in the delta tier.
2. Delta runs a cold read may touch: up to (T - 1) runs per delta tier, plus up to 4 L0 SSTs.
3. Today's tiered compaction (compaction-1, T = 4, whole keyspace) for a fair amplification baseline: 1 + ceil(log4(D / 8 MiB)).
4. The deletion trigger as written in section 4 ("a delete envelope older than 24 hours" rewrites the group): when deletes
   reach every group every day, the sealed tier rewrites D per day.
"""
import importlib.util, io, contextlib, math, os, sys

brain = sys.argv[1] if len(sys.argv) > 1 else "/home/user/fenvix-brain"
src = os.path.join(brain, "sessions/2026-11-07-compaction-2-plan/plan_cost.py")
spec = importlib.util.spec_from_file_location("plan", src)
p = importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()):
    spec.loader.exec_module(p)
m = p.m
SEC = p.SEC


def tiered_today(gb):
    return 1 + math.ceil(math.log(gb / p.L0_GB, 4))


def rewrite_cost(gb_month, cpu=12.0):
    put = gb_month * p.PUT_PER_GB * m.S3_PUT
    get = gb_month * p.GET_PER_GB * m.S3_GET
    core_h = gb_month * cpu / 3600
    spot = core_h * m.EC2[m.LAV_NODE][1] / m.EC2[m.LAV_NODE][2] * m.LAV_OVERHEAD
    return put + get + spot, core_h


out = []
for gb, label in ((1024, "1 TB"), (10240, "10 TB")):
    for pname in ("medium", "busy"):
        prof = m.PROFILES[pname]
        _, info = m.variable(gb, prof)
        ins_day = info["ins_gb"] / 30.4
        for theta in (0.25, 0.10):
            a = p.ours(gb, prof, theta, 12.0)
            period = theta * gb / ins_day
            nd = a["nd"]
            out.append(f"{label} {pname} theta {theta:.2f}: inserted {ins_day:.1f} GB/day; reseal period {period:.1f} days; "
                       f"delta tiers {nd}, delta runs at most {3 * nd} + 4 L0; plan amplification {a['amp']:.0f}, "
                       f"today's tiered {tiered_today(gb)}, reference mid {info['amp'][1]:.1f}")
        # deletion trigger: every group rewritten once a day
        extra_gb_month = gb * 30.4
        cost, core_h = rewrite_cost(extra_gb_month)
        plan_mid = p.ours(gb, prof, 0.25, 12.0)
        plan_cost, _ = rewrite_cost(plan_mid["rewritten"])
        out.append(f"{label} {pname} deletion trigger on every group daily: +{extra_gb_month:,.0f} GB/month rewritten "
                   f"(+{gb / ins_day:.0f} amplification, {gb * 1e3 / 86400:.0f} MB/s average), {core_h:,.0f} core-h, "
                   f"{cost:,.2f} $/month against the plan's {plan_cost:,.2f} $")
text = "\n".join(out)
print(text)
open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "review_cost.txt"), "w").write(text + "\n")
