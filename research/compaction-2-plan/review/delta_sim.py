#!/usr/bin/env python3
"""Review of the compaction 2 plan, question 4: GETs of a cold read for a customer who wrote recently (ESTIMATED, a model
of the plan's section 4, not of code). Delta tier: absorbed in 32 MiB units (4 L0 SSTs of 8 MiB), tiered with T = 4 (4 runs
of one tier merge into one run of the next tier). The tracked customer's group is resealed every theta * D / inserted-per-day
days (uniform phase); a reseal makes the customer's rows at or below that time dead (X_g), and a merge drops dead rows.
A cold read pays 1 GET for the sealed run plus 1 GET per delta run holding a live row of the customer (lower bound: the
run's customer filter and sequence range let the read skip runs holding only dead rows) or holding any row of the customer
(upper bound: the filter says yes and the run's sequence range overlaps (X_g, X_d], which is true for any run newer than
the last reseal). L0 SSTs not yet absorbed are assumed cached (0 GETs). Run: python3 -I delta_sim.py"""
import math, os, random

T = 4
UNIT_GB = 32 / 1024
PROFILES = {"medium": 1165 / 30.4, "busy": 5825 / 30.4}   # inserted GB/day, from plan_cost.txt
WRITERS = {"once, 0-3 days ago": None, "weekly": 7.0, "daily": 1.0, "hourly": 1 / 24}


def run(data_gb, ins_day, theta, every, seed=1):
    rnd = random.Random(seed)
    period = theta * data_gb / ins_day
    steps_day = ins_day / UNIT_GB
    dt = 1 / steps_day
    horizon = max(4 * period, 30.0)
    phase = rnd.random() * period
    xg = 0.0
    tiers = []            # tiers[k] = list of runs; a run = list of the customer's write times
    pending = []          # writes not yet absorbed
    next_write = rnd.random() * (every or 1)
    if every is None:
        next_write = horizon - 3.0
    samples_lo, samples_hi = [], []
    t = 0.0
    next_sample = horizon * 0.5
    while t < horizon:
        t += dt
        if t >= next_write:
            pending.append(next_write)
            next_write = next_write + every if every else float("inf")
        if t >= phase:                      # reseal of the customer's group
            xg = t
            phase += period
        run_rows = pending
        pending = []
        k = 0
        carry = run_rows
        while True:
            if len(tiers) <= k:
                tiers.append([])
            tiers[k].append(carry)
            if len(tiers[k]) < T:
                break
            merged = [w for r in tiers[k] for w in r if w > xg]
            tiers[k] = []
            carry = merged
            k += 1
        if t >= next_sample and (every is not None or t >= horizon - 3.0 + 0.01):
            runs = [r for tier in tiers for r in tier]
            lo = sum(1 for r in runs if any(w > xg for w in r))
            hi = sum(1 for r in runs if r)
            samples_lo.append(1 + lo)
            samples_hi.append(1 + hi)
            next_sample = t + rnd.random() * 0.5
    if not samples_lo:
        return 1.0, 1.0
    return sum(samples_lo) / len(samples_lo), sum(samples_hi) / len(samples_hi)


out = []
for gb, label in ((1024, "1 TB"), (10240, "10 TB")):
    for pname, ins in PROFILES.items():
        parts = []
        for wname, every in WRITERS.items():
            lo, hi = zip(*(run(gb, ins, 0.25, every, seed=s) for s in range(3)))
            parts.append(f"{wname} {sum(lo)/3:.1f}-{sum(hi)/3:.1f}")
        out.append(f"{label} {pname}, theta 0.25 (reseal every {0.25 * gb / ins:.1f} days): GETs per cold read: " + "; ".join(parts))
text = "\n".join(out)
print(text)
open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "delta_sim.txt"), "w").write(text + "\n")
