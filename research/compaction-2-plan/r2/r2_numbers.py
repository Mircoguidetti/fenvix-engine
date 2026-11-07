#!/usr/bin/env python3
"""Compaction 2 plan, round 2 (fenvix-brain prompts/P-compaction-2-plan-fix.md): the corrected numbers. ESTIMATED unless
labelled: a model of the plan's write and read paths, not a measurement of code.

Imports the plan's plan_cost.py unchanged (which imports the reference cost.py unchanged). Run from anywhere:
    python3 -I r2_numbers.py <fenvix-brain checkout>        (default /home/user/fenvix-brain)
Output: r2_numbers.txt next to this file.

1. GETs per cold read by write pattern. The delta model is the reviewer's (review/delta_sim.py: absorbed in 32 MiB
   units, tiered T = 4, a merge drops rows at or below the group's X_g), with one change in what a read pays: a delta run
   costs a GET when its customer filter holds the customer and its sequence range reaches above X_g (a run whose newest
   row is at or below X_g is skipped). The reviewer's lower and upper bounds are printed next to it for the plan as
   written. Options of round 2:
     plan   theta = 0.25, every delta run on S3 (the plan as written, the review's case);
     R1     the small delta tiers (runs up to 2 GiB: tiers 0-3, at most about 8 GB per database) kept on the waiter's
            NVMe, written through by the compactor, so they cost 0 GETs;
     R1+R2  R1, and a group is also resealed when its oldest unsealed row is older than A days (A = 7 or 3).
   Write patterns: once (a single write 0-3 days before the read, the review's), idle (last write 14-30 days before),
   weekly, daily, hourly (the review's), and session (an agent-memory pattern: one 2-hour session a day with a write
   every 5 minutes; the cold read comes at the start of the next session, about 22 hours after the last write).
2. Write amplification: the plan's formula, 1 + delta tiers + (1 + theta_eff) / theta_eff, with
   theta_eff = min(theta, A * inserted-per-day / D); against today's tiered compaction (compaction-1, T = 4):
   1 + ceil(log4(D / 8 MiB)), the review's baseline.
3. Monthly cost per database: compaction (plan_cost.ours with theta_eff, 12 s of CPU per GB) and cold-read GETs (the
   profile's cold reads per second times GETs per cold read), before and after the first course and the new-user filter.
   After them, only the cold first accesses that are neither a user seen within 14 days (70%) nor a new user (21%) pay
   GETs: 9% (MEASURED shares on chat traces, first-course-traces; their sum is an upper bound), at the idle pattern's GETs.
4. Cold p99 of a wave of n parallel GETs, without and with hedging (a second GET for any GET not answered after the
   single-GET p95). Per-GET latency lognormal with median 30 ms and p99 130 ms (NUMBERS.md: 20-40 ms per GET; p99
   126-138 ms MEASURED in M25). The shape of S3's tail is UNKNOWN until the AWS round.
5. CPU across databases: compaction cores per database and databases per spot c8g.xlarge (4 vCPU) at a 70% cap.
"""
import contextlib, importlib.util, io, math, os, random, sys

brain = sys.argv[1] if len(sys.argv) > 1 else "/home/user/fenvix-brain"
spec = importlib.util.spec_from_file_location("plan", os.path.join(brain, "sessions/2026-11-07-compaction-2-plan/plan_cost.py"))
p = importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()):
    spec.loader.exec_module(p)
m = p.m
SEC = p.SEC

T = 4
UNIT_GB = 32 / 1024
NVME_RUN_GB = 2.0
THETA = 0.25
SIZES = ((1024, "1 TB"), (10240, "10 TB"))
PROFS = ("medium", "busy")
OPTIONS = (("plan", False, None), ("R1", True, None), ("R1+R2 A=7d", True, 7.0), ("R1+R2 A=3d", True, 3.0))
PATTERNS = ("once", "idle", "weekly", "daily", "hourly", "session")


def ins_day(gb, pname):
    _, info = m.variable(gb, m.PROFILES[pname])
    return info["ins_gb"] / 30.4


def period_days(gb, ins, age):
    per = THETA * gb / ins
    return min(per, age) if age else per


def schedule(pattern, horizon, rnd):
    """Write times and cold-read sample times (days) for one customer."""
    half = horizon / 2
    if pattern == "once":
        w = horizon - 3.0
        return [w], [w + 0.01 + 2.98 * rnd.random() for _ in range(40)]
    if pattern == "session":
        writes, samples = [], []
        day = rnd.random()
        while day < horizon:
            if day > half:
                samples.append(day - 1e-4)
            writes += [day + i * 5 / 1440 for i in range(24)]
            day += 1.0
        return writes, samples
    every = {"weekly": 7.0, "daily": 1.0, "hourly": 1 / 24}[pattern]
    t, writes = rnd.random() * every, []
    while t < horizon:
        writes.append(t)
        t += every
    return writes, [half + rnd.random() * (horizon - half) for _ in range(60)]


def run(gb, ins, nvme, age, pattern, seed):
    """Mean GETs per cold read (and the review's lower and upper bounds) for one customer."""
    rnd = random.Random(seed)
    per = period_days(gb, ins, age)
    horizon = max(4 * per, 45.0)
    dt = UNIT_GB / ins
    cached_tiers = sum(1 for k in range(30) if UNIT_GB * T ** k <= NVME_RUN_GB) if nvme else 0
    if pattern == "idle":
        # One write 14-30 days before each sample: simulate each sample on its own (few samples, cheap enough).
        out = [run_idle(gb, ins, cached_tiers, age, rnd.random()) for _ in range(12)]
        return tuple(sum(x[i] for x in out) / len(out) for i in range(3))
    writes, samples = schedule(pattern, horizon, rnd)
    return simulate(per, dt, horizon, cached_tiers, writes, sorted(samples), rnd.random() * per)


def run_idle(gb, ins, cached_tiers, age, u):
    per = period_days(gb, ins, age)
    ago = 14 + 16 * u
    horizon = max(4 * per, ago + 5)
    dt = UNIT_GB / ins
    w = horizon - ago
    return simulate(per, dt, horizon, cached_tiers, [w], [horizon - 1e-3], u * per)


def simulate(per, dt, horizon, cached_tiers, writes, samples, phase):
    xg = -1.0
    tiers = []              # tiers[k] = list of runs; run = [rows (write times), newest row time]
    wi = si = 0
    t = 0.0
    got = []
    pending = []
    while t < horizon and si < len(samples):
        t += dt
        while wi < len(writes) and writes[wi] <= t:
            pending.append(writes[wi])
            wi += 1
        if t >= phase:
            xg = t
            phase += per
        carry = [pending, t]
        pending = []
        k = 0
        while True:
            if len(tiers) <= k:
                tiers.append([])
            tiers[k].append(carry)
            if len(tiers[k]) < T:
                break
            rows = [w for r in tiers[k] for w in r[0] if w > xg]
            newest = max(r[1] for r in tiers[k])
            tiers[k] = []
            carry = [rows, newest]
            k += 1
        while si < len(samples) and samples[si] <= t:
            real = lo = hi = 0
            for k, tier in enumerate(tiers):
                if k < cached_tiers:
                    continue
                for rows, newest in tier:
                    if rows:
                        hi += 1
                        if newest > xg:
                            real += 1
                        if any(w > xg for w in rows):
                            lo += 1
            got.append((1 + real, 1 + lo, 1 + hi))
            si += 1
    if not got:
        return 1.0, 1.0, 1.0
    return tuple(sum(g[i] for g in got) / len(got) for i in range(3))


def gets_table():
    res = {}
    for gb, label in SIZES:
        for pname in PROFS:
            ins = ins_day(gb, pname)
            for oname, nvme, age in OPTIONS:
                for pat in PATTERNS:
                    vals = [run(gb, ins, nvme, age, pat, s) for s in range(3)]
                    res[(label, pname, oname, pat)] = tuple(sum(v[i] for v in vals) / 3 for i in range(3))
    return res


def amp(gb, pname, age):
    ins = ins_day(gb, pname)
    th = min(THETA, age * ins / gb) if age else THETA
    a = p.ours(gb, m.PROFILES[pname], th, 12.0)
    return th, a


def tiered_today(gb):
    return 1 + math.ceil(math.log(gb / p.L0_GB, 4))


def money(x):
    return f"{x:,.2f} $"


def hedge_p99(n, hedge, trials=200_000, seed=7):
    rnd = random.Random(seed)
    mu, sig = math.log(30.0), math.log(130 / 30) / 2.326
    h = math.exp(mu + 1.645 * sig)
    worst, extra = [], 0
    for _ in range(trials):
        w = 0.0
        for _ in range(n):
            a = rnd.lognormvariate(mu, sig)
            if hedge and a > h:
                extra += 1
                a = min(a, h + rnd.lognormvariate(mu, sig))
            w = max(w, a)
        worst.append(w)
    worst.sort()
    return worst[len(worst) // 2], worst[int(0.99 * len(worst))], extra / (trials * n)


def main():
    out = []
    say = out.append
    res = gets_table()
    say("== 1. GETs per cold read (mean; ESTIMATED, model) ==")
    for gb, label in SIZES:
        for pname in PROFS:
            ins = ins_day(gb, pname)
            say(f"{label} {pname}: inserted {ins:.1f} GB/day; reseal period {THETA * gb / ins:.1f} days as written")
            for oname, nvme, age in OPTIONS:
                parts = []
                for pat in PATTERNS:
                    real, lo, hi = res[(label, pname, oname, pat)]
                    parts.append(f"{pat} {real:.1f}" + (f" (review bounds {lo:.1f}-{hi:.1f})" if oname == "plan" else ""))
                say(f"  {oname}: " + "; ".join(parts))
    say("")
    say("== 2. Write amplification (ESTIMATED, the plan's formula) ==")
    for gb, label in SIZES:
        for pname in PROFS:
            parts = []
            for oname, nvme, age in OPTIONS:
                th, a = amp(gb, pname, age)
                parts.append(f"{oname} {a['amp']:.0f} (theta_eff {th:.3f}, delta tiers {a['nd']}, sealed {a['a_s']:.1f})")
            say(f"{label} {pname}: today's tiered {tiered_today(gb)}; " + "; ".join(parts))
    say("")
    say("== 3. Monthly cost per database, Ireland prices as in cost.py (ESTIMATED) ==")
    for gb, label in SIZES:
        for pname in PROFS:
            prof = m.PROFILES[pname]
            get1 = prof["cold"] * SEC * m.S3_GET
            say(f"{label} {pname} ({prof['cold']} cold reads/s; 1 GET per cold read = {money(get1)} a month):")
            for oname, nvme, age in OPTIONS:
                th, a = amp(gb, pname, age)
                comp = a["put"] + a["get"] + a["spot"]
                cores = a["core_h"] / (30.4 * 24)
                daily = res[(label, pname, oname, "daily")][0]
                sess = res[(label, pname, oname, "session")][0]
                idle = res[(label, pname, oname, "idle")][0]
                say(f"  {oname}: compaction {money(comp)} ({a['rewritten']:,.0f} GB rewritten, {a['mbps']:.1f} MB/s, {cores:.2f} cores); "
                    f"cold-read GETs before the first course: all daily writers {money(daily * get1)}, all session writers {money(sess * get1)}; "
                    f"after the first course and the new-user filter (9% of cold reads, idle pattern {idle:.1f} GETs): {money(0.09 * idle * get1)}")
    say("")
    say("== 4. Cold p99 of one wave of n parallel GETs (ESTIMATED, lognormal median 30 ms, p99 130 ms) ==")
    for n in (1, 2, 4, 6, 9, 12):
        p50a, p99a, _ = hedge_p99(n, False)
        p50b, p99b, ex = hedge_p99(n, True)
        say(f"  n = {n}: no hedge p50 {p50a:.0f} ms, p99 {p99a:.0f} ms; hedged at the single-GET p95 ({math.exp(math.log(30) + 1.645 * math.log(130 / 30) / 2.326):.0f} ms): p50 {p50b:.0f} ms, p99 {p99b:.0f} ms, extra GETs {100 * ex:.1f}%")
    say("")
    say("== 5. Compaction CPU across databases (ESTIMATED, 12 s per GB rewritten) ==")
    for gb, label in SIZES:
        for pname in PROFS:
            parts = []
            for oname, nvme, age in OPTIONS:
                _, a = amp(gb, pname, age)
                cores = a["core_h"] / (30.4 * 24)
                parts.append(f"{oname} {cores:.2f} cores, {4 * 0.7 / cores:.0f} databases per c8g.xlarge at 70%")
            say(f"{label} {pname}: " + "; ".join(parts))
    text = "\n".join(out)
    print(text)
    open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "r2_numbers.txt"), "w").write(text + "\n")


if __name__ == "__main__":
    main()
