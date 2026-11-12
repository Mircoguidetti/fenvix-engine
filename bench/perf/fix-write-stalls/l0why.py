"""Cause 3 (fix-write-stalls): for every write SlateDB held 1 s or more (`SLOW_STAGE`), what the cook logged around it:
the L0 timeline (`L0_TRACE`, once a second), SlateDB's compaction jobs (`SLATE_JOB` start/end, one at a time) and our
absorptions (`ABSORB`, with their time split). Then totals over the whole run: jobs by kind and duration, absorption
times, and how long L0 jobs waited behind sorted-run merges.

    python l0why.py RUN_DIR|COOK_LOG [--before-ms 8000]
"""
import gzip, os, re, sys


def cook_log(p):
    if os.path.isdir(p):
        for c in ("logs/cook.log.gz", "cook.log.gz", "logs/cook.log"):
            if os.path.exists(os.path.join(p, c)):
                p = os.path.join(p, c)
                break
    return gzip.open(p, "rt", errors="replace") if p.endswith(".gz") else open(p, errors="replace")


def kv(line):
    return dict(re.findall(r"(\w+)=(\S+)", line))


before = 8000.0
args = [a for a in sys.argv[1:]]
if "--before-ms" in args:
    i = args.index("--before-ms")
    before = float(args[i + 1])
    del args[i : i + 2]
src = args[0]

events = []  # (u, kind, line)
for line in cook_log(src):
    line = line.rstrip("\n")
    for k in ("SLOW_STAGE", "L0_TRACE", "SLATE_JOB start", "SLATE_JOB end", "ABSORB", "SLOW_PUT", "FIRST_PUBLISH", "L0_RECOGNISE"):
        if line.startswith(k):
            m = re.search(r"u=([\d.]+)", line)
            if m:
                events.append((float(m.group(1)), k, line))
            break
events.sort()
if not events:
    sys.exit("no diagnostic lines (a binary before aa14107?)")
t0 = events[0][0]

holds = [(u, kv(l)) for u, k, l in events if k == "SLOW_STAGE"]
print(f"holds of 1 s or more: {len(holds)}")
for u, h in holds:
    ms = float(h["ms"])
    lo = u - ms - before
    print(f"\n== hold {ms:.0f} ms ending at +{(u - t0) / 1000:.0f} s (l0 at end {h.get('l0')}, largest {h.get('l0_max_held')})")
    for v, k, l in events:
        if lo <= v <= u + 500 and k != "SLOW_STAGE":
            print(f"  {(v - u) / 1000:+7.1f}s {l[:230]}")

# totals
jobs = [kv(l) | {"_u": u} for u, k, l in events if k == "SLATE_JOB end"]
l0_jobs = [j for j in jobs if j.get("l0_srcs", "0") != "0"]
sr_jobs = [j for j in jobs if j.get("l0_srcs", "0") == "0"]


def s(xs):
    xs = sorted(xs)
    if not xs:
        return "none"
    return f"n={len(xs)} sum={sum(xs) / 1000:.0f}s p50={xs[len(xs) // 2]:.0f}ms max={xs[-1]:.0f}ms"


print("\n== totals")
print("SlateDB jobs with L0 sources:", s([float(j["ms"]) for j in l0_jobs]))
print("SlateDB sorted-run merges   :", s([float(j["ms"]) for j in sr_jobs]))
big = sorted(sr_jobs, key=lambda j: -float(j["ms"]))[:8]
for j in big:
    print(f"   merge {float(j['ms']) / 1000:.1f} s at +{(j['_u'] - t0) / 1000:.0f} s srs_mib={j.get('srs_mib')}")
ab = [kv(l) for u, k, l in events if k == "ABSORB"]
for f in ("total_ms", "lock_ms", "read_ms", "write_ms", "commit_ms"):
    print(f"ABSORB {f:9s}:", s([float(a[f]) for a in ab if f in a]))
print("ABSORB ssts per absorption:", s([float(a["ssts"]) for a in ab]))
# L0 time at 8, and with an L0 job wanted but a sorted-run merge running
tr = [(u, kv(l), l) for u, k, l in events if k == "L0_TRACE"]
at8 = sum(1 for _, t, _ in tr if int(t["l0"]) >= 8)
ge6 = sum(1 for _, t, _ in tr if int(t["l0"]) >= 6)
blocked = sum(1 for _, t, l in tr if int(t["l0"]) >= 4 and "l0_srcs=0" in l and "slate_job=-" not in l)
print(f"L0_TRACE samples {len(tr)}: L0 at 8 in {at8}, at 6+ in {ge6}; L0 at 4+ while a sorted-run merge ran: {blocked}")
