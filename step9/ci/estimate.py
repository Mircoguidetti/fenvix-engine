"""Step 9 · cost and time estimate of the AWS round, per data size, with the arithmetic printed (every number labelled).
No AWS call. Prices DECLARED (machine_types.py: AWS Price List eu-west-1 2026-11-04; S3: fact check 2026-11-08, S3
Standard $0.023 per GB-month, PUT $0.005 and GET $0.0004 per 1,000; cross-zone traffic $0.01 per GB each way, DECLARED by
AWS, EC2 on-demand pricing page, "Data Transfer within the same AWS Region").
Atlas side (only if the founder chooses to rerun Atlas at 1 TB): Infinite M30 Gen2 $0.677/h (DECLARED, fact check
2026-11-08, mongodb.com/pricing) or $0.77/h (DECLARED, Atlas console screenshot 2026-11-05); storage $0.30 per GB-month
(DECLARED) on Atlas's "Data Size", which was 2.6 times the BSON loaded right after a load (MEASURED, P1 2026-11-04:
10 GB of documents -> 26.45 GiB with indexes and oplog).
Usage: estimate.py [RESOLVED_JSON]   (without it: the three sizes with the default machines)"""
import json
import sys

from machine_types import TYPES, EBS_GB_MONTH, IPV4_HOUR

DISK_GB = 20
GET, PUT, GBM = 0.0004 / 1000, 0.005 / 1000, 0.023
XZ = 0.02            # cross-zone, $0.01 out + $0.01 in per GB
DOC_B = 340          # bytes per small document (the script's documents, MEASURED on Atlas: 3.1 GB / 9.1 M documents)
BATCH = 1000         # documents per insert batch of the script's load (its default --lotto)
WRITE_AMP = 4        # bytes the compaction rewrites per byte loaded, ESTIMATED (compaction-1: 3.9 MEASURED in M26)
PART_MB = 16         # S3 multipart part size of an SST upload, ESTIMATED


def hour(t):
    return TYPES[t][3] + DISK_GB * EBS_GB_MONTH / 730 + IPV4_HOUR


def line(label, value, how):
    print(f"  {label:<44} {value:>9.2f} $   {how}")
    return value


def size(name, bson_gb, per_proc_mb_s=1.55, cook="m7i.large", log="c7g.medium", loader="c7i.xlarge",
         client="c7i.xlarge", filler="c7i.xlarge", extras_client="c7i.large", m30_h=3.0, extras_h=1.0):
    """The 8-vCPU plan (founder, 2026-11-09): job 1 loads the Atlas databases (loader + m30 client), ballast jobs load
    the ballast (loader + filler), the last job measures (cook with log-0 + 2 log nodes + client, then cook + logs +
    spare + extras client). Never more than 8 vCPU at once."""
    print(f"\n== data {name}: {bson_gb:.0f} GB of BSON")
    procs = TYPES[filler][0] - 1
    atlas_h = 2.5 + 0.3   # the five Atlas databases: the old one (10.7 GB at 1.28 MB/s, 1 process) is the long pole, MEASURED on Atlas
    ballast_gb = max(0.0, bson_gb - 26.6)
    rate = procs * per_proc_mb_s
    ballast_h = ballast_gb * 1000 / rate / 3600 if ballast_gb else 0
    jobs_b = int(-(-ballast_h // 5.0)) if ballast_h else 0
    ballast_h_total = ballast_h + 0.3 * jobs_b
    print(f"  job 1 (Atlas databases): {atlas_h:.1f} h; ballast: {ballast_gb:.0f} GB / ({procs} processes x {per_proc_mb_s} MB/s = "
          f"{rate:.2f} MB/s) = {ballast_h:.1f} h in {jobs_b} jobs of at most 5 h; measure job {m30_h + extras_h + 0.5:.1f} h")
    print(f"  wall time: about {atlas_h + ballast_h_total + m30_h + extras_h + 0.5:.0f} h of jobs, one after the other; ESTIMATED "
          f"(per-process rate MEASURED on Atlas; whether our loader {loader} keeps up is UNKNOWN)")
    tot = 0.0
    tot += line("load job 1: loader + m30 client", (hour(loader) + hour(client)) * atlas_h, f"{atlas_h:.1f} h x ({hour(loader):.4f} + {hour(client):.4f}) $/h")
    if ballast_gb:
        tot += line("ballast jobs: loader + filler", (hour(loader) + hour(filler)) * ballast_h_total, f"{ballast_h_total:.1f} h x ({hour(loader):.4f} + {hour(filler):.4f})")
    mh = m30_h + 0.5
    tot += line("measure, m30: cook+log-0, 2 log nodes, client", (hour(cook) + 2 * hour(log) + hour(client)) * mh,
                f"{mh:.1f} h x ({hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(client):.4f})")
    tot += line("measure, extras: cook, logs, spare, extras client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * extras_h,
                f"{extras_h:.1f} h x (2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    docs = bson_gb * 1e9 / DOC_B
    wal = docs / BATCH * PUT
    sst = bson_gb * WRITE_AMP * 1000 / PART_MB * PUT
    tot += line("S3 PUT: WAL objects of the load", wal, f"{docs / 1e6:.0f} M documents / {BATCH} per batch x {PUT * 1000:.3f} $/1000")
    tot += line("S3 PUT: compaction output", sst, f"{bson_gb:.0f} GB x {WRITE_AMP} rewritten / {PART_MB} MB parts x PUT")
    tot += line("S3 GET: compaction input and cold reads", sst * 0.4 + 0.10, "about as many GETs as parts written (x 0.08 price ratio) + 0.10 $ reads")
    keep_days = (atlas_h + ballast_h_total + m30_h + extras_h + 0.5) / 24 + 0.5
    tot += line("S3 storage (kept pantry, data x 1.6)", bson_gb * 1.6 * GBM / 30 * keep_days, f"{bson_gb * 1.6:.0f} GB x {GBM} $/GB-month x {keep_days:.2f} days")
    tot += line("cross-zone: log copies of the measured writes", 5 * XZ * 2, "about 5 GB written during measure (ESTIMATED) x 2 zones x 0.02 $/GB")
    tot += line("extras: S3 programs", 0.02, "s3-first-byte, sealed-run: about 4,000 requests and 1.3 GB for an hour")
    tot += line("agent benchmark: cook, logs, Postgres machine, client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * 1.0,
                f"1.0 h x (cook {hour(cook):.4f} + Postgres m7i.large {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    print(f"  TOTAL ESTIMATED: {tot:.2f} $ ({1 + jobs_b + 1} jobs; never more than 8 vCPU at once)")
    return tot


def pilot():
    print("\n== pilot: Atlas-size load job, then 1 h of the measured kitchen with the extra programs (5 takeovers), no m30 program")
    t = (hour("c7i.xlarge") * 2) * 2.8 + (2 * hour("m7i.large") + 2 * hour("c7g.medium") + hour("c7i.large")) * 1.3 + 0.39 + 0.15
    print(f"  about {t:.2f} $ ESTIMATED")


def atlas_1tb(price_h, days_load):
    print(f"\n== OPTION, Atlas Infinite M30 rerun at 1 TB ({price_h} $/h)")
    logical = 1000 * 2.6
    hours = days_load * 24 + 4
    c = price_h * hours
    s = logical * 0.30 / 30 * (days_load + 0.5)
    print(f"  cluster: {hours:.0f} h x {price_h} = {c:.2f} $; storage: 1,000 GB x 2.6 (MEASURED ratio) = {logical:.0f} GB x 0.30 $/GB-month"
          f" x {days_load + 0.5:.1f} days / 30 = {s:.2f} $; EC2 loader in Ireland about 10 $  -> about {c + s + 10:.0f} $ ESTIMATED")
    print("  Atlas storage cap: must be raised to at least 2.6 TB before the load (the 60 GB cap stopped the 2026-11-05 load)")


if __name__ == "__main__":
    print("Step 9 cost estimate, 8-vCPU plan (ESTIMATED; prices DECLARED, sources in the docstring)")
    pilot()
    size("atlas (the size measured on Atlas)", 26.6)
    size("100gb", 100)
    size("1tb", 1000)
    # no Atlas rerun (founder, 2026-11-09): the comparison uses the Atlas numbers measured in Ireland on 2026-11-05
