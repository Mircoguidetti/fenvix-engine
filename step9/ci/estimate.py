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


def size(name, bson_gb, filler="c7i.2xlarge", per_proc_mb_s=1.55, cook="m7i.large", log="m7g.medium",
         loader="c7i.xlarge", client="c7i.2xlarge", m30_h=3.0, extras_h=1.0):
    print(f"\n== data {name}: {bson_gb:.0f} GB of BSON")
    procs = TYPES[filler][0] - 1
    atlas_dbs_h = 2.5   # the five Atlas databases: the old one (10.7 GB at 1.28 MB/s) is the long pole, 2.3 h MEASURED on Atlas
    ballast_gb = max(0.0, bson_gb - 26.6)
    rate = procs * per_proc_mb_s
    ballast_h = ballast_gb * 1000 / rate / 3600 if ballast_gb else 0
    load_h = max(atlas_dbs_h, ballast_h) + 0.3
    print(f"  load time: max(Atlas databases {atlas_dbs_h} h, ballast {ballast_gb:.0f} GB / ({procs} processes x {per_proc_mb_s} MB/s"
          f" = {rate:.1f} MB/s) = {ballast_h:.1f} h) + 0.3 h boot = {load_h:.1f} h  ESTIMATED (per-process rate MEASURED on"
          f" Atlas; whether our loader cook {loader} keeps up is UNKNOWN)")
    jobs = max(1, -(-load_h // 5.5))
    print(f"  GitHub jobs for the load (5.5 h each): {jobs:.0f}; then one job to measure ({m30_h} h m30 + {extras_h} h extras + 0.4 h boot)")
    tot = 0.0
    tot += line("load: loader cook", hour(loader) * load_h, f"{load_h:.1f} h x {hour(loader):.4f} $/h ({loader}, gp3 20 GB, IPv4)")
    tot += line("load: m30 client (Atlas databases)", hour(client) * (atlas_dbs_h + 0.3), f"{atlas_dbs_h + 0.3:.1f} h x {hour(client):.4f}")
    if ballast_gb:
        tot += line("load: filler", hour(filler) * (ballast_h + 0.3), f"{ballast_h + 0.3:.1f} h x {hour(filler):.4f} ({filler})")
    mh = m30_h + extras_h + 0.4
    tot += line("measure: 3 log nodes", 3 * hour(log) * mh, f"3 x {mh:.1f} h x {hour(log):.4f} ({log})")
    tot += line("measure: cook + warm spare", 2 * hour(cook) * mh, f"2 x {mh:.1f} h x {hour(cook):.4f} ({cook})")
    tot += line("measure: m30 client, then extras client", hour(client) * mh, f"{mh:.1f} h x {hour(client):.4f} ({client})")
    docs = bson_gb * 1e9 / DOC_B
    wal = docs / BATCH * PUT
    sst = bson_gb * WRITE_AMP * 1000 / PART_MB * PUT
    tot += line("S3 PUT: WAL objects of the load", wal, f"{docs / 1e6:.0f} M documents / {BATCH} per batch x {PUT * 1000:.3f} $/1000")
    tot += line("S3 PUT: compaction output", sst, f"{bson_gb:.0f} GB x {WRITE_AMP} rewritten / {PART_MB} MB parts x PUT")
    tot += line("S3 GET: compaction input and cold reads", sst * 0.4 + 0.10, "about as many GETs as parts written (x 0.08 price ratio) + 0.10 $ reads")
    keep_days = (load_h + mh) / 24 + 0.5
    tot += line("S3 storage (kept pantry, data x 1.6)", bson_gb * 1.6 * GBM / 30 * keep_days, f"{bson_gb * 1.6:.0f} GB x {GBM} $/GB-month x {keep_days:.2f} days")
    tot += line("cross-zone: log copies of the measured writes", 5 * XZ * 2, "about 5 GB written during measure (ESTIMATED) x 2 zones x 0.02 $/GB")
    tot += line("extras: S3 programs", 0.02, "s3-first-byte, sealed-run: about 4,000 requests and 1.3 GB for an hour")
    print(f"  TOTAL ESTIMATED: {tot:.2f} $ (upper side; the SPESA line measures the real one; cap per job: the founder's)")
    return tot


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
    print("Step 9 cost estimate (ESTIMATED; prices DECLARED, sources in the docstring)")
    size("atlas (the size measured on Atlas)", 26.6)
    size("100gb", 100)
    size("1tb, filler c7i.2xlarge (7 processes)", 1000)
    size("1tb, filler c7i.8xlarge (31 processes)", 1000, filler="c7i.8xlarge")
    for p in (0.677, 0.77):
        atlas_1tb(p, days_load=1.0)
