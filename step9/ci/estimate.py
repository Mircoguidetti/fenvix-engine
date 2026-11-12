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
         client="c7i.xlarge", filler="c7i.xlarge", extras_client="c7i.large", m30_h=3.0, extras_h=2.6):
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
    # total review B4: 5 minutes of write-peak in the pilot (its first run through our cook): 0.27 h of the kitchen and a
    # c7i.xlarge client, 2.6 GB at 25,800 documents/s crossing a zone 3 times, its batch PUTs
    wp_gb = 25800 * 341 * 5 * 60 / 1e9
    wp = (hour("m7i.large") + 2 * hour("c7g.medium") + hour("c7i.xlarge")) * 0.27 + wp_gb * XZ * 3 + wp_gb * 1e9 / 341 / BATCH * PUT
    print(f"  about {t:.2f} $ ESTIMATED, plus write-peak 5 minutes {wp:.2f} $ = {t + wp:.2f} $")


def atlas_1tb(price_h, days_load):
    print(f"\n== OPTION, Atlas Infinite M30 rerun at 1 TB ({price_h} $/h)")
    logical = 1000 * 2.6
    hours = days_load * 24 + 4
    c = price_h * hours
    s = logical * 0.30 / 30 * (days_load + 0.5)
    print(f"  cluster: {hours:.0f} h x {price_h} = {c:.2f} $; storage: 1,000 GB x 2.6 (MEASURED ratio) = {logical:.0f} GB x 0.30 $/GB-month"
          f" x {days_load + 0.5:.1f} days / 30 = {s:.2f} $; EC2 loader in Ireland about 10 $  -> about {c + s + 10:.0f} $ ESTIMATED")
    print("  Atlas storage cap: must be raised to at least 2.6 TB before the load (the 60 GB cap stopped the 2026-11-05 load)")


def paper_direct(name, bson_gb, gen_mb_s=100.0, cook="m7i.large", log="c7g.medium", loader="c7i.xlarge",
                 client="c7i.xlarge", extras_client="c7i.large", m30_h=3.0, extras_h=1.6, agent_h=1.0):
    """ON PAPER (coordinator, 2026-11-09; RULES.md 10): the ballast is GENERATED on the loader machine straight into S3
    in our own stored format, with no client and no wire; only the five Atlas-shaped databases go through the frozen
    script. The generator does not exist yet (section 4b of the report)."""
    print(f"\n== ON PAPER, data {name}: ballast generated in our format on the loader machine")
    ballast_gb = max(0.0, bson_gb - 26.6)
    gen_h = ballast_gb * 1000 / gen_mb_s / 3600 + (0.3 if ballast_gb else 0)
    atlas_h = 2.8
    tot = 0.0
    tot += line("job 1: loader + m30 client, the 5 Atlas databases", (hour(loader) + hour(client)) * atlas_h, f"{atlas_h} h x ({hour(loader):.4f} + {hour(client):.4f})")
    tot += line("job 1: S3 PUTs of that load (WAL)", 26.6e9 / DOC_B / BATCH * PUT, "78 M documents / 1,000 per batch x $0.005 per 1,000")
    if ballast_gb:
        tot += line("job 2: generator on the loader machine", hour(loader) * gen_h,
                    f"{ballast_gb:.0f} GB / {gen_mb_s:.0f} MB/s = {gen_h - 0.3:.2f} h + 0.3 h boot, x {hour(loader):.4f}")
        tot += line("job 2: S3 PUTs of the generated runs", ballast_gb * 1000 / PART_MB * PUT,
                    f"{ballast_gb:.0f} GB / {PART_MB} MB parts x $0.005 per 1,000 (no WAL, no compaction rewrite)")
    mh = m30_h + 0.5
    tot += line("job 3, m30: cook+log-0, 2 log nodes, client", (hour(cook) + 2 * hour(log) + hour(client)) * mh,
                f"{mh} h x ({hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(client):.4f})")
    tot += line("job 3, extras: cook, logs, spare, client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * extras_h,
                f"{extras_h} h x (2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    tot += line("job 3, agent benchmark: cook, logs, Postgres, client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * agent_h,
                f"{agent_h} h x (cook and Postgres 2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    days = (atlas_h + gen_h + mh + extras_h + agent_h) / 24 + 0.5
    tot += line("S3 storage while the pantry lives", bson_gb * 1.1 * GBM / 30 * days, f"{bson_gb * 1.1:.0f} GB (x 1.1, no unmerged runs) x {GBM} x {days:.2f} days / 30")
    tot += line("S3 GETs (reads, programs) and cross-zone log copies", 0.10 + 0.20 + 0.02, "0.10 reads + 0.20 cross-zone + 0.02 S3 programs")
    print(f"  TOTAL ON PAPER: {tot:.2f} $, 3 jobs, about {atlas_h + gen_h + mh + extras_h + agent_h:.0f} h of jobs; generator rate "
          f"{gen_mb_s:.0f} MB/s ESTIMATED (compaction writes about 42 MB/s per core, MEASURED M26: 22.5-24.5 s of CPU per GB; "
          f"3 cores minus document generation)")
    return tot


# MEASURED on the 10 GB local proof (2026-11-10, fenvix-engine branch aws-ready, 4-vCPU sandbox, MinIO on the same
# machine): mongo-server --ballast wrote 3 databases of 3.4 GiB (10.95e9 bytes of BSON) in 327 s, 11.2 MB/s per database,
# each database using about one vCPU (292% CPU for 3), RSS 1.2 GB for 3; stored on S3 0.74 byte per BSON byte (8.1 GB:
# documents and every index entry, lz4 blocks); sorted spill on local disk 0.104 byte per BSON byte (zstd), deleted at
# the end of each database; the publish of each database 16-17 s (5% of its time).
GEN_MB_S_PER_DB = 11.2
GEN_STORED_PER_BSON = 0.74
GEN_SPILL_PER_BSON = 0.104
GEN_PUBLISH_FRACTION = 0.05
# generator-1tb (2026-11-12, fenvix-engine branch generator-1tb, MEASURED in a 4-vCPU sandbox, Intel Xeon @ 2.1 GHz,
# MinIO on the same machine): ONE database on several writers. One writer alone 10.16 MB/s of BSON (1 GiB); four
# writers on the four vCPU 9.17 MB/s each while all are busy (3 GiB; MinIO's CPU included). The partition files of the
# shared rows are kept in the pantry until the publish (0.105 byte per BSON byte, MEASURED), the publish merges them in
# parallel. On AWS the vCPU of a c7i are hyperthreads, two per core (DECLARED by AWS); what 8 busy threads give against
# this sandbox's 4 is NOT measured: GEN_VCPU_YIELD is the ESTIMATED share of a sandbox vCPU one AWS vCPU gives when
# all are busy (c7i: 4 cores x 1.25 for 8 threads = 0.625, the cautious end; 1.0 if a thread were worth a sandbox vCPU).
GEN_MB_S_PER_WRITER = 9.17
GEN_VCPU_YIELD = {"c7i": 0.625}
GEN_PARTS_PER_BSON = 0.105


def generator_hours(gib, gen_type, writers=None):
    """Hours to generate and publish one ballast database of `gib` GiB of BSON on `gen_type` with `writers` writers
    (default: its vCPU). ESTIMATED from the MEASURED rate per busy writer and the ESTIMATED yield of the type's vCPU."""
    vcpu = TYPES[gen_type][0]
    w = min(int(writers or vcpu), vcpu)
    eff = w * GEN_VCPU_YIELD.get(gen_type.split(".")[0], 1.0)
    return gib * GIB * 1000 / (eff * GEN_MB_S_PER_WRITER) / 3600 * (1 + GEN_PUBLISH_FRACTION)
# Aurora PostgreSQL (DECLARED, AWS Price List AmazonRDS eu-west-1, publication 2026-10-29T07:06:35Z): db.r7g.large
# Single-AZ $0.304/h, storage $0.11 per GB-month, $0.22 per million I/O requests (Aurora Standard)
AURORA_H, AURORA_GBM, AURORA_IO_M = 0.304, 0.11, 0.22
BALLAST_DB_GIB = 24.3
GIB = 1.073741824   # GB per GiB: the script's --gb is GiB


def gen_hour(t, disk_gb):
    return TYPES[t][3] + disk_gb * EBS_GB_MONTH / 730 + IPV4_HOUR


def generated(name, ballast_dbs, aurora=True, cook="m7i.large", log="c7g.medium", loader="c7i.xlarge", client="c7i.xlarge",
              extras_client="c7i.large", m30_h=3.0, extras_h=1.6, agent_h=1.0, aurora_h=0.5, disk_gb=60):
    """The plan with the ballast generator BUILT (mongo-server --ballast, step9 ballast_mode "generate"): job 1 loads the
    five Atlas-shaped databases through the frozen script (unchanged), job 2 generates the ballast alone on its machine,
    job 3 measures. Generator rate MEASURED locally per database; the machine runs one database per vCPU (ESTIMATED:
    the local run used about one vCPU per database and S3 PUTs are asynchronous multipart uploads)."""
    gen = "c7i.2xlarge" if ballast_dbs > 4 else "c7i.xlarge"
    par = min(TYPES[gen][0], ballast_dbs)
    bson_gb = ballast_dbs * BALLAST_DB_GIB * GIB
    print(f"\n== BUILT, data {name}: {ballast_dbs} ballast databases of {BALLAST_DB_GIB} GiB = {bson_gb:.0f} GB of BSON generated "
          f"on one {gen} ({par} at a time), plus the five Atlas databases (26.6 GB) through the frozen script")
    waves = -(-ballast_dbs // par) if ballast_dbs else 0
    per_db_h = BALLAST_DB_GIB * GIB * 1000 / GEN_MB_S_PER_DB / 3600 * (1 + GEN_PUBLISH_FRACTION)
    gen_h = waves * per_db_h + 0.3 if ballast_dbs else 0.0
    atlas_h = 2.8
    tot = 0.0
    tot += line("job 1: loader + m30 client, the 5 Atlas databases", (hour(loader) + hour(client)) * atlas_h,
                f"{atlas_h} h (MEASURED rate on Atlas) x ({hour(loader):.4f} + {hour(client):.4f}) $/h")
    tot += line("job 1: S3 PUTs of that load (WAL)", 26.6e9 / DOC_B / BATCH * PUT, "78 M documents / 1,000 per batch x $0.005 per 1,000")
    stored = bson_gb * GEN_STORED_PER_BSON
    if ballast_dbs:
        tot += line(f"job 2: generator {gen} ({disk_gb} GB gp3)", gen_hour(gen, disk_gb) * gen_h,
                    f"{waves} waves x {per_db_h:.2f} h ({BALLAST_DB_GIB} GiB at {GEN_MB_S_PER_DB} MB/s MEASURED, +5% publish) + 0.3 h boot "
                    f"= {gen_h:.2f} h x {gen_hour(gen, disk_gb):.4f} $/h")
        tot += line("job 2: S3 PUTs of the generated runs", stored * 1000 / PART_MB * PUT,
                    f"{stored:.0f} GB stored ({GEN_STORED_PER_BSON} per BSON byte, MEASURED) / {PART_MB} MB parts x $0.005 per 1,000")
    mh = m30_h + 0.5
    tot += line("job 3, m30: cook+log-0, 2 log nodes, client", (hour(cook) + 2 * hour(log) + hour(client)) * mh,
                f"{mh} h x ({hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(client):.4f})")
    tot += line("job 3, extras: cook, logs, spare, client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * extras_h,
                f"{extras_h} h x (2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    ah = agent_h + (aurora_h + 0.3 if aurora else 0)
    tot += line("job 3, agent benchmark: cook, logs, Postgres, client", (2 * hour(cook) + 2 * hour(log) + hour(extras_client)) * ah,
                f"{ah:.1f} h (+0.5 h Aurora target, +0.3 h waiting for the cluster) x (2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})"
                if aurora else f"{ah:.1f} h x (2 x {hour(cook):.4f} + 2 x {hour(log):.4f} + {hour(extras_client):.4f})")
    if aurora:
        au_h = 0.3 + aurora_h + 0.2
        io = 0.58 * 3 * AURORA_IO_M
        tot += line("job 3, Aurora db.r7g.large (Standard)", AURORA_H * max(1.0, au_h) + io + 2 * AURORA_GBM / 730 * au_h,
                    f"{max(1.0, au_h):.1f} h (create 0.3 + bench {aurora_h} + delete 0.2; 1 h billed as an upper bound) x {AURORA_H} "
                    f"+ I/O 0.58 M x 3 x {AURORA_IO_M} $/M + 2 GB storage")
    days = (atlas_h + gen_h + mh + extras_h + ah) / 24 + 0.5
    pantry_gb = stored + 26.6 * 1.6
    tot += line("S3 storage while the pantry lives", pantry_gb * GBM / 30 * days,
                f"{pantry_gb:.0f} GB ({stored:.0f} generated + 26.6 x 1.6 Atlas databases) x {GBM} x {days:.2f} days / 30")
    tot += line("S3 GETs (reads, programs) and cross-zone log copies", 0.10 + 0.20 + 0.02, "0.10 reads + 0.20 cross-zone + 0.02 S3 programs")
    print(f"  TOTAL ESTIMATED: {tot:.2f} $, {3 if ballast_dbs else 2} jobs, about {atlas_h + gen_h + mh + extras_h + ah:.1f} h of jobs one after the other "
          f"(job 2 alone {gen_h:.2f} h); spill disk needed {par * BALLAST_DB_GIB * GIB * GEN_SPILL_PER_BSON:.0f} GB of {disk_gb} GB; "
          f"never more than 8 vCPU of EC2 at once (Aurora is RDS)")
    return tot


def split_plan(name, ballast_dbs, aurora=True, cook="m7i.large", log="c7g.medium", loader="c7i.xlarge", client="c7i.xlarge",
               extras_client="c7i.large", pgbox="m7i.large", disk_gb=60, peak_docs_s=25800, agent_h_per_target=0.6):
    """aws-fixes (B2, B4, B10; 2026-11-11): the job plan that fits the job ceiling. One kept pantry for every size (the
    total review: the 100 GB and 1 TB jobs only add ballast to it, so the Atlas databases are loaded once). The measure
    is THREE jobs, each checked by decide.py against 90% of the 330-minute ceiling:
      M1 m30 alone (the measured cook settles first: prepare, settle cap 30 min);
      M2 the extra programs (spare + extras client), then write-peak (spare off, a c7i.xlarge client);
      M3 the agent benchmark: Aurora FIRST and deleted right after its target, then ours, then the Postgres machine.
    Clients in eu-west-1b, the cook, the Postgres machine and Aurora in eu-west-1a (as on Atlas): the client's traffic
    crosses a zone. Durations from decide.JOB_HOURS (ESTIMATED; the agent benchmark per target UNKNOWN until bench-local
    measures it at the standard profile)."""
    print(f"\n== THE JOB PLAN (aws-fixes), data {name}: one kept pantry, three measure jobs")
    tot = 0.0
    jobs = []
    # L: the five Atlas databases through the frozen script (2.8 h at Atlas's rate; ours UNKNOWN) + boot + the settle
    lh = 0.3 + 2.8 + 0.25
    c = (hour(loader) + hour(client)) * lh + 26.6e9 / DOC_B / BATCH * PUT
    jobs.append(("L, load the five Atlas databases (loader + m30 client)", lh, c,
                 f"{lh:.2f} h (boot 0.3 + 2.8 at Atlas's rate + settle 0.25 expected, cap 1.0) x ({hour(loader):.4f} + "
                 f"{hour(client):.4f}) + WAL PUTs 78 M documents / 1,000 x $0.005 per 1,000; client in the cook's zone "
                 f"(client_zone same, total review N5): no cross-zone traffic (in another zone it would add up to 0.53 $)"))
    if ballast_dbs:
        gen = "c7i.2xlarge" if ballast_dbs > 4 else "c7i.xlarge"
        par = min(TYPES[gen][0], ballast_dbs)
        per_db_h = BALLAST_DB_GIB * GIB * 1000 / GEN_MB_S_PER_DB / 3600 * (1 + GEN_PUBLISH_FRACTION)
        gh = -(-ballast_dbs // par) * per_db_h + 0.3
        stored = ballast_dbs * BALLAST_DB_GIB * GIB * GEN_STORED_PER_BSON
        c = gen_hour(gen, disk_gb) * gh + stored * 1000 / PART_MB * PUT
        jobs.append((f"G, generate {ballast_dbs} ballast databases on {gen}", gh, c,
                     f"{gh:.2f} h x {gen_hour(gen, disk_gb):.4f} + PUTs {stored:.0f} GB / {PART_MB} MB parts"))
    kitchen = hour(cook) + 2 * hour(log)
    m1 = 0.3 + 0.25 + 3.0
    c = (kitchen + hour(client)) * m1 + 0.05 + 0.10
    jobs.append(("M1, m30 program alone (cook + log-0, 2 log nodes, m30 client)", m1, c,
                 f"{m1:.2f} h (boot 0.3 + settle 0.25 expected, cap 0.5 + m30 3.0) x ({kitchen:.4f} + {hour(client):.4f})"
                 f" + cross-zone client traffic and log copies about $0.05 + reads $0.10"))
    ex = 0.3 + 0.2 + 1.6
    peak_h = 0.6
    peak_gb = peak_docs_s * 341 * 25 * 60 / 1e9
    xz_peak = peak_gb * XZ * 3        # client -> cook across a zone, and the cook's log copies to two other zones
    c = (kitchen + hour(cook) + hour(extras_client)) * ex + (kitchen + hour(client)) * peak_h + 0.02 + 0.20 + \
        xz_peak + peak_gb * 1e9 / 341 / BATCH * PUT
    jobs.append(("M2, extra programs (spare, extras client) then write-peak (c7i.xlarge client)", ex + peak_h, c,
                 f"{ex:.1f} h x ({kitchen:.4f} + spare {hour(cook):.4f} + {hour(extras_client):.4f}) + write-peak {peak_h} h x "
                 f"({kitchen:.4f} + {hour(client):.4f}) + S3 programs 0.02 + log-ack and takeovers cross-zone 0.20 + "
                 f"write-peak at {peak_docs_s:,} documents/s: {peak_gb:.1f} GB x 0.02 $/GB x 3 crossings = {xz_peak:.2f} "
                 f"+ its batch PUTs {peak_gb * 1e9 / 341 / BATCH * PUT:.2f} (upper bound)"))
    targets = 2 + (1 if aurora else 0)
    m3 = 0.3 + 0.2 + targets * agent_h_per_target + (0.3 if aurora else 0)
    c = (kitchen + hour(pgbox) + hour(extras_client)) * m3 + 0.10
    how = (f"{m3:.1f} h (boot 0.5 + Aurora's creation 0.3 + {targets} targets x {agent_h_per_target} h) x ({kitchen:.4f} + "
           f"Postgres {hour(pgbox):.4f} + {hour(extras_client):.4f}) + cross-zone 0.10")
    if aurora:
        au_h = 0.3 + agent_h_per_target + 0.2
        au = AURORA_H * max(1.0, au_h) + 0.58 * 3 * AURORA_IO_M + 5 * AURORA_GBM / 730 * au_h
        c += au
        how += (f" + Aurora {max(1.0, au_h):.1f} h billed (create 0.3 + its own target {agent_h_per_target} + delete 0.2; "
                f"run first, deleted at once) x {AURORA_H} + I/O 1.74 M x {AURORA_IO_M} $/M = {au:.2f}")
    jobs.append(("M3, agent benchmark (Aurora first, ours, the Postgres machine)", m3, c, how))
    if ballast_dbs:
        # total review N5: job B, the ballast rounds on the measured kitchen, was in the commands but not in the total
        bh = 2.1   # decide.py's estimate of step9/commands/06-B-ballast-rounds-100gb.json (boot 0.3 + rounds 1.8)
        c = (kitchen + hour(client)) * bh + 0.05
        jobs.append(("B, ballast rounds on the measured kitchen", bh, c,
                     f"{bh} h (decide.py's estimate) x ({kitchen:.4f} + {hour(client):.4f}) + cross-zone 0.05 (upper bound)"))
    hours = sum(j[1] for j in jobs)
    days = hours / 24 + 0.5
    pantry_gb = 26.6 * 1.6 + (ballast_dbs * BALLAST_DB_GIB * GIB * GEN_STORED_PER_BSON if ballast_dbs else 0)
    storage = pantry_gb * GBM / 30 * days
    for label, h, c, how in jobs:
        tot += line(f"{label[:44]}", c, f"{h:.2f} h; {how}")
    tot += line("S3 storage of the kept pantry", storage, f"{pantry_gb:.0f} GB x {GBM} x {days:.2f} days / 30")
    print(f"  TOTAL ESTIMATED: {tot:.2f} $, {len(jobs)} jobs, about {hours:.1f} h of jobs one after the other; each job "
          f"under the default $5 cap; the largest job {max(j[2] for j in jobs):.2f} $")
    print("  (the SPESA line of run.py now adds each job's cross-zone allowance, ci/outcomes.py XZ_ALLOWANCE: before, it "
          "printed about $1 less than the real figure in job M2)")
    return tot


if __name__ == "__main__":
    print("Step 9 cost estimate, 8-vCPU plan (ESTIMATED; prices DECLARED, sources in the docstring)")
    pilot()
    size("atlas (the size measured on Atlas)", 26.6)
    size("100gb", 100)
    size("1tb", 1000)
    paper_direct("100gb", 100)
    paper_direct("1tb", 1000)
    print("\n######## with the generator BUILT (aws-ready, 2026-11-10): the numbers to use ########")
    generated("atlas", 0)
    generated("100gb", 3)
    generated("1tb", 40)
    print("\n######## aws-fixes (2026-11-11): the job plan that fits the ceiling: THE NUMBERS TO USE ########")
    print("(the BUILT plan above has ONE measure job of about 7 h: refused by decide.py, total review B2)")
    split_plan("atlas", 0)
    split_plan("100gb", 3)
    split_plan("atlas, write-peak at 10,000 documents/s", 0, peak_docs_s=10000)
    # no Atlas rerun (founder, 2026-11-09): the comparison uses the Atlas numbers measured in Ireland on 2026-11-05
