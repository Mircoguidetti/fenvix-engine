"""Step 9 · reads step9/command.json, refuses anything not allowed, fills the defaults and writes the RESOLVED command
that run.py, packages.sh and the cost estimate use. Nothing here touches AWS.

Fields of step9/command.json (defaults in brackets):
  mode          "permissions" ($0 check) | "run" | "teardown" | "drop-pantry" | anything else: nothing happens ["none"]
  data          "atlas" (the five m30-corto databases, about 27 GB of BSON, as loaded on Atlas M30) | "100gb" | "1tb"
                (the same five databases plus ONE ballast database of the same shape, app_data_b01, of ballast_gib)
                ["atlas"]
  ballast_gib   GiB of BSON of the one ballast database (generator-1tb, 2026-11-12: one database of the chosen size,
                not several of a fixed size) [100gb: 93.2 (100 GB), 1tb: 931.4 (1,000 GB)]; any size from 1 to 1100
  phases        any of "load", "measure", "extras", in this order [all three]. A 1 TB load does not fit in one GitHub
                job (6 hours): load in several rounds into a kept pantry, then measure in a last round.
  pantry        "new" (deleted at the end) | "keep" (kept for later rounds as fenvix-kitchen-pantry-<run>) | the name
                of a kept pantry ["new"]
  ballast_slice [FROM, TO]: [0, 0] loads no ballast (the Atlas databases only), [0, 1] the ballast database [0, 1]
  ballast_mode  "generate": the ballast written straight into the pantry in our stored format by the ballast generator
                (`mongo-server <server_options> --ballast`, mongo-server/src/ballast.rs; sealed runs, our manifest, the
                keys of the round's layout), on its own machine, no cook 0 and no client; needs "--sealed-runs" and
                "--customer-key" in server_options (the round reads it through compaction 2's reader) | "script": loaded
                through cook 0 by the founder's script on the filler machine (the slow path, 1.55 MB/s per process)
                ["generate"]
  generator     the generator machine [c7i.2xlarge (8 vCPU)]; generator_disk_gb its disk (each writer's sorted
                spill of one slice, then one key partition at a time at the publish: a few GB) [60];
                generator_parallel the writers of the one database [its vCPU]. The generator commits its work slice by
                slice in the pantry (generator/app_data_b01/), so a job cut by its ceiling loses at most the slices in
                flight: the same command again on the kept pantry resumes it. A generation estimated above one job is
                accepted only with a kept pantry ("pantry": "keep" or its name), and decide.py says how many jobs.
  engine        REQUIRED for mode run: the FULL 40-character fenvix-engine commit id the servers and the generator are
                built from, the final merged head of compaction 2 (no default: a round never runs on a commit chosen by
                accident). Never a branch or tag name (total review R5, 2026-11-12): a branch moves, so two jobs of one
                round could run two engines, and a commit added after the local report would close the gate between
                jobs. The gate (gate.py) stays closed unless the local benchmark report was made on that commit.
  atlas_states  code-irlanda commit with the Atlas state files the Atlas round started from ["501c8fd"]
  cook, spare   type of the measured cook and of the warm spare; spare null = no spare (not like for like) ["m7i.large"]
  log           type of the three log nodes ["c7g.medium"]
  compactor     type of the separate compactor, or null [null]; compactor_options: its whole command line (step 4)
  loader        type of cook 0 ["c7i.xlarge"] (it loads without the log)
  client        the m30 client ["c7i.xlarge"] (the program's macchina.txt says c7i.2xlarge with 5 copies: with the
                8-vCPU limit the maximum-load test is NOT like for like, see the report)
  filler        the ballast loader, for 100gb and 1tb ["c7i.xlarge": 3 script processes]
  extras_client the client of the extra programs ["c7i.large"]
  log_colocate  log-0 on the cook's machine (true) or on its own machine [true]
  pgbox         the Postgres machine of the agent benchmark (Supabase stand-in) ["m7i.large"]
  aurora        true: an Aurora PostgreSQL cluster (Standard, db.r7g.large, default VPC, tag project=fenvix-coldread) is
                created for the agent benchmark, runs the same benchmark as the Postgres machine, and is deleted with no
                final snapshot (ci/aurora.py; RDS, not in the EC2 vCPU quota; needs the RDS policy of `aurora.py
                policy` and "network": "default-sg") [false]
  network       "default-sg": every machine in the default VPC's security group (its members talk to each other, no
                port from the internet; needs no security-group permission) | "own-groups": two new tagged groups
                ["default-sg"]
  server_options extra options of mongo-server on the cook and the spare, one string [""]
  prepare       open every database and settle before the m30 program (kitchen K5) [true: aws-fixes B5, the measured
                cook waits for compaction 2 to finish, on the server's own counters and the fx/ objects, and the
                program does not run if it never does: NOT_SETTLED]
  settle_max_min the cap of that wait, and of the loader's after the load [30]; reaching it stops (never silent)
  programs      any of "m30", "s3-first-byte", "sealed-run", "log-ack", "first-next", "ballast-rounds", "first-next-ops",
                "reads-after-writes", "takeovers", "write-peak", "agent-bench" [all but the two that write into the
                measured database, "first-next-ops" and "reads-after-writes": only when named]. "first-next-ops"
                (2026-11-13): FIRST and NEXT of each request of the agent benchmark (resume, last 10, whole history, time
                travel, save) on the largest database, each operation with its own cold customers
                (programs/first_next_ops.py; it saves one message per measured customer). "reads-after-writes"
                (2026-11-13): the stato_ts index read before and after about 13,000 order updates and after the compactor
                settled, with the rows read from sealed runs per document (programs/reads_after_writes.py), then
                first-next-ops again on other customers (label after-writes). Both run after the ballast rounds; across
                jobs, only after every job that must read the pantry as loaded. "first-next" (total review R1 point 2): for N cold customers of the
                largest databases, the first request and three different next requests of the same customer, with the
                cook's GETs and bytes per request (programs/first_next.py; first_next_args); it runs before the ballast
                rounds, on customers the founder's script never uses (not its reserved ones). "write-peak" (aws-fixes B4): the founder's script's own load (the Atlas load's
                shape: 6 databases at once, 16 threads each, batches of 1,000) for 25 minutes through the MEASURED cook
                (log on, the round's server_options), from a client of the m30 client's type, after the extra programs
                (the spare is off by then); write_peak_args
  client_zone   "other": every client (m30, extras, write-peak, agent) in eu-west-1b while the cook, the Postgres machine
                and Aurora are in eu-west-1a, as on Atlas (client in eu-west-1a, primary in another zone: euw1-az2,
                MEASURED from the hello tags) | "same": clients in the cook's zone ["other"]. Every machine records its
                zone name and zone id (IMDS) in results/<name>/zone.json; run.json lists them.
  takeovers_args, log_ack_args, s3_first_byte_args, sealed_run_args, ballast_rounds_args, first_next_args,
  first_next_ops_args, reads_after_writes_args: extra arguments
                [see DEFAULTS]
  spend_cap     dollars; the round blocks itself there [5]. Above 5 only with "cap_approved": "<date, the founder's yes>"
  ceiling_min   minutes after which the round stops and every machine powers off [330, at most 340]
                aws-fixes (B2): decide.py estimates the job's duration from the phases and programs (JOB_HOURS below,
                ESTIMATED) and REFUSES a job whose estimate is above 90% of ceiling_min: the measure is split into three
                jobs on the kept pantry (m30 alone; the extra programs with write-peak; the agent benchmark), see
                step9/commands/. A command that would do nothing (e.g. "phases": ["extras"] alone: no cook, no program)
                is refused too.
Usage: decide.py COMMAND_JSON RESOLVED_OUT [VCPU_QUOTA]   (RUN_ID in the environment: the GitHub run id)
Prints key=value lines (for GITHUB_OUTPUT) and exits 1 with "REFUSED: reason" if the command is not allowed."""
import json
import os
import re
import sys

from machine_types import ROLES, TYPES

PROGRAMS = ["m30", "s3-first-byte", "sealed-run", "log-ack", "first-next", "ballast-rounds", "first-next-ops",
            "reads-after-writes", "takeovers", "write-peak", "agent-bench"]
EXTRA_PROGRAMS = ["s3-first-byte", "sealed-run", "log-ack", "first-next", "ballast-rounds", "first-next-ops",
                  "reads-after-writes", "takeovers"]   # the extras client's
# 2026-11-13 (coordinator: the 1 TB plan): the two programs that WRITE into the measured database. first-next-ops saves
# one message per measured customer (150 by default); reads-after-writes updates about 13,000 orders. Never in the
# default list; in a job they run after the founder's script's read rounds (extras.sh's order); across jobs, only after
# every job that must read the pantry as loaded (M1 for the Atlas databases, the ballast rounds for the big one).
WRITING_PROGRAMS = ["first-next-ops", "reads-after-writes"]
# their own time on top of the extras (ESTIMATED upper bounds: first-next-ops 150 customers x 5 requests, twice when
# reads-after-writes follows it; reads-after-writes 3 stages x --reads x --max-read-s at worst, the updates, the settle)
PROGRAM_HOURS = {"first-next-ops": 0.3, "reads-after-writes": 1.5}
# How long each part of a job takes, in hours (ESTIMATED; aws-fixes B2). Sources: the Atlas load 2.8 h (MEASURED rate on
# Atlas M30, 2026-11-05; OUR load rate on real S3 is UNKNOWN until the load job runs: the job's own 4.3 h script limit
# and the ceiling stop it, and the settle cap is added); m30-corto about 3.0 h (kitchen README, the Atlas round's
# length); the extra programs 1.6 h with 50 + 50 takeovers (step9-prep and aws-ready estimates); write-peak 25 minutes
# plus 10 to start and drop its databases; the agent benchmark at the standard profile per target: UNKNOWN (never
# measured at the standard profile, the total review's question 2 point 6), 0.6 h each until the local bench-local run
# measures it; Aurora's creation about 0.3 h (DECLARED order of magnitude by AWS; measured on the first run).
JOB_HOURS = {"boot": 0.3, "atlas_load": 2.8, "m30": 3.0, "spare_boot": 0.2, "extras": 1.6, "write_peak": 0.6,
             "agent_per_target": 0.9, "agent_per_target_small": 0.2, "aurora_create": 0.3, "pgbox_boot": 0.2}
CEILING_USE = 0.9   # a job is refused when its estimate is above 90% of ceiling_min (teardown, results, margin)
DATA = {  # BSON GB of the five Atlas databases: 4 x 3.1 + 10.74 (MEASURED, Atlas state files of code-irlanda) + the 2 KB
          # collections (2 x 1.22 GiB + about 1 GiB): about 26.6 GB. generator-1tb (2026-11-12): the size is ONE ballast
          # database of ballast_gib GiB of BSON (the founder's goal: one database of 1 TB), not 3 or 40 of 24.3 GiB
    "atlas": {"target_gb": 26.6, "ballast_dbs": 0, "ballast_gib": 0.0},
    "100gb": {"target_gb": 126.6, "ballast_dbs": 1, "ballast_gib": 93.2},
    "1tb": {"target_gb": 1026.6, "ballast_dbs": 1, "ballast_gib": 931.4},
}
BALLAST_GIB_MAX = 1100.0


def gib_arg(gib):
    """The --gb argument of the plan: the exact value of ballast_gib (shortest round-trip form, "130" for 130.0), never a
    rounded one: extras.sh checks the generated state file's size against ballast_gib (review of 148bee9: with "%g",
    931.3225746 became 931.323 and the check refused the database)."""
    s = repr(float(gib))
    return s[:-2] if s.endswith(".0") else s
DEFAULTS = {
    "data": "atlas", "phases": ["load", "measure", "extras"], "pantry": "new", "ballast_slice": None, "ballast_gib": None,
    "ballast_mode": "generate", "generator": None, "generator_disk_gb": 60, "generator_parallel": None,
    "engine": None, "engine_commit": None, "atlas_states": "501c8fd",
    "cook": "m7i.large", "spare": "m7i.large", "log": "c7g.medium", "compactor": None, "compactor_options": "",
    "loader": "c7i.xlarge", "loader_options": "", "client": "c7i.xlarge", "filler": "c7i.xlarge",
    "extras_client": "c7i.large", "log_colocate": True, "pgbox": "m7i.large", "aurora": False, "network": "default-sg",
    "server_options": "", "prepare": True, "settle_max_min": 30,
    "programs": [p for p in PROGRAMS if p not in WRITING_PROGRAMS], "client_zone": "other",
    "write_peak_args": "--dbs 6 --threads 16 --minutes 25 --gb 10",
    "takeovers_args": "--n 50 --writers 8 --between-s 3 --lease-ttl 4 --log on",
    "takeovers_off_args": "--n 50 --writers 8 --between-s 3 --lease-ttl 4 --log off --label logoff",
    "log_ack_args": "--n 1000 --conc 8,64 --secs 20",
    "s3_first_byte_args": "--objects 40 --reads 300",
    "first_next_args": "--customers 30 --max-dbs 2",
    "first_next_ops_args": "--per-op 30 --max-s 120",
    "reads_after_writes_args": "--reads 6 --updates 13000 --write-customers 150 --max-read-s 180 --settle-min 20",
    # read sample and erased users as the local run (bench-local review, item 8): p99 of 300 reads, not of 30
    "agent_bench_args": "--profile standard --workers 1 --read-sample 300 --erase-users 100",
    "sealed_run_args": "--run-mib 1024 --block-kib 256 --reads 300",
    "ballast_rounds_args": "--n-q2 300 --n-q3 300 --n-q1 300 --n-q1d 300 --n-q4 300 --dorm-q2 300 --dorm-q3 300 "
                           "--dorm-q1 300 --dorm-q1d 300 --dorm-q4 300",
    "spend_cap": 5, "cap_approved": "", "ceiling_min": 330, "keep_days": 7, "kept_max_days": 5,
    "s3_fallback_dollars": 1.5,
}
# The founder's limit (2026-11-09): the on-demand standard vCPU quota stays 8; no job may run more at the same time.
# S9_VCPU_LIMIT overrides it for the LOCAL mode only (x86 containers replace the 1-vCPU Graviton log nodes).
VCPU_LIMIT = int(os.environ.get("S9_VCPU_LIMIT", "8"))
SAFE_ARGS = re.compile(r"^[A-Za-z0-9 ,._:=-]{0,400}$")
RESERVED = {"--listen", "--store", "--bucket", "--region", "--endpoint", "--local-dir", "--replica-set", "--advertise",
            "--auth", "--tls-mode", "--tls-certificate-key-file", "--tls-ca-file", "--holder", "--no-leases",
            "--unsafe-skip-ownership-checks", "--quaderno", "--quaderno-id", "--log-commands"}


def refuse(msg):
    print(f"REFUSED: {msg}")
    sys.exit(1)


def job_hours(c):
    """The job's expected duration, part by part (hours, ESTIMATED from JOB_HOURS and, for the generator, from the
    measured rate in estimate.py)."""
    h = {"boot": JOB_HOURS["boot"]}
    if "load" in c["phases"]:
        if c.get("atlas_load"):
            h["atlas_load"] = JOB_HOURS["atlas_load"]
            h["load_settle_cap"] = float(c["settle_max_min"]) / 60
        if c.get("filler"):
            # through the script: 3 processes at 1.55 MB/s each (MEASURED on Atlas)
            gb = len(c["ballast"]["lines"]) * c["ballast_gib"] * 1.073741824
            h["ballast_script"] = gb * 1000 / (c["ballast"]["parallel"] * 1.55) / 3600
        if c.get("generator"):
            import estimate
            gen_h = estimate.generator_hours(c["ballast_gib"], c["generator"], int(c["ballast"]["parallel"]))
            c["generator_hours_estimated"] = round(gen_h, 2)
            # a resumable generation longer than one job runs until the ceiling; the next job resumes it (kept pantry)
            cap_h = CEILING_USE * float(c["ceiling_min"]) / 60 - sum(h.values())
            c["generator_jobs_estimated"] = max(1, -(-int(gen_h * 100) // max(1, int(cap_h * 100))))
            h["generator"] = min(gen_h, max(0.0, cap_h))
    if "measure" in c["phases"]:
        if "m30" in c["programs"]:
            if c.get("prepare"):
                h["measured_settle_cap"] = float(c["settle_max_min"]) / 60
            h["m30"] = JOB_HOURS["m30"]
    if "extras" in c["phases"] and "measure" in c["phases"]:
        if any(p in EXTRA_PROGRAMS for p in c["programs"]):
            # the extras client's programs: the fixed estimate for the older ones, each newer one its own
            older = [p for p in c["programs"] if p in EXTRA_PROGRAMS and p not in PROGRAM_HOURS]
            h["extras"] = JOB_HOURS["spare_boot"] + (JOB_HOURS["extras"] if older else 0.0)
            for p, hours in PROGRAM_HOURS.items():
                if p in c["programs"]:
                    if p == "first-next-ops":   # by its group size (30 customers per operation by default)
                        fa = (c.get("first_next_ops_args") or "").split()
                        hours *= float(fa[fa.index("--per-op") + 1]) / 30 if "--per-op" in fa else 1.0
                    h[p.replace("-", "_")] = hours
        if "write-peak" in c["programs"]:
            # its own --minutes plus the fixed part of JOB_HOURS["write_peak"] (0.6 h for the 25 minutes of the full
            # program: about 11 minutes of setup and the load's end); the pilot runs 5 minutes (total review B4)
            a = (c.get("write_peak_args") or "").split()
            mins = float(a[a.index("--minutes") + 1]) if "--minutes" in a else 25.0
            h["write_peak"] = JOB_HOURS["write_peak"] - 25 / 60 + mins / 60
        if "agent-bench" in c["programs"]:
            targets = 2 + (1 if c.get("aurora") else 0)
            # total review (2026-11-12, question 3): per target at the standard profile 0.9 h, ESTIMATED from the local
            # repeat 1 of 2026-11-11 (the write phase alone: Postgres 19 minutes, mongod 23, ours 33, MEASURED; plus the
            # reads, the erase, our settle of at least 12 minutes and the cold phase); the small profile about 0.2 h
            ab = (c.get("agent_bench_args") or "").split()
            profile = ab[ab.index("--profile") + 1] if "--profile" in ab else "standard"
            per = JOB_HOURS["agent_per_target_small"] if profile in ("small", "tiny") else JOB_HOURS["agent_per_target"]
            h["agent_bench"] = JOB_HOURS["pgbox_boot"] + targets * per + (JOB_HOURS["aurora_create"] if c.get("aurora") else 0)
    return {k: round(v, 2) for k, v in h.items()}


def resolve(raw, quota=None):
    c = dict(DEFAULTS)
    unknown = [k for k in raw if k not in DEFAULTS and k != "mode"]
    if unknown:
        refuse(f"unknown fields {unknown}")
    c.update(raw)
    c["mode"] = raw.get("mode", "none")
    if c["mode"] not in ("permissions", "run", "teardown", "drop-pantry"):
        c["mode"] = "none"
    if c["data"] not in DATA:
        refuse(f"data: one of {list(DATA)}")
    if not (isinstance(c["phases"], list) and c["phases"] and all(p in ("load", "measure", "extras") for p in c["phases"])):
        refuse("phases: a non-empty list of load, measure, extras")
    if not set(c["programs"]) <= set(PROGRAMS):
        refuse(f"programs: any of {PROGRAMS}")
    if c["ballast_gib"] is None:
        c["ballast_gib"] = DATA[c["data"]]["ballast_gib"]
    try:
        c["ballast_gib"] = float(c["ballast_gib"])
    except (TypeError, ValueError):
        refuse("ballast_gib: a number of GiB")
    if DATA[c["data"]]["ballast_dbs"] and not 1.0 <= c["ballast_gib"] <= BALLAST_GIB_MAX:
        refuse(f"ballast_gib: from 1 to {BALLAST_GIB_MAX} GiB of BSON")
    if c["generator"] is None:   # the default: one database on all 8 vCPU of the quota
        c["generator"] = "c7i.2xlarge"
    for role in ("cook", "spare", "log", "compactor", "loader", "client", "filler", "extras_client", "pgbox", "generator"):
        t = c[role]
        if t is None and role in ("spare", "compactor"):
            continue
        if t not in ROLES[role]:
            refuse(f"{role}: {t} not allowed ({', '.join(ROLES[role])})")
    if c["spare"] and c["spare"] != c["cook"]:
        refuse("spare: must be the same type as the cook (the fair-comparison rule of 2026-11-06)")
    if c["compactor"] and not c["compactor_options"]:
        refuse("compactor: give its command line in compactor_options (compaction 2 step 4 decides it)")
    if c["mode"] == "run" and not c["engine"]:
        refuse('engine: give the full 40-character commit id of the final merged engine, e.g. "engine": '
               '"27014ff2e3e3e4ff223aa778746f3dce433c6549"; there is no default (step 9 runs on the commit holding '
               'compaction 2 steps 3-8, never on one chosen by accident)')
    if c["engine"] is not None and not re.match(r"^[0-9a-f]{40}$", str(c["engine"])):
        refuse(f"engine: the full 40-character commit id, never a branch or tag name (total review R5: a branch moves "
               f"between the jobs of one round); got {c['engine']!r}")
    # the build checks out exactly this commit; the workflow passes it as ENGINE_COMMIT too, and the two must agree
    env_commit = os.environ.get("ENGINE_COMMIT") or None
    if c["engine"] and env_commit and env_commit != c["engine"]:
        refuse(f"engine: the command names {c['engine']} but the workflow resolved {env_commit}")
    c["engine_commit"] = c["engine"] or env_commit
    if c["ballast_mode"] not in ("generate", "script"):
        refuse("ballast_mode: generate or script")
    if not re.match(r"^[0-9a-f]{7,40}$", c["atlas_states"]):
        refuse("atlas_states: a commit")
    for k in [k for k in c if k.endswith("_args") or k.endswith("_options")]:
        if not SAFE_ARGS.match(c[k] or ""):
            refuse(f"{k}: only letters, digits, spaces and , . _ : = -")
    for tok in (c["server_options"] + " " + c["loader_options"]).split():
        if tok.startswith("--") and tok.split("=")[0] in RESERVED:
            refuse(f"server_options: {tok} is set by the machine scripts")
    # the pantry
    p = c["pantry"]
    if p in ("new", "keep"):
        c["pantry_mode"] = p
    elif re.match(r"^fenvix-kitchen-pantry-[0-9]+$", str(p)):
        c["pantry_mode"] = "existing"
        # (mode run only: aws-fixes found that the documented {"mode": "drop-pantry", "pantry": ...} was refused here
        # through the default phases, so a kept pantry could not be dropped by the workflow; the local run dropped it
        # without decide.py and never showed it)
        if c["mode"] == "run" and "load" in c["phases"] and c["data"] == "atlas":
            refuse("an existing pantry already has the Atlas databases: phase load only adds ballast (data 100gb or 1tb)")
    else:
        refuse("pantry: new, keep, or fenvix-kitchen-pantry-<digits>")
    # the pantry's name, known before the packages are built (the client needs it): new = deleted at the end, keep =
    # the prefix of the minimal kitchen's kept pantries (teardown never deletes it), or the existing one
    run = os.environ.get("RUN_ID", "0")
    c["pantry_name"] = {"new": f"fenvix-coldread-s9d-{run}", "keep": f"fenvix-kitchen-pantry-{run}"}.get(c["pantry_mode"], p)
    if c["pantry_mode"] in ("new", "keep") and "load" not in c["phases"]:
        refuse("a new pantry is empty: the round must load")
    if c["network"] not in ("default-sg", "own-groups"):
        refuse("network: default-sg (the default VPC's group: no security-group permission needed) or own-groups")
    if c["aurora"] not in (True, False):
        refuse("aurora: true or false")
    if c["aurora"] and c["network"] != "default-sg":
        refuse("aurora: only with network default-sg (the cluster joins the default VPC's security group, like every machine)")
    if c["aurora"] and c["mode"] == "run" and not ("extras" in c["phases"] and "agent-bench" in c["programs"]):
        refuse("aurora: the cluster exists only for the agent benchmark: phases must include extras and programs agent-bench")
    if c["mode"] == "drop-pantry" and c["pantry_mode"] != "existing":
        refuse("drop-pantry: give the name of the kept pantry")
    # the data: the Atlas databases are loaded by the m30 client when the pantry is new or kept; ballast by the filler
    d = DATA[c["data"]]
    c["atlas_load"] = "load" in c["phases"] and c["pantry_mode"] in ("new", "keep")
    n = d["ballast_dbs"]
    lo, hi = (c["ballast_slice"] or [0, n])
    if not (0 <= lo <= hi <= n):
        refuse(f"ballast_slice: within [0, {n}]")
    # the one ballast database; --tetto-logico-gb only reaches the founder's script on the "script" path, where it is
    # its logical ceiling: above the database
    lines = [[f"app_data_b{i + 1:02d}", "--gb", gib_arg(c["ballast_gib"]), "--seme", f"{2001 + i}", "--thread", "16",
              "--quota-riservati", "0.4", "--tetto-logico-gb", f"{max(60, int(c['ballast_gib'] * 1.2) + 1)}",
              "--max-ore-carico", "48"] for i in range(lo, hi)]
    # the one ballast database, also in the jobs that only measure it on a kept pantry
    c["ballast_db"] = "app_data_b01" if n else None
    if "load" not in c["phases"]:
        lines = []
    c["ballast"] = {"parallel": max(1, TYPES[c["filler"]][0] - 1), "lines": lines}
    if lines and c["ballast_mode"] == "generate":
        # the generator: one database per vCPU at a time (MEASURED locally: the server's insert code takes one core per
        # database); no filler, no cook 0 for the ballast
        c["ballast"]["parallel"] = int(c["generator_parallel"] or TYPES[c["generator"]][0])
        c["filler"] = None
        opts = c["server_options"].split()
        if "--sealed-runs" not in [o.split("=")[0] for o in opts]:
            refuse('ballast_mode generate: server_options must turn on compaction 2\'s reader ("--sealed-runs 0" or N): '
                   "the generated ballast is sealed runs and our manifest, which the server reads only with it on")
        if "--envelopes" not in [o.split("=")[0] for o in opts] or "write" not in opts:
            refuse('ballast_mode generate: server_options must have "--envelopes write" (release E1): a version of our '
                   "manifest is published only with envelopes on")
        if "--customer-key" not in [o.split("=")[0] for o in opts]:
            refuse('ballast_mode generate: server_options must give the customer key of layout v2 (e.g. "--customer-key '
                   'messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"); the generator writes the keys the round reads')
    else:
        c["generator"] = None
    if not lines:
        c["filler"] = None
        c["generator"] = None
    c["data_gb_upper"] = round(d["target_gb"] * 1.6, 1)   # BSON plus indexes and not yet compacted SSTs, upper bound
    # money and time
    cap = float(c["spend_cap"])
    if cap <= 0:
        refuse("spend_cap: a positive number of dollars")
    if cap > 5 and not str(c["cap_approved"]).strip():
        refuse("spend_cap above 5 $ needs cap_approved: the date and words of the founder's yes (RULES.md)")
    if not 10 <= float(c["ceiling_min"]) <= 340:
        refuse("ceiling_min: 10 to 340 (a GitHub job lasts at most 360 minutes)")
    if "ballast-rounds" in c["programs"] and c["data"] == "atlas":
        c["programs"] = [x for x in c["programs"] if x != "ballast-rounds"]
    if c["client_zone"] not in ("other", "same"):
        refuse("client_zone: other (clients in another zone than the cook, as on Atlas) or same")
    if not 5 <= float(c["settle_max_min"]) <= 90:
        refuse("settle_max_min: 5 to 90 minutes")
    # aws-fixes (B2): a command that does nothing is refused (run.py starts the measured cook only in phase "measure";
    # the m30 program runs in "measure", every other program in "extras")
    if c["mode"] == "run":
        extra_work = [p for p in c["programs"] if p != "m30"]
        if "extras" in c["phases"] and "measure" not in c["phases"]:
            refuse('phases: "extras" needs "measure" (the extra programs run against the measured cook, which only the '
                   'measure phase starts): this command would start nothing and measure nothing')
        if "measure" in c["phases"] and "m30" not in c["programs"] and not ("extras" in c["phases"] and extra_work):
            refuse('phases: "measure" without the program m30 and without extra programs measures nothing')
        if "extras" in c["phases"] and not extra_work:
            refuse(f'phases: "extras" with no extra program to run (programs {c["programs"]})')
    # the vCPU on at the same time, phase by phase (the phases never overlap their machines; run.py)
    v = lambda t: TYPES[t][0] if t else 0
    log_machines = 2 if c["log_colocate"] else 3            # log-0 on the cook's machine, or on its own
    peaks = {}
    if "load" in c["phases"]:
        if c["atlas_load"] or c["filler"]:
            peaks["load"] = v(c["loader"]) + (v(c["client"]) if c["atlas_load"] else 0) + v(c["filler"])
        if c["generator"]:   # after the Atlas databases, alone (run.py)
            peaks["load (ballast generator)"] = v(c["generator"])
    if "measure" in c["phases"]:
        peaks["measure (m30)"] = v(c["cook"]) + log_machines * v(c["log"]) + v(c["compactor"]) + \
            (v(c["client"]) if "m30" in c["programs"] else 0)
    if "extras" in c["phases"]:
        if any(p in EXTRA_PROGRAMS for p in c["programs"]):
            peaks["extras"] = v(c["cook"]) + log_machines * v(c["log"]) + v(c["compactor"]) + v(c["spare"]) + v(c["extras_client"])
        if "write-peak" in c["programs"]:   # the spare is off by then; a client of the m30 client's type
            peaks["write-peak"] = v(c["cook"]) + log_machines * v(c["log"]) + v(c["compactor"]) + v(c["client"])
        if "agent-bench" in c["programs"]:   # the spare is off by then; the Postgres machine is on
            peaks["agent-bench"] = v(c["cook"]) + log_machines * v(c["log"]) + v(c["compactor"]) + v(c["pgbox"]) + v(c["extras_client"])
    c["vcpu_peaks"] = peaks
    c["vcpu_peak"] = max(peaks.values()) if peaks else 0
    if c["mode"] == "run" and c["vcpu_peak"] > VCPU_LIMIT:
        hint = ""
        if peaks.get("load", 0) > VCPU_LIMIT and c["atlas_load"] and c.get("filler"):
            hint = (' Load the Atlas databases and the ballast in separate rounds: first "phases": ["load"], "pantry": "keep",'
                    ' "ballast_slice": [0, 0]; then "phases": ["load"] on the kept pantry with the ballast slices.')
        refuse(f"{c['vcpu_peak']} vCPU at once ({peaks}); the limit is {VCPU_LIMIT} (on-demand quota).{hint}")
    if quota is not None and c["mode"] == "run" and c["vcpu_peak"] > quota:
        refuse(f"{c['vcpu_peak']} vCPU at once, the quota is {quota}")
    c["job_hours"] = job_hours(c)
    c["job_hours_total"] = round(sum(c["job_hours"].values()), 2)
    limit_h = CEILING_USE * float(c["ceiling_min"]) / 60
    if os.environ.get("S9_MODE", "aws") == "local":
        # LOCAL mode only (shrunk programs, like S9_VCPU_LIMIT): the estimate is scaled; on AWS never
        limit_h /= float(os.environ.get("S9_DURATION_SCALE", "1"))
    if c.get("generator") and c.get("generator_jobs_estimated", 1) > 1 and c["pantry_mode"] == "new":
        refuse(f"the generation of {c['ballast_gib']:g} GiB takes about {c['generator_hours_estimated']} h on "
               f"{c['generator']} (ESTIMATED), about {c['generator_jobs_estimated']} jobs: it resumes across jobs only "
               f'from a kept pantry: "pantry": "keep" (then its name in the next jobs)')
    if c["mode"] == "run" and c["job_hours_total"] > limit_h:
        refuse(f"the job would last about {c['job_hours_total']:.1f} h (ESTIMATED: {c['job_hours']}), above "
               f"{CEILING_USE:.0%} of ceiling_min {float(c['ceiling_min']):.0f} = {limit_h:.1f} h: it would be cut before its "
               f"end. Split it into jobs on the kept pantry (step9/commands/: m30 alone; the extra programs and "
               f"write-peak; the agent benchmark)")
    if c["mode"] == "run" and os.environ.get("S9_MODE", "aws") == "aws":
        # RULES.md 8-9 (founder, 2026-11-09): no AWS run while a local result loses a write or is worse than the competitors
        import subprocess
        g = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gate.py")],
                           capture_output=True, text=True, env={**os.environ, "ENGINE_REF": str(c["engine"]),
                                                                "ENGINE_COMMIT": c["engine_commit"] or ""})
        if g.returncode != 0:
            refuse("the gate before any AWS run is closed:\n" + g.stdout.strip())
    c["arches"] = sorted({TYPES[c[r]][2] for r in ("cook", "spare", "log", "compactor", "loader", "generator") if c[r]} | {"x86_64"})
    c["loader_uses_log"] = False
    return c


def main():
    raw = json.load(open(sys.argv[1]))
    quota = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None
    c = resolve(raw, quota)
    json.dump(c, open(sys.argv[2], "w"), indent=1)
    for k in ("mode", "engine", "engine_commit", "data", "pantry_mode", "vcpu_peak", "vcpu_peaks", "job_hours_total", "job_hours"):
        print(f"{k}={c[k]}")
    print(f"x86={'1' if 'x86_64' in c['arches'] else '0'}")
    print(f"arm={'1' if 'aarch64' in c['arches'] else '0'}")
    print(f"aurora={'1' if c['aurora'] else '0'}")


if __name__ == "__main__":
    main()
