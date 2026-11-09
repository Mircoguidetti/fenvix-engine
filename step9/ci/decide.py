"""Step 9 · reads step9/command.json, refuses anything not allowed, fills the defaults and writes the RESOLVED command
that run.py, packages.sh and the cost estimate use. Nothing here touches AWS.

Fields of step9/command.json (defaults in brackets):
  mode          "permissions" ($0 check) | "run" | "teardown" | "drop-pantry" | anything else: nothing happens ["none"]
  data          "atlas" (the five m30-corto databases, about 27 GB of BSON, as loaded on Atlas M30) | "100gb" | "1tb"
                (the same five databases plus ballast databases of the same shape) ["atlas"]
  phases        any of "load", "measure", "extras", in this order [all three]. A 1 TB load does not fit in one GitHub
                job (6 hours): load in several rounds into a kept pantry, then measure in a last round.
  pantry        "new" (deleted at the end) | "keep" (kept for later rounds as fenvix-kitchen-pantry-<run>) | the name
                of a kept pantry ["new"]
  ballast_slice [FROM, TO]: the ballast databases this round loads (0-based, TO excluded) [all of them]
  engine        branch, tag or commit of fenvix-engine the servers are built from ["integration"]
  atlas_states  code-irlanda commit with the Atlas state files the Atlas round started from ["501c8fd"]
  cook, spare   type of the measured cook and of the warm spare; spare null = no spare (not like for like) ["m7i.large"]
  log           type of the three log nodes ["m7g.medium"]
  compactor     type of the separate compactor, or null [null]; compactor_options: its whole command line (step 4)
  loader        type of cook 0 ["c7i.xlarge"]; loader_uses_log [false]
  client        the m30 client (the program's macchina.txt: c7i.2xlarge with 5 copies) ["c7i.2xlarge"]
  filler        the ballast loader, for 100gb and 1tb ["c7i.4xlarge"]
  server_options extra options of mongo-server on the cook and the spare, one string [""]
  prepare       open every database and settle before the m30 program (kitchen K5) [false]
  programs      any of "m30", "s3-first-byte", "sealed-run", "log-ack", "ballast-rounds", "takeovers" [all]
  takeovers_args, log_ack_args, s3_first_byte_args, sealed_run_args, ballast_rounds_args: extra arguments [see DEFAULTS]
  spend_cap     dollars; the round blocks itself there [5]. Above 5 only with "cap_approved": "<date, the founder's yes>"
  ceiling_min   minutes after which the round stops and every machine powers off [330, at most 340]
Usage: decide.py COMMAND_JSON RESOLVED_OUT [VCPU_QUOTA]
Prints key=value lines (for GITHUB_OUTPUT) and exits 1 with "REFUSED: reason" if the command is not allowed."""
import json
import math
import re
import sys

from machine_types import ROLES, TYPES

PROGRAMS = ["m30", "s3-first-byte", "sealed-run", "log-ack", "ballast-rounds", "takeovers"]
DATA = {  # BSON GB of the five Atlas databases: 4 x 3.1 + 10.74 (MEASURED, Atlas state files of code-irlanda) + the 2 KB
          # collections (2 x 1.22 GiB + about 1 GiB): about 26.6 GB; ballast databases of 24.3 GB to reach the size
    "atlas": {"target_gb": 26.6, "ballast_dbs": 0},
    "100gb": {"target_gb": 100.0, "ballast_dbs": 3},
    "1tb": {"target_gb": 1000.0, "ballast_dbs": 40},
}
BALLAST_DB_GB = 24.3
DEFAULTS = {
    "data": "atlas", "phases": ["load", "measure", "extras"], "pantry": "new", "ballast_slice": None,
    "engine": "integration", "atlas_states": "501c8fd",
    "cook": "m7i.large", "spare": "m7i.large", "log": "m7g.medium", "compactor": None, "compactor_options": "",
    "loader": "c7i.xlarge", "loader_uses_log": False, "loader_options": "", "client": "c7i.2xlarge", "filler": "c7i.2xlarge",
    "server_options": "", "prepare": False, "programs": PROGRAMS,
    "takeovers_args": "--n 50 --writers 8 --between-s 3 --lease-ttl 4 --log on",
    "log_ack_args": "--n 1000 --conc 8,64 --secs 20",
    "s3_first_byte_args": "--objects 40 --reads 300",
    "sealed_run_args": "--run-mib 1024 --block-kib 256 --reads 300",
    "ballast_rounds_args": "--n-q2 300 --n-q3 300 --n-q1 300 --n-q1d 300 --n-q4 300 --dorm-q2 300 --dorm-q3 300 "
                           "--dorm-q1 300 --dorm-q1d 300 --dorm-q4 300",
    "spend_cap": 5, "cap_approved": "", "ceiling_min": 330, "keep_days": 7, "kept_max_days": 5,
    "s3_fallback_dollars": 1.5,
}
SAFE_ARGS = re.compile(r"^[A-Za-z0-9 ,._:-]{0,400}$")
RESERVED = {"--listen", "--store", "--bucket", "--region", "--endpoint", "--local-dir", "--replica-set", "--advertise",
            "--auth", "--tls-mode", "--tls-certificate-key-file", "--tls-ca-file", "--holder", "--no-leases",
            "--unsafe-skip-ownership-checks", "--quaderno", "--quaderno-id", "--log-commands"}


def refuse(msg):
    print(f"REFUSED: {msg}")
    sys.exit(1)


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
    for role in ("cook", "spare", "log", "compactor", "loader", "client", "filler"):
        t = c[role]
        if t is None and role in ("spare", "compactor"):
            continue
        if t not in ROLES[role]:
            refuse(f"{role}: {t} not allowed ({', '.join(ROLES[role])})")
    if c["spare"] and c["spare"] != c["cook"]:
        refuse("spare: must be the same type as the cook (the fair-comparison rule of 2026-11-06)")
    if c["compactor"] and not c["compactor_options"]:
        refuse("compactor: give its command line in compactor_options (compaction 2 step 4 decides it)")
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$", c["engine"]):
        refuse("engine: a branch, tag or commit")
    if not re.match(r"^[0-9a-f]{7,40}$", c["atlas_states"]):
        refuse("atlas_states: a commit")
    for k in [k for k in c if k.endswith("_args") or k.endswith("_options")]:
        if not SAFE_ARGS.match(c[k] or ""):
            refuse(f"{k}: only letters, digits, spaces and , . _ : -")
    for tok in (c["server_options"] + " " + c["loader_options"]).split():
        if tok.startswith("--") and tok.split("=")[0] in RESERVED:
            refuse(f"server_options: {tok} is set by the machine scripts")
    # the pantry
    p = c["pantry"]
    if p in ("new", "keep"):
        c["pantry_mode"] = p
    elif re.match(r"^fenvix-kitchen-pantry-[0-9]+$", str(p)):
        c["pantry_mode"] = "existing"
        if "load" in c["phases"] and c["data"] == "atlas":
            refuse("an existing pantry already has the Atlas databases: phase load only adds ballast (data 100gb or 1tb)")
    else:
        refuse("pantry: new, keep, or fenvix-kitchen-pantry-<digits>")
    if c["pantry_mode"] in ("new", "keep") and "load" not in c["phases"]:
        refuse("a new pantry is empty: the round must load")
    if c["mode"] == "drop-pantry" and c["pantry_mode"] != "existing":
        refuse("drop-pantry: give the name of the kept pantry")
    # the data: the Atlas databases are loaded by the m30 client when the pantry is new or kept; ballast by the filler
    d = DATA[c["data"]]
    c["atlas_load"] = "load" in c["phases"] and c["pantry_mode"] in ("new", "keep")
    n = d["ballast_dbs"]
    lo, hi = (c["ballast_slice"] or [0, n])
    if not (0 <= lo <= hi <= n):
        refuse(f"ballast_slice: within [0, {n}]")
    lines = [[f"app_data_b{i + 1:02d}", "--gb", f"{BALLAST_DB_GB}", "--seme", f"{2001 + i}", "--thread", "16",
              "--quota-riservati", "0.4", "--tetto-logico-gb", "60", "--max-ore-carico", "48"] for i in range(lo, hi)]
    if "load" not in c["phases"]:
        lines = []
    c["ballast"] = {"parallel": max(1, TYPES[c["filler"]][0] - 1), "lines": lines}
    if not lines:
        c["filler"] = None
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
    # the vCPU on at the same time, against the on-demand standard quota (L-1216C47A)
    v = lambda t: TYPES[t][0] if t else 0
    logs = 3 * v(c["log"]) if ("measure" in c["phases"] or c["loader_uses_log"]) else 0
    load_peak = logs + v(c["loader"]) + (v(c["client"]) if c["atlas_load"] else 0) + v(c["filler"])
    measure_peak = logs + v(c["cook"]) + v(c["spare"]) + v(c["compactor"]) + v(c["client"])
    c["vcpu_peak"] = max(load_peak if "load" in c["phases"] else 0, measure_peak if "measure" in c["phases"] else 0)
    if quota is not None and c["mode"] == "run" and c["vcpu_peak"] > quota:
        refuse(f"{c['vcpu_peak']} vCPU on at once, the quota is {quota}: ask for more (Service Quotas, L-1216C47A) "
               f"or choose smaller machines")
    c["arches"] = sorted({TYPES[c[r]][2] for r in ("cook", "spare", "log", "compactor", "loader") if c[r]} | {"x86_64"})
    return c


def main():
    raw = json.load(open(sys.argv[1]))
    quota = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None
    c = resolve(raw, quota)
    json.dump(c, open(sys.argv[2], "w"), indent=1)
    for k in ("mode", "engine", "data", "pantry_mode", "vcpu_peak"):
        print(f"{k}={c[k]}")
    print(f"x86={'1' if 'x86_64' in c['arches'] else '0'}")
    print(f"arm={'1' if 'aarch64' in c['arches'] else '0'}")


if __name__ == "__main__":
    main()
