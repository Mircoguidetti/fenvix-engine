"""Step 9 · the round, from the runner (GitHub Actions in mode aws, this machine in mode local).

The kitchen it builds (all in eu-west-1, the zones of Atlas's three M30 nodes):
  log-0, log-1, log-2   the 3-zone log, one copy per zone (a, b, c); log-0 runs on the cook's machine
At most 8 vCPU are on at the same time (founder, 2026-11-09: the on-demand quota stays 8): decide.py refuses a command
that would need more, and the phases below never overlap their machines.
  server-0              cook 0, the loader (zone a): the data go in through it; switched off after the load
  server-1              the measured cook (zone a, like the client), new machine, new process, cold
  spare                 the warm spare (zone b), same type as the cook, same pantry and log
  compactor             only if the command gives one (compaction 2, step 4), zone c
  client                the m30 client (zone a): kitchen/client.sh UNCHANGED, the founder's script unchanged, the Atlas
                        program m30-corto, the Atlas state files (PLANS: SAME before any measurement)
  filler                only for data 100gb and 1tb with ballast_mode "script": the ballast through the founder's
                        script, at the same time as the client's load
  generator             only for data 100gb and 1tb with ballast_mode "generate" (the default): `mongo-server
                        <server_options> --ballast`, the ballast written straight into the pantry as sealed runs and
                        our manifest (mongo-server/src/ballast.rs), alone on its machine after the Atlas databases
  extras                after the m30 client: the separate programs (S3 first byte, sealed run, log ack, ballast
                        rounds, takeovers)
Order: buckets -> packages, CA, password -> network -> load: cook 0 + the m30 client (Atlas databases) OR cook 0 + the
filler (ballast) -> cook 0 off -> cook 1 with log-0, log-1, log-2 (+ compactor) -> the m30 program -> client off ->
spare + extras client -> everything off -> results.

RULES.md (founder, 2026-11-04), in this file:
  - a SPESA line every minute: machines (each one's own minutes at its DECLARED price, gp3 disk, IPv4) plus S3 (our
    servers' own request counters at the DECLARED prices, plus storage, plus a margin);
  - BLOCCO: when the estimated spend reaches the approved cap every machine is terminated at once and the round stops;
  - the teardown (teardown.py) and the final check (final_check.py) are separate steps of the workflow that always run.
Every machine powers itself off at the time ceiling (first line of its user-data), so none outlives the job.

Usage: run.py RESOLVED_COMMAND_JSON OUT_DIR   (mode, run id, password in the environment: S9_MODE, RUN_ID, CM_PW)
Exit 0 = the round finished (outcomes inside), 1 = it stopped early (BLOCCO, time ceiling, a machine failed)."""
# DECISIONS 2026-11-11 point 3: with the 8-vCPU quota the m30 program's maximum-load cells are not like for like; they
# are printed by the founder's program (unchanged) and must be read as NOT VALID, never promised.
MAX_LOAD_NOTE = ("m30 maximum-load cells \"hot reads per second\" and \"peak writes\": NOT VALID by design (DECISIONS "
                 "2026-11-11, 8-vCPU quota; not promised). Peak writes with the real indexes: the write-peak program.")
import datetime as dt
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import outcomes as outcomes_rules  # noqa: E402
import infra  # noqa: E402
from machine_types import TYPES, minute_cost  # noqa: E402
if os.environ.get("S9_MODE", "aws") == "aws":
    import aurora  # noqa: E402  (boto3 RDS; local mode has no Aurora)

HERE = os.path.dirname(os.path.abspath(__file__))
cmd = json.load(open(sys.argv[1]))
OUT = sys.argv[2]
os.makedirs(OUT, exist_ok=True)
MODE = os.environ.get("S9_MODE", "aws")
RUN = os.environ.get("RUN_ID", "0")
MINUTE_S = float(os.environ.get("MINUTE_S", "60"))
CAP = float(cmd["spend_cap"])
CEILING_MIN = float(cmd["ceiling_min"])
DISK_GB = int(os.environ.get("DISK_GB", "20"))
BUCKET = f"fenvix-coldread-s9-{RUN}"
TK_BUCKET = f"fenvix-coldread-s9t-{RUN}"
PANTRY = cmd["pantry_name"]   # decided by decide.py (new, kept, or an existing kept pantry)
# aws-fixes (total review, question 1 point 4): on Atlas the client was in eu-west-1a and the primary in another zone
# (euw1-az2, MEASURED from the hello tags): every client goes to another zone than the cook (eu-west-1b) unless the
# command says "client_zone": "same". The cook, the Postgres machine and Aurora stay in eu-west-1a.
CLIENT_ZONE = infra.ZONES[1] if cmd.get("client_zone", "other") == "other" else infra.ZONES[0]
T0 = time.time()
plat = infra.make(MODE)
s3 = plat.s3
log_f = open(os.path.join(OUT, "runner.log"), "a")


def say(msg):
    line = f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}"
    print(line, flush=True)
    log_f.write(line + "\n")
    log_f.flush()


# ---------- credentials (aws mode: the OIDC credentials last one hour; renewed every 45 minutes) ----------
last_renew = time.time()


def renew(force=False):
    global last_renew, plat, s3
    if MODE != "aws" or not os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
        return
    if not force and time.time() - last_renew < 2700:
        return
    import urllib.request
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    req = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
                                 headers={"Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    tok = json.loads(urllib.request.urlopen(req).read())["value"]
    sts = boto3.client("sts", region_name="eu-west-1", config=Config(signature_version=UNSIGNED))
    c = sts.assume_role_with_web_identity(RoleArn=os.environ["CM_ROLE"], RoleSessionName="step9-run", WebIdentityToken=tok,
                                          DurationSeconds=3600)["Credentials"]
    for k, v in (("AWS_ACCESS_KEY_ID", c["AccessKeyId"]), ("AWS_SECRET_ACCESS_KEY", c["SecretAccessKey"]),
                 ("AWS_SESSION_TOKEN", c["SessionToken"])):
        os.environ[k] = v
        if k != "AWS_ACCESS_KEY_ID":
            print(f"::add-mask::{v}")
    keep = (plat.subnet, plat.group, plat.ami)
    plat = infra.make(MODE)
    plat.subnet, plat.group, plat.ami = keep
    s3 = plat.s3
    last_renew = time.time()


# ---------- the run bucket: messages between the runner and the machines ----------
def put(key, obj):
    body = obj if isinstance(obj, bytes) else (obj if isinstance(obj, str) else json.dumps(obj)).encode()
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)


def get(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def last_line(key):
    try:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode(errors="replace").strip().splitlines()[-1]
    except Exception:
        return None


# ---------- spend ----------
machines = {}   # id -> {"name", "type", "start", "end"}
au = {"start": None, "end": None}   # the Aurora cluster of the agent benchmark (aurora.py), if any
S3_PRICES = {"storage_gb_month": 0.023}     # DECLARED, S3 Standard eu-west-1 (fact check 2026-11-08)
S3_MARGIN = float(os.environ.get("S3_MARGIN", "0.10"))
spend_now = 0.0


def s3_dollars():
    """Our servers' own S3 request counters (results/<name>/s3-counters.json, written every 30 s by s3_counters.py),
    plus the storage of the data for the hours so far, plus a margin for the client's files and the counters' delay."""
    tot = 0.0
    for m in machines.values():
        if m["role"] in ("loader", "cook", "spare", "compactor", "generator"):
            c = get(f"results/{m['name']}/s3-counters.json") or {}
            if c.get("available"):
                tot += float(c.get("dollars", 0))
            else:
                tot += float(cmd["s3_fallback_dollars"]) / 3   # no counters: a fixed allowance per server (upper bound)
    hours = (time.time() - T0) / 3600
    storage = float(cmd["data_gb_upper"]) * S3_PRICES["storage_gb_month"] / 730 * hours
    return tot + storage + S3_MARGIN + float(cmd.get("extras_s3_dollars", 0))


def spend():
    """The SPESA line. Machines: every launched machine from its launch to its end (or now), at its price."""
    global spend_now
    now = time.time()
    alive_ids = {i for i, _ in plat.alive()}
    mach = 0.0
    for i, m in machines.items():
        if m["end"] is None and i not in alive_ids and now - m["start"] > 120:
            m["end"] = now
        end = m["end"] or now
        mach += minute_cost(m.get("disk", DISK_GB), [m["type"]]) * max(1.0, (end - m["start"]) / 60)
    s3d = s3_dollars()
    aud = 0.0
    if au["start"]:
        # DECLARED instance price for every started hour (RDS bills by the second after the first 10 minutes; the hour
        # is an upper bound), plus the I/O and storage allowance
        hours = ((au["end"] or now) - au["start"]) / 3600
        aud = aurora.PRICE_HOUR[aurora.CLASS] * max(1.0, hours) + aurora.IO_ALLOWANCE
    xz = outcomes_rules.cross_zone_allowance(cmd, (now - T0) / 3600)   # total review N5
    spend_now = mach + s3d + aud + xz
    alive = sorted(m["type"] for i, m in machines.items() if m["end"] is None)
    say(f"SPESA: {spend_now:.2f} $ so far at {int((now - T0) / 60)} minutes (upper bound; machines {mach:.2f} $, alive: "
        f"{', '.join(alive) or 'none'}; S3 {s3d:.2f} $ from the servers' counters, storage and margin"
        + (f"; Aurora {aud:.2f} $ ({'deleted' if au['end'] else 'on'})" if au["start"] else "")
        + f"; cross-zone allowance {xz:.2f} $ ESTIMATED), cap {CAP:.2f} $")
    return spend_now


class Stop(Exception):
    pass


def minute(watch=None):
    """One minute of waiting: credentials, the SPESA line, BLOCCO, the time ceiling, the last line of a machine."""
    time.sleep(MINUTE_S)
    renew()
    if spend() >= CAP:
        say(f"BLOCCO: estimated spend {spend_now:.2f} $ >= {CAP:.2f} $: terminating every machine, the teardown follows")
        plat.terminate([i for i, _ in plat.alive()])
        for m in machines.values():
            m["end"] = m["end"] or time.time()
        if au["start"] and not au["end"]:
            aurora_off(wait=False)
        raise Stop("BLOCCO_SPESA")
    if (time.time() - T0) / 60 >= CEILING_MIN:
        say(f"TIME CEILING: {CEILING_MIN:.0f} minutes")
        raise Stop("TIME_CEILING")
    if watch:
        line = last_line(f"results/{watch}/steps.log")
        if line:
            say(f"  {watch}: {line[:220]}")


def wait_for(key, watch=None, limit_min=None, what=None):
    t = time.time()
    while not exists(key):
        minute(watch)
        if limit_min and (time.time() - t) / 60 > limit_min:
            raise Stop(f"NO_{(what or key).upper().replace('/', '_').replace('.', '_').replace('-', '_')}")


# ---------- machines ----------
def user_data(name, role, package, ceiling_min):
    """The script a machine runs at boot. First line: power off at the time ceiling (aws). Then the package from a
    presigned link (no AWS keys needed for it), then node.sh. A failed download powers the machine off at once."""
    if MODE == "local":
        url = infra.boto3.client("s3", region_name="eu-west-1", endpoint_url=plat.machine_endpoint).generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": f"program/{package}"}, ExpiresIn=21600)
        return (f"set -u; mkdir -p /opt/step9 && cd /opt/step9 && for i in 1 2 3 4 5; do curl -fsS -o p.tgz '{url}' && break; sleep 1; done; "
                f"tar xzf p.tgz && exec bash ./node.sh {role} {name} {BUCKET} > node.log 2>&1")
    url = plat.presign(BUCKET, f"program/{package}")
    return f"""#!/bin/bash
shutdown -h +{int(ceiling_min)} "step9: time ceiling" || true
mkdir -p /opt/step9 && cd /opt/step9 || {{ shutdown -h now "step9: no folder"; exit 1; }}
ok=0; for i in 1 2 3 4 5; do curl -fsS -o p.tgz '{url}' && {{ ok=1; break; }}; sleep 10; done
[ "$ok" = 1 ] || {{ shutdown -h now "step9: download failed"; exit 1; }}
tar xzf p.tgz || {{ shutdown -h now "step9: tar failed"; exit 1; }}
nohup bash ./node.sh {role} {name} {BUCKET} > node.log 2>&1 &
"""


def launch(name, role, mtype, zone, disk_gb=None):
    renew()
    arch = TYPES[mtype][2]
    package = "client.tar.gz" if role in ("client", "filler", "extras") else f"node-{arch}.tar.gz"
    left = CEILING_MIN - (time.time() - T0) / 60 + 5
    mid, ip = plat.launch(name, role, mtype, zone, user_data(name, role, package, left), arch, disk_gb=disk_gb)
    machines[mid] = {"name": name, "role": role, "type": mtype, "zone": zone, "ip": ip, "start": time.time(), "end": None,
                     "disk": disk_gb or DISK_GB}
    say(f"{name}: {role} {mtype} in {zone}, {mid}, private IP {ip}")
    return mid, ip


def aurora_off(wait=True):
    """Delete the Aurora cluster (no final snapshot). wait=False (BLOCCO): start the writer's deletion only; the
    teardown step finishes it and the final check looks."""
    try:
        if wait:
            # a SPESA line on every poll; never BLOCCO in the middle of a deletion
            gone = aurora.delete(RUN, say=say, wait=lambda: (time.sleep(min(20, MINUTE_S)), spend()))
            say(f"aurora: {'deleted' if gone else 'NOT deleted yet (the teardown step retries)'}")
        else:
            au["blocked"] = True
            n = aurora.names(RUN)
            aurora.clients()[0].delete_db_instance(DBInstanceIdentifier=n["instance"], SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
            say("aurora: writer deletion started (BLOCCO); the teardown step deletes the cluster")
    except Exception as e:
        say(f"aurora: delete: {type(e).__name__}: {str(e)[:200]} (the teardown step retries)")
    au["end"] = au["end"] or time.time()


def quota_free(ids):
    """aws-fixes: wait until machines just terminated are gone before launching the next ones of the 8-vCPU plan (on
    AWS a machine shutting down can still count against the vCPU quota for a short while). Never longer than 10 min."""
    w = getattr(plat, "wait_terminated", None)
    if w and ids:
        try:
            w([i for i in ids if i])
        except Exception as e:  # noqa: BLE001 - the launch that follows says it if the quota is still taken
            say(f"waiting for termination: {type(e).__name__}: {str(e)[:120]}")


def cert_for(name, ip):
    pem = os.path.join(OUT, f".{name}.pem")
    subprocess.run(["bash", os.path.join(HERE, "..", "..", "kitchen", "ci", "certs.sh"), "server", os.environ["CA_DIR"], ip, pem], check=True)
    s3.upload_file(pem, BUCKET, f"{name}/server.pem")
    os.remove(pem)


def stop(name, mid, wait_s=180, terminate=True):
    """Ask a machine to stop by itself (final logs into the bucket), then terminate it anyway (not when another
    process of the same machine still has to stop: the cook beside log-0)."""
    put(f"{name}/stop", "1")
    t = time.time()
    while time.time() - t < wait_s and not exists(f"{name}/stopped"):
        time.sleep(min(10, MINUTE_S))
    say(f"{name}: {'stopped by itself' if exists(f'{name}/stopped') else 'no answer to stop' + (': terminating' if terminate else '')}")
    if terminate:
        plat.terminate([mid])
        machines[mid]["end"] = time.time()


def cook_sha():
    """sha256 of the measured cook's mongo-server, from its ready.json (cook.sh): every program result carries it."""
    try:
        return (get("server-1/ready.json") or {}).get("mongo_server_sha256", "")
    except Exception:  # noqa: BLE001 - an empty hash makes the results fail the gate's binary check, never pass it
        return ""


def server_config(name, role, logs):
    opts = cmd["server_options"] if role in ("cook", "spare", "generator") else (cmd["compactor_options"] if role == "compactor" else cmd.get("loader_options", ""))
    put(f"{name}/config.json", {"pantry": PANTRY, "server_options": opts, "logs": logs, "log_id": "cell1", "takeover_bucket": TK_BUCKET})


# ---------- the round ----------
PHASES = cmd["phases"]
outcome, outcomes = None, {}
red_programs = []   # total review R6: reasons the job is red although it ran to its end
client_id = None
try:
    say(f"step 9 round {RUN} ({MODE}): phases {PHASES}, data {cmd['data']}, pantry {PANTRY} ({cmd['pantry_mode']}), "
        f"cook {cmd['cook']}, spare {cmd['spare']}, log {cmd['log']} x3, compactor {cmd.get('compactor') or 'none'}, "
        f"client {cmd['client']}, programs {cmd['programs']}, cap {CAP} $, {CEILING_MIN:.0f} minutes")
    say("1/8 buckets: the run bucket (1 day) and the pantry (our server's data)")
    plat.new_bucket(BUCKET, 1)
    if cmd["pantry_mode"] == "new":
        plat.new_bucket(PANTRY, 1)
    elif cmd["pantry_mode"] == "keep":
        plat.new_bucket(PANTRY, int(cmd["keep_days"]))
        say(f"pantry {PANTRY}: KEPT after this run for the next ones; its objects expire after {cmd['keep_days']} days")
    else:
        s3.head_bucket(Bucket=PANTRY)
        born = [b["CreationDate"] for b in s3.list_buckets()["Buckets"] if b["Name"] == PANTRY][0]
        age = (dt.datetime.now(dt.timezone.utc) - born).total_seconds() / 86400
        if age > float(cmd["kept_max_days"]):
            raise Stop(f"PANTRY_TOO_OLD ({age:.1f} days; its oldest objects expire first and would break the databases)")
        say(f"pantry {PANTRY}: existing, created {age:.2f} days ago")
    say("2/8 packages, CA and the password into the run bucket (the password passes through no file and no log)")
    for f in sorted(os.listdir(os.environ["PACKAGES"])):
        if f.endswith(".tar.gz"):
            s3.upload_file(os.path.join(os.environ["PACKAGES"], f), BUCKET, f"program/{f}")
    s3.upload_file(os.path.join(os.environ["CA_DIR"], "ca.pem"), BUCKET, "program/ca.pem")
    put("secret/pw", os.environ["CM_PW"])
    if cmd["pantry_mode"] == "keep":
        # the root user lives in the pantry: later rounds on this pantry must use the same password (the workflow and
        # the local mode read it back from here, masked); the object goes away with the pantry
        s3.put_object(Bucket=PANTRY, Key="step9-secret/pw", Body=os.environ["CM_PW"].encode())
    if "takeovers" in cmd["programs"] and "extras" in PHASES:
        # the takeover pair's own data bucket: its databases and its users stay apart from the measured cook's (the
        # cook's process owns the pantry's admin database, where the users live, for as long as it runs)
        plat.new_bucket(TK_BUCKET, 1)
    if cmd.get("filler"):
        put("filler/plan.json", {"pantry": PANTRY, **cmd["ballast"]})
    if cmd.get("generator") and "load" in PHASES:
        # the generator's plan, made here from the founder's script itself (imported unchanged, md5 checked): its
        # customers, sizes, dormant ones, vocabulary and timeline (step9/machine/ballast_plan.py)
        lines_f, plan_f = os.path.join(OUT, "ballast-lines.json"), os.path.join(OUT, "ballast-plan.json")
        json.dump(cmd["ballast"]["lines"], open(lines_f, "w"))
        r = subprocess.run([sys.executable, os.path.join(HERE, "..", "machine", "ballast_plan.py"),
                            os.path.join(HERE, "..", "..", "kitchen", "irlanda"), lines_f, plan_f, "--spill-dir", "work/spill",
                            "--out", "out/ballast-gen", "--parallel", str(cmd["ballast"]["parallel"])],
                           capture_output=True, text=True)
        say(f"ballast plan: {(r.stdout or r.stderr).strip()[-300:]}")
        if r.returncode != 0:
            raise Stop("NO_BALLAST_PLAN")
        s3.upload_file(plan_f, BUCKET, "generator/plan.json")
    say("3/8 network")
    say(f"network: {json.dumps(plat.setup(RUN, cmd['network']))}")

    logs, LOGS = {}, ""
    say("4/8 the load runs without the log (the loader writes straight to S3; nothing is measured while loading)")

    if "load" in PHASES and (cmd["atlas_load"] or cmd.get("filler")):
        who = (["the m30 client (the Atlas databases)"] if cmd["atlas_load"] else []) + (["the filler (ballast)"] if cmd.get("filler") else [])
        say(f"5/8 the load through cook 0 (the loader): {', '.join(who)}")
        server_config("server-0", "loader", LOGS if cmd["loader_uses_log"] else "")
        mid0, ip0 = launch("server-0", "loader", cmd["loader"], infra.ZONES[0])
        cert_for("server-0", ip0)
        put("server-0/go.json", {"type": cmd["loader"], "k": 0, "pantry": PANTRY, "prepare": False,
                                 "settle_max_min": cmd.get("settle_max_min", 30)})
        if cmd["atlas_load"]:
            client_id, _ = launch("client", "client", cmd["client"], CLIENT_ZONE)
        filler_id = launch("filler", "filler", cmd["filler"], infra.ZONES[0])[0] if cmd.get("filler") else None
        while (client_id and not exists("client/done-0.json") and not exists("client/end.json")) or (filler_id and not exists("filler/end.json")):
            minute("filler" if filler_id and not client_id else "client")
        if client_id:
            outcomes["load"] = (get("client/done-0.json") or {}).get("outcome", "NOT DONE")
        if filler_id:
            outcomes["ballast"] = (get("filler/end.json") or {}).get("outcome", "NOT DONE")
            plat.terminate([filler_id])
            machines[filler_id]["end"] = machines[filler_id]["end"] or time.time()
        say(f"load: {json.dumps(outcomes)}")
        stop("server-0", mid0)
        if outcomes.get("load") == "LOADED_NOT_SETTLED":
            # aws-fixes (B5): the data are in the pantry, but its compaction did not finish within settle_max_min
            # (client/settle.json says why): stop and say so; nothing is measured on it without reading that first
            raise Stop("LOADED_NOT_SETTLED")
        if outcomes.get("load", "LOADED") != "LOADED" or outcomes.get("ballast", "BALLAST_LOADED") != "BALLAST_LOADED":
            raise Stop("LOAD_FAILED")
    if "load" in PHASES and cmd.get("generator"):
        # alone on its machine (8 vCPU), after cook 0 is off: no cook, no client, no wire
        say(f"5/8 the ballast generator: {len(cmd['ballast']['lines'])} databases in our stored format, "
            f"{cmd['ballast']['parallel']} at a time, on {cmd['generator']}")
        server_config("generator", "generator", "")
        gen_id, _ = launch("generator", "generator", cmd["generator"], infra.ZONES[0], disk_gb=int(cmd["generator_disk_gb"]))
        while not exists("generator/end.json"):
            minute("generator")
        outcomes["ballast"] = (get("generator/end.json") or {}).get("outcome", "NOT DONE")
        plat.terminate([gen_id])
        machines[gen_id]["end"] = machines[gen_id]["end"] or time.time()
        say(f"ballast generator: {outcomes['ballast']}")
        if outcomes["ballast"] != "BALLAST_GENERATED":
            raise Stop("BALLAST_GENERATION_FAILED")
    if "load" not in PHASES:
        say("5/8 no load in this round: the pantry was loaded by an earlier round")

    mid1 = ip1 = sp = comp = None
    if "measure" in PHASES:
        # at most 8 vCPU at once (founder, 2026-11-09): log-0 lives on the cook's own machine (as Atlas keeps its oplog
        # on its nodes), log-1 and log-2 on small machines in zones b and c; the spare comes later, with the extras
        say("6/8 the measured kitchen: cook 1 with log-0 (zone a), log-1 (zone b), log-2 (zone c)"
            + (", the compactor (zone c)" if cmd.get("compactor") else ""))
        mid1, ip1 = launch("server-1", "cooklog" if cmd["log_colocate"] else "cook", cmd["cook"], infra.ZONES[0])
        cert_for("server-1", ip1)
        logs["log-0"] = (mid1, ip1) if cmd["log_colocate"] else launch("log-0", "log", cmd["log"], infra.ZONES[0])
        for i in (1, 2):
            logs[f"log-{i}"] = launch(f"log-{i}", "log", cmd["log"], infra.ZONES[i])
        put("log/peers.json", {"ips": [logs[f"log-{i}"][1] for i in range(3)]})
        for i in range(3):
            wait_for(f"log-{i}/ready.json", watch=f"log-{i}", limit_min=20, what=f"log-{i}")
        LOGS = ",".join(f"{logs[f'log-{i}'][1]}:7200" for i in range(3))
        say(f"log ready: {LOGS}")
        server_config("server-1", "cook", LOGS)
        if cmd.get("compactor"):
            server_config("compactor", "compactor", LOGS)
            comp = launch("compactor", "compactor", cmd["compactor"], infra.ZONES[2])
            cert_for("compactor", comp[1])
        if "m30" in cmd["programs"]:
            if not client_id:
                client_id, _ = launch("client", "client", cmd["client"], CLIENT_ZONE)
            put("server-1/go.json", {"type": cmd["cook"], "k": 1, "pantry": PANTRY, "prepare": bool(cmd.get("prepare")),
                                     "settle_max_min": cmd.get("settle_max_min", 30)})
            while not exists("client/done-1.json") and not exists("client/end.json"):
                minute("client")
            outcomes["m30"] = (get("client/done-1.json") or {}).get("outcome", "NOT DONE")
            say(f"m30 program on cook 1: {outcomes['m30']}")
            say(MAX_LOAD_NOTE)
        else:
            wait_for("server-1/ready.json", watch="server-1", limit_min=20, what="cook")
    if client_id:
        put("client/enough", "1")
        for _ in range(40):
            if exists("client/end.json"):
                break
            time.sleep(min(15, MINUTE_S))
        end_client = get("client/end.json") or {}
        outcomes["client"] = end_client.get("outcome", "NO END")
        plat.terminate([client_id])
        machines[client_id]["end"] = time.time()
        # total review intermediate 2: a client that found PLANS: DIFFERENT turns the job red (it ended green before)
        red = outcomes_rules.client_end_blocker(end_client)
        if red:
            raise Stop(red)

    extras = [p for p in cmd["programs"] if p not in ("m30", "agent-bench", "write-peak")]
    if "extras" in PHASES and extras and mid1:
        if cmd.get("spare"):
            say("7/8 the warm spare (zone b), now that the m30 client is off")
            server_config("spare", "spare", LOGS)
            sp = launch("spare", "spare", cmd["spare"], infra.ZONES[1])
            cert_for("spare", sp[1])
            wait_for("spare/ready.json", watch="spare", limit_min=20, what="spare")
        say(f"7/8 the extras client: {extras}")
        put("extras/config.json", {"engine_sha256": cook_sha(), "programs": extras, "pantry": PANTRY, "logs": LOGS, "bench_logs": LOGS.replace(":7200", ":7201"),
                                   "cook": {"ip": ip1, "port": 27017}, "spare": {"ip": sp[1]} if sp else {},
                                   **{k: v for k, v in cmd.items() if k.endswith("_args")}})
        ex_id, _ = launch("extras", "extras", cmd["extras_client"], CLIENT_ZONE)
        wait_for("extras/end.json", watch="extras")
        end_ex = get("extras/end.json")
        outcomes["extras"] = (end_ex or {}).get("programs", "")
        red_programs.append(outcomes_rules.programs_blocker("extras", end_ex, extras))
        plat.terminate([ex_id])
        machines[ex_id]["end"] = time.time()
    else:
        say("7/8 no extra program in this round")
    if "extras" in PHASES and "write-peak" in cmd["programs"] and mid1:
        # aws-fixes (B4): peak writes with the real indexes through the MEASURED cook (log on, the round's options): the
        # founder's script's own load, the shape of the Atlas load that gave 25,800 documents per second (6 databases,
        # 16 threads each); 8 vCPU: the spare goes, a client of the m30 client's type comes
        if sp:
            stop("spare", sp[0], wait_s=120)
            quota_free([sp[0]])
            sp = None
        quota_free([i for i, m in machines.items() if m["name"] == "extras"])
        say(f"7/8 write-peak: {cmd['write_peak_args']} through the measured cook, client {cmd['client']} in {CLIENT_ZONE}")
        put("peak/config.json", {"engine_sha256": cook_sha(), "programs": ["write-peak"], "pantry": PANTRY, "logs": LOGS,
                                 "cook": {"ip": ip1, "port": 27017}, "write_peak_args": cmd["write_peak_args"]})
        pk_id, _ = launch("peak", "extras", cmd["client"], CLIENT_ZONE)
        wait_for("peak/end.json", watch="peak")
        end_pk = get("peak/end.json")
        outcomes["write-peak"] = (end_pk or {}).get("programs", "")
        red_programs.append(outcomes_rules.programs_blocker("write-peak", end_pk, ["write-peak"]))
        plat.terminate([pk_id])
        machines[pk_id]["end"] = time.time()
    pg = None
    if "extras" in PHASES and "agent-bench" in cmd["programs"] and mid1:
        # 8 vCPU: the spare goes, the Postgres machine (the Supabase stand-in) comes, then the agent client
        if sp:
            stop("spare", sp[0], wait_s=120)
            quota_free([sp[0]])
            sp = None
        quota_free([i for i, m in machines.items() if m["name"] in ("extras", "peak")])
        say("7/8 the agent benchmark: the Postgres machine (zone a; single node, no copy: NOT equal durability)"
            + (", the Aurora cluster (the equal-durability line: 4 of 6 copies in 3 zones, DECLARED by AWS)" if cmd.get("aurora") else "")
            + f" and the agent client ({CLIENT_ZONE})")
        if cmd.get("aurora"):
            if MODE != "aws":
                raise Stop("AURORA_ONLY_ON_AWS")
            aurora.create(RUN, os.environ["CM_PW"], say=say)
            au["start"] = time.time()
        pg = launch("pgbox", "pgbox", cmd["pgbox"], infra.ZONES[0])
        wait_for("pgbox/ready.json", watch="pgbox", limit_min=20, what="pgbox")
        if cmd.get("aurora"):
            t_au = time.time()
            while True:
                cs, ins, ep = aurora.status(RUN)
                say(f"aurora: cluster {cs}, writer {ins}")
                if cs == "available" and ins == "available" and ep:
                    break
                if (time.time() - t_au) / 60 > 40:
                    raise Stop("AURORA_NOT_READY")
                minute(None)
            put("secret/aurora-uri", aurora.uri(ep, os.environ["CM_PW"]))
            say(f"aurora: ready after {(time.time() - au['start']) / 60:.1f} minutes")
            try:   # B10: the cluster's and the writer's settings as AWS describes them, kept with the results
                json.dump(aurora.describe(RUN), open(os.path.join(OUT, "aurora-describe.json"), "w"), indent=1, default=str)
            except Exception as e:  # noqa: BLE001 - never stops the round; the SQL side (pg_settings) is saved anyway
                say(f"aurora: describe: {type(e).__name__}: {str(e)[:160]}")
        put("agent/config.json", {"engine_sha256": cook_sha(), "programs": ["agent-bench"], "pantry": PANTRY, "logs": LOGS,
                                  "cook": {"ip": ip1, "port": 27017}, "pgbox": {"ip": pg[1]},
                                  "agent_bench_args": cmd["agent_bench_args"]})
        ag_id, _ = launch("agent", "extras", cmd["extras_client"], CLIENT_ZONE)
        # B10: Aurora runs FIRST (agent_bench.py's order), and is deleted as soon as its target is done
        # (agent/aurora-done), not after the other targets: it is not paid while idle
        while not exists("agent/end.json"):
            if au["start"] and not au["end"] and exists("agent/aurora-done"):
                say("aurora: its target is done: deleting the cluster now, while the other targets run")
                aurora_off()
            minute("agent")
        end_ag = get("agent/end.json")
        outcomes["agent"] = (end_ag or {}).get("programs", "")
        red_programs.append(outcomes_rules.programs_blocker("agent-bench", end_ag, ["agent-bench"]))
        plat.terminate([ag_id])
        machines[ag_id]["end"] = time.time()
        if au["start"] and not au["end"]:
            say("aurora: the agent benchmark is done: deleting the cluster now")
            aurora_off()
    say("8/8 everything off")
    # the cook first (on its machine log-0 keeps running until its own stop), log-0 last: it powers that machine off
    if mid1:
        stop("server-1", mid1, wait_s=120, terminate=not cmd["log_colocate"])
    for name, pair in ([("spare", sp)] if sp else []) + ([("pgbox", pg)] if pg else []) + ([("compactor", comp)] if comp else []) \
            + sorted(logs.items(), key=lambda kv: kv[0] == "log-0"):
        stop(name, pair[0], wait_s=120)
    if "measure" in PHASES and "m30" in cmd["programs"]:
        outcome = "FINITO" if outcomes.get("m30", "").startswith("FINITO") else outcomes.get("m30", "NOT DONE")
    else:
        outcome = "FINITO"
    # total review R6: a program that failed, was skipped or did not run makes the job red, m30 or not
    red_programs = [r for r in red_programs if r]
    if red_programs:
        outcome = "PROGRAM_FAILED: " + " | ".join(red_programs)
        say(f"round red: {outcome}")
except Stop as e:
    outcome = str(e)
    say(f"round stopped: {outcome}")
except Exception as e:
    outcome = f"RUNNER_ERROR {type(e).__name__}: {e}"
    say(outcome)
finally:
    try:
        plat.terminate([i for i, m in machines.items() if m["end"] is None])
        for m in machines.values():
            m["end"] = m["end"] or time.time()
        if au["start"] and not au["end"]:
            au["blocked"] = True   # no BLOCCO loop from here: plain waits
            aurora_off()
        spend()
        say("copying the results from the run bucket")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="results/"):
            for o in page.get("Contents", []):
                dest = os.path.join(OUT, o["Key"][len("results/"):])
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                s3.download_file(BUCKET, o["Key"], dest)
    except Exception as e:
        say(f"after the round: {type(e).__name__}: {e} (the teardown step runs anyway)")
    zones = {}
    for m in machines.values():
        z = None
        try:
            z = json.load(open(os.path.join(OUT, m["name"], "zone.json")))
        except Exception:  # noqa: BLE001
            z = get(f"results/{m['name']}/zone.json")
        if z:
            zones[m["name"]] = z
            m["zone_id"] = z.get("zone_id")
    if zones:
        say("zones (name, id): " + "; ".join(f"{k} {v.get('zone')} {v.get('zone_id')}" for k, v in sorted(zones.items())))
    json.dump({"outcome": outcome, "outcomes": outcomes, "client_zone": CLIENT_ZONE, "zones": zones, "minutes": round((time.time() - T0) / 60, 1),
               "estimated_spend_upper_bound": round(spend_now, 4), "cap": CAP, "command": cmd,
               "machines": [{k: v for k, v in m.items() if k != "ip"} | {"id": i} for i, m in machines.items()]},
              open(os.path.join(OUT, "run.json"), "w"), indent=1)
    say(f"outcome: {outcome}; {json.dumps(outcomes)}")
sys.exit(0 if outcome == "FINITO" else 1)
