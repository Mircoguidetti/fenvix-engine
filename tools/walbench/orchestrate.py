#!/usr/bin/env python3
"""walbench orchestrator (GitHub Actions runner, OIDC role session; eu-west-1).

 1. current prices (infra.prices) and an estimate of the whole plan: stop before creating anything if it is above
    spend.max_estimate_usd
 2. resources (infra.create), files to s3://<bucket>/runs/<run>/files/, instances: client + 3 quorum nodes (the EBS
    standby is launched when the client asks for it, and the nodes are terminated as soon as the quorum part is done)
 3. every 20 s: live spend from the instance-side request counters + instance/volume/IPv4 time at the current prices,
    one row in cost_live.csv; at spend.stop_usd (4 USD) the run stops: stop marker, then teardown (cap 5 USD)
 4. EBS reattach cycles: force-detach the log volume from the instance writing to it, attach it to the other one,
    timestamps on both sides
 5. results downloaded to results-walbench/raw/, then teardown + verify (the workflow runs teardown again, always)
Nothing secret is printed: AWS errors are reduced to their code; ARNs and the account id never reach the log."""
import base64
import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import infra  # noqa: E402
from infra import c, code, log  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("WALBENCH_OUT", "results-walbench")
RAW = f"{OUT}/raw"
RUN = os.environ.get("GITHUB_RUN_ID", "local")
PLAN = json.load(open(f"{HERE}/plan.json"))
BIN = os.environ.get("WALBENCH_BIN", f"{HERE}/bench/target/release/walbench")
ROOT = f"runs/{RUN}/"
os.makedirs(RAW, exist_ok=True)
ST = {"run": RUN}
STOP = threading.Event()
ORCH_REQ = {"s3_put": 0, "s3_get": 0, "s3_list": 0}


def now():
    """The clock of the spend logic (replaced in tests)."""
    return time.time()


def save_state():
    json.dump(ST, open(f"{OUT}/state.json", "w"), indent=1, default=str)


# ------------------------------------------------------------------------------------------------- credentials
class Creds:
    at = 0.0  # unknown age at start (the workflow's session was made before the build): renewed on the first call

    @classmethod
    def renew(cls, max_age=25 * 60):
        """OIDC token -> new 1 h role session (only on the GitHub runner; values are masked, never written)."""
        if time.time() - cls.at < max_age or "ACTIONS_ID_TOKEN_REQUEST_URL" not in os.environ:
            return
        for attempt in (1, 2, 3):
            try:
                req = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
                                             headers={"Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
                tok = json.load(urllib.request.urlopen(req, timeout=30))["value"]
                print(f"::add-mask::{tok}", flush=True)
                from botocore import UNSIGNED
                from botocore.config import Config
                sts = infra.boto3.session.Session().client("sts", region_name=infra.REGION, config=Config(signature_version=UNSIGNED))
                r = sts.assume_role_with_web_identity(RoleArn=os.environ["WB_ROLE"], RoleSessionName=f"walbench-{RUN}-{int(time.time())}",
                                                      WebIdentityToken=tok, DurationSeconds=3600)
                cr = r["Credentials"]
                for v in (cr["SecretAccessKey"], cr["SessionToken"], cr["AccessKeyId"]):
                    print(f"::add-mask::{v}", flush=True)
                os.environ.update(AWS_ACCESS_KEY_ID=cr["AccessKeyId"], AWS_SECRET_ACCESS_KEY=cr["SecretAccessKey"], AWS_SESSION_TOKEN=cr["SessionToken"])
                cls.at = time.time()
                log("credentials renewed")
                return
            except Exception as e:
                log(f"credential renewal attempt {attempt} failed: {code(e)}")
                time.sleep(5)


# ----------------------------------------------------------------------------------------------------- S3 helpers
def s3put(key, body):
    ORCH_REQ["s3_put"] += 1
    c("s3").put_object(Bucket=ST["bucket"], Key=ROOT + key, Body=body)


def s3get_json(key):
    ORCH_REQ["s3_get"] += 1
    try:
        return json.loads(c("s3").get_object(Bucket=ST["bucket"], Key=ROOT + key)["Body"].read())
    except Exception as e:
        if code(e) in ("NoSuchKey", "404"):
            return None
        raise


def s3list(prefix):
    keys, tok = [], None
    while True:
        ORCH_REQ["s3_list"] += 1
        kw = {"Bucket": ST["bucket"], "Prefix": ROOT + prefix}
        if tok:
            kw["ContinuationToken"] = tok
        r = c("s3").list_objects_v2(**kw)
        keys += [o["Key"][len(ROOT):] for o in r.get("Contents", [])]
        tok = r.get("NextContinuationToken")
        if not r.get("IsTruncated"):
            return keys


def marker(name):
    return s3get_json(f"markers/{name}.json")


# -------------------------------------------------------------------------------------------------------- estimate
def estimate(F):
    """Upper-side estimate of the whole plan, priced at the fetched prices (requests counted as if nothing batched)."""
    P = PLAN
    ncells = len(P["sizes"]) * len(P["concs"])
    cell_writes = (P["cell"]["writes"] + P["cell"]["warmup"]) * ncells * len(P["cloud_variants"])
    fleet_writes = P["fleet"]["dbs"] * P["fleet"]["rate"] * (P["fleet"]["secs"] + P["fleet"]["warm_secs"]) * len(P["fleet"]["cells"])
    cr = P["crash"]
    per = cr["est_requests_per_cycle"]  # per backend: commits per cycle (writes are capped at max_writes per cycle)
    reqs = cell_writes + fleet_writes
    s3 = (reqs + cr["cycles"] * per["s3"]) * F["s3_put"] + cr["cycles"] * per["s3"] * F["s3_get"]  # recovery GETs one object per commit
    xz = (reqs + cr["cycles"] * per["express"]) * (F["xz_put"] + F["xz_get"] + 1100 / 2**30 * F["xz_upload_gb"])
    ddb = (reqs + cr["cycles"] * per["ddb"]) * 2 * F["ddb_wru"]
    inst_h = P["estimate_instance_hours"]
    ec2 = inst_h * (F["ec2_hour"] + F["ipv4_hour"]) + (inst_h * 10 + 4 * 20) * F["gp3_gb_month"] / 730
    total = s3 + xz + ddb + ec2 + 0.05
    return {"requests_per_cloud_backend_cells_fleet": reqs, "crash_commits_per_cycle": per, "s3": s3, "express": xz, "ddb": ddb, "ec2_ebs_ipv4": ec2, "usd": round(total, 3),
            "instance_hours_assumed": inst_h}


# ------------------------------------------------------------------------------------------------------ live cost
INST_T = {}  # instance id -> [launch_unix, end_unix or None]
VOL_T0 = [None]


ACTIVE_RATE = {"client": 2000}  # worst request rate assumed while a role's progress is stale (PUT/s at S3 standard price)


def role_active(role):
    """A role's instance is still running (no end time). Roles map to ST["instances"] keys: client, node0..2, standby."""
    iid = ST.get("instances", {}).get(role)
    return iid is not None and iid in INST_T and INST_T[iid][1] is None


class Live:
    steps = {}  # role -> steps running at its last progress upload
    last = {}  # role -> (unix, counters)
    usd = 0.0

    @classmethod
    def compute(cls, F, fetch=True):
        now_ = now()
        try:
            roles = [k[len("progress/"):-5] for k in s3list("progress/")] if fetch else []
        except Exception as e:
            log(f"progress list: {code(e)}")
            roles = []
        for role in roles:
            try:
                p = s3get_json(f"progress/{role}.json")
            except Exception as e:
                log(f"progress {role}: {code(e)}")
                p = None
            if p:
                cls.last[role] = (p["unix"], p["counters"])
                cls.steps[role] = p.get("steps") or []
        tot = {}
        stale_s = 0.0
        stale_margin = 0.0
        for role, (t, cn) in cls.last.items():
            if role_active(role):
                # only an instance that still runs can be making requests we have not seen yet
                st = max(now_ - t, 30.0)
                stale_s = max(stale_s, st)
                stale_margin += st * ACTIVE_RATE.get(role, 5) * F["s3_put"]
            for k, v in cn.items():
                if isinstance(v, list):
                    tot[k] = [a + b for a, b in zip(tot.get(k, [0] * len(v)), v)]
                else:
                    tot[k] = tot.get(k, 0) + v
        g = lambda k: tot.get(k, 0)  # noqa: E731
        req = ((g("s3_put") + g("s3_list") + ORCH_REQ["s3_put"] + ORCH_REQ["s3_list"]) * F["s3_put"] + (g("s3_get") + ORCH_REQ["s3_get"]) * F["s3_get"]
               + (g("xz_put") + g("xz_list")) * F["xz_put"] + g("xz_get") * F["xz_get"]
               + g("xz_put_bytes") / 2**30 * F["xz_upload_gb"] + g("xz_get_bytes") / 2**30 * F["xz_retrieval_gb"]
               + g("ddb_wcu") * F["ddb_wru"] + g("ddb_rcu") * F["ddb_rru"])
        qs, qr = tot.get("q_sent", [0, 0, 0]), tot.get("q_recv", [0, 0, 0])
        xaz = (qs[1] + qs[2] + qr[1] + qr[2]) / 2**30 * F["inter_az_gb"] * 2  # charged on both sides of the AZ boundary
        inst_s = sum(max(60.0, (e or now_) - s) for s, e in INST_T.values())
        ec2 = inst_s / 3600 * (F["ec2_hour"] + F["ipv4_hour"])
        ebs = (inst_s * 10 + (now_ - VOL_T0[0] if VOL_T0[0] else 0) * 20) / 3600 / 730 * F["gp3_gb_month"]
        # margins: +10% on counted requests, plus, for each role whose instance still runs, what it may have done
        # since its last progress upload (at least 30 s) at a worst rate (client 2000 PUT/s, others 5/s)
        margin = req * 0.10 + stale_margin
        cls.usd = req + xaz + ec2 + ebs + margin + 0.01
        return {"requests_usd": req, "inter_az_usd": xaz, "ec2_ipv4_usd": ec2, "ebs_usd": ebs, "margin_usd": margin, "total_usd": cls.usd,
                "instance_seconds": inst_s, "progress_stale_s": stale_s, "counters": tot}


# --------------------------------------------------------------------------------------- live spend visibility
class Visible:
    """(1) one spend line in the Actions log every 60 s; (2) cost_live.csv committed and pushed to branch walbench
    every 5 minutes (only that file; the path never matches the workflow's trigger paths go.json / teardown.json);
    (3) the final spend at the end. Pushing is on only with WALBENCH_GIT_PUSH=1 (set by the workflow)."""
    t0 = None
    last_line = 0.0
    last_push = 0.0
    pushes = []

    @classmethod
    def line(cls, cost, phase, force=False, prefix="SPEND"):
        t = now()
        if cls.t0 is None:
            cls.t0 = t
        if not force and t - cls.last_line < PLAN.get("visibility", {}).get("line_every_s", 60):
            return None
        cls.last_line = t
        el = int(t - cls.t0)
        steps = ", ".join(s for r in sorted(Live.steps) for s in Live.steps[r]) or "-"
        stop = PLAN["spend"]["stop_usd"]
        msg = (f"{prefix} | elapsed {el // 3600:d}h{el % 3600 // 60:02d}m{el % 60:02d}s | phase {phase} | running: {steps} | "
               f"{cost['total_usd']:.3f} USD = {100 * cost['total_usd'] / stop:.1f}% of the {stop:.2f} USD stop (cap {PLAN['spend']['cap_usd']:.2f})")
        print(msg, flush=True)
        return msg

    @classmethod
    def push(cls, force=False):
        if os.environ.get("WALBENCH_GIT_PUSH") != "1":
            return None
        t = now()
        if not force and t - cls.last_push < PLAN.get("visibility", {}).get("push_every_s", 300):
            return None
        cls.last_push = t
        return git_push_file(f"{OUT}/cost_live.csv", f"walbench run {RUN}: live spend")


TRIGGERS = ("tools/walbench/go.json", "tools/walbench/teardown.json")


def git_push_file(path, msg, cwd=None):
    """Commit ONLY `path` and push it to walbench, with pull --rebase retries. Never raises; never pushes a trigger
    path; refuses content that looks like an account id, ARN or key."""
    import re
    rel = os.path.relpath(path, cwd or os.getcwd())
    if rel in TRIGGERS or rel.startswith("tools/"):
        log(f"refusing to push {rel}: not a results path")
        return False
    data = open(path, "rb").read()
    if re.search(rb"arn:aws:|\b(AKIA|ASIA)[A-Z0-9]{16}\b|\b\d{12}\b", data):
        log(f"refusing to push {rel}: it contains something that looks like an identifier")
        return False
    g = lambda *a: subprocess.run(["git", *a], cwd=cwd, capture_output=True, text=True, timeout=120)  # noqa: E731
    g("add", "-f", "--", rel)
    r = g("commit", "-q", "-m", msg, "--", rel)
    if r.returncode and "nothing" not in (r.stdout + r.stderr):
        log(f"spend push: commit failed ({r.returncode})")
        return False
    for attempt in range(4):
        if g("pull", "--rebase", "--autostash", "-q", "origin", "walbench").returncode == 0 and g("push", "-q", "origin", "HEAD:walbench").returncode == 0:
            log(f"spend log pushed to walbench ({rel})")
            Visible.pushes.append(now())
            return True
        g("rebase", "--abort")
        time.sleep(2 * (attempt + 1))
    log("spend push: failed after 4 attempts (the run continues)")
    return False


# --------------------------------------------------------------------------------------------------------- launch
def user_data(role, extra):
    cfg = dict(run=RUN, bucket=ST["bucket"], express_bucket=ST["express_bucket"], table=ST["table"], volume=ST["volume"],
               region=infra.REGION, role=role, **extra)
    t = open(f"{HERE}/instance/userdata.sh.tmpl").read()
    return (t.replace("__MAX_MINUTES__", str(PLAN["instance_max_minutes"]))
             .replace("__CFG_B64__", base64.b64encode(json.dumps(cfg).encode()).decode())
             .replace("__FETCH_B64__", base64.b64encode(open(f"{HERE}/instance/s3fetch.py", "rb").read()).decode())
             .replace("__BUCKET__", ST["bucket"]).replace("__REGION__", infra.REGION).replace("__RUN__", RUN))


def launch(role, zone_key, extra):
    iid = infra.launch(RUN, ST, zone_key, user_data(role, extra))  # recorded as ST["instances"][zone_key]
    INST_T[iid] = [now(), None]
    if os.environ.get("WALBENCH_LOCAL_AGENTS"):  # local emulator test only: run the agent here instead of on EC2
        _local_agent(role, zone_key, extra)
    save_state()
    return iid


def _local_agent(role, key, extra):
    import shutil
    d = os.path.join(os.environ["WALBENCH_LOCAL_AGENTS"], key)
    os.makedirs(d, exist_ok=True)
    cfg = dict(run=RUN, bucket=ST["bucket"], express_bucket=ST["express_bucket"], table=ST["table"], volume=ST["volume"], region=infra.REGION, role=role, **extra)
    if role == "node":
        cfg["node_port"] = 7730 + extra["node_index"]
    json.dump(cfg, open(f"{d}/cfg.json", "w"))
    for f, src in [("walbench", BIN), ("agent.py", f"{HERE}/instance/agent.py"), ("node-supervisor.sh", f"{HERE}/instance/node-supervisor.sh"), ("plan.json", f"{HERE}/plan.json")]:
        shutil.copy(src, f"{d}/{f}")
    if role != "standby":
        subprocess.Popen([sys.executable, f"{d}/agent.py"], cwd=d, env=dict(os.environ, WALBENCH_HOME=d, WALBENCH_LOCAL_TEST="1"),
                         stdout=open(f"{d}/out.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)


def terminate(iids):
    """Never raises (the teardown after it must run): 3 attempts, then the teardown's own terminate step."""
    iids = [i for i in iids if i]
    if not iids:
        return True
    for attempt in range(3):
        try:
            c("ec2").terminate_instances(InstanceIds=iids)
            for i in iids:
                if i in INST_T and INST_T[i][1] is None:
                    INST_T[i][1] = now()
            log(f"terminated {len(iids)} instance(s)")
            return True
        except Exception as e:
            log(f"terminate attempt {attempt + 1} failed: {code(e)}")
            if code(e) in ("ExpiredToken", "ExpiredTokenException", "RequestExpired"):
                Creds.renew(max_age=0)
            time.sleep(3)
    return False


def terminate_all():
    return terminate([i for i, (s, e) in INST_T.items() if e is None])


# --------------------------------------------------------------------------------------------------------- quota
QUOTA_CODE = "L-1216C47A"  # EC2: Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances, in vCPUs


def instance_budget():
    """How many c6i.large (2 vCPUs) may run at once: 4 only if the free vCPU quota is >= 8, else 3 (also when the
    quota cannot be read: AccessDenied etc.). Used vCPUs come from DescribeInstances (pending/running, any type)."""
    info = {"quota_code": QUOTA_CODE}
    try:
        q = c("service-quotas").get_service_quota(ServiceCode="ec2", QuotaCode=QUOTA_CODE)["Quota"]["Value"]
        info["quota_vcpus"] = q
    except Exception as e:
        info["quota_error"] = code(e)
        info["max_instances"] = 3
        log(f"vCPU quota not readable ({code(e)}): at most 3 instances at once")
        return info
    used = 0
    for page in c("ec2").get_paginator("describe_instances").paginate(Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}]):
        for res in page["Reservations"]:
            for i in res["Instances"]:
                co = i.get("CpuOptions", {})
                used += co.get("CoreCount", 1) * co.get("ThreadsPerCore", 1)
    info["used_vcpus"] = used
    info["free_vcpus"] = q - used
    info["max_instances"] = 4 if q - used >= 8 else 3
    log(f"vCPU quota {q:.0f}, in use {used}, free {q - used:.0f}: at most {info['max_instances']} c6i.large at once")
    if q - used < 6:
        raise SystemExit(f"free vCPU quota {q - used:.0f} < 6: not even 3 instances fit; nothing launched")
    return info


def wait_running(iid, timeout=600):
    ec2 = c("ec2")
    t = time.time()
    while time.time() - t < timeout:
        st = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if st == "running":
            return True
        if st in ("terminated", "shutting-down"):
            return False
        time.sleep(5)
    return False


# ------------------------------------------------------------------------------------------------- EBS reattach
def vol_state():
    v = c("ec2").describe_volumes(VolumeIds=[ST["volume"]])["Volumes"][0]
    att = [a for a in v.get("Attachments", [])]
    return v["State"], (att[0]["State"] if att else None), (att[0]["InstanceId"] if att else None)


def wait_vol(cond, timeout):
    t = time.time()
    while time.time() - t < timeout and not STOP.is_set():
        if cond(vol_state()):
            return True
        time.sleep(0.2)
    return False


def wait_session(me, seen, timeout):
    t = time.time()
    while time.time() - t < timeout and not STOP.is_set():
        for k in sorted(s3list("ebs/")):
            if k.endswith(".json") and not k.endswith("-acked.json") and k not in seen:
                s = s3get_json(k)
                if s and s.get("me") == me:
                    seen.add(k)
                    return k, s
        time.sleep(1)
    return None, None


def reattach_cycles():
    holders = {"A": ST["instances"]["client"], "B": ST["instances"]["standby"]}
    cur, seen, cycles = "A", set(), []
    ids_acked_all = set()
    sk, sess = wait_session("A", seen, 600)
    if not sess:
        return {"error": "holder A never started writing"}
    for k in range(PLAN["reattach"]["cycles"]):
        if STOP.is_set():
            break
        time.sleep(PLAN["reattach"]["write_secs"])
        other = "B" if cur == "A" else "A"
        ec2 = c("ec2")
        t0 = time.time()
        try:
            ec2.detach_volume(VolumeId=ST["volume"], InstanceId=holders[cur], Force=True)
        except Exception as e:
            cycles.append({"cycle": k, "error": f"detach {code(e)}"})
            break
        if not wait_vol(lambda v: v[0] == "available", 300):
            cycles.append({"cycle": k, "error": "volume not available 300 s after force-detach", "lost": None})
            break
        t_det = time.time()
        ec2.attach_volume(VolumeId=ST["volume"], InstanceId=holders[other], Device="/dev/sdf")
        if not wait_vol(lambda v: v[1] == "attached", 300):
            cycles.append({"cycle": k, "error": "volume not attached 300 s after attach", "lost": None})
            break
        t_att = time.time()
        nk, ns = wait_session(other, seen, 300)
        acked = s3wait_json(sk.replace(".json", "-acked.json"), 120)
        if acked:
            from_ranges(acked["acked"], ids_acked_all)
        if not ns:
            cycles.append({"cycle": k, "from": cur, "to": other, "error": "no session on the new holder within 300 s", "lost": None})
            break
        rec = set()
        from_ranges(ns["recovered"], rec)
        if not acked:
            # without the previous session's ack list nothing can be checked: a failed cycle, never "0 lost"
            cycles.append({"cycle": k, "from": cur, "to": other, "error": "acked file of the previous session missing: loss not verifiable",
                           "lost": None, "recovered": len(rec)})
            log(f"reattach cycle {k}: previous session's acked file missing -> cycle failed")
            sk, cur = nk, other
            continue
        lost = len(ids_acked_all - rec)
        cyc = {"cycle": k, "from": cur, "to": other,
               "detach_api_to_available_s": t_det - t0, "attach_api_to_attached_s": t_att - t_det,
               "attach_call_to_device_visible_s": ns["t_dev_ms"] / 1000 - t_det,
               "mount_ms": ns["t_mount_ms"] - ns["t_dev_ms"], "recover_ms": ns["t_recovered_ms"] - ns["t_mount_ms"],
               "first_ack_after_recovery_ms": ns["t_first_ack_ms"] - ns["t_recovered_ms"],
               "total_detach_call_to_first_ack_s": ns["t_first_ack_ms"] / 1000 - t0,
               "acked_before_total": len(ids_acked_all), "acked_in_previous_session": acked["acked_n"] if acked else None,
               "previous_session_acked_file": bool(acked), "recovered": len(rec), "lost": lost,
               "bad_payload": ns.get("bad_payload"), "bad": ns.get("bad"), "mount_tries": ns.get("mount_tries")}
        log(f"reattach cycle {k}: {cur}->{other} total {cyc['total_detach_call_to_first_ack_s']:.1f} s, lost {lost}")
        cycles.append(cyc)
        sk, cur = nk, other
    return {"cycles": cycles, "note": "times *_s on the runner clock, *_ms on the instance clock; cross-clock values assume NTP-synced clocks"}


def from_ranges(rs, s):
    for a, b in rs:
        s.update(range(a, b + 1))


def s3wait_json(key, timeout):
    t = time.time()
    while time.time() - t < timeout:
        v = s3get_json(key)
        if v:
            return v
        time.sleep(1)
    return None


# ------------------------------------------------------------------------------------------------------------ main
def collect():
    n = 0
    for k in s3list(""):
        if k.startswith("files/") or k.startswith("wal/"):
            continue
        dest = os.path.join(RAW, k)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        ORCH_REQ["s3_get"] += 1
        c("s3").download_file(ST["bucket"], ROOT + k, dest)
        n += 1
    log(f"collected {n} objects into {RAW}")


def main():
    Creds.renew(max_age=0)
    P = infra.prices()
    json.dump(P, open(f"{OUT}/prices.json", "w"), indent=1)
    F = infra.flat_prices(P)
    est = estimate(F)
    json.dump({"estimate": est, "prices_used": F, "plan": PLAN}, open(f"{OUT}/estimate.json", "w"), indent=1)
    log(f"plan estimate {est['usd']:.2f} USD (limit {PLAN['spend']['max_estimate_usd']})")
    if est["usd"] > PLAN["spend"]["max_estimate_usd"]:
        sys.exit("estimate above the limit: nothing created")
    cost_f = open(f"{OUT}/cost_live.csv", "w", newline="")
    w = csv.writer(cost_f)
    w.writerow(["utc", "elapsed_s", "phase", "running", "total_usd", "pct_of_stop", "requests_usd", "ec2_ipv4_usd", "ebs_usd", "inter_az_usd", "margin_usd", "stop_usd", "cap_usd"])
    t_start = now()

    def row(phase_name, cst):
        running = " ".join(x for r in sorted(Live.steps) for x in Live.steps[r]) or "-"
        w.writerow([time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), int(now() - t_start), phase_name, running, round(cst["total_usd"], 4),
                    round(100 * cst["total_usd"] / PLAN["spend"]["stop_usd"], 1), round(cst["requests_usd"], 4), round(cst["ec2_ipv4_usd"], 4),
                    round(cst["ebs_usd"], 4), round(cst["inter_az_usd"], 5), round(cst["margin_usd"], 4), PLAN["spend"]["stop_usd"], PLAN["spend"]["cap_usd"]])
        cost_f.flush()
    phase = ["create"]
    deadline = time.time() + PLAN["max_run_minutes"] * 60
    try:
        infra.create(RUN, ST)
        VOL_T0[0] = now()
        save_state()
        for f, src in [("walbench", BIN), ("agent.py", f"{HERE}/instance/agent.py"), ("node-supervisor.sh", f"{HERE}/instance/node-supervisor.sh"),
                       ("plan.json", f"{HERE}/plan.json")]:
            ORCH_REQ["s3_put"] += 3
            c("s3").upload_file(src, ST["bucket"], f"{ROOT}files/{f}")
        ST["quota"] = instance_budget()
        node0_on_client = ST["quota"]["max_instances"] < 4
        ST["node0_on_client"] = node0_on_client
        cl = launch("client", "client", {"node0_on_client": node0_on_client})
        for i in range(1 if node0_on_client else 0, 3):
            launch("node", f"node{i}", {"node_index": i})
        if not wait_running(cl):
            raise RuntimeError("client did not start")
        c("ec2").attach_volume(VolumeId=ST["volume"], InstanceId=cl, Device="/dev/sdf")
        phase[0] = "run"
        reattach = {"thread": None, "result": None}
        done_markers = set()
        boot_deadline = time.time() + PLAN.get("client_boot_minutes", 10) * 60
        while time.time() < deadline:
            Creds.renew()
            if time.time() > boot_deadline and "client" not in Live.last:
                Live.compute(F)
                if "client" not in Live.last:
                    log("ABORT: the client never uploaded progress (did not boot or cannot reach S3)")
                    ST["error"] = "client did not boot"
                    STOP.set()
                    break
            cost = Live.compute(F)
            row(phase[0], cost)
            json.dump(cost, open(f"{OUT}/cost_last.json", "w"), indent=1)
            Visible.line(cost, phase[0])
            Visible.push()
            if cost["total_usd"] >= PLAN["spend"]["stop_usd"]:
                log("AUTOMATIC STOP: spend estimate reached the stop threshold")
                ST["automatic_stop"] = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "usd": cost["total_usd"], "phase": phase[0]}
                STOP.set()
                s3put("markers/stop.json", b"{}")
                break
            for m in ("smoke-done", "quorum-done", "standby-needed", "reattach-ready", "client-done", "error-client"):
                if m in done_markers:
                    continue
                v = marker(m)
                if v is None:
                    continue
                done_markers.add(m)
                log(f"marker {m}: {json.dumps(v)[:300]}")
                if m == "quorum-done":
                    terminate([ST["instances"].get(f"node{i}") for i in range(3)])
                    phase[0] = "ebs+cloud"
                elif m == "standby-needed":
                    live = sum(1 for s_, e_ in INST_T.values() if e_ is None)
                    if live + 1 > ST["quota"]["max_instances"]:
                        log(f"standby not launched: {live} instances still running, limit {ST['quota']['max_instances']}")
                        ST["standby_skipped"] = "instance limit"
                    else:
                        launch("standby", "standby", {})
                elif m == "reattach-ready":
                    phase[0] = "reattach"

                    def ra():
                        if "standby" in ST["instances"] and wait_running(ST["instances"]["standby"]):
                            reattach["result"] = reattach_cycles()
                        else:
                            reattach["result"] = {"error": "standby did not start"}
                        s3put("markers/reattach-done.json", b"{}")
                    reattach["thread"] = threading.Thread(target=ra, daemon=True)
                    reattach["thread"].start()
            if "client-done" in done_markers or "error-client" in done_markers:
                break
            time.sleep(20)
        else:
            log("run deadline reached")
            ST["deadline_reached"] = True
            STOP.set()
        if STOP.is_set():
            # automatic stop, deadline or abort: stop the spending first, then wait for / collect anything
            terminate_all()
        if reattach["thread"]:
            reattach["thread"].join(timeout=60)
        json.dump(reattach["result"], open(f"{RAW}/reattach.json", "w"), indent=1)
    except (Exception, KeyboardInterrupt) as e:
        log(f"orchestrator error: {code(e)}: {str(e)[:300] if not hasattr(e, 'response') else ''}")
        ST["error"] = code(e)
    finally:
        STOP.set()
        Creds.renew(max_age=0)
        terminate_all()  # always before collecting: nothing keeps writing while results are downloaded
        save_state()
        try:
            collect()
        except Exception as e:
            log(f"collect failed: {code(e)}")
        try:
            steps = infra.teardown(RUN)
        except Exception as e:
            steps = [{"step": "teardown", "ok": False, "error": code(e)}]
        try:
            v = infra.verify()
        except Exception as e:
            v = {"clean": False, "error": code(e), "left": [], "tag_api_extra": []}
        for i in INST_T:
            if INST_T[i][1] is None:
                INST_T[i][1] = now()
        final = Live.compute(F, fetch=False)
        json.dump({"steps": steps, "verify": v}, open(f"{OUT}/teardown.json", "w"), indent=1)
        json.dump(final, open(f"{OUT}/cost_final.json", "w"), indent=1)
        row("after-teardown", final)
        cost_f.close()
        save_state()
        log(f"teardown clean: {v['clean']}")
        Visible.line(final, "after teardown", force=True, prefix="FINAL SPEND")
        Visible.push(force=True)


if __name__ == "__main__":
    def on_term(*_):
        raise KeyboardInterrupt("SIGTERM")
    signal.signal(signal.SIGTERM, on_term)
    main()
