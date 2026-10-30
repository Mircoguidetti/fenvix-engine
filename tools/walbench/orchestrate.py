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


def save_state():
    json.dump(ST, open(f"{OUT}/state.json", "w"), indent=1, default=str)


# ------------------------------------------------------------------------------------------------- credentials
class Creds:
    at = time.time()

    @classmethod
    def renew(cls, max_age=40 * 60):
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
    crash_writes = P["crash"]["cycles"] * P["crash"]["est_requests_per_cycle"]
    reqs = cell_writes + fleet_writes + crash_writes
    s3 = reqs * F["s3_put"] + P["crash"]["cycles"] * 2000 * F["s3_get"]
    xz = reqs * F["xz_put"] + reqs * 1100 / 2**30 * F["xz_upload_gb"]
    ddb = reqs * 2 * F["ddb_wru"]
    inst_h = P["estimate_instance_hours"]
    ec2 = inst_h * (F["ec2_hour"] + F["ipv4_hour"]) + (inst_h * 10 + 4 * 20) * F["gp3_gb_month"] / 730
    total = s3 + xz + ddb + ec2 + 0.05
    return {"requests_per_cloud_backend": reqs, "s3": s3, "express": xz, "ddb": ddb, "ec2_ebs_ipv4": ec2, "usd": round(total, 3),
            "instance_hours_assumed": inst_h}


# ------------------------------------------------------------------------------------------------------ live cost
INST_T = {}  # instance id -> [launch_unix, end_unix or None]
VOL_T0 = [None]


class Live:
    last = {}  # role -> (unix, counters)
    usd = 0.0

    @classmethod
    def compute(cls, F, fetch=True):
        now = time.time()
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
        tot = {}
        stale_s = 0.0
        for role, (t, cn) in cls.last.items():
            stale_s = max(stale_s, now - t)
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
        inst_s = sum(max(60.0, (e or now) - s) for s, e in INST_T.values())
        ec2 = inst_s / 3600 * (F["ec2_hour"] + F["ipv4_hour"])
        ebs = (inst_s * 10 + (now - VOL_T0[0] if VOL_T0[0] else 0) * 20) / 3600 / 730 * F["gp3_gb_month"]
        # margins: +10% on counted requests, plus what may have happened since the last progress upload at the
        # client's recent request rate (bounded below by 30 s of 2000 PUT/s on S3 standard)
        margin = req * 0.10 + max(stale_s, 30) * 2000 * F["s3_put"]
        cls.usd = req + xaz + ec2 + ebs + margin + 0.01
        return {"requests_usd": req, "inter_az_usd": xaz, "ec2_ipv4_usd": ec2, "ebs_usd": ebs, "margin_usd": margin, "total_usd": cls.usd,
                "instance_seconds": inst_s, "progress_stale_s": stale_s, "counters": tot}


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
    INST_T[iid] = [time.time(), None]
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
    if iids:
        c("ec2").terminate_instances(InstanceIds=iids)
        for i in iids:
            if i in INST_T and INST_T[i][1] is None:
                INST_T[i][1] = time.time()


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
        while vol_state()[0] != "available":
            time.sleep(0.2)
        t_det = time.time()
        ec2.attach_volume(VolumeId=ST["volume"], InstanceId=holders[other], Device="/dev/sdf")
        while vol_state()[1] != "attached":
            time.sleep(0.2)
        t_att = time.time()
        nk, ns = wait_session(other, seen, 300)
        acked = s3wait_json(sk.replace(".json", "-acked.json"), 120)
        if acked:
            from_ranges(acked["acked"], ids_acked_all)
        if not ns:
            cycles.append({"cycle": k, "from": cur, "to": other, "error": "no session on the new holder within 300 s"})
            break
        rec = set()
        from_ranges(ns["recovered"], rec)
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
    w.writerow(["utc", "phase", "total_usd", "requests_usd", "ec2_ipv4_usd", "ebs_usd", "inter_az_usd", "margin_usd", "stop_usd", "cap_usd"])
    phase = ["create"]
    deadline = time.time() + PLAN["max_run_minutes"] * 60
    try:
        infra.create(RUN, ST)
        VOL_T0[0] = time.time()
        save_state()
        for f, src in [("walbench", BIN), ("agent.py", f"{HERE}/instance/agent.py"), ("node-supervisor.sh", f"{HERE}/instance/node-supervisor.sh"),
                       ("plan.json", f"{HERE}/plan.json")]:
            ORCH_REQ["s3_put"] += 3
            c("s3").upload_file(src, ST["bucket"], f"{ROOT}files/{f}")
        cl = launch("client", "client", {})
        for i in range(3):
            launch("node", f"node{i}", {"node_index": i})
        if not wait_running(cl):
            raise RuntimeError("client did not start")
        c("ec2").attach_volume(VolumeId=ST["volume"], InstanceId=cl, Device="/dev/sdf")
        phase[0] = "run"
        reattach = {"thread": None, "result": None}
        done_markers = set()
        while time.time() < deadline:
            Creds.renew()
            cost = Live.compute(F)
            w.writerow([time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), phase[0], round(cost["total_usd"], 4), round(cost["requests_usd"], 4),
                        round(cost["ec2_ipv4_usd"], 4), round(cost["ebs_usd"], 4), round(cost["inter_az_usd"], 5), round(cost["margin_usd"], 4),
                        PLAN["spend"]["stop_usd"], PLAN["spend"]["cap_usd"]])
            cost_f.flush()
            log(f"spend {cost['total_usd']:.3f} / {PLAN['spend']['cap_usd']} USD (stop at {PLAN['spend']['stop_usd']}); phase {phase[0]}")
            json.dump(cost, open(f"{OUT}/cost_last.json", "w"), indent=1)
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
                    terminate([ST["instances"][f"node{i}"] for i in range(3) if f"node{i}" in ST["instances"]])
                    phase[0] = "ebs+cloud"
                elif m == "standby-needed":
                    launch("standby", "standby", {})
                elif m == "reattach-ready":
                    phase[0] = "reattach"

                    def ra():
                        if wait_running(ST["instances"]["standby"]):
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
        if reattach["thread"]:
            reattach["thread"].join(timeout=60)
        json.dump(reattach["result"], open(f"{RAW}/reattach.json", "w"), indent=1)
        cost = Live.compute(F)
        ST["final_spend_estimate"] = cost
    except (Exception, KeyboardInterrupt) as e:
        log(f"orchestrator error: {code(e)}: {str(e)[:300] if not hasattr(e, 'response') else ''}")
        ST["error"] = code(e)
    finally:
        STOP.set()
        save_state()
        try:
            Creds.renew(max_age=0)
            collect()
        except Exception as e:
            log(f"collect failed: {code(e)}")
        terminate([i for i, (s, e) in INST_T.items() if e is None])
        steps = infra.teardown(RUN)
        v = infra.verify()
        for i in INST_T:
            if INST_T[i][1] is None:
                INST_T[i][1] = time.time()
        final = Live.compute(F, fetch=False)
        json.dump({"steps": steps, "verify": v}, open(f"{OUT}/teardown.json", "w"), indent=1)
        json.dump(final, open(f"{OUT}/cost_final.json", "w"), indent=1)
        w.writerow([time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "after-teardown", round(final["total_usd"], 4), round(final["requests_usd"], 4),
                    round(final["ec2_ipv4_usd"], 4), round(final["ebs_usd"], 4), round(final["inter_az_usd"], 5), round(final["margin_usd"], 4),
                    PLAN["spend"]["stop_usd"], PLAN["spend"]["cap_usd"]])
        cost_f.close()
        save_state()
        log(f"teardown clean: {v['clean']}; final spend estimate {final['total_usd']:.3f} USD")


if __name__ == "__main__":
    def on_term(*_):
        raise KeyboardInterrupt("SIGTERM")
    signal.signal(signal.SIGTERM, on_term)
    main()
