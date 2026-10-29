"""round3 on real S3 (GitHub Actions runner; no EC2, no other AWS service than S3 and STS for the OIDC role session).

Runs tools/round3/run.py step by step against one new bucket (bucket.sh), with the engine's own request counters
(tenantStats) instead of MinIO's. Before every step it prints an estimate of the step's cost and skips the step (and
every later one) if the spend so far plus the estimate would pass SPEND_STOP_USD; the spend so far is priced from the
counted requests of every finished step, the runner's own listings and the storage (with 24 h of margin).
Credentials: renewed (OIDC token + sts:AssumeRoleWithWebIdentity) before a step once they are 15 minutes old; every step
starts its own engine processes, which read them from the environment. Values are masked in the log, never written."""
import csv
import json
import os
import signal
import subprocess
import threading
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
REPO = os.path.dirname(os.path.dirname(ROOT))
OUT = os.path.join(REPO, "results-round3", "real-s3")
os.makedirs(OUT, exist_ok=True)
RUN = os.environ.get("GITHUB_RUN_ID", "local")
SPEND_CAP = 10.0
# automatic stop: at this estimate the running step is stopped, nothing else starts, and the workflow deletes the bucket
SPEND_STOP = float(os.environ.get("SPEND_STOP_USD", "8.0"))
# priced at the higher of the brief's prices and eu-central-1's list prices, so the cap is never underestimated
PUT_USD, GET_USD, GB_MONTH = 0.0054 / 1000, 0.00043 / 1000, 0.0245
PLAN = json.load(open(os.path.join(HERE, "run.json")))
LOG = open(os.path.join(OUT, "driver.log"), "a")


def log(msg):
    line = time.strftime("%H:%M:%S ") + msg
    print(line, flush=True)
    LOG.write(line + "\n")
    LOG.flush()


class Creds:
    at = time.time()

    @classmethod
    def renew(cls, max_age=15 * 60):
        if time.time() - cls.at < max_age:
            return
        for attempt in (1, 2):
            try:
                req = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
                                             headers={"Authorization": "bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
                tok = json.load(urllib.request.urlopen(req, timeout=30))["value"]
                print(f"::add-mask::{tok}", flush=True)
                env = {k: v for k, v in os.environ.items() if k not in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")}
                r = subprocess.run(["aws", "sts", "assume-role-with-web-identity", "--role-arn", os.environ["R3_ROLE"],
                                    "--role-session-name", f"r3-{RUN}-{int(time.time())}", "--web-identity-token", tok,
                                    "--duration-seconds", "3600", "--output", "json"], capture_output=True, text=True, env=env, timeout=60)
                if r.returncode:
                    raise RuntimeError("assume-role failed")
                c = json.loads(r.stdout)["Credentials"]
                for v in (c["SecretAccessKey"], c["SessionToken"], c["AccessKeyId"]):
                    print(f"::add-mask::{v}", flush=True)
                os.environ.update(AWS_ACCESS_KEY_ID=c["AccessKeyId"], AWS_SECRET_ACCESS_KEY=c["SecretAccessKey"], AWS_SESSION_TOKEN=c["SessionToken"])
                cls.at = time.time()
                log("credentials renewed")
                return
            except Exception as e:
                log(f"credential renewal attempt {attempt} failed: {type(e).__name__}")
                time.sleep(5)
        raise SystemExit("credential renewal failed twice: stopping")


class Live:
    """Every minute: the request counters of the running engine (tenantStats on its port; a counter resets when an engine
    restarts, so the highest value seen per process is kept), priced, written to cost_live.csv and printed to the log.
    At SPEND_STOP the running step is stopped."""
    per_pid = {}          # pid -> (put_tier_without_list, list, get_tier)
    done_usd = 0.0        # storage and runner listings of finished steps
    step = "-"
    proc = None
    stopped = None
    lock = threading.Lock()

    @classmethod
    def poll(cls):
        try:
            from pymongo import MongoClient
            st = MongoClient("127.0.0.1", 27200, directConnection=True, serverSelectionTimeoutMS=2000).admin.command("tenantStats")
            r = st["requests"]
            lst = r.get("list", 0)
            with cls.lock:
                old = cls.per_pid.get(st["pid"], (0, 0, 0))
                cls.per_pid[st["pid"]] = (max(old[0], r["billedPutTier"] - lst), max(old[1], lst), max(old[2], r["billedGetTier"]))
        except Exception:
            pass

    @classmethod
    def totals(cls):
        with cls.lock:
            put = sum(v[0] for v in cls.per_pid.values())
            lst = sum(v[1] for v in cls.per_pid.values())
            get = sum(v[2] for v in cls.per_pid.values())
            n = len(cls.per_pid)
        # margin: 10% plus 300 requests per engine process (what it sent after the last poll, while closing)
        usd = (put + lst) * 1.1 * PUT_USD + get * 1.1 * GET_USD + n * 300 * PUT_USD + cls.done_usd
        return put, lst, get, usd

    @classmethod
    def run(cls):
        path = os.path.join(OUT, "cost_live.csv")
        new = not os.path.exists(path)
        f = open(path, "a", newline="")
        w = csv.writer(f)
        if new:
            w.writerow(["utc", "step", "put_requests", "list_requests", "get_requests", "usd_estimate", "cap_usd"])
        while True:
            for _ in range(6):
                cls.poll()
                time.sleep(10)
            put, lst, get, usd = cls.totals()
            w.writerow([time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), cls.step, put, lst, get, round(usd, 4), SPEND_CAP])
            f.flush()
            print(f"spesa {usd:.2f} / {SPEND_CAP:.2f} USD (PUT {put}, LIST {lst}, GET {get}; passo {cls.step})".replace(".", ","), flush=True)
            if usd >= SPEND_STOP and cls.stopped is None:
                cls.stopped = {"at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "step": cls.step, "usd_estimate": round(usd, 4)}
                log(f"AUTOMATIC STOP: estimate {usd:.2f} USD reached {SPEND_STOP} USD during step {cls.step}; stopping it")
                if cls.proc and cls.proc.poll() is None:
                    os.killpg(cls.proc.pid, signal.SIGTERM)


def estimate(step):
    """Upper estimate of a step's requests (PUT tier, GET tier): prefill, the load, the background."""
    n, rate, secs = step["dbs"], step["rate"], step["secs"] + 30
    writes = n * rate * secs * (1.0 if step["mix"] == "write" else 0.5 if step["mix"] == "half" else 0.0)
    if step["mode"] in ("immediate", "adaptive", "before"):
        wal = writes * 1.05
    elif step["mode"] == "relaxed":
        wal = min(writes, n * secs / 1.0)
    else:
        wal = min(writes, n * secs / 10.0)
    lease = n * secs / 10 * 2 if step["mode"] == "before" else secs / 3
    put = n * 120 + wal + lease + n * secs / 20  # prefill and restarts, WAL, leases, everything else
    get = n * 250 + n * rate * secs * 0.2 + n * secs
    return put, get


def priced(put, get):
    return put * PUT_USD + get * GET_USD


def main():
    bucket = f"fenvix-test-r3-{RUN}"
    os.environ.update(ROUND3_REAL_S3="1", ROUND3_BUCKET=bucket, AWS_REGION=os.environ["ROUND3_REGION"], AWS_DEFAULT_REGION=os.environ["ROUND3_REGION"])
    spend = {"put": 0.0, "get": 0.0, "runner_list": 0, "storage_usd": 0.0, "usd": 0.0}
    done, skipped = [], []
    deadline = float(os.environ.get("R3_DEADLINE_EPOCH", time.time() + 4 * 3600))
    threading.Thread(target=Live.run, daemon=True).start()
    for step in PLAN["steps"]:
        name = step["name"]
        if Live.stopped:
            skipped.append({"name": name, "reason": "automatic stop"})
            continue
        if os.path.exists(os.path.join(OUT, f"{name}.json")):
            continue
        put, get = estimate(step)
        est = priced(put, get)
        spend["usd"] = max(spend["usd"], round(Live.totals()[3], 4))
        log(f"step {name}: estimate {put:.0f} PUT-tier + {get:.0f} GET-tier = {est:.4f} USD; spent so far {spend['usd']:.4f} USD")
        if spend["usd"] + est > SPEND_STOP:
            log(f"step {name} skipped: it could take the spend past {SPEND_STOP} USD (cap {SPEND_CAP})")
            skipped.append({"name": name, "reason": "spend", "estimate_usd": round(est, 4)})
            continue
        if time.time() + step["secs"] + 600 > deadline:
            log(f"step {name} skipped: not enough time left in the job")
            skipped.append({"name": name, "reason": "time"})
            continue
        Creds.renew()
        cmd = [sys.executable, os.path.join(ROOT, "run.py"), name, "--mode", step["mode"], "--dbs", str(step["dbs"]), "--rate", str(step["rate"]),
               "--mix", step["mix"], "--secs", str(step["secs"]), "--warm", str(step.get("warm", 60)), "--out-dir", OUT]
        t0 = time.time()
        Live.step = name
        Live.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            out, err = Live.proc.communicate(timeout=step["secs"] + 1800)
        except subprocess.TimeoutExpired:
            os.killpg(Live.proc.pid, signal.SIGKILL)
            out, err = Live.proc.communicate()
        Live.poll()
        log(f"step {name}: exit {Live.proc.returncode} in {time.time() - t0:.0f} s; " + (out.strip().splitlines() or [""])[-1][:400])
        if Live.proc.returncode:
            log("stderr tail: " + err[-1500:].replace("\n", " | "))
        res_path = os.path.join(OUT, f"{name}.json")
        if os.path.exists(res_path):
            res = json.load(open(res_path))
            # requests of the step's engine processes, plus a margin for the close-time requests they made after the count
            spend["put"] += res.get("step_requests_put_tier", 0) * 1.1 + 200
            spend["get"] += res.get("step_requests_get_tier", 0) * 1.1 + 200
            spend["runner_list"] += res.get("storage_objects", 0) // 1000 + 1
            spend["storage_usd"] += res.get("storage_bytes", 0) / 2**30 * GB_MONTH / 30 * 2  # 2 days of the bytes, a generous margin
            Live.done_usd = (spend["runner_list"]) * PUT_USD + spend["storage_usd"]
            done.append(name)
        else:
            # failed step: charge its estimate
            spend["put"] += put
            spend["get"] += get
        # the higher of the two estimates: per step from the results, and live from the counters
        spend["usd"] = round(max(priced(spend["put"] + spend["runner_list"], spend["get"]) + spend["storage_usd"], Live.totals()[3]), 4)
        json.dump({"spend": spend, "done": done, "skipped": skipped, "automatic_stop": Live.stopped, "cap_usd": SPEND_CAP, "stop_usd": SPEND_STOP},
                  open(os.path.join(OUT, "spend.json"), "w"), indent=1)
    final = max(spend["usd"], round(Live.totals()[3], 4))
    json.dump({"spend": spend, "final_usd_estimate": final, "done": done, "skipped": skipped, "automatic_stop": Live.stopped, "cap_usd": SPEND_CAP, "stop_usd": SPEND_STOP},
              open(os.path.join(OUT, "spend.json"), "w"), indent=1)
    log(f"all steps: done {len(done)}, skipped {len(skipped)}; automatic stop: {Live.stopped}; final estimated spend {final:.4f} USD (cap {SPEND_CAP})")


if __name__ == "__main__":
    main()
