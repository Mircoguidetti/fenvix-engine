"""Step 9 · runs on the cook and on the warm spare, beside the measured server. It obeys the takeover program
(programs/takeovers.py on the extras client) through the run bucket:
  tk/<NAME>/cmd-<n>.json  {"action": "start" | "kill9" | "stop" | "freeze" | "thaw", "log": true|false, "lease_ttl": 4}
  tk/<NAME>/ack-<n>.json  written back: {"n", "action", "t" (epoch seconds when done), "pid", "ok", "error"}
Commands are numbered 1, 2, 3... and run in order. "start" launches the TAKEOVER PAIR process: a second mongo-server on
port 27018, on the same pantry, with a short lease (lease_ttl seconds) so a takeover takes seconds, not the default 30 s,
and with the 3-zone log unless "log": false. Its databases (tk_*) are not the measured ones, so the measured server on
27017 is not touched. The pair process uses the same TLS certificate, the same users (they live in the pantry) and a
fixed holder id per machine.
Usage: takeover_agent.py BUCKET NAME PRIVATE_IP   (PANTRY in the environment; out/config.json gives the log)"""
import json
import os
import signal
import subprocess
import sys
import time

import boto3
from botocore.exceptions import ClientError

BUCKET, NAME, IP = sys.argv[1:4]
PANTRY = os.environ["PANTRY"]
PORT = 27018
POLL_S = 0.5
s3 = boto3.client("s3", region_name="eu-west-1")
cfg = json.load(open("out/config.json"))


def get(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except ClientError:
        return None


def put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj).encode())


def log(msg):
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)


proc = None
n_start = 0


def start(c):
    global proc, n_start
    n_start += 1
    cmd = ["./mongo-server", "--listen", f"0.0.0.0:{PORT}", "--store", "s3", "--bucket", PANTRY, "--region", "eu-west-1",
           "--auth", "--tls-mode", "requireTLS", "--tls-certificate-key-file", "cert/server.pem",
           "--lease-ttl-secs", str(int(c.get("lease_ttl", 4))), "--holder", f"tk-{NAME}-{n_start}"]
    if os.environ.get("LOCAL_TEST") and os.environ.get("AWS_ENDPOINT_URL"):
        cmd += ["--endpoint", os.environ["AWS_ENDPOINT_URL"]]
    if c.get("log", True) and cfg.get("logs"):
        cmd += ["--quaderno", cfg["logs"], "--quaderno-id", "tk"]
    cmd += c.get("extra", [])
    out = open(f"out/takeover-pair-{n_start}.log", "w")
    proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT)
    log(f"started pair process {proc.pid}: {' '.join(cmd)}")
    return proc.pid


def signal_pair(sig):
    if proc is None or proc.poll() is not None:
        return None
    os.kill(proc.pid, sig)
    if sig in (signal.SIGKILL, signal.SIGTERM):
        proc.wait(timeout=60)
    return proc.pid


n = 1
log(f"takeover agent of {NAME} ({IP}), pantry {PANTRY}")
while True:
    c = get(f"tk/{NAME}/cmd-{n}.json")
    if c is None:
        time.sleep(POLL_S)
        continue
    ack = {"n": n, "action": c.get("action"), "ok": True, "error": None, "pid": None}
    try:
        a = c.get("action")
        if a == "start":
            signal_pair(signal.SIGKILL)
            ack["pid"] = start(c)
        elif a == "kill9":
            ack["pid"] = signal_pair(signal.SIGKILL)
        elif a == "stop":
            ack["pid"] = signal_pair(signal.SIGTERM)
        elif a == "freeze":
            ack["pid"] = signal_pair(signal.SIGSTOP)
        elif a == "thaw":
            ack["pid"] = signal_pair(signal.SIGCONT)
        else:
            raise ValueError(f"unknown action {a}")
    except Exception as e:
        ack.update(ok=False, error=f"{type(e).__name__}: {e}")
    ack["t"] = time.time()
    put(f"tk/{NAME}/ack-{n}.json", ack)
    log(f"command {n}: {ack}")
    n += 1
