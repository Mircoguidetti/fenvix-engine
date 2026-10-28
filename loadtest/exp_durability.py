#!/usr/bin/env python3
"""How long can an acknowledged write wait before it is durable? Acknowledge-before-durable mode with a flush interval of
X ms, one tenant, writes as fast as possible, SIGKILL at a random moment, restart, read back: the age (ack to kill) of the
oldest acknowledged write that was lost is the measured window. Baseline (durable ack) must lose nothing."""
import json, os, random, signal, subprocess, sys, time

# The engine has no built-in S3 settings: MinIO of the tests, explicit (same values as tools/minio-env.sh). They replace any AWS_* already in the environment: a test must never talk to MinIO with real credentials, nor to real S3 with these.
for _k, _v in {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:9000", "AWS_ALLOW_HTTP": "true"}.items():
    os.environ[_k] = _v
for _k in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI"):
    os.environ.pop(_k, None)
from pymongo import MongoClient
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release", "mongo-server")
OUT = os.path.join(ROOT, "loadtest", "out", "t1"); RES = os.path.join(ROOT, "loadtest", "results", "t1")
os.makedirs(OUT, exist_ok=True); os.makedirs(RES, exist_ok=True)
EP = os.environ.get("MINIO_ENDPOINT", "http://172.18.0.2:9000")

def start(flags):
    p = subprocess.Popen([BIN, "--listen", "127.0.0.1:28001", "--idle-secs", "100000", "--holder", "D", "--lease-ttl-secs", "30", "--endpoint", EP] + flags, stdout=open(os.path.join(OUT, "dur-server.log"), "ab"), stderr=subprocess.STDOUT)
    for _ in range(100):
        try: MongoClient("mongodb://127.0.0.1:28001", serverSelectionTimeoutMS=200).admin.command("ping"); return p
        except Exception: time.sleep(0.1)
    raise RuntimeError("no start")

def trial(flags, tag, trial_no):
    p = start(flags)
    db = f"dur_{tag}_{trial_no}_{int(time.time())}"
    c = MongoClient("mongodb://127.0.0.1:28001", retryWrites=False, serverSelectionTimeoutMS=3000)
    c[db].c.insert_one({"_id": -1})
    acked = {}
    t_end = time.time() + random.uniform(2.5, 4.0)
    i = 0
    while time.time() < t_end:
        c[db].c.insert_one({"_id": i}); acked[i] = time.time(); i += 1
    t_kill = time.time(); p.send_signal(signal.SIGKILL); p.wait()
    p = start(flags)
    c2 = MongoClient("mongodb://127.0.0.1:28001", serverSelectionTimeoutMS=30000)
    present = {d["_id"] for d in c2[db].c.find({}, {"_id": 1})}
    lost = [k for k in acked if k not in present]
    r = {"acked": len(acked), "lost": len(lost), "oldest_lost_age_ms": round(max((t_kill - acked[k]) * 1000 for k in lost), 1) if lost else 0.0,
         "newest_present_age_ms": round(min((t_kill - acked[k]) * 1000 for k in present if k in acked), 1) if present & set(acked) else None,
         "ack_rate_per_s": round(len(acked) / (t_end - (t_end - (t_kill - min(acked.values())))), 0) if acked else 0}
    p.send_signal(signal.SIGTERM)
    try: p.wait(60)
    except Exception: p.kill()
    return r

def main():
    out = []
    for name, flags in [("baseline durable ack (explicit flush)", []), ("durable ack, flush 1000 ms", ["--wal-flush-ms", "1000"]),
                        ("ack before durable, flush 100 ms", ["--wal-flush-ms", "100", "--ack-before-durable"]), ("ack before durable, flush 1000 ms", ["--wal-flush-ms", "1000", "--ack-before-durable"])]:
        trials = [trial(flags, name.split(",")[0][:6].replace(" ", ""), n) for n in range(5)]
        out.append({"config": name, "trials": trials, "max_oldest_lost_age_ms": max(t["oldest_lost_age_ms"] for t in trials), "total_lost": sum(t["lost"] for t in trials), "total_acked": sum(t["acked"] for t in trials)})
        print(name, out[-1]["total_lost"], "lost of", out[-1]["total_acked"], "max age", out[-1]["max_oldest_lost_age_ms"], flush=True)
        json.dump(out, open(os.path.join(RES, "durability.json"), "w"), indent=1)

main()
