"""Step 9 · program "takeovers": N takeovers between the cook's machine (zone a) and the warm spare's machine (zone b)
on the real network, with writers running all the time; the gate is 0 acknowledged writes lost.
It does not touch the measured server (port 27017). On each of the two machines the takeover agent
(machine/takeover_agent.py) runs a TAKEOVER PAIR process on port 27018, on the same pantry, with a short lease
(--lease-ttl 4 s by default) and the 3-zone log (--log on) or only S3 (--log off). The pair serves database tk_app.
Each round: WRITERS threads insert new documents with w: "majority" into whichever process answers (the one that
answered last first, the other on error); every acknowledged _id is kept with the time and the process that confirmed
it. After BETWEEN_S seconds the process that is serving is killed (SIGKILL, through its agent); the takeover time is
from the kill's acknowledgement to the first write the other process confirms; then the killed process is started
again and becomes the spare of the next round. At the end every acknowledged _id is looked for in the surviving
process: LOST = acknowledged but missing (must be 0). Also counted: confirmations from a process after its kill (must
be 0) and writes that failed (an unknown outcome, not counted as acknowledged).
Raw lines in OUT/takeovers.jsonl, summary OUT/takeovers-summary.json; final line "TAKEOVERS: PASS" or "FAIL".
Usage: takeovers.py BUCKET OUT --a IP --b IP --ca CA_FILE [--n 50] [--writers 8] [--between-s 3] [--log on|off]
  (password from CM_PW in the environment; A = the cook's machine, B = the spare's machine)"""
import argparse
import json
import os
import threading
import time
import urllib.parse

import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient, WriteConcern

ap = argparse.ArgumentParser()
ap.add_argument("bucket")
ap.add_argument("out")
ap.add_argument("--a", required=True, help="private IP of the cook's machine (agent name server-1)")
ap.add_argument("--b", required=True, help="private IP of the spare's machine (agent name spare)")
ap.add_argument("--a-name", default="server-1")
ap.add_argument("--b-name", default="spare")
ap.add_argument("--ca", required=True)
ap.add_argument("--n", type=int, default=50)
ap.add_argument("--writers", type=int, default=8)
ap.add_argument("--between-s", type=float, default=3.0)
ap.add_argument("--lease-ttl", type=int, default=4)
ap.add_argument("--log", default="on")
ap.add_argument("--max-takeover-s", type=float, default=120)
ap.add_argument("--label", default="")
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
tag = f"takeovers{('-' + a.label) if a.label else ''}"
raw = open(os.path.join(a.out, f"{tag}.jsonl"), "a")
s3 = boto3.client("s3", region_name="eu-west-1")
pw = urllib.parse.quote(os.environ["CM_PW"], safe="")
NODES = {"A": (a.a, a.a_name), "B": (a.b, a.b_name)}
cmd_n = {"A": 0, "B": 0}
lock = threading.Lock()


def note(**kw):
    kw["t"] = time.time()
    with lock:
        raw.write(json.dumps(kw) + "\n")
        raw.flush()


def command(node, action, wait_s=120):
    """Send a command to a node's agent and wait for its acknowledgement; returns the ack (with its time 't')."""
    ip, name = NODES[node]
    cmd_n[node] += 1
    n = cmd_n[node]
    while True:  # an earlier program run may have used numbers: skip to the first free one
        try:
            s3.head_object(Bucket=a.bucket, Key=f"tk/{name}/cmd-{n}.json")
            n += 1
        except ClientError:
            break
    cmd_n[node] = n
    body = {"action": action, "log": a.log == "on", "lease_ttl": a.lease_ttl}
    s3.put_object(Bucket=a.bucket, Key=f"tk/{name}/cmd-{n}.json", Body=json.dumps(body).encode())
    end = time.time() + wait_s
    while time.time() < end:
        try:
            ack = json.loads(s3.get_object(Bucket=a.bucket, Key=f"tk/{name}/ack-{n}.json")["Body"].read())
            note(kind="command", node=node, action=action, ack=ack)
            if not ack.get("ok"):
                raise RuntimeError(f"agent {name}: {ack}")
            return ack
        except ClientError:
            time.sleep(0.2)
    raise RuntimeError(f"agent {name} did not acknowledge {action} in {wait_s} s")


def client(node):
    ip, _ = NODES[node]
    return MongoClient(f"mongodb://root:{pw}@{ip}:27018/?directConnection=true&tls=true&tlsCAFile={a.ca}&authSource=admin",
                       appname="app-demo", serverSelectionTimeoutMS=2000, connectTimeoutMS=2000, socketTimeoutMS=15000,
                       retryWrites=False)


def wait_ping(node, wait_s=120):
    end = time.time() + wait_s
    c = client(node)
    while time.time() < end:
        try:
            c.admin.command("ping")
            return True
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{node} does not answer on 27018 after {wait_s} s")


colls = {k: client(k)["tk_app"].get_collection("w", write_concern=WriteConcern(w="majority", j=True)) for k in NODES}
acked = {}            # _id -> (node, time)
state = {"serving": "A", "killed_at": {"A": None, "B": None}, "stop": False, "failed": 0, "after_kill": 0}


def writer(t):
    seq = 0
    while not state["stop"]:
        first = state["serving"]
        for node in (first, "B" if first == "A" else "A"):
            _id = f"w{t}-{seq}"
            seq += 1
            try:
                colls[node].insert_one({"_id": _id, "ts": time.time()})
                now = time.time()
                with lock:
                    acked[_id] = (node, now)
                    k = state["killed_at"][node]
                    if k is not None and now > k:
                        state["after_kill"] += 1   # a killed process cannot confirm: this must stay 0
                    state["serving"] = node
                break
            except Exception as e:
                with lock:
                    state["failed"] += 1
                note(kind="write_error", node=node, id=_id, error=type(e).__name__)
                time.sleep(0.05)


summary = {"n": a.n, "writers": a.writers, "lease_ttl_s": a.lease_ttl, "log": a.log, "takeovers": []}
command("A", "start"); wait_ping("A")
colls["A"].insert_one({"_id": "seed", "ts": time.time()})   # A owns tk_app before B starts
command("B", "start"); wait_ping("B")
ths = [threading.Thread(target=writer, args=(t,), daemon=True) for t in range(a.writers)]
for t in ths:
    t.start()
ok = True
for i in range(a.n):
    time.sleep(a.between_s)
    victim = state["serving"]
    other = "B" if victim == "A" else "A"
    n_before = len(acked)
    ack = command(victim, "kill9")
    with lock:
        state["killed_at"][victim] = ack["t"]
    t_kill = ack["t"]
    end = time.time() + a.max_takeover_s
    first = None
    while time.time() < end:
        with lock:
            firsts = [ts for (node, ts) in acked.values() if node == other and ts > t_kill]
        if firsts:
            first = min(firsts)
            break
        time.sleep(0.05)
    row = {"i": i + 1, "killed": victim, "takeover_s": round(first - t_kill, 3) if first else None, "acked_before": n_before}
    summary["takeovers"].append(row)
    note(kind="takeover", **row)
    print(f"takeover {i + 1}/{a.n}: {victim} killed, {other} confirmed after "
          f"{row['takeover_s'] if first else 'NEVER'} s; acknowledged so far {len(acked)}", flush=True)
    if first is None:
        ok = False
        break
    command(victim, "start")
    wait_ping(victim)
    with lock:
        state["killed_at"][victim] = None
state["stop"] = True
for t in ths:
    t.join(timeout=30)
# every acknowledged _id must be in the surviving process
survivor = state["serving"]
present = set()
for d in colls[survivor].with_options(write_concern=WriteConcern(w=1)).find({}, {"_id": 1}):
    present.add(d["_id"])
lost = sorted(set(acked) - present)
times = sorted(r["takeover_s"] for r in summary["takeovers"] if r["takeover_s"] is not None)
pct = lambda p: times[min(len(times) - 1, int(round(p / 100 * (len(times) - 1))))] if times else None
summary.update({"done": len(times), "acknowledged": len(acked), "lost": len(lost), "lost_ids_first": lost[:50],
                "confirmed_after_kill": state["after_kill"], "failed_writes": state["failed"], "survivor": survivor,
                "takeover_s_p50": pct(50), "takeover_s_p99": pct(99), "takeover_s_max": times[-1] if times else None})
for k in ("A", "B"):
    try:
        command(k, "stop", wait_s=60)
    except Exception as e:
        print(f"stop {k}: {e}")
passed = ok and len(times) == a.n and not lost and state["after_kill"] == 0
summary["verdict"] = "PASS" if passed else "FAIL"
json.dump(summary, open(os.path.join(a.out, f"{tag}-summary.json"), "w"), indent=1)
print(json.dumps({k: v for k, v in summary.items() if k != "takeovers"}))
print(f"TAKEOVERS: {summary['verdict']} ({len(times)}/{a.n} takeovers, {len(acked)} acknowledged, {len(lost)} lost, "
      f"{state['after_kill']} confirmed after a kill)")
