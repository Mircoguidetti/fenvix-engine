"""generator-1tb, second session: why a hinted stato_ts read took about 77 s on server B after the 20 GiB takeover and
about 3.5 s on server A before it (MEASURED, big.sh check1 and check2). The same read on a small generated database
(3 GiB, determinism plan), timed at each stage, with the bytes our server fetched from the pantry for it:
  1 fresh server on the generated version (as check1);   2 after identical-shape writes and the compactor's absorption;
  3 server killed, a second server takes over and only reads (as check2);   4 that server writes, its compactor runs.
Usage: probe_fetch.py BUCKET PLAN OUT.json   (local MinIO, the release build; the bucket holds the generated database)"""
import json
import os
import random
import signal
import subprocess
import sys
import time
import datetime as dt

import pymongo

E = "/home/user/fenvix-engine"
BUCKET, PLAN, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
PORT = 27031
URI = f"mongodb://127.0.0.1:{PORT}/?directConnection=true"
OPTS = ["--envelopes", "write", "--customer-key", "messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts", "--replica-set", "rs0",
        "--listen", f"127.0.0.1:{PORT}"]
plan = json.load(open(PLAN))
T0, SPAN = plan["t_inizio"], plan["t_span_s"]
res = {"stages": []}


def serve(name):
    log = open(os.path.join(os.path.dirname(OUT), f"probe-{name}.log"), "w")
    p = subprocess.Popen([f"{E}/target/release/mongo-server", "--store", "s3", "--bucket", BUCKET, "--region", "us-east-1",
                          "--endpoint", "http://127.0.0.1:9000"] + OPTS, stdout=log, stderr=log)
    c = pymongo.MongoClient(URI, serverSelectionTimeoutMS=120000)
    c.admin.command("ping")
    for _ in range(90):   # open the database (waits for an earlier lease)
        try:
            c["app_data_b01"]["ordini"].find_one({"cliente": "cli-000000"})
            break
        except Exception as e:
            time.sleep(2)
    return p, c


def fx(c):
    f = c.admin.command("serverStatus")["fenvix"]
    return {"getBytes": f["s3"]["getBytes"], "getRange": f["s3"]["getRange"], "rowsFromRuns": f["sealedRuns"]["rowsFromRuns"],
            "compactor": f["compactor"]}


def timed(c, stage):
    rng = random.Random(5)
    out = []
    for k in range(4):
        cli = f"cli-{rng.randrange(1536):06d}"
        t = dt.datetime.fromtimestamp(T0 + rng.uniform(0.1, 0.9) * SPAN, dt.timezone.utc)
        q = {"stato": "spedito", "cliente": cli, "ts": {"$gte": t, "$lt": t + dt.timedelta(days=1)}}
        a = fx(c)
        t1 = time.time()
        e = c["app_data_b01"]["ordini"].find(q).hint("stato_ts").explain()
        secs = time.time() - t1
        b = fx(c)
        ex = e["executionStats"]
        out.append({"secs": round(secs, 2), "docsExamined": ex["totalDocsExamined"], "MB_fetched": round((b["getBytes"] - a["getBytes"]) / 1e6, 1),
                    "getRange": b["getRange"] - a["getRange"], "rows_from_runs": b["rowsFromRuns"] - a["rowsFromRuns"]})
    r = {"stage": stage, "reads": out, "compactor": fx(c)["compactor"]}
    print(json.dumps(r), flush=True)
    res["stages"].append(r)


def settle(c, need_start):
    last, since, t0 = None, time.time(), time.time()
    while time.time() - t0 < 1800:
        s = fx(c)["compactor"]
        if s != last:
            last, since = s, time.time()
        if (not need_start or s.get("started", 0) >= 1) and time.time() - since > 60:
            return s
        time.sleep(5)
    return last


def writes(c, seed):
    rng = random.Random(seed)
    d = c["app_data_b01"]["ordini"]
    n = 0
    for i in rng.sample(range(1536), 150):
        cli = f"cli-{i:06d}"
        ids = [x["_id"] for x in d.find({"cliente": cli}, {"_id": 1})]
        for oid in rng.sample(ids, max(1, len(ids) // 10)):
            d.update_one({"cliente": cli, "_id": oid}, {"$set": {"stato": rng.choice(["nuovo", "pagato", "spedito", "consegnato", "reso"])}})
            n += 1
    return n


p, c = serve("A")
timed(c, "1 server A, generated version, cold")
timed(c, "1b server A, same reads again (warm)")
res["writes_A"] = writes(c, 1)
res["settle_A"] = settle(c, True)
timed(c, "2 server A after writes and absorption")
p.send_signal(signal.SIGKILL)
p.wait()
p, c = serve("B")
timed(c, "3 server B after the takeover, reads only")
timed(c, "3b server B, same reads again")
res["writes_B"] = writes(c, 2)
res["settle_B"] = settle(c, True)
timed(c, "4 server B after its own writes and its compactor")
p.terminate()
p.wait()
json.dump(res, open(OUT, "w"), indent=1)
