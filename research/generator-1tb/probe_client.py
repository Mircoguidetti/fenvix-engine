"""generator-1tb, second session: the control for probe_fetch.py on a database written by a client through the wire
(no generator). Orders of 1,536 customers inserted in batches of 1,000 with the stato_ts and cliente_ts indexes, the
compactor left to absorb and publish them into sealed runs, then the same hinted stato_ts read timed before and after
about 13,000 order updates (the same writes as probe_fetch.py stage 2).
Usage: probe_client.py BUCKET OUT.json [ORDERS_PER_CUSTOMER]"""
import datetime as dt
import json
import os
import random
import subprocess
import sys
import time

import pymongo
from bson import ObjectId

E = "/home/user/fenvix-engine"
BUCKET, OUT = sys.argv[1], sys.argv[2]
PER = int(sys.argv[3]) if len(sys.argv) > 3 else 600
PORT = 27032
URI = f"mongodb://127.0.0.1:{PORT}/?directConnection=true"
OPTS = ["--envelopes", "write", "--customer-key", "messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts", "--replica-set", "rs0",
        "--listen", f"127.0.0.1:{PORT}"]
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
SPAN = 365 * 86400
STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
res = {"stages": []}
log = open(os.path.join(os.path.dirname(OUT), "probe-client-server.log"), "w")
p = subprocess.Popen([f"{E}/target/release/mongo-server", "--store", "s3", "--bucket", BUCKET, "--region", "us-east-1",
                      "--endpoint", "http://127.0.0.1:9000"] + OPTS, stdout=log, stderr=log)
c = pymongo.MongoClient(URI, serverSelectionTimeoutMS=120000)
c.admin.command("ping")
d = c["app_data_b01"]["ordini"]
c["app_data_b01"].create_collection("ordini")
d.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
d.create_index([("stato", 1), ("ts", -1)], name="stato_ts")


def fx():
    f = c.admin.command("serverStatus")["fenvix"]
    return {"getBytes": f["s3"]["getBytes"], "getRange": f["s3"]["getRange"], "rowsFromRuns": f["sealedRuns"]["rowsFromRuns"],
            "compactor": f["compactor"]}


def settle(need):
    last, since, t0 = None, time.time(), time.time()
    while time.time() - t0 < 1800:
        s = fx()["compactor"]
        if s != last:
            last, since = s, time.time()
        if all(s.get(k, 0) >= v for k, v in need.items()) and time.time() - since > 60:
            return s
        time.sleep(5)
    return last


def timed(stage):
    rng = random.Random(5)
    out = []
    for k in range(4):
        cli = f"cli-{rng.randrange(1536):06d}"
        t = T0 + dt.timedelta(seconds=rng.uniform(0.1, 0.9) * SPAN)
        q = {"stato": "spedito", "cliente": cli, "ts": {"$gte": t, "$lt": t + dt.timedelta(days=30)}}
        a = fx()
        t1 = time.time()
        e = d.find(q).hint("stato_ts").explain()
        secs = time.time() - t1
        b = fx()
        ex = e["executionStats"]
        out.append({"secs": round(secs, 2), "docsExamined": ex["totalDocsExamined"], "MB_fetched": round((b["getBytes"] - a["getBytes"]) / 1e6, 1),
                    "getRange": b["getRange"] - a["getRange"], "rows_from_runs": b["rowsFromRuns"] - a["rowsFromRuns"]})
    r = {"stage": stage, "reads": out, "compactor": fx()["compactor"]}
    print(json.dumps(r), flush=True)
    res["stages"].append(r)


rng = random.Random(1)
t1 = time.time()
batch, n = [], 0
for i in range(1536):
    cli = f"cli-{i:06d}"
    for j in range(PER):
        ts = T0 + dt.timedelta(seconds=rng.uniform(0, SPAN))
        batch.append({"_id": ObjectId(), "cliente": cli, "ts": ts, "stato": rng.choice(STATES), "totale": round(rng.uniform(5, 500), 2),
                      "citta": rng.choice(["Milano", "Roma", "Torino"]), "note": "x" * rng.randint(100, 400)})
        if len(batch) == 1000:
            d.insert_many(batch, ordered=True)
            n += len(batch)
            batch = []
if batch:
    d.insert_many(batch)
    n += len(batch)
res["inserted"] = n
res["insert_secs"] = round(time.time() - t1, 1)
print("inserted", n, res["insert_secs"], flush=True)
res["settle_insert"] = settle({"started": 1, "publishes": 1})
timed("1 client-written, after the compactor published")
res["updates"] = 0
for i in rng.sample(range(1536), 150):
    cli = f"cli-{i:06d}"
    ids = [x["_id"] for x in d.find({"cliente": cli}, {"_id": 1})]
    for oid in rng.sample(ids, max(1, len(ids) // 10)):
        d.update_one({"cliente": cli, "_id": oid}, {"$set": {"stato": rng.choice(STATES)}})
        res["updates"] += 1
time.sleep(60)
timed("2 client-written, after the updates")
res["settle_updates"] = settle({"started": 1})
timed("3 client-written, after the updates, compactor still")
p.terminate()
p.wait()
json.dump(res, open(OUT, "w"), indent=1)
