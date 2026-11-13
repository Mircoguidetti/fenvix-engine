"""P-fix-read-amplification: the client-written control of probe_fetch.py (generator-1tb's probe_client.py), in two
phases so that every binary is measured on the same stored data:
  load     a client writes the orders of 1,536 customers through the wire (stato_ts and cliente_ts indexes), the
           compactor publishes and absorbs them; the server stops (the caller snapshots the bucket);
  measure  BIN opens the database with a run cache smaller than the data (--sealed-cache-mb 32), the hinted stato_ts
           read of probe_client.py is timed (stage 1), 10% of 150 customers' orders are updated (the same orders for
           every binary: ids sorted, seeded), the read again (stage 2), the compactor settles, the read again (stage 3).
Usage: probe_client2.py load BUCKET OUT.json [PER]  |  probe_client2.py measure BUCKET OUT.json   (env BIN)
"""
import datetime as dt
import json
import os
import random
import subprocess
import sys
import time

import pymongo
from bson import ObjectId

PHASE, BUCKET, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
PER = int(sys.argv[4]) if len(sys.argv) > 4 else 600
BIN = os.environ.get("BIN", "/home/user/fenvix-engine/target/release/mongo-server")
PORT = 27032
URI = f"mongodb://127.0.0.1:{PORT}/?directConnection=true"
OPTS = ["--envelopes", "write", "--customer-key", "messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts", "--replica-set", "rs0",
        "--listen", f"127.0.0.1:{PORT}"]
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
SPAN = 365 * 86400
STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
res = {"phase": PHASE, "bin": BIN, "stages": []}
extra = ["--sealed-cache-mb", "32"] if PHASE == "measure" else []
log = open(os.path.join(os.path.dirname(os.path.abspath(OUT)), f"probe-client2-{PHASE}.log"), "a")
p = subprocess.Popen([BIN, "--store", "s3", "--bucket", BUCKET, "--region", "us-east-1", "--endpoint", "http://127.0.0.1:9000"] + OPTS + extra,
                     stdout=log, stderr=log)
c = pymongo.MongoClient(URI, serverSelectionTimeoutMS=120000, socketTimeoutMS=900000)
c.admin.command("ping")
d = c["app_data_b01"]["ordini"]


def fx():
    f = c.admin.command("serverStatus")["fenvix"]
    return {"getBytes": f["s3"]["getBytes"], "rowsFromRuns": f["sealedRuns"]["rowsFromRuns"], "fetch": f.get("fetch", {}),
            "compactor": f["compactor"]}


def settle(need, quiet=60):
    last, since, t0 = None, time.time(), time.time()
    while time.time() - t0 < 1800:
        s = {k: v for k, v in fx()["compactor"].items() if isinstance(v, int)}
        if s != last:
            last, since = s, time.time()
        if all(s.get(k, 0) >= v for k, v in need.items()) and time.time() - since > quiet:
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
        rows = b["rowsFromRuns"] - a["rowsFromRuns"]
        out.append({"secs": round(secs, 2), "docsExamined": ex["totalDocsExamined"], "nReturned": ex["nReturned"],
                    "MB_fetched": round((b["getBytes"] - a["getBytes"]) / 1e6, 1), "rows_from_runs": rows,
                    "rows_per_doc": round(rows / max(1, ex["totalDocsExamined"]), 1)})
    r = {"stage": stage, "reads": out}
    print(json.dumps(r), flush=True)
    res["stages"].append(r)


if PHASE == "load":
    c["app_data_b01"].create_collection("ordini")
    d.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
    d.create_index([("stato", 1), ("ts", -1)], name="stato_ts")
    rng = random.Random(1)
    batch, n, t1 = [], 0, time.time()
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
    res["inserted"], res["insert_secs"] = n, round(time.time() - t1, 1)
    print("inserted", n, res["insert_secs"], flush=True)
    res["settle"] = settle({"started": 1, "publishes": 1, "absorbs": 1})
else:
    for _ in range(90):
        try:
            d.find_one({"cliente": "cli-000000"})
            break
        except Exception:
            time.sleep(2)
    timed("1 client-written, published, cache 32 MB")
    rng = random.Random(1)
    n = 0
    t1 = time.time()
    for i in rng.sample(range(1536), 150):
        cli = f"cli-{i:06d}"
        ids = sorted(x["_id"] for x in d.find({"cliente": cli}, {"_id": 1}))
        for oid in rng.sample(ids, max(1, len(ids) // 10)):
            d.update_one({"cliente": cli, "_id": oid}, {"$set": {"stato": rng.choice(STATES)}})
            n += 1
    res["updates"], res["update_secs"] = n, round(time.time() - t1, 1)
    timed("2 after the updates, before absorption")
    res["settle"] = settle({"started": 1})
    timed("3 after the updates, compactor settled")
p.terminate()
p.wait()
json.dump(res, open(OUT, "w"), indent=1, default=str)
