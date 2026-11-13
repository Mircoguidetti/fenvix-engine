"""P-fix-read-amplification, rule 8: the same writes and the same reads on one server, every answer hashed per stage.

Run once per server (the base binary, the fix binary, mongod 7.0), then compare the outputs with `compare.py`. The
scenario touches every path the prompt names:
  1 a client-written customer-keyed collection, published into runs and absorbed by the compactor;
  2 updates (state and total of 10% of the orders), read before any absorption;
  3 deletes: single orders, and every order of a few customers (the erasure's whole-customer delete);
  4 the compactor settles (absorption, merges);
  5 SIGKILL and a new process opens the database (takeover after kill -9);
  6 more updates on the new process, its compactor settles.
mongod gets the same writes; it has no compactor and no takeover (stage 5 is a restart of the client only).

Every query is answered twice: sorted by the query's own sort when it has one (order compared), else as a set of
documents (sorted by `_id` here). Documents are hashed as canonical extended JSON.
Usage: ab_answers.py --uri URI --kind ours|mongod --out OUT.json [--bin BIN --bucket B --port P]
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import random
import signal
import subprocess
import time

import pymongo
from bson import ObjectId, json_util

ap = argparse.ArgumentParser()
ap.add_argument("--kind", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--uri")
ap.add_argument("--bin")
ap.add_argument("--bucket")
ap.add_argument("--port", type=int, default=27041)
ap.add_argument("--customers", type=int, default=400)
ap.add_argument("--per", type=int, default=300)
ap.add_argument("--extra", default="")
a = ap.parse_args()

DB, COLL = "app_ab", "ordini"
STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
SPAN = 365 * 86400
OPTS = ["--envelopes", "write", "--customer-key", f"{COLL}=cliente:ts", "--replica-set", "rs0"]
res = {"kind": a.kind, "stages": [], "fetch": []}
proc = None


def serve(tag):
    global proc
    log = open(os.path.join(os.path.dirname(os.path.abspath(a.out)), f"ab-{a.kind}-{tag}.log"), "w")
    # the judge's compactor settings (judge/run.sh with JUDGE_SEALED_RUNS): T_pin 1 s and small knobs, so that a
    # database of this size is absorbed, merged and resealed during the run
    env = dict(os.environ, FENVIX_SEALED_TPIN_MS="1000", FENVIX_SEALED_E1_WAIT_MS="0", FENVIX_SEALED_TEST_SHORT_E1_WAIT="1",
               FENVIX_SEALED_COMPACTOR="small")
    proc = subprocess.Popen([a.bin, "--store", "s3", "--bucket", a.bucket, "--region", "us-east-1", "--endpoint",
                             "http://127.0.0.1:9000", "--listen", f"127.0.0.1:{a.port}"] + OPTS + a.extra.split(),
                            stdout=log, stderr=log, env=env)
    c = pymongo.MongoClient(f"mongodb://127.0.0.1:{a.port}/?directConnection=true", serverSelectionTimeoutMS=120000,
                            socketTimeoutMS=600000)
    c.admin.command("ping")
    for _ in range(90):
        try:
            c[DB][COLL].find_one({"cliente": "cli-000000"})
            break
        except Exception:
            time.sleep(2)
    return c


def connect():
    if a.kind == "ours":
        return serve("A")
    return pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=600000)


def compactor(c):
    if a.kind != "ours":
        return {}
    return c.admin.command("serverStatus")["fenvix"]["compactor"]


def settle(c, need=None, quiet=45, limit=1500):
    """Wait until the compactor has done `need` and then nothing for `quiet` seconds. A new database's compactor starts
    after a later committed write: a small write to a side collection (never read) every few seconds until it starts,
    the same on every server."""
    if a.kind != "ours":
        for i in range(3):
            c[DB]["side"].insert_one({"k": i})
        return {}
    last, since, t0 = None, time.time(), time.time()
    nudges = 0
    while time.time() - t0 < limit:
        s = compactor(c)
        if s.get("started", 0) < 1 and nudges < 200:
            c[DB]["side"].insert_one({"k": nudges})
            nudges += 1
        if s != last:
            last, since = s, time.time()
        ok = all(s.get(k, 0) >= v for k, v in (need or {}).items())
        if ok and s.get("running", 0) >= 0 and time.time() - since > quiet:
            return s
        time.sleep(3)
    return last


def h(docs):
    return hashlib.md5("\n".join(json_util.dumps(d, sort_keys=True, json_options=json_util.CANONICAL_JSON_OPTIONS)
                                 for d in docs).encode()).hexdigest()


def queries(rng_seed):
    rng = random.Random(rng_seed)
    q = []
    for _ in range(6):
        cli = f"cli-{rng.randrange(a.customers):06d}"
        t = T0 + dt.timedelta(seconds=rng.uniform(0.05, 0.85) * SPAN)
        q.append(("stato_ts one customer", {"filter": {"stato": "spedito", "cliente": cli, "ts": {"$gte": t, "$lt": t + dt.timedelta(days=40)}}, "hint": "stato_ts"}))
        q.append(("stato_ts all customers", {"filter": {"stato": rng.choice(STATES), "ts": {"$gte": t, "$lt": t + dt.timedelta(days=3)}}, "hint": "stato_ts"}))
        q.append(("stato_ts reverse", {"filter": {"stato": rng.choice(STATES), "ts": {"$gte": t, "$lt": t + dt.timedelta(days=2)}}, "hint": "stato_ts", "sort": [("stato", 1), ("ts", -1)]}))
    for _ in range(8):
        cli = f"cli-{rng.randrange(a.customers):06d}"
        q.append(("last 10 of a customer", {"filter": {"cliente": cli}, "sort": [("ts", -1)], "limit": 10}))
        q.append(("whole customer", {"filter": {"cliente": cli}}))
        q.append(("customer and state", {"filter": {"cliente": cli, "stato": {"$in": ["pagato", "reso"]}}}))
    clis = [f"cli-{rng.randrange(a.customers):06d}" for _ in range(40)]
    q.append(("many customers by $in", {"filter": {"cliente": {"$in": clis}, "stato": "consegnato"}}))
    q.append(("many customers sorted by total", {"filter": {"cliente": {"$in": clis}}, "sort": [("totale", -1), ("_id", 1)], "limit": 100}))
    q.append(("collection sorted by total, filtered", {"filter": {"stato": "reso", "totale": {"$gt": 450}}, "sort": [("totale", 1), ("_id", 1)]}))
    q.append(("collection top 50 by total", {"filter": {}, "sort": [("totale", -1), ("_id", 1)], "limit": 50}))
    q.append(("range on total, no index", {"filter": {"totale": {"$gte": 100, "$lt": 101}}}))
    return q


def aggs(rng_seed):
    rng = random.Random(rng_seed + 1)
    out = []
    for _ in range(6):
        cli = f"cli-{rng.randrange(a.customers):06d}"
        out.append(("sum by state of a customer", [{"$match": {"cliente": cli}}, {"$group": {"_id": "$stato", "n": {"$sum": 1}, "tot": {"$sum": "$totale"}, "avg": {"$avg": "$totale"}}}, {"$sort": {"_id": 1}}]))
    out.append(("count by state", [{"$group": {"_id": "$stato", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]))
    out.append(("count all", [{"$count": "n"}]))
    return out


def read_all(c, stage):
    d = c[DB][COLL]
    out = {}
    before = None
    if a.kind == "ours":
        before = c.admin.command("serverStatus")["fenvix"]
    t0 = time.time()
    for i, (name, spec) in enumerate(queries(11)):
        cur = d.find(spec["filter"])
        if "hint" in spec:
            cur = cur.hint(spec["hint"])
        if "sort" in spec:
            cur = cur.sort(spec["sort"])
        if "limit" in spec:
            cur = cur.limit(spec["limit"])
        docs = list(cur)
        if "sort" not in spec:
            docs.sort(key=lambda x: str(x["_id"]))
        out[f"q{i:02d} {name}"] = [len(docs), h(docs)]
    for i, (name, p) in enumerate(aggs(11)):
        docs = list(d.aggregate(p))
        out[f"a{i:02d} {name}"] = [len(docs), h(docs)]
    secs = time.time() - t0
    if before is not None:
        after = c.admin.command("serverStatus")["fenvix"]
        res["fetch"].append({"stage": stage, "secs": round(secs, 2),
                             "rows_from_runs": after["sealedRuns"]["rowsFromRuns"] - before["sealedRuns"]["rowsFromRuns"],
                             "get_bytes": after["s3"]["getBytes"] - before["s3"]["getBytes"],
                             "fetch": {k: after.get("fetch", {}).get(k, 0) - before.get("fetch", {}).get(k, 0) for k in after.get("fetch", {})}})
    res["stages"].append({"stage": stage, "answers": out, "compactor": compactor(c)})
    print(stage, json.dumps(res["fetch"][-1] if res["fetch"] else {}), flush=True)


def ids_of(c, rng, frac):
    d = c[DB][COLL]
    picked = []
    for i in range(a.customers):
        cli = f"cli-{i:06d}"
        ids = sorted(x["_id"] for x in d.find({"cliente": cli}, {"_id": 1}))
        picked += [(cli, x) for x in ids if rng.random() < frac]
    return picked


def update(c, seed, frac):
    rng = random.Random(seed)
    d = c[DB][COLL]
    n = 0
    for cli, oid in ids_of(c, rng, frac):
        d.update_one({"cliente": cli, "_id": oid}, {"$set": {"stato": rng.choice(STATES)}, "$inc": {"totale": 1.5}})
        n += 1
    return n


c = connect()
d = c[DB][COLL]
if a.kind == "mongod":
    c.drop_database(DB)
c[DB].create_collection(COLL)
d.create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
d.create_index([("stato", 1), ("ts", -1)], name="stato_ts")
rng = random.Random(1)
batch = []
for i in range(a.customers):
    cli = f"cli-{i:06d}"
    for j in range(a.per):
        # _id from the seed: the same documents on every server
        oid = ObjectId(hashlib.md5(f"{i}-{j}".encode()).hexdigest()[:24])
        batch.append({"_id": oid, "cliente": cli, "ts": T0 + dt.timedelta(milliseconds=int(rng.uniform(0, SPAN) * 1000)),
                      "stato": rng.choice(STATES), "totale": round(rng.uniform(5, 500), 2),
                      "citta": rng.choice(["Milano", "Roma", "Torino"]), "note": "x" * rng.randint(100, 400)})
        if len(batch) == 1000:
            d.insert_many(batch, ordered=True)
            batch = []
if batch:
    d.insert_many(batch)
res["settle_1"] = settle(c, {"publishes": 1, "absorbs": 1})
read_all(c, "1 published and absorbed")
res["updates_2"] = update(c, 2, 0.10)
read_all(c, "2 after updates, before absorption")
rng3 = random.Random(3)
gone = 0
for cli, oid in ids_of(c, rng3, 0.05):
    gone += d.delete_one({"cliente": cli, "_id": oid}).deleted_count
erased = [f"cli-{i:06d}" for i in random.Random(4).sample(range(a.customers), 5)]
for cli in erased:
    gone += d.delete_many({"cliente": cli}).deleted_count
res["deleted_3"] = gone
read_all(c, "3 after deletes and whole-customer erasures")
res["settle_4"] = settle(c, {})
read_all(c, "4 compactor settled")
if a.kind == "ours":
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    c = serve("B")
else:
    c = connect()
d = c[DB][COLL]
read_all(c, "5 after kill -9 and takeover")
res["updates_6"] = update(c, 6, 0.10)
res["settle_6"] = settle(c, {"started": 1})
read_all(c, "6 updates on the new process, compactor settled")
if proc:
    proc.terminate()
    proc.wait()
json.dump(res, open(a.out, "w"), indent=1, default=str)
