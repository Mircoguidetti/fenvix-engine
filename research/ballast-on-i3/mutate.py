"""ballast-on-i3 · give the compactor work on generated ballast databases, identically on our server and on mongod 7.0.

For each database: the reference (mongod) must already hold the sampled customers (ballast_check.py ran once).
The same writes go to both servers, chosen by _id from the reference, so the two stay equal:
  - inserts: about --insert-mb MB of BSON of new messages (2 KB text) into the sampled customers of the first
    customer-space group (the customers with the lowest numbers), so that group's absorbed bytes pass theta (25%) and
    the compactor must reseal it;
  - updates on every sampled customer: 10% of orders change state and total (the stato_ts index moves: rows of the
    shared keyspace), 10% of events get valori.batt + 1, 3% of messages get a new text;
  - deletes on every sampled customer: 10% of events and 5% of messages (never a Q2 order of the state file).
Writes the expected counts per collection (generator's counts + inserts - deletes) to --counts-out, for the second
ballast_check run (--keep-ref --counts).
Usage: mutate.py --ours URI --ref URI --plan PLAN --gen-dir DIR --counts-out JSON [--insert-mb 300] [--seed 11]"""
import argparse
import datetime as dt
import json
import os
import random
import time

import bson
import pymongo

ap = argparse.ArgumentParser()
ap.add_argument("--ours", required=True)
ap.add_argument("--ref", required=True)
ap.add_argument("--plan", required=True)
ap.add_argument("--gen-dir", required=True)
ap.add_argument("--counts-out", required=True)
ap.add_argument("--insert-mb", type=float, default=300)
ap.add_argument("--first-group-customers", type=int, default=300, help="sampled customers numbered below this get the inserts")
ap.add_argument("--seed", type=int, default=11)
a = ap.parse_args()

ours = pymongo.MongoClient(a.ours, directConnection=True)
ref = pymongo.MongoClient(a.ref, directConnection=True)
plan = json.load(open(a.plan))
rng = random.Random(a.seed)
T0, SPAN = plan["t_inizio"], plan["t_span_s"]
STATES = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
WORDS = plan["vocab"]
out = {}


def both(fn):
    r1, r2 = fn(ours), fn(ref)
    return r1, r2


for d in plan["databases"]:
    name = d["db"]
    g = json.load(open(os.path.join(a.gen_dir, f"{name}.gen.json")))
    sampled = g["sample_customers"]
    q2 = {x for x in g["q2_id"] if x}
    t0 = time.time()
    st = {"inserted": {"messaggi": 0}, "deleted": {"messaggi": 0, "eventi": 0}, "updated": {"ordini": 0, "eventi": 0, "messaggi": 0}}
    rdb = ref[name]
    # 1. inserts into the first group's sampled customers
    targets = [i for i in sampled if i < a.first_group_customers] or sampled[:1]
    per = a.insert_mb * 1e6 / len(targets)
    for i in targets:
        cli = f"cli-{i:06d}"
        done = 0
        batch = []
        while done < per:
            testo = " ".join(rng.choice(WORDS) for _ in range(300))[:2000]
            doc = {"_id": bson.ObjectId(), "cliente": cli,
                   "ts": dt.datetime.fromtimestamp(T0 + rng.random() * SPAN, dt.timezone.utc).replace(tzinfo=None, microsecond=0),
                   "conv": f"conv-{rng.randint(0, 40)}", "ruolo": rng.choice(["user", "assistant"]), "modello": "medium-2026",
                   "token": rng.randint(10, 2000), "testo": testo}
            done += len(bson.encode(doc))
            batch.append(doc)
            if len(batch) >= 500:
                both(lambda c: c[name]["messaggi"].insert_many([dict(x) for x in batch], ordered=True))
                st["inserted"]["messaggi"] += len(batch)
                batch = []
        if batch:
            both(lambda c: c[name]["messaggi"].insert_many([dict(x) for x in batch], ordered=True))
            st["inserted"]["messaggi"] += len(batch)
    # 2. updates and deletes on every sampled customer, chosen by _id on the reference
    for i in sampled:
        cli = f"cli-{i:06d}"
        oids = [x["_id"] for x in rdb["ordini"].find({"cliente": cli}, {"_id": 1}) if str(x["_id"]) not in q2]
        for oid in rng.sample(oids, len(oids) // 10):
            s = rng.choice(STATES)
            r1, r2 = both(lambda c: c[name]["ordini"].update_one({"_id": oid}, {"$set": {"stato": s}, "$inc": {"totale": 1.5}}))
            assert r1.matched_count == r2.matched_count == 1, (name, oid, r1.raw_result, r2.raw_result)
            st["updated"]["ordini"] += 1
        eids = [x["_id"] for x in rdb["eventi"].find({"cliente": cli}, {"_id": 1})]
        rng.shuffle(eids)
        k = len(eids) // 10
        upd, dele = eids[:k], eids[k:2 * k]
        r1, r2 = both(lambda c: c[name]["eventi"].update_many({"_id": {"$in": upd}}, {"$inc": {"valori.batt": 1}}))
        assert r1.modified_count == r2.modified_count == len(upd), (name, cli, r1.raw_result, r2.raw_result)
        st["updated"]["eventi"] += len(upd)
        r1, r2 = both(lambda c: c[name]["eventi"].delete_many({"_id": {"$in": dele}}))
        assert r1.deleted_count == r2.deleted_count == len(dele), (name, cli, r1.raw_result, r2.raw_result)
        st["deleted"]["eventi"] += len(dele)
        mids = [x["_id"] for x in rdb["messaggi"].find({"cliente": cli}, {"_id": 1})]
        rng.shuffle(mids)
        k3, k5 = len(mids) * 3 // 100, len(mids) * 5 // 100
        upd, dele = mids[:k3], mids[k3:k3 + k5]
        testo = "riscritto " + " ".join(rng.choice(WORDS) for _ in range(20))
        r1, r2 = both(lambda c: c[name]["messaggi"].update_many({"_id": {"$in": upd}}, {"$set": {"testo": testo}}))
        assert r1.modified_count == r2.modified_count == len(upd), (name, cli, r1.raw_result, r2.raw_result)
        st["updated"]["messaggi"] += len(upd)
        r1, r2 = both(lambda c: c[name]["messaggi"].delete_many({"_id": {"$in": dele}}))
        assert r1.deleted_count == r2.deleted_count == len(dele), (name, cli, r1.raw_result, r2.raw_result)
        st["deleted"]["messaggi"] += len(dele)
    pc = dict(g["per_collezione"])
    pc["messaggi"] += st["inserted"]["messaggi"] - st["deleted"]["messaggi"]
    pc["eventi"] -= st["deleted"]["eventi"]
    out[name] = {"per_collezione": pc, "documenti": sum(pc.values()), "writes": st, "seconds": round(time.time() - t0, 1)}
    print(name, out[name], flush=True)
json.dump(out, open(a.counts_out, "w"), indent=1)
print("MUTATE: DONE")
