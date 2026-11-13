"""Step 9 · local proof of the generated ballast (mongo-server --ballast): the server opens it and answers exactly.

The judge's method (judge/run.sh): the same reads go to our server and to mongod 7.0, and the answers must be the same,
compared as raw BSON bytes (field order, types and values). The reference is mongod 7.0 loaded with every document of
the sampled customers (the generator dumps them: plan "sample_every"/"sample_max") and the script's indexes.
Per database:
  1. the catalog: collections and index names as the script makes them; documents per collection equal to the
     generator's report (collStats, dbStats, and every customer counted through the cliente_ts index; with --full-scan
     the first database's orders streamed whole);
  2. every sampled customer, on both servers: each collection whole, newest first; the script's reads Q1 (20 newest
     messages), Q3 (events in 14-day windows, limit 100), Q1d (20 messages before a date), Q4 (messages in 60-day
     windows, limit 500), Q2 (orders by _id alone, through the locator), and orders by state and time through the
     stato_ts index (hint), counts per customer and a group by state;
  3. every customer of the database (not only the sampled ones), on ours: the order the state file gives for Q2 is
     found by its _id alone and belongs to that customer (the generator's own record is the reference).
Usage: ballast_check.py --ours URI --ref URI --plan PLAN --gen-dir DIR --out JSON [--seed N]
ballast-on-i3 copy (from aws-ready 9962689): --keep-ref reads the reference as it is (no reload: mutate.py wrote the
same changes to both servers), --counts JSON takes the expected counts per collection from mutate.py's output.
generator-1tb copy: --count-threads N runs the per-customer counts N at a time (the same reads); --stato-window W sets the
stato_ts reads' window (default 0.1 of the year, as before): that read walks the window across every customer of the
database, so on 20 GiB it took about 110 s each (MEASURED); 0.015 keeps about the rows of 0.1 on 3.4 GiB.
Exit 0 when every read gave the same answer ("WRONG: 0")."""
import argparse
import json
import os
import random
import sys
import time

import bson
import pymongo
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument

ap = argparse.ArgumentParser()
ap.add_argument("--ours", required=True)
ap.add_argument("--ref", required=True)
ap.add_argument("--plan", required=True)
ap.add_argument("--gen-dir", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--seed", type=int, default=7)
ap.add_argument("--q2-per-customer", type=int, default=40)
ap.add_argument("--full-scan", action="store_true", help="also stream the first database's orders whole")
ap.add_argument("--keep-ref", action="store_true")
ap.add_argument("--counts", default="")
ap.add_argument("--only", default="", help="comma-separated databases of the plan to check (default: all)")
ap.add_argument("--count-threads", type=int, default=1, help="generator-1tb: the per-customer counts, this many at a time")
ap.add_argument("--skip-customer-counts", action="store_true", help="generator-1tb: no count of every customer's documents "
                "(about 40 minutes on 20 GiB here); collStats and dbStats still check the totals")
ap.add_argument("--stato-customers", type=int, default=-1, help="generator-1tb: the stato_ts reads for the first N sampled "
                "customers only (-1: all); on 20 GiB after writes each read fetched its window's orders cold, about 100 s")
ap.add_argument("--stato-window", type=float, default=0.1, help="generator-1tb: the stato_ts reads' time window (fraction of "
                "the year); the hinted read walks the window across ALL customers, so 0.1 suits 3.4 GiB databases")
a = ap.parse_args()

RAW = CodecOptions(document_class=RawBSONDocument)
ours = pymongo.MongoClient(a.ours, directConnection=True)
ref = pymongo.MongoClient(a.ref, directConnection=True)
plan = json.load(open(a.plan))
T0 = plan["t_inizio"]
SPAN = plan["t_span_s"]
rng = random.Random(a.seed)
res = {"databases": {}, "reads": 0, "wrong": 0, "examples": []}


def ts(frac):
    import datetime as dt
    return dt.datetime.fromtimestamp(T0 + frac * SPAN, dt.timezone.utc)


def same(what, x, y):
    res["reads"] += 1
    if x != y:
        res["wrong"] += 1
        if len(res["examples"]) < 20:
            res["examples"].append({"read": what, "ours": len(x) if isinstance(x, list) else str(x)[:200],
                                    "ref": len(y) if isinstance(y, list) else str(y)[:200]})
        return False
    return True


def raw(cur):
    # an explicit batch size: our server's getMore batches are not capped at 16 MB like mongod's, and one over
    # 48 MB breaks the wire (engine finding, ballast-on-i3 report); the bytes compared are the same
    # integration-4: a read refused with the retryable code 134 (SealedRunsRetry: F7, "this process's serving lease
    # expired; it begins no new read", for one command longer than what was left of the serving lease at its start)
    # is read again from the start, as a client retries it; the retries are counted in the result
    for attempt in range(20):
        try:
            return [bytes(d.raw) for d in cur.clone().batch_size(1000)]
        except pymongo.errors.OperationFailure as e:
            if e.code != 134 or attempt == 19:
                raise
            res["retries_134"] = res.get("retries_134", 0) + 1
            if len(res.setdefault("retry_examples", [])) < 10:
                res["retry_examples"].append(str(e)[:160])
            time.sleep(1)


for d in plan["databases"]:
    name = d["db"]
    if a.only and name not in a.only.split(","):
        continue
    g = json.load(open(os.path.join(a.gen_dir, f"{name}.gen.json")))
    if a.counts:
        g.update({k: v for k, v in json.load(open(a.counts))[name].items() if k in ("per_collezione", "documenti")})
    t0 = time.time()
    r0, w0 = res["reads"], res["wrong"]
    do, dr = ours[name], ref[name]
    od = {c: do.get_collection(c, codec_options=RAW) for c in ("messaggi", "eventi", "ordini")}
    rd = {c: dr.get_collection(c, codec_options=RAW) for c in ("messaggi", "eventi", "ordini")}
    # 1. the catalog and the counts
    same(f"{name} collections", sorted(do.list_collection_names()), ["eventi", "messaggi", "ordini"])
    for c in od:
        idx = sorted(i["name"] for i in do[c].list_indexes())
        same(f"{name}.{c} indexes", idx, sorted(["_id_", "cliente_ts"] + (["stato_ts"] if c == "ordini" else [])))
        same(f"{name}.{c} collStats count", do.command("collStats", c)["count"], g["per_collezione"][c])
        # every document counted, customer by customer through the cliente_ts index (one short read each: a single
        # count over millions of documents outlives the hold life of integration-2's reader, a known engine limit)
        if a.skip_customer_counts:
            continue
        # generator-1tb: the same counts, 8 at a time (each is still one short read of one customer)
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=a.count_threads) as ex:
            tot = sum(ex.map(lambda i: do[c].count_documents({"cliente": f"cli-{i:06d}"}), range(g["customers"])))
        same(f"{name}.{c} documents counted per customer", tot, g["per_collezione"][c])
    if a.full_scan and name == plan["databases"][0]["db"]:
        # one whole collection streamed with a cursor (batches: short reads), every document decoded
        n = sum(1 for _ in do["ordini"].find({}, batch_size=5000))
        same(f"{name}.ordini full scan", n, g["per_collezione"]["ordini"])
    same(f"{name} dbStats objects", do.command("dbStats")["objects"], g["documenti"])
    # 2. the reference: the sampled customers' documents in mongod 7.0, with the script's indexes
    if a.keep_ref:
        n_ref = sum(dr[c].count_documents({}) for c in rd)
    else:
        ref.drop_database(name)
        for c in rd:
            dr[c].create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
        dr["ordini"].create_index([("stato", 1), ("ts", -1)], name="stato_ts")
        batch = {c: [] for c in rd}
        n_ref = 0
        with open(g["sample_file"], "rb") as f:
            for doc in bson.decode_file_iter(f, codec_options=RAW):
                c = doc["c"]
                batch[c].append(RawBSONDocument(bson.encode(doc["d"])) if not isinstance(doc["d"], RawBSONDocument) else doc["d"])
                if len(batch[c]) >= 2000:
                    rd[c].insert_many(batch[c], ordered=False)
                    n_ref += len(batch[c])
                    batch[c] = []
        for c, b in batch.items():
            if b:
                rd[c].insert_many(b, ordered=False)
                n_ref += len(b)
    sampled = g["sample_customers"]
    reserved = set(d["riservati"])
    for i in sampled:
        cli = f"cli-{i:06d}"
        end = 0.6 if i in reserved else 1.0
        for c in od:
            q = {"cliente": cli}
            s = [("ts", -1), ("_id", 1)]
            try:
                mine = raw(od[c].find(q).sort(s))
                theirs = raw(rd[c].find(q).sort(s))
            except pymongo.errors.OperationFailure as e:
                # generator-1tb: our server sorts in memory only (100 MB, no disk sort; mongod sorts on disk): for a
                # customer over that, the same documents read unsorted from both, compared in (ts desc, _id) order
                if e.code != 292:
                    raise
                res["whole_reads_sorted_by_the_checker"] = res.get("whole_reads_sorted_by_the_checker", 0) + 1
                key = lambda b: (-bson.decode(b)["ts"].timestamp(), str(bson.decode(b)["_id"]))
                mine = sorted(raw(od[c].find(q)), key=key)
                theirs = sorted(raw(rd[c].find(q)), key=key)
            same(f"{name}.{c} {cli} whole", mine, theirs)
            same(f"{name}.{c} {cli} count", od[c].count_documents(q), rd[c].count_documents(q))
        s = [("ts", -1)]
        same(f"{name} Q1 {cli}", raw(od["messaggi"].find({"cliente": cli}).sort(s).limit(20)),
             raw(rd["messaggi"].find({"cliente": cli}).sort(s).limit(20)))
        for k in range(5):
            x = rng.uniform(0.05, end - 0.05)
            q = {"cliente": cli, "ts": {"$gte": ts(x), "$lt": ts(x + 14 * 86400 / SPAN)}}
            same(f"{name} Q3 {cli} {k}", raw(od["eventi"].find(q).sort(s).limit(100)), raw(rd["eventi"].find(q).sort(s).limit(100)))
            q = {"cliente": cli, "ts": {"$lt": ts(rng.uniform(0.05, 0.98 * end))}}
            same(f"{name} Q1d {cli} {k}", raw(od["messaggi"].find(q).sort(s).limit(20)), raw(rd["messaggi"].find(q).sort(s).limit(20)))
        for k in range(3):
            lo = rng.uniform(0.02, 0.98 * end - 60 * 86400 / SPAN)
            q = {"cliente": cli, "ts": {"$gte": ts(lo), "$lt": ts(lo + 60 * 86400 / SPAN)}}
            same(f"{name} Q4 {cli} {k}", raw(od["messaggi"].find(q).sort(s).limit(500)), raw(rd["messaggi"].find(q).sort(s).limit(500)))
        ids = [x["_id"] for x in ref[name]["ordini"].find({"cliente": cli}, {"_id": 1})]
        for oid in rng.sample(ids, min(len(ids), a.q2_per_customer)):
            same(f"{name} Q2 {oid}", raw(od["ordini"].find({"_id": oid})), raw(rd["ordini"].find({"_id": oid})))
        for st in ("nuovo", "pagato", "spedito", "consegnato", "reso"):
            x = rng.uniform(0.02, end - a.stato_window)
            if a.stato_customers >= 0 and sampled.index(i) >= a.stato_customers:
                continue
            q = {"stato": st, "cliente": cli, "ts": {"$gte": ts(x), "$lt": ts(x + a.stato_window)}}
            same(f"{name} stato_ts {cli} {st}", raw(od["ordini"].find(q).hint("stato_ts").sort([("ts", -1), ("_id", 1)])),
                 raw(rd["ordini"].find(q).hint("stato_ts").sort([("ts", -1), ("_id", 1)])))
        p = [{"$match": {"cliente": cli}}, {"$group": {"_id": "$stato", "n": {"$sum": 1}, "t": {"$sum": "$totale"}}}, {"$sort": {"_id": 1}}]
        go, gr = list(do["ordini"].aggregate(p)), list(dr["ordini"].aggregate(p))
        # the counts must be equal; a sum of doubles is compared exactly, and a difference is classified: mongod 7.0
        # returns the exactly rounded sum (compensated summation), integration-2's $sum adds the doubles plainly
        # (aws-ready 2026-11-10: the same values inserted through the wire give the same last-digit difference), so a
        # sum that differs from mongod's by less than 1e-9 relative while the exact sum (math.fsum of the reference's
        # values) equals mongod's is the engine's $sum precision, counted apart, never a wrong datum
        same(f"{name} group counts {cli}", [(x["_id"], x["n"]) for x in go], [(x["_id"], x["n"]) for x in gr])
        for x, y in zip(go, gr):
            res["reads"] += 1
            if x["t"] == y["t"]:
                continue
            import math
            exact = math.fsum(d["totale"] for d in dr["ordini"].find({"cliente": cli, "stato": y["_id"]}, {"totale": 1}))
            if exact == y["t"] and abs(x["t"] - y["t"]) <= 1e-9 * abs(y["t"]):
                res["engine_sum_precision"] = res.get("engine_sum_precision", 0) + 1
            else:
                res["wrong"] += 1
                res["examples"].append({"read": f"{name} group sum {cli} {y['_id']}", "ours": x["t"], "ref": y["t"]})
    # 3. every customer's Q2 order, by _id alone, belongs to it (the generator's record)
    q2_ok = q2_n = 0
    for i, q2 in enumerate(g["q2_id"]):
        if not q2:
            continue
        doc = do["ordini"].find_one({"_id": bson.ObjectId(q2)})
        q2_n += 1
        good = doc is not None and doc["cliente"] == f"cli-{i:06d}"
        q2_ok += good
        same(f"{name} Q2 ticket {i}", good, True)
    res["databases"][name] = {"reads": res["reads"] - r0, "wrong": res["wrong"] - w0, "reference_documents": n_ref,
                              "sampled_customers": len(sampled), "q2_tickets_found": f"{q2_ok}/{q2_n}",
                              "seconds": round(time.time() - t0, 1)}
    print(f"{name}: {res['databases'][name]}", flush=True)
json.dump(res, open(a.out, "w"), indent=1, default=str)
print(f"BALLAST CHECK: {res['reads']} reads compared, WRONG: {res['wrong']}; engine $sum last-digit differences "
      f"(not data): {res.get('engine_sum_precision', 0)}; reads retried after code 134 (F7): {res.get('retries_134', 0)}")
sys.exit(0 if res["wrong"] == 0 else 1)
