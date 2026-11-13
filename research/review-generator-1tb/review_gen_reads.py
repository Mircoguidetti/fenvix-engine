"""Review of generator-1tb (2026-11-13): a generated database read through our server against mongod holding the same
documents, through many read paths, before and after identical writes, a kill -9 of the server while its compactor works,
a takeover by another process, compaction and a restart. Independent of the owner's `ballast_check.py`.

Usage: review_gen_reads.py --bucket B --sample S.bson --tag T [--customers N] [--wait-compactor S]
  --bucket   a bucket holding a database `app_data_b01` made by `mongo-server --ballast`
  --sample   the generator's sample file of that database (every document of the sampled customers, generation order)
  --customers  compare only the first N customers of the sample (0: all, and then whole-database reads are compared too)
Exit 0 when every comparison is equal. Every read is compared exactly (decoded BSON, field order included).
"""
import argparse
import os
import random
import sys
import time

import bson
from bson import SON
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from lib import Server, wait_until

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", required=True)
ap.add_argument("--sample", required=True)
ap.add_argument("--tag", required=True)
ap.add_argument("--customers", type=int, default=0)
ap.add_argument("--every", type=int, default=1, help="keep every K-th customer of the sample (whole-database reads then off)")
ap.add_argument("--wait-compactor", type=int, default=120)
ap.add_argument("--memtable-mb", type=int, default=0, help="--memtable-max-mb for our servers (0: the default)")
ap.add_argument("--small-compactor", action="store_true", help="FENVIX_SEALED_COMPACTOR=small (reseals and splits at small sizes)")
ap.add_argument("--writes", type=int, default=1, help="multiplier of the writes")
a = ap.parse_args()

DB = "app_data_b01"
COLLS = ["messaggi", "eventi", "ordini"]
STATI = ["nuovo", "pagato", "spedito", "consegnato", "reso"]
OPTS = ["--envelopes", "write", "--customer-key", "messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"]
ENV = {}
CO = bson.CodecOptions(document_class=SON)

mongod = MongoClient("mongodb://127.0.0.1:27020/?directConnection=true", document_class=SON)
REF = f"rv_{a.tag}"

# ---------------------------------------------------------------- mongod: the same documents, the same indexes
# the sample is streamed (it can be large); with --every K only every K-th customer (in generation order) is kept
order, docs = {}, []
with open(a.sample, "rb") as fh:
    for d in bson.decode_file_iter(fh, CO):
        c = d["d"]["cliente"]
        if c not in order:
            order[c] = len(order)
        if order[c] % a.every == 0:
            docs.append(d)
custs = [c for c in order if order[c] % a.every == 0]
sel = custs if a.customers == 0 else custs[: a.customers]
sels = set(sel)
whole = a.customers == 0 and a.every == 1
mongod.drop_database(REF)
r = mongod[REF]
for coll in COLLS:
    r[coll].create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
r["ordini"].create_index([("stato", 1), ("ts", -1)], name="stato_ts")
t0 = time.time()
for coll in COLLS:
    mine = [d["d"] for d in docs if d["c"] == coll and d["d"]["cliente"] in sels]
    for i in range(0, len(mine), 5000):
        r[coll].insert_many(mine[i:i + 5000], ordered=True)
print(f"mongod loaded: {sum(r[c].estimated_document_count() for c in COLLS)} documents of {len(sel)} customers in {time.time() - t0:.0f} s", flush=True)
ids = {c: [d["d"]["_id"] for d in docs if d["c"] == c and d["d"]["cliente"] in sels] for c in COLLS}
tmin = min(d["d"]["ts"] for d in docs)
tmax = max(d["d"]["ts"] for d in docs)
del docs

# ---------------------------------------------------------------- the comparisons
stats = {"reads": 0, "wrong": 0, "retried": 0}
wrongs = []


def run(f, tries=5):
    for i in range(tries):
        try:
            return f()
        except PyMongoError as e:
            if getattr(e, "code", None) in (134, 6, 7, 89, 91, 189, 11600, 11602, 262) and i < tries - 1:
                stats["retried"] += 1
                time.sleep(1)
                continue
            raise


def same(what, f_ours, f_ref):
    x = run(f_ours)
    y = f_ref()
    stats["reads"] += 1
    if x != y:
        stats["wrong"] += 1
        if len(wrongs) < 20:
            wrongs.append((what, str(x)[:300], str(y)[:300]))


def battery(o, label, rng):
    t = time.time()
    n0, w0 = stats["reads"], stats["wrong"]
    ref = mongod[REF]
    for coll in COLLS:
        q = {} if whole else {"cliente": {"$in": sel}}
        same(f"{coll} count", lambda: o[coll].count_documents(q), lambda: ref[coll].count_documents(q))
        # every document, by _id (a full scan of the selection)
        same(f"{coll} all by _id", lambda: list(o[coll].find(q).sort("_id", 1)), lambda: list(ref[coll].find(q).sort("_id", 1)))
        if whole:
            same(f"{coll} estimated count", lambda: o[coll].estimated_document_count(), lambda: ref[coll].estimated_document_count())
            same(f"{coll} distinct cliente", lambda: sorted(o[coll].distinct("cliente")), lambda: sorted(ref[coll].distinct("cliente")))
            # natural order (insertion order): the ids, in order
            same(f"{coll} natural order", lambda: [d["_id"] for d in o[coll].find({}, {"_id": 1}).hint([("$natural", 1)])],
                 lambda: [d["_id"] for d in ref[coll].find({}, {"_id": 1}).hint([("$natural", 1)])])
    pick = rng.sample(sel, min(40, len(sel)))
    for c in pick:
        for coll in COLLS:
            f = {"cliente": c}
            same(f"{coll} {c} newest 30", lambda: list(o[coll].find(f).sort([("ts", -1), ("_id", -1)]).limit(30)),
                 lambda: list(ref[coll].find(f).sort([("ts", -1), ("_id", -1)]).limit(30)))
            same(f"{coll} {c} oldest getMore", lambda: list(o[coll].find(f).sort([("ts", 1), ("_id", 1)]).batch_size(7).limit(60)),
                 lambda: list(ref[coll].find(f).sort([("ts", 1), ("_id", 1)]).batch_size(7).limit(60)))
            same(f"{coll} {c} skip", lambda: list(o[coll].find(f, {"ts": 1}).sort([("ts", 1), ("_id", 1)]).skip(25).limit(10)),
                 lambda: list(ref[coll].find(f, {"ts": 1}).sort([("ts", 1), ("_id", 1)]).skip(25).limit(10)))
            mid = tmin + (tmax - tmin) / 2
            same(f"{coll} {c} ts range count", lambda: o[coll].count_documents({"cliente": c, "ts": {"$gte": mid}}),
                 lambda: ref[coll].count_documents({"cliente": c, "ts": {"$gte": mid}}))
        g = {"messaggi": "$ruolo", "eventi": "$tipo", "ordini": "$stato"}
        for coll in COLLS:
            p = [{"$match": {"cliente": c}}, {"$group": {"_id": g[coll], "n": {"$sum": 1}, "t": {"$max": "$ts"}}}, {"$sort": {"_id": 1}}]
            same(f"{coll} {c} group", lambda: list(o[coll].aggregate(p)), lambda: list(ref[coll].aggregate(p)))
        p = [{"$match": {"cliente": c}}, {"$group": {"_id": "$modello", "tok": {"$sum": "$token"}}}, {"$sort": {"_id": 1}}]
        same(f"messaggi {c} token sum", lambda: list(o["messaggi"].aggregate(p)), lambda: list(ref["messaggi"].aggregate(p)))
        p = [{"$match": {"cliente": c}}, {"$unwind": "$righe"}, {"$group": {"_id": None, "q": {"$sum": "$righe.q"}, "n": {"$sum": 1}}}]
        same(f"ordini {c} unwind", lambda: list(o["ordini"].aggregate(p)), lambda: list(ref["ordini"].aggregate(p)))
        same(f"eventi {c} nested filter", lambda: list(o["eventi"].find({"cliente": c, "valori.batt": {"$lt": 10}}).sort("_id", 1)),
             lambda: list(ref["eventi"].find({"cliente": c, "valori.batt": {"$lt": 10}}).sort("_id", 1)))
    for s in STATI:
        q = {"stato": s} if whole else {"stato": s, "cliente": {"$in": sel}}
        same(f"ordini stato {s} newest", lambda: list(o["ordini"].find(q).sort([("ts", -1), ("_id", -1)]).limit(50)),
             lambda: list(ref["ordini"].find(q).sort([("ts", -1), ("_id", -1)]).limit(50)))
    if whole:
        p = [{"$group": {"_id": "$stato", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
        same("ordini group stato (whole)", lambda: list(o["ordini"].aggregate(p)), lambda: list(ref["ordini"].aggregate(p)))
    for coll in COLLS:
        live = [i for i in rng.sample(ids[coll], min(150, len(ids[coll])))]
        same(f"{coll} point reads", lambda: [o[coll].find_one({"_id": i}) for i in live], lambda: [ref[coll].find_one({"_id": i}) for i in live])
    print(f"[{label}] reads {stats['reads'] - n0}, wrong {stats['wrong'] - w0}, {time.time() - t:.0f} s", flush=True)


def mutate(o, rng, n_ins, tag):
    """The same writes on both: new documents, updates, deletes; each applied to mongod after our server acknowledged it."""
    ref = mongod[REF]
    done = 0
    for k in range(n_ins):
        c = rng.choice(sel)
        coll = rng.choice(COLLS)
        d = SON([("_id", f"{tag}-{k}"), ("cliente", c), ("ts", tmin + (tmax - tmin) * rng.random()), ("w", k), ("pad", "x" * rng.randint(100, 3000))])
        if coll == "ordini":
            d["stato"] = rng.choice(STATI)
        o[coll].insert_one(d)
        ref[coll].insert_one(d)
        ids[coll].append(d["_id"])
        if k % 5 == 0:
            i = rng.choice(ids[coll])
            o[coll].update_one({"_id": i}, {"$set": {"upd": f"{tag}-{k}"}, "$inc": {"nupd": 1}})
            ref[coll].update_one({"_id": i}, {"$set": {"upd": f"{tag}-{k}"}, "$inc": {"nupd": 1}})
        if k % 11 == 0:
            i = rng.choice(ids[coll])
            o[coll].delete_one({"_id": i})
            ref[coll].delete_one({"_id": i})
        if k % 97 == 0:
            cc = rng.choice(sel)
            o["ordini"].update_many({"cliente": cc, "stato": "nuovo"}, {"$set": {"stato": "pagato"}})
            ref["ordini"].update_many({"cliente": cc, "stato": "nuovo"}, {"$set": {"stato": "pagato"}})
        done += 1
    return done


def compactor(o):
    return o.admin.command("serverStatus")["fenvix"]["compactor"]


if a.memtable_mb:
    OPTS += ["--memtable-max-mb", str(a.memtable_mb)]
if a.small_compactor:
    ENV["FENVIX_SEALED_COMPACTOR"] = "small"
rng = random.Random(70)
srv = Server(a.bucket, f"rg{a.tag}a", idle=3600, holder=f"rg-{a.tag}-A", extra=OPTS, env=ENV).start()
o = srv.client(document_class=SON)[DB]
battery(o, "A, after the generation (reader)", rng)
mutate(o, rng, 1500 * a.writes, "a1")
battery(o, "A, after writes (writer)", rng)
# a burst of writes and the kill while the compactor works on them
mutate(o, rng, 1500 * a.writes, "a2")
s = compactor(o.client)
print(f"A compactor before the kill: { {k: s[k] for k in s if isinstance(s[k], (int, float))} }", flush=True)
srv.kill9()
srv2 = Server(a.bucket, f"rg{a.tag}b", idle=3600, holder=f"rg-{a.tag}-B", extra=OPTS, env=ENV).start()
o2 = srv2.client(document_class=SON)[DB]


def _ok(f):
    try:
        f()
        return True
    except PyMongoError:
        return False


wait_until(lambda: _ok(lambda: o2["ordini"].find_one({}, {"_id": 1})), 120, what="the takeover")
battery(o2, "B, after the takeover", rng)
mutate(o2, rng, 2500 * a.writes, "b1")
t = time.time()
while time.time() - t < a.wait_compactor:
    o2["side"].insert_one({"t": time.time()})   # keeps the compactor and its GC running
    o2["side"].delete_many({})
    time.sleep(1)
s = compactor(o2.client)
print(f"B compactor: { {k: s[k] for k in s if isinstance(s[k], (int, float))} }", flush=True)
battery(o2, "B, after writes and compaction", rng)
srv2.term()
srv2.start()
o3 = srv2.client(document_class=SON)[DB]
battery(o3, "B, after a restart", rng)
srv2.term()
print(f"TOTAL reads {stats['reads']}, wrong {stats['wrong']}, retried {stats['retried']}")
for w in wrongs:
    print("WRONG", w)
sys.exit(1 if stats["wrong"] else 0)
