#!/usr/bin/env python3
"""Controprova con il motore vero (mongo-server, store locale): storico chat piccolo, find fredde, GET contate dal motore.

Il bancone del motore: blocchi 1 MB (quasi vuoto tra una query e l'altra), indici dei blocchi e filtri 64 MB (caldi dopo
il riscaldamento). Le GET si leggono con il comando tenantAdmin del motore (contatore globale delle richieste S3).

Uso: controprova_motore.py carica|misura PORTA
"""
import random, sys, time, json, datetime
from pymongo import MongoClient, DESCENDING
from bson import ObjectId

PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 27018
c = MongoClient(f"mongodb://127.0.0.1:{PORT}/?directConnection=true", serverSelectionTimeoutMS=20000)
db = c["chat"]
U, M, DAYS = 20000, 600_000, 30
T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
rng = random.Random(42)


def owner(u):
    return f"u{u:07d}"


def carica():
    w = [1.0 / (r + 1) ** 0.7 for r in range(U)]
    perm = list(range(U))
    rng.shuffle(perm)
    db.msgs.create_index([("user", 1), ("ts", -1)])
    per_day = M // DAYS
    t = time.time()
    for d in range(DAYS):
        users = rng.choices(perm, weights=w, k=per_day)
        secs = sorted(rng.randrange(86400) for _ in range(per_day))
        docs = []
        for s, u in zip(secs, users):
            ts = T0 + datetime.timedelta(days=d, seconds=s)
            docs.append({"_id": ObjectId.from_datetime(ts), "user": owner(u), "ts": ts,
                         "text": "".join(rng.choice("abcdefghij ") for _ in range(rng.randint(150, 450)))})
        # ObjectId.from_datetime non è unico: aggiungo un contatore negli ultimi byte
        for i, doc in enumerate(docs):
            b = bytearray(doc["_id"].binary)
            b[4:12] = (d * per_day + i).to_bytes(8, "big")
            doc["_id"] = ObjectId(bytes(b))
        for k in range(0, len(docs), 1000):
            db.msgs.insert_many(docs[k:k + 1000], ordered=False)
        print(f"giorno {d}: {(d + 1) * per_day} documenti, {time.time() - t:.0f} s", flush=True)


def gets():
    r = db.command({"tenantAdmin": 1})
    return r["requests"]["getTier"]


def misura():
    users = db.msgs.aggregate([{"$group": {"_id": "$user", "n": {"$sum": 1}}}, {"$match": {"n": {"$gte": 20}}}])
    users = [u["_id"] for u in users]
    ids = [d["_id"] for d in db.msgs.aggregate([{"$sample": {"size": 2000}}, {"$project": {"_id": 1}}])]
    print(f"{len(users)} utenti con almeno 20 messaggi", flush=True)
    rr = random.Random(7)
    # riscaldamento degli indici dei blocchi e dei filtri (e di tutto il catalogo)
    for i in range(1500):
        u = rr.choice(users)
        list(db.msgs.find({"user": u}).sort("ts", DESCENDING).limit(20))
        db.msgs.find_one({"_id": rr.choice(ids)})
    out = {"Q1": [], "Q2": []}
    for i in range(300):
        u = rr.choice(users)
        g0 = gets()
        docs = list(db.msgs.find({"user": u}).sort("ts", DESCENDING).limit(20))
        out["Q1"].append(gets() - g0)
        assert len(docs) == 20
        g0 = gets()
        d = db.msgs.find_one({"_id": rr.choice(ids)})
        out["Q2"].append(gets() - g0)
        assert d is not None
    # controllo: il comando tenantAdmin da solo non fa GET
    g0 = gets(); g1 = gets()
    for q, v in out.items():
        v.sort()
        print(json.dumps({"tipo": "motore", "q": q, "n": len(v), "get_media": sum(v) / len(v), "get_p50": v[len(v) // 2],
                          "get_p90": v[int(len(v) * 0.9)], "get_max": v[-1], "get_del_contatore": g1 - g0}), flush=True)


if __name__ == "__main__":
    {"carica": carica, "misura": misura}[sys.argv[1]]()
