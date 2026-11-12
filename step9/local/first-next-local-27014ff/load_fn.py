import datetime as dt, json, os, random, sys
import pymongo
uri, states = sys.argv[1], sys.argv[2]
c = pymongo.MongoClient(uri)
db = c.app_fn
r = random.Random(1)
N = 300
for coll, per in (("messaggi", 40), ("eventi", 60), ("ordini", 12)):
    db[coll].create_index([("cliente", 1), ("ts", -1)], name="cliente_ts")
    docs = []
    for i in range(N):
        for j in range(per):
            d = {"cliente": f"cli-{i:06d}", "ts": dt.datetime(2026, 1, 1) + dt.timedelta(minutes=j * 37 + i), "testo": "x" * r.randint(200, 900)}
            if coll == "ordini":
                d.update(stato=r.choice(["nuovo", "pagato", "spedito"]), totale=round(r.uniform(5, 500), 2))
            docs.append(d)
    db[coll].insert_many(docs)
os.makedirs(os.path.join(states, "app_fn"), exist_ok=True)
json.dump({"db": "app_fn", "clienti": {"nomi_da": "cli-%06d", "n": N, "riservati": list(range(0, N, 3))},
           "caricamento": {"byte_bson": 1}}, open(os.path.join(states, "app_fn", "risultati_p1.json"), "w"))
print("loaded", {k: db[k].count_documents({}) for k in ("messaggi", "eventi", "ordini")})
