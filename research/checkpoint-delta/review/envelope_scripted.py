"""Keys / envelope overhead on the owner's scripted runs (MEASURED, replaces the 5-10% ESTIMATE), and the
number of objects a log read fetches. Usage: envelope_scripted.py -> results/envelope_scripted.json"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient
from langgraph.checkpoint.postgres import PostgresSaver
import options as o
from real_run import envelope_bytes
MIX = {10: 0.40, 40: 0.35, 100: 0.20, 300: 0.05}
out = {}
for n in MIX:
    db = MongoClient("mongodb://127.0.0.1:27017/")[f"ck_default_{n}"]
    tids = sorted(db.checkpoints.distinct("thread_id"))
    env = sum(envelope_bytes(db, t)[0] for t in tids) / len(tids)
    conn = psycopg.connect(o.PG + f"ck_default_{n}", autocommit=True, prepare_threshold=0, row_factory=dict_row)
    saver = PostgresSaver(conn); serde = saver.serde
    objs = {"latest": [], "old": []}
    for t in tids[:4]:
        recs = o.records_for_thread(saver, t)
        blocks, where, sks = o.log_encode(serde, recs)
        for lab, k in (("latest", n - 1), ("old", n // 4)):
            sk = serde.loads_typed(("msgpack", o.ormsgpack.unpackb(o.unz(sks[0][sks[1][k][0]]))[sks[1][k][1]]))
            need = set(sk["checkpoint"]["channel_values"]["messages"]["$f"]) if isinstance(sk["checkpoint"]["channel_values"].get("messages"), dict) else set()
            o._refs_in({"m": sk["metadata"], "w": sk["writes"]}, need)
            objs[lab].append(1 + len({where[i][0] for i in need if isinstance(i, int)}))
    out[n] = {"envelope_per_thread": env, "log_objects_latest": sum(objs["latest"]) / 4, "log_objects_old": sum(objs["old"]) / 4, "log_blocks_total_mean": None}
    print(n, out[n], flush=True)
out["mix_envelope"] = sum(MIX[n] * out[n]["envelope_per_thread"] for n in MIX)
json.dump(out, open("results/envelope_scripted.json", "w"), indent=1)
print("mix envelope", out["mix_envelope"])
