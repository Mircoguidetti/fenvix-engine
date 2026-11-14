"""Poll a fenvix mongo-server every 2 s: stored L0 / sorted runs of a db (fenvixSstInfo), the compaction counters and
the L0-watch counters of serverStatus. Usage: mon.py PORT DB OUT.jsonl"""
import json, sys, time
from pymongo import MongoClient

port, db, out = int(sys.argv[1]), sys.argv[2], sys.argv[3]
c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=5000, socketTimeoutMS=10000)
f = open(out, "a")
t0 = time.time()


def walk(d, pre=""):
    for k, v in d.items():
        if isinstance(v, dict):
            yield from walk(v, pre + k + ".")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            yield pre + k, v


while True:
    rec = {"t": round(time.time() - t0, 1)}
    try:
        si = c.admin.command({"fenvixSstInfo": db})
        rec["l0"] = len(si["l0"])
        rec["runs"] = len(si["runs"])
    except Exception as e:
        rec["sst_err"] = str(e)[:100]
    try:
        ss = c.admin.command("serverStatus")
        fx = ss.get("fenvix", {})
        flat = dict(walk(fx))
        rec["fx"] = {k: v for k, v in flat.items() if any(s in k.lower() for s in ("compaction", "l0", "stall", "flush", "backpressure", "watch", "unflushed", "pass", "replay"))}
    except Exception as e:
        rec["ss_err"] = str(e)[:100]
    f.write(json.dumps(rec) + "\n")
    f.flush()
    print(rec["t"], rec.get("l0"), rec.get("runs"), rec.get("fx", {}).get("compaction.passes"), rec.get("fx", {}).get("compaction.running"), rec.get("sst_err", ""), rec.get("ss_err", ""), flush=True)
    time.sleep(2)
