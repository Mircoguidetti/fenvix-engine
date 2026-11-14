"""upd_big.py BUCKET: update_one cold then warm x3 on the 1,000,000-document database, by _id alone and (head only, the
base reads the collection for it) by customer and _id; base and head."""
import os, sys, time
from pymongo import MongoClient
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server
for label, b in (("base", "/home/user/bin/mongo-server-base-0b143c4"), ("head", "/home/user/bin/mongo-server-head3")):
    s = Server(sys.argv[1], "updbig" + label, idle=900, binary=b).start()
    for _ in range(60):
        try:
            MongoClient(s.uri).app_cperf.command("count", "docs"); break
        except Exception:
            time.sleep(1)
    c = MongoClient(s.uri, socketTimeoutMS=1_800_000); db = c.app_cperf
    shapes = [("_id alone", lambda j: {"_id": f"{j:05d}-009"})] + ([("customer and _id", lambda j: {"thread_id": f"t{j:05d}", "_id": f"{j:05d}-009"})] if label == "head" else [])
    for name, f in shapes:
        ms = []
        for j in range(4):
            if j == 0:
                db.command({"tenantAdmin": 1, "close": True}); db.command("ping")
            t = time.time(); db.docs.update_one(f(100 + j), {"$inc": {"v": 1}}); ms.append(round((time.time() - t) * 1000, 1))
        print(f"{label} update_one {name}: cold then warm ms {ms}", flush=True)
    s.term(); time.sleep(8)
