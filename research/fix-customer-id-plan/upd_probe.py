import sys, time
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server, make_bucket
for label, b in (("base", "/home/user/bin/mongo-server-base-0b143c4"), ("head", "/home/user/bin/mongo-server-head3")):
    s = Server(make_bucket("upd"), "upd" + label, idle=600, binary=b).start()
    db = s.client().app
    db.create_collection("c", customerKey="thread_id")
    db.c.insert_many([{"_id": f"{c:03d}-{i:03d}", "thread_id": f"t{c:03d}", "i": i} for c in range(100) for i in range(100)])
    for shape, f in (("customer+_id", lambda j: {"thread_id": f"t{j:03d}", "_id": f"{j:03d}-007"}), ("_id alone", lambda j: {"_id": f"{j:03d}-008"})):
        ms = []
        for j in range(6):
            t = time.time(); db.c.update_one(f(j), {"$inc": {"u": 1}}); ms.append(round((time.time() - t) * 1000, 1))
        print(label, shape, ms, flush=True)
    s.term()
