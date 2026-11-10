import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from lib import Server, make_bucket
import pymongo
res = {}
for hold in (500, 120000):
    b = make_bucket("n1")
    s = Server(b, "n1", idle=300, extra=["--sealed-runs", "50", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait", "--sealed-hold-life-ms", str(hold)]).start()
    try:
        c = s.client()
        for k in range(20):
            c.n1.big.insert_many([{"_id": k * 5000 + i, "x": (i * 7919) % 100000, "s": "y" * 200} for i in range(5000)])
        t0 = time.time()
        try:
            c.n1.big.create_index([("x", 1)])
            out = "ok"
        except pymongo.errors.PyMongoError as e:
            out = f"error: {str(e)[:200]}"
        el = time.time() - t0
        t1 = time.time()
        try:
            n = len(list(c.n1.big.aggregate([{"$lookup": {"from": "big", "localField": "_id", "foreignField": "_id", "as": "j"}}, {"$limit": 100000}])))
            agg = f"ok ({n} docs)"
        except pymongo.errors.PyMongoError as e:
            agg = f"error: {str(e)[:200]}"
        res[hold] = (out, round(el, 2), agg, round(time.time() - t1, 2), [i["name"] for i in c.n1.big.list_indexes()])
    finally:
        s.term()
for k, v in res.items():
    print("hold_life_ms", k, "createIndexes:", v[0], "in", v[1], "s; $lookup over 100k:", v[2], "in", v[3], "s; indexes:", v[4])
