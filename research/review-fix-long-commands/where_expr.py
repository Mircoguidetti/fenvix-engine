import sys; sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from test_58r_review_fix_long_commands import both, drain
from bson import SON
srv, c, m = both(lambda db: db.t.insert_many([{"_id": i, "k": i % 7} for i in range(1000)]))
try:
    for flt in ({"$where": "this.k == 3"}, {"$expr": {"$lt": ["$$NOW", "$$NOW"]}}, {"$expr": {"$eq": [{"$mod": ["$k", 2]}, 0]}}):
        cmd = SON([("find", "t"), ("filter", flt), ("batchSize", 10)])
        out = []
        for cl in (c, m):
            try: out.append(len(drain(cl, "r58", cmd, 10)))
            except Exception as e: out.append(str(e)[:80])
        print(flt, "ours", out[0], "mongod", out[1])
finally:
    srv.term()
