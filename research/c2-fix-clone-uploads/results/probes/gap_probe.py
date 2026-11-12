"""Rounds: write, close (the final flush leaves an unabsorbed L0 SST), let the old pass run, reopen, write more, check every doc."""
import sys, time
sys.path.insert(0, "/home/user/fenvix-engine/ctl-tests")
from lib import Server, make_bucket, wait_until, list_prefix
from pymongo import UpdateOne
bucket = make_bucket("gap")
srv = Server(bucket, "gap", idle=600, extra=["--l0-sst-bytes", "262144"]).start()
db = "g"; N = 2000
expect = {}
bad_total = 0
try:
    c = srv.client(); col = c[db]["c"]
    def st():
        s = c.admin.command("serverStatus")["fenvix"]
        return s["compaction"]["passes"], dict(s["compactor"])
    for rnd in range(10):
        for b in range(8):
            ops = []
            for i in range(rnd * N, rnd * N + N):
                if (i + b) % 3 == 0:
                    v = rnd * 100 + b
                    ops.append(UpdateOne({"_id": i}, {"$set": {"v": v, "pad": "x" * 200}}, upsert=True)); expect[i] = v
            col.bulk_write(ops)
        p0, a0 = st(); k0 = {k for k,_ in list_prefix(bucket, f'tenants/{db}/compacted/')}
        c[db].command("tenantAdmin", close=True)
        wait_until(lambda: srv.admin.get(f"/v1/databases/{db}?storage=0")[1]["state"] == "asleep", 10, what="asleep")
        time.sleep(3)
        p1, a1 = st(); k1 = {k for k,_ in list_prefix(bucket, f'tenants/{db}/compacted/')}; print('new compacted while closed', len(k1-k0))
        got = {d["_id"]: d.get("v") for d in col.find({}, {"v": 1})}
        bad = [i for i, v in expect.items() if got.get(i) != v]
        bad_total += len(bad)
        print(f"round {rnd}: passes {p0}->{p1} absorbs {a1} wrong {len(bad)} e.g. {[(i, expect[i], got.get(i)) for i in bad[:3]]}", flush=True)
    print("TOTAL WRONG", bad_total)
finally:
    srv.term()
