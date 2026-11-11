"""Review of c2-fix-outage-loss, fix check: the cost of the fix round's extra read per deleted document (coll.rs, the
natural-order entry is read before it is deleted). 200,000 documents, a restart (cold reads from the store), then one
deleteMany of all of them, timed; the store's GET count from tenantStats when available. Usage: delete_cost.py BIN LABEL"""
import sys, time
sys.path.insert(0, "/home/user/rv/ctl-tests")
from lib import Server, make_bucket
bin_, label = sys.argv[1], sys.argv[2]
b = make_bucket("rvdel")
s = Server(b, "rvdel", idle=600, binary=bin_).start()
c = s.client()
for k in range(40):
    c.d.t.insert_many([{"_id": k * 5000 + i, "p": "x" * 100} for i in range(5000)])
s.restart()
c = s.client()
t0 = time.time()
r = c.d.t.delete_many({})
dt = time.time() - t0
print(f"{label}: deleted {r.deleted_count} in {dt:.1f} s ({r.deleted_count / dt:.0f} per s)", flush=True)
s.term()
