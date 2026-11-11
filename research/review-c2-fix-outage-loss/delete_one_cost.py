"""Fix check: 5,000 single-document deletes (delete_one, one client, one after the other) on 50,000 documents after a
restart; the time per delete. Usage: delete_one_cost.py BIN LABEL"""
import sys, time
sys.path.insert(0, "/home/user/rv/ctl-tests")
from lib import Server, make_bucket
bin_, label = sys.argv[1], sys.argv[2]
b = make_bucket("rvdel1")
s = Server(b, "rvdel1", idle=600, binary=bin_).start()
c = s.client()
for k in range(10):
    c.d.t.insert_many([{"_id": k * 5000 + i, "p": "x" * 100} for i in range(5000)])
s.restart()
c = s.client()
t0 = time.time()
for i in range(0, 50000, 10):
    c.d.t.delete_one({"_id": i})
dt = time.time() - t0
print(f"{label}: 5000 delete_one in {dt:.1f} s ({1000 * dt / 5000:.2f} ms each)", flush=True)
s.term()
