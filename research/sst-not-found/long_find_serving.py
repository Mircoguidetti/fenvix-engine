"""A single find longer than the serving window (F7) with no lease fault at all. The tenant's serving deadline is set
when a command enters the database (Dbs::read, ensure_open) and checked at every read step; nothing renews it inside one
command. With --lease-ttl-secs T the window is at most T*5/6. One find over N documents, retried on code 134 for 90 s.
Usage: long_find_serving.py BIN TTL N  (sealed exports from the environment)"""
import sys, time
sys.path.insert(0, "/home/user/rv/ctl-tests")
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from lib import Server, make_bucket
binary, ttl, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
b = make_bucket("rvlongfind")
srv = Server(b, "rvlongfind", idle=600, binary=binary, extra=["--lease-ttl-secs", ttl])
srv.start()
try:
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=600000, retryReads=False)
    for k in range(0, n, 10000):
        c.d.t.insert_many([{"_id": k + i, "p": "x" * 200} for i in range(10000)])
    c.d.command("tenantAdmin", close=True)
    refused, t_start, kinds = 0, time.time(), {}
    while time.time() - t_start < 90:
        t0 = time.time()
        try:
            got = sum(1 for _ in c.d.t.find({}, {"_id": 1}))
            print(f"ttl {ttl} s, {n} documents: find answered {got} in {time.time() - t0:.1f} s after {refused} refusals {kinds}")
            break
        except PyMongoError as e:
            refused += 1
            k = "F7 serving lease" if "F7" in str(e) else ("T_pin" if "T_pin" in str(e) or "last read" in str(e) else str(e)[:60])
            kinds[k] = kinds.get(k, 0) + 1
            last = f"{getattr(e, 'code', None)} {str(e)[:110]} after {time.time() - t0:.1f} s"
    else:
        print(f"ttl {ttl} s, {n} documents: never answered in 90 s, {refused} refusals {kinds}, last: {last}")
finally:
    srv.term()
