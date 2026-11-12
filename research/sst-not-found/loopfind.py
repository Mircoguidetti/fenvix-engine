"""sst-not-found: N documents, close, then full finds back to back for SECS seconds (spanning several SST GC passes) on a
reopened (reader) database; counts answers and errors by kind. Usage: loopfind.py BIN N SECS TTL"""
import sys, time, re
sys.path.insert(0, "/home/user/rv/ctl-tests")
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from lib import Server, make_bucket
binary, n, secs, ttl = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
b = make_bucket("rvloopfind")
srv = Server(b, "rvloopfind", idle=600, binary=binary, extra=["--lease-ttl-secs", ttl]).start()
try:
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=600000, retryReads=False)
    for k in range(0, n, 10000):
        c.d.t.insert_many([{"_id": k + i, "p": "x" * 200} for i in range(10000)])
    c.d.command("tenantAdmin", close=True)
    t0, ok, kinds, first_nf = time.time(), 0, {}, None
    while time.time() - t0 < secs:
        try:
            got = sum(1 for _ in c.d.t.find({}, {"_id": 1}))
            ok += 1
            if got != n:
                kinds["WRONG COUNT"] = kinds.get("WRONG COUNT", 0) + 1
        except PyMongoError as e:
            s = str(e)
            k = "F7" if "F7" in s else ("T_pin" if "T_pin" in s else ("SST not found" if "not found" in s else s[:60]))
            kinds[k] = kinds.get(k, 0) + 1
            if k == "SST not found" and first_nf is None:
                first_nf = (round(time.time(), 3), re.search(r"compacted/(\w+)\.sst", s).group(1) if re.search(r"compacted/(\w+)\.sst", s) else s[:200])
    print(f"ttl {ttl} s, {n} documents, {secs} s of finds: {ok} answered, errors {kinds}; first not-found {first_nf}; log {srv.log_path}")
finally:
    srv.term()
