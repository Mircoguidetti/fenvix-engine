"""test_52r keep_answering's scenario with a long client timeout: how long does one dropIndexes take while the lease is lost
repeatedly (sealed2+derive exports from the environment)? Prints the time and the server's lease events meanwhile.
Usage: idx_drop_timing.py BIN"""
import os, sys, tempfile, threading, time
sys.path.insert(0, "/home/user/rv/ctl-tests")
from pymongo import MongoClient
from lib import Server, make_bucket, free_port
binary = sys.argv[1]
TTL = 2
ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults").name
open(ff, "w").close()
b = make_bucket("rvdroptime")
srv = Server(b, "rvdroptime", idle=600, holder="H1", binary=binary, env={"MONGO_TEST_FAULTS_FILE": ff, "MONGO_BUILD_CHUNK": "200"}, extra=["--lease-ttl-secs", str(TTL)]).start()
stop = threading.Event()
def rounds():
    while not stop.is_set():
        time.sleep(1.5)
        open(ff, "w").write(f"delay_put processes/H1/_lease {TTL * 1000 + 1500}\n")
        time.sleep(TTL + 1.0)
        open(ff, "w").write("")
        time.sleep(TTL + 2)
try:
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=900000, retryWrites=False, retryReads=False)
    for k in range(10):
        c.d.t.insert_many([{"_id": k * 5000 + i, "k": i % 977} for i in range(5000)])
    c.d.t.create_index("k", name="k_1")
    th = threading.Thread(target=rounds, daemon=True); th.start()
    t0 = time.time()
    try:
        c.d.t.drop_index("k_1")
        out = "ok"
    except Exception as e:
        out = f"{type(e).__name__} {getattr(e, 'code', None)} {str(e)[:150]}"
    dt = time.time() - t0
    stop.set()
    log = srv.log_text()
    print(f"dropIndexes: {out} after {dt:.1f} s; lease losses {log.count('LEASE lost')}, wakes {log.count('takeover_wake')}, SLOW lines: {[l for l in log.splitlines() if 'Indexes' in l or 'index build' in l][:5]}")
finally:
    stop.set()
    srv.term()
