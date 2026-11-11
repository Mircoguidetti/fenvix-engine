"""test_31's outage scenario (16 writers, store frozen 40 s past the 6 s lease, back, clean stop, restart, read every
acknowledged id) through TraceProxy. Writes a run folder with: the proxy trace, saved WAL/SST bodies, the batch log
(start, ack or error time per batch), a marker per document (to find it in the saved bodies), the server logs, and the
result (missing ids with the merged reader and with sealed runs off).

usage: probe.py <outdir> <binary> <mode: on|small|off0|sealedoff> [freeze_s]"""
import json
import os
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.environ.get("CTL_DIR", "/home/user/wt-i3/ctl-tests"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pymongo import MongoClient  # noqa: E402
import lib  # noqa: E402
from lib import Server, make_bucket, MINIO, list_prefix  # noqa: E402
from traceproxy import TraceProxy  # noqa: E402

out, binary, mode = sys.argv[1], sys.argv[2], sys.argv[3]
FREEZE_S = float(sys.argv[4]) if len(sys.argv) > 4 else 40
os.makedirs(out, exist_ok=True)
lib.OUT = out
TTL_S, RECOVER_S, SHUTDOWN_S, THREADS, BATCH = 6, 20, 30, 16, 500
env = {"FENVIX_SEALED_TPIN_MS": "30000", "FENVIX_SEALED_E1_WAIT_MS": "0", "FENVIX_SEALED_TEST_SHORT_E1_WAIT": "1"}
if mode == "sealedoff":
    env = {"FENVIX_SEALED_RUNS": "off"}
else:
    env.update({"FENVIX_SEALED_RUNS": "2", "FENVIX_SEALED_DERIVE": "1", "FENVIX_SEALED_CODEC": "zstd"})
    env["FENVIX_SEALED_COMPACTOR"] = {"on": "", "small": "small", "off0": "0"}[mode]
for k in list(os.environ):
    if k.startswith("FENVIX_SEALED"):
        del os.environ[k]
host, port = MINIO.split("//")[1].split(":")
px = TraceProxy((host, int(port)), os.path.join(out, "trace.log"), save_dir=os.path.join(out, "bodies"))
bucket = make_bucket("otl")
res = {"bucket": bucket, "mode": mode, "binary": binary, "freeze_s": FREEZE_S}
srv = Server(bucket, "w1", idle=600, endpoint=px.endpoint, binary=binary, env=env,
             extra=["--lease-ttl-secs", str(TTL_S), "--memtable-max-mb", "1"]).start()
stop = threading.Event()
acked, lock, nxt = [], threading.Lock(), [0]
blog = open(os.path.join(out, "batches.log"), "w", buffering=1)
mk = open(os.path.join(out, "markers.txt"), "w", buffering=1)


def read_ids(server):
    """Every _id, retried while the killed server's lease is still valid (code 134)."""
    for _ in range(60):
        try:
            return set(d["_id"] for d in server.client().lst.t.find({}, {"_id": 1}))
        except Exception as e:
            if "134" not in str(getattr(e, "code", "")) and "owned by" not in str(e):
                raise
            time.sleep(1)
    raise RuntimeError("the database stayed owned by the killed server")


def loader(tid):
    cl = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, retryWrites=False, maxPoolSize=1)
    while not stop.is_set():
        with lock:
            b = nxt[0]
            nxt[0] += 1
        ids = [b * BATCH + i for i in range(BATCH)]
        docs = [{"_id": i, "f": os.urandom(500).hex()} for i in ids]
        with lock:
            mk.write(f"{b} {docs[0]['f'][:32]} {docs[-1]['f'][:32]} " + " ".join(d["f"][:16] for d in docs) + "\n")
        t0 = time.time()
        try:
            cl.lst.t.insert_many(docs, ordered=False)
            with lock:
                acked.extend(ids)
            blog.write(f"{b} t{tid} start={t0:.3f} ack={time.time():.3f}\n")
        except Exception as e:
            blog.write(f"{b} t{tid} start={t0:.3f} err={time.time():.3f} {repr(e)[:300]}\n")
            time.sleep(0.5)
    cl.close()


try:
    srv.client().lst.t.insert_one({"_id": -1})
    acked.append(-1)
    threads = [threading.Thread(target=loader, args=(i,), daemon=True) for i in range(THREADS)]
    for t in threads:
        t.start()
    time.sleep(4)
    px.freeze()
    time.sleep(FREEZE_S)
    px.thaw()
    time.sleep(RECOVER_S)
    stop.set()
    for t in threads:
        t.join(70)
    probe = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, retryWrites=False)
    try:
        probe.lst.t.insert_one({"_id": "after"})
        acked.append("after")
        res["after"] = "ok"
    except Exception as e:
        res["after"] = repr(e)[:300]
    probe.close()
    px.note("sigterm")
    srv.proc.send_signal(signal.SIGTERM)
    t0 = time.time()
    try:
        srv.proc.wait(SHUTDOWN_S)
        res["sigterm_s"] = round(time.time() - t0, 1)
    except subprocess.TimeoutExpired:
        res["sigterm_s"] = None
        res["sigterm_hang"] = True
        try:
            subprocess.run(["gdb", "-p", str(srv.proc.pid), "-batch", "-ex", "thread apply all bt"], stdout=open(os.path.join(out, "hang-bt.txt"), "w"), stderr=subprocess.STDOUT, timeout=120)
        except Exception as e:
            res["gdb"] = repr(e)
        try:
            srv.proc.wait(180)
            res["sigterm_exit_s"] = round(time.time() - t0, 1)
        except subprocess.TimeoutExpired:
            res["sigterm_exit_s"] = "over 210"
finally:
    srv.kill9()
px.note("restart")
res["acked"] = len(acked)
srv2 = Server(bucket, "r1", idle=600, binary=binary, env=env).start()
try:
    present = read_ids(srv2)
    res["missing_merged"] = sorted(i for i in acked if i not in present and isinstance(i, int))
finally:
    srv2.term()
srv3 = Server(bucket, "r2", idle=600, binary=binary, env={"FENVIX_SEALED_RUNS": "off"}).start()
try:
    present3 = read_ids(srv3)
    res["missing_slatedb"] = sorted(i for i in acked if i not in present3 and isinstance(i, int))
finally:
    srv3.term()
res["n_missing_merged"], res["n_missing_slatedb"] = len(res["missing_merged"]), len(res["missing_slatedb"])
json.dump(res, open(os.path.join(out, "result.json"), "w"))
print(json.dumps({k: v for k, v in res.items() if not k.startswith("missing")}))
