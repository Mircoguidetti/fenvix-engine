"""Minimal reproduction: an insertMany (retryable writes on) started while the process lease's renewal is delayed.
The first insert command's attempt is answered with a retryable error; the driver's retry (same lsid/txnNumber) gets
E11000 on _id 0 instead of the recorded result, and all of the command's documents are stored: the client is told
nInserted=0 while 100,000 documents are in the collection. Run with MinIO up and target/release/mongo-server built: python research/fix-ctl-flaky/repro_52r_retry.py"""
import os, sys, tempfile, threading, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from pymongo import MongoClient, monitoring
from pymongo.errors import BulkWriteError, PyMongoError
from lib import Server, make_bucket, free_port

TTL = 2
events = []
class L(monitoring.CommandListener):
    def started(self, e):
        if e.command_name == "insert":
            events.append(("start", e.request_id, len(e.command.get("documents", [])), e.command.get("txnNumber"), e.command["documents"][0]["_id"] if e.command.get("documents") else None, round(time.time() % 1000, 3)))
    def succeeded(self, e):
        if e.command_name == "insert":
            r = e.reply
            events.append(("reply", e.request_id, r.get("n"), [(w.get("index"), w.get("code"), str(w.get("errmsg"))[:90]) for w in r.get("writeErrors", [])[:1]], r.get("retriedStmtIds") is not None, round(time.time() % 1000, 3)))
    def failed(self, e):
        if e.command_name == "insert":
            f = e.failure
            events.append(("FAILED", e.request_id, f.get("code"), f.get("codeName"), str(f.get("errmsg"))[:160], f.get("errorLabels"), round(time.time() % 1000, 3)))

ff = tempfile.NamedTemporaryFile("w", delete=False, suffix=".faults"); ff.close(); ff = ff.name
port = free_port()
srv = Server(make_bucket("repro52r"), "repro52r", idle=600, holder="S1", port=port, env={"MONGO_TEST_FAULTS_FILE": ff},
             extra=["--lease-ttl-secs", str(TTL), "--replica-set", "rs0", "--advertise", f"127.0.0.1:{port}"]).start()
try:
    c = MongoClient(srv.uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=120000, retryWrites=True, event_listeners=[L()])
    # no write before the call: the call creates the database (as in test_52r)
    time.sleep(1.5)
    with open(ff, "w") as fh:  # the process lease's renewals are held past its TTL: the lease is lost during the call
        fh.write(f"delay_put processes/S1/_lease {TTL * 1000 + 1500}\n")
    def clear():
        time.sleep(TTL + 1.0)
        open(ff, "w").close()
    threading.Thread(target=clear).start()
    try:
        r = c.d.t.insert_many([{"_id": i, "f": "x" * 20} for i in range(101_000)], ordered=True)
        told = len(r.inserted_ids)
    except BulkWriteError as e:
        told = e.details.get("nInserted")
    except PyMongoError as e:
        told = f"error {e}"
    time.sleep(TTL + 2)
    for _ in range(60):
        try:
            stored = c.d.t.count_documents({}); break
        except PyMongoError:
            time.sleep(1)
    def find(d, keys, out):
        if isinstance(d, dict):
            for k, v in d.items():
                if k in keys:
                    out[k] = v
                find(v, keys, out)
        return out
    for cmd in ("serverStatus", "tenantStats"):
        try:
            print(cmd, find(c.admin.command(cmd), {"outcomesUnknown", "retryRecordsReloaded", "reopenedWritesRefused"}, {}))
        except PyMongoError as e:
            print(cmd, "error", e)
    for ev in events:
        print(*ev)
    print(f"told nInserted={told}; stored {stored}; lease losses {srv.log_text().count('LEASE lost')}")
    print("REPRODUCED" if isinstance(told, int) and stored != told else "not reproduced")
finally:
    srv.term(); os.unlink(ff)
