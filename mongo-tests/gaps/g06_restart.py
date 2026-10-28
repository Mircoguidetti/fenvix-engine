#!/usr/bin/env python3
"""Item 6, engine only: the record of a retryable write is stored in the same batch as the write, so a replay after an engine
restart (the case a retry is for) still returns the stored result and does not run the write again.
usage: g06_restart.py [uri] [restart-command...]   (default: the engine on 27060, an engine on the S3 store at localhost:9000 (moto or MinIO) restarted through ../server.sh)"""
import os, subprocess, sys, time
from bson import Int64
from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
URI = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27060/?directConnection=true"
EXTRA = "--ttl-sweep-secs 1 --replica-set rs0 --advertise localhost:%PORT%"


def restart():
    env = dict(os.environ, SERVER_EXTRA=EXTRA)
    subprocess.run([os.path.join(HERE, "..", "server.sh"), "27060", "600"], env=env, check=True, stdout=subprocess.DEVNULL)


def main():
    c = MongoClient(URI, serverSelectionTimeoutMS=15000)
    db = c["g06_restart"]; c.drop_database("g06_restart")
    s = c.start_session(); b = int(time.time_ns() // 1000)
    def w(cmd, n): return {k: v for k, v in db.command({**cmd, "txnNumber": Int64(n)}, session=s).items() if k in ("n", "nModified", "retriedStmtIds", "upserted", "value", "retriedStmtId")}
    out = {}
    cmds = {
        "insert": ({"insert": "t", "documents": [{"_id": 1}, {"_id": 2}]}, 0),
        "update": ({"update": "t", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"x": 1}}}]}, 1),
        "fam": ({"findAndModify": "t", "query": {"_id": 2}, "update": {"$inc": {"x": 5}}, "new": True}, 2),
    }
    sid = None
    for name, (cmd, k) in cmds.items():
        out[name] = w(cmd, b + k)
        sid = s.session_id
        c.close(); restart()
        c = MongoClient(URI, serverSelectionTimeoutMS=15000); db = c["g06_restart"]
        s = c.start_session(); s.session_id; s._server_session.session_id = sid  # the same logical session
        out[name + "_replay"] = w(cmd, b + k)
    try:
        w({"insert": "t", "documents": [{"_id": 3}]}, b - 1); out["older"] = "accepted"
    except Exception as e:
        out["older"] = getattr(e, "code", str(e))
    out["docs"] = list(db.t.find().sort("_id"))
    ok = (out["insert_replay"].get("retriedStmtIds") == [0, 1] and out["update_replay"]["nModified"] == 1 and out["update_replay"].get("retriedStmtIds") == [0]
          and out["fam_replay"].get("retriedStmtId") == 0 and out["fam_replay"]["value"]["x"] == 5 and out["older"] == 225
          and out["docs"] == [{"_id": 1, "x": 1}, {"_id": 2, "x": 5}])
    for k, v in out.items(): print(k, v)
    print("g06_restart:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
