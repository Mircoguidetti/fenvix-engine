# Samples serverStatus().fenvix (s3 counters, compactor) every second into a jsonl file until killed.
import json, sys, time
from pymongo import MongoClient
uri, out = sys.argv[1], sys.argv[2]
c = MongoClient(uri, serverSelectionTimeoutMS=3000)
with open(out, "a") as f:
    while True:
        try:
            s = c.admin.command("serverStatus").get("fenvix", {})
            f.write(json.dumps({"ts": time.time(), "s3": s.get("s3"), "compactor": s.get("compactor"), "sealed": s.get("sealed")}, default=str) + "\n"); f.flush()
        except Exception as e:
            f.write(json.dumps({"ts": time.time(), "error": str(e)}) + "\n"); f.flush()
        time.sleep(1)
