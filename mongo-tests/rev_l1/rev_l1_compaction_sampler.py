import time, json, sys
from pymongo import MongoClient
c = MongoClient("mongodb://127.0.0.1:27801/?directConnection=true", serverSelectionTimeoutMS=5000)
while True:
    try:
        f = c.admin.command("serverStatus")["fenvix"]
        print(time.strftime("%H:%M:%S"), json.dumps(f.get("compaction")), flush=True)
    except Exception as e:
        print("ERR", e, flush=True)
        if "refused" in str(e): break
    time.sleep(30)
