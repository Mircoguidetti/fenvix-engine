"""Poll the erasure pass until every erasure is done; print request -> scrubbed (live runs clean) -> done (no object of
ours left), in seconds. A tiny write every poll keeps the database open (its compactor runs GC)."""
import json, sys, time, urllib.request
import pymongo
TOKEN = "test-token-0123456789abcdef"
admin, db, out = sys.argv[1], sys.argv[2], sys.argv[3]
c = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true")
def run():
    req = urllib.request.Request(f"{admin}/v1/databases/{db}/erasures/run", method="POST", data=b"{}", headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=7200).read())["erasures"]
t0 = time.time()
while True:
    c[db]["probe"].insert_one({"t": time.time()})
    es = run()
    if all(e["done_ms"] for e in es) or time.time() - t0 > 3600:
        break
    time.sleep(30)
rows = [{"id": e["id"], "cleaned_by": e.get("cleaned_by"), "scrubbed_s": (e["scrubbed_ms"] - e["requested_ms"]) / 1000 if e.get("scrubbed_ms") else None,
         "done_s": (e["done_ms"] - e["requested_ms"]) / 1000 if e.get("done_ms") else None, "overdue": e["overdue"]} for e in es]
json.dump(rows, open(out, "w"), indent=1)
print(json.dumps(rows[-3:], indent=1))
