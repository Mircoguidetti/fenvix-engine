"""Reviewer: write a generated database's documents through clients, in the generator's order, into a fresh database
with the same catalog, then seal every row into one version (fenvixSealedPublish, reseal all, compactor off).
Usage: replay.py SAMPLE.bson TAG  -> prints the bucket name"""
import sys, time, bson
from pymongo import MongoClient
from lib import Server, make_bucket
sample, tag = sys.argv[1], sys.argv[2]
DB = "app_data_b01"
bucket = make_bucket(tag)
extra = ["--sealed-runs", "2", "--sealed-t-pin-ms", "1000", "--sealed-e1-wait-ms", "0", "--sealed-test-short-e1-wait",
         "--envelopes", "write", "--customer-key", "messaggi=cliente:ts,eventi=cliente:ts,ordini=cliente:ts"]
# phase 1: the clients' writes with sealed runs off (SlateDB only); phase 2: the first build seals every row
srv = Server(bucket, tag, idle=3600, extra=["--no-sealed-runs"] + extra[7:]).start()
c = MongoClient(srv.uri, retryWrites=False)
for coll in ["messaggi", "eventi", "ordini"]:
    c[DB].command({"createIndexes": coll, "indexes": [{"key": {"cliente": 1, "ts": -1}, "name": "cliente_ts"}]})
c[DB].command({"createIndexes": "ordini", "indexes": [{"key": {"stato": 1, "ts": -1}, "name": "stato_ts"}]})
docs = bson.decode_all(open(sample, "rb").read(), bson.CodecOptions(document_class=bson.SON))
t0, n = time.time(), 0
# insertion numbers are per collection: each collection in the generator's order, 1,000 documents per insert
for coll in ["messaggi", "eventi", "ordini"]:
    mine = [d["d"] for d in docs if d["c"] == coll]
    for i in range(0, len(mine), 1000):
        c[DB][coll].insert_many(mine[i:i + 1000], ordered=True)
        n += len(mine[i:i + 1000])
print(f"inserted {n} documents in {time.time() - t0:.0f} s", flush=True)
srv.term()
srv = Server(bucket, tag + "b", idle=3600, extra=extra, env={"FENVIX_SEALED_COMPACTOR": "0"}).start()
c = MongoClient(srv.uri, retryWrites=False)
for _ in range(60):
    r = c.admin.command({"fenvixSealedPublish": DB, "reseal": "all"})
    if r.get("published"):
        break
    time.sleep(0.5)
print("publish", r.get("published"), r.get("millis"), flush=True)
srv.term()
print("BUCKET", bucket)
