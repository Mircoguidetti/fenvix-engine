# per sampled customer of one database: compare the documents of ours and the reference (by cliente, index reads)
import sys, json, pymongo, bson
from bson.codec_options import CodecOptions
RAW = CodecOptions(document_class=bson.raw_bson.RawBSONDocument)
db, gen = sys.argv[1], json.load(open(sys.argv[2]))
o = pymongo.MongoClient("mongodb://127.0.0.1:27017")[db]; r = pymongo.MongoClient("mongodb://127.0.0.1:27018")[db]
bad = 0
for i in gen["sample_customers"]:
    cli = f"cli-{i:06d}"
    for c in ("ordini", "eventi", "messaggi"):
        a = {d["_id"]: d.raw for d in o.get_collection(c, codec_options=RAW).find({"cliente": cli}, batch_size=500)}
        b = {d["_id"]: d.raw for d in r.get_collection(c, codec_options=RAW).find({"cliente": cli}, batch_size=500)}
        if a != b:
            bad += 1
            print(cli, c, "only ours", len(a.keys() - b.keys()), "only ref", len(b.keys() - a.keys()),
                  "differ", sum(1 for k in a.keys() & b.keys() if a[k] != b[k]), flush=True)
print("DIFF", db, "customer-collections differing:", bad)
