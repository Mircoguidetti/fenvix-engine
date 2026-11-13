"""generator-1tb, second session: the one WRONG of check3 (group sum cli-008760 spedito: ours 78996.43000000001, mongod
78996.43). Compares the customer's documents byte for byte on both servers and the exact sum of the values.
Usage: sum_case.py OURS_URI REF_URI CUSTOMER STATE"""
import math
import sys

import bson
import pymongo
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument

RAW = CodecOptions(document_class=RawBSONDocument)
ours, ref, cli, st = sys.argv[1:5]
o = pymongo.MongoClient(ours, directConnection=True).get_database("app_data_b01", codec_options=RAW)["ordini"]
r = pymongo.MongoClient(ref, directConnection=True).get_database("app_data_b01", codec_options=RAW)["ordini"]
do = {d["_id"]: d.raw for d in o.find({"cliente": cli})}
dr = {d["_id"]: d.raw for d in r.find({"cliente": cli})}
print("documents ours", len(do), "ref", len(dr), "same _ids", set(do) == set(dr), "differing documents", sum(do[k] != dr.get(k) for k in do))
vals = [bson.decode(v)["totale"] for v in dr.values() if bson.decode(v)["stato"] == st]
print(st, "n", len(vals), "exact (math.fsum)", repr(math.fsum(vals)), "plain sum", repr(sum(vals)))
for c, name in ((o, "ours"), (r, "mongod")):
    a = list(c.aggregate([{"$match": {"cliente": cli, "stato": st}}, {"$group": {"_id": None, "t": {"$sum": "$totale"}}}]))
    print(name, "$sum", repr(a[0]["t"]))
