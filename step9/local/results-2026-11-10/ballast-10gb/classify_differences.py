import pymongo, math, json, sys, bson
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
name=sys.argv[1]
RAW = CodecOptions(document_class=RawBSONDocument)
ours = pymongo.MongoClient("mongodb://127.0.0.1:27119", directConnection=True)[name]
ref = pymongo.MongoClient("mongodb://127.0.0.1:27021", directConnection=True)[name]
g=json.load(open(f"/tmp/claude-0/g10/out/{name}.gen.json"))
sumdiff=other=0; n=0
for i in g["sample_customers"]:
    cli=f"cli-{i:06d}"
    p=[{"$match":{"cliente":cli}},{"$group":{"_id":"$stato","n":{"$sum":1},"t":{"$sum":"$totale"}}},{"$sort":{"_id":1}}]
    a=list(ours.ordini.aggregate(p)); b=list(ref.ordini.aggregate(p))
    if [(x["_id"],x["n"]) for x in a]!=[(x["_id"],x["n"]) for x in b]: other+=1; print("COUNT DIFF", cli); continue
    for x,y in zip(a,b):
        n+=1
        if x["t"]!=y["t"]:
            exact=math.fsum(d["totale"] for d in ref.ordini.find({"cliente":cli,"stato":y["_id"]},{"totale":1}))
            if exact==y["t"] and abs(x["t"]-y["t"])<=1e-9*abs(y["t"]): sumdiff+=1
            else: other+=1; print("OTHER", cli, x, y, exact)
    for c in ("messaggi","eventi","ordini"):
        s=[("ts",-1),("_id",1)]
        if [bytes(d.raw) for d in ours.get_collection(c,codec_options=RAW).find({"cliente":cli}).sort(s)] != [bytes(d.raw) for d in ref.get_collection(c,codec_options=RAW).find({"cliente":cli}).sort(s)]:
            other+=1; print("WHOLE DIFF", cli, c)
print(json.dumps({"db":name,"group_sums":n,"sum_last_digit":sumdiff,"other_differences":other}))
