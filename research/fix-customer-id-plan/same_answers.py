"""same_answers.py: the streamed customer shapes of cperf.py on base and head, the same data, no writes in between:
the documents and their order must be identical (`u`, written by cperf's update_one, included)."""
import hashlib, os, random, sys, time
import bson
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
from lib import Server
bucket = sys.argv[1]
rng = random.Random(7)
CS = [[rng.randrange(10000) for _ in range(3)] for _ in range(3)]
T = lambda cu: f"t{cu:05d}"
Q = {f"customer alone t{t}": {"thread_id": T(CS[t][0])} for t in range(3)}
Q.update({f"customer $in 3 t{t}": {"thread_id": {"$in": [T(x) for x in CS[t]]}} for t in range(3)})
Q["customer limit 1"] = None
out = {}
for label, b in (("base", "/home/user/bin/mongo-server-base-0b143c4"), ("head", "/home/user/bin/mongo-server-head3")):
    s = Server(bucket, "same" + label, idle=900, binary=b).start()
    for _ in range(60):
        try:
            MongoClient(s.uri).app_cperf.command("count", "docs"); break
        except Exception:
            time.sleep(1)
    docs = MongoClient(s.uri, socketTimeoutMS=1_800_000).app_cperf.get_collection("docs", codec_options=CodecOptions(document_class=RawBSONDocument))
    for name, f in Q.items():
        res = list(docs.find({"thread_id": T(CS[0][1])}).limit(1)) if f is None else list(docs.find(f))
        out[(label, name)] = (len(res), hashlib.sha1(b"".join(x.raw for x in res)).hexdigest()[:12])
    s.term()
    time.sleep(8)
for name in Q:
    a, b = out[("base", name)], out[("head", name)]
    print(f"{name:22} base {a} head {b} {'SAME' if a == b else 'DIFFERENT'}", flush=True)
