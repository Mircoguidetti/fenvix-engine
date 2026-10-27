"""Runs search_ops_cases.json (see search_ops_gen.py) with pymongo. Usage: search_ops_test.py [mongodb-uri]"""
import sys, json, datetime, pymongo
from bson import json_util
uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?directConnection=true"
data = json_util.loads(open(__file__.rsplit("/", 1)[0] + "/search_ops_cases.json").read())
db = pymongo.MongoClient(uri)["search_ops_test"]
db.drop_collection("ops"); db.ops.insert_many(data["docs"])

def norm(v, drop=()):
    if isinstance(v, dict): return {k: norm(x, drop) for k, x in sorted(v.items()) if k not in drop}
    if isinstance(v, list): return [norm(x, drop) for x in v]
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return round(float(v), 9)
    if isinstance(v, datetime.datetime): return ("date", int(v.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000))
    return v

fails = []
for c in data["cases"]:
    kind, want = c["kind"], c["expect"]
    try:
        out = list(db.ops.aggregate(c["pipeline"]))
    except pymongo.errors.OperationFailure as e:
        if kind != "error": fails.append(f"{c['name']}: unexpected error {e}")
        continue
    if kind == "error": fails.append(f"{c['name']}: expected an error, got {out}"); continue
    if kind == "ids": got, want = sorted(d["_id"] for d in out), sorted(want)
    elif kind == "ordered": got = [d["_id"] for d in out]
    elif kind == "scoresById": got, want = {str(d["_id"]): d["s"] for d in out}, want
    elif kind == "docsById": got = {str(d["_id"]): {k: v for k, v in d.items() if k != "_id"} for d in out}
    elif kind == "docsNoScore": got = out
    else: got = out
    drop = ("score",) if kind == "docsNoScore" else ()
    if norm(got, drop) != norm(want, drop): fails.append(f"{c['name']}: got {got} want {want}")
n = len(data["cases"])
print(f"search operator tests (pymongo): {n - len(fails)} passed, {len(fails)} failed"); [print(" ", f[:600]) for f in fails]
sys.exit(1 if fails else 0)
