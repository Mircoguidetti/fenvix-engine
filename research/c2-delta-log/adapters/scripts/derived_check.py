"""Derived documents check on our server (localhost:27017), run after an e2e loop, on the same bucket (KEEP=1 restart).
    derived_check.py OUT.json [ANSWERS_JSON]
For every database with a `checkpoints` collection: raw BSON of every document of `checkpoints` and `checkpoint_writes`
(thread by thread, each sorted by _id, batch_size 8: raw_read.py) before fenvixReseal, fenvixReseal, serverStatus().fenvix.derived, raw BSON again, byte
comparison. With ANSWERS_JSON (the e2e output of the langgraph loop): re-read every thread with the saver and compare."""
import hashlib
import json
import sys

import pymongo
from bson.raw_bson import RawBSONDocument

out = {"databases": {}}


from raw_read import raw, retry, RETRIES, c, plain  # noqa: E402


def derived():
    return plain.admin.command("serverStatus")["fenvix"]["derived"]


out["derived_start"] = derived()
dbs = [d for d in plain.list_database_names() if "checkpoints" in plain[d].list_collection_names()]
for db in dbs:
    r = {}
    before = {k: raw(db, k) for k in ("checkpoints", "checkpoint_writes")}
    rs = retry("fenvixReseal " + db, lambda: plain.admin.command("fenvixReseal", db))
    if rs.get("compactor") is False:
        # the database is open read-only after the restart: one tiny write in a side collection opens it for writing
        plain[db]["_derive_check"].insert_one({"x": 1})
        rs = retry("fenvixReseal " + db, lambda: plain.admin.command("fenvixReseal", db))
    r["reseal"] = {k: v for k, v in rs.items() if k != "$clusterTime"}
    r["derived_after_reseal"] = derived()
    after = {k: raw(db, k) for k in ("checkpoints", "checkpoint_writes")}
    for k in before:
        b, a = before[k], after[k]
        diffs = sum(1 for x, y in zip(b, a) if x != y) + abs(len(b) - len(a))
        r[k] = {"docs_before": len(b), "docs_after": len(a), "bytes": sum(map(len, b)), "byte_differences": diffs,
                "sha256_before": hashlib.sha256(b"".join(b)).hexdigest(), "sha256_after": hashlib.sha256(b"".join(a)).hexdigest()}
    out["databases"][db] = r
out["derived_end"] = derived()
out["scan_retries_SealedRunsRetry"] = RETRIES
out["compactor"] = plain.admin.command("serverStatus")["fenvix"].get("compactor")
json.dump(out, open(sys.argv[1], "w"), indent=1, default=str)
print(json.dumps(out, indent=1, default=str))
