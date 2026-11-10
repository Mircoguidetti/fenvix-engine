"""After a cold restart: raw BSON of checkpoints/checkpoint_writes again (raw_read.py), compared with the sha256 recorded by derived_check.py."""
import hashlib, json, sys
from raw_read import raw, RETRIES
ref = json.load(open(sys.argv[1]))
for db, r in ref["databases"].items():
    for k in ("checkpoints", "checkpoint_writes"):
        docs = raw(db, k)
        h = hashlib.sha256(b"".join(docs)).hexdigest()
        print(f"cold restart {db}.{k}: {len(docs)} docs, identical to before the reseal: {h == r[k]['sha256_before']}")
print(f"cold restart scans repeated after SealedRunsRetry: {len(RETRIES)}")
