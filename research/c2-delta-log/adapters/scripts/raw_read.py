"""Raw BSON of every document of a collection, thread by thread, each thread's documents sorted by _id with batch_size 8.
With FENVIX_SEALED_TPIN_MS=1000 any single operation that runs longer than T_pin (1 s) fails with SealedRunsRetry (code
134): a whole-collection cursor, and even one `distinct` over a 270 MB collection. So the thread ids come from the e2e
answers file (THREADS_JSON) and each thread's short cursor is scanned again on that error."""
import json
import os

import pymongo
from bson.raw_bson import RawBSONDocument

URI = "mongodb://localhost:27017/?directConnection=true"
c = pymongo.MongoClient(URI, document_class=RawBSONDocument)
plain = pymongo.MongoClient(URI)
RETRIES = []
THREADS = sorted(k for k in json.load(open(os.environ["THREADS_JSON"]))["answers"] if not k.startswith("_"))


def retry(what, fn):
    for attempt in range(50):
        try:
            return fn()
        except pymongo.errors.OperationFailure as e:
            if e.code != 134:
                raise
            RETRIES.append(what)
    raise RuntimeError(f"{what}: 50 SealedRunsRetry in a row")


def raw(db, coll):
    out = []
    for t in THREADS:
        docs = retry(f"{db}.{coll} {t}", lambda: [bytes(d.raw) for d in c[db][coll].find({"thread_id": t}, batch_size=8).sort("_id", 1)])
        n = retry(f"count {db}.{coll} {t}", lambda: plain[db][coll].count_documents({"thread_id": t}))
        assert n == len(docs), f"{db}.{coll} {t}: {len(docs)} documents read, count says {n}"
        out += docs
    return out
