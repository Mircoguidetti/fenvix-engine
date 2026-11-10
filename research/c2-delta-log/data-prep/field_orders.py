"""Count the distinct top-level field orders of the documents in every dump.

MongoDBSaver writes with upserts. On insert, mongod builds the new document from the filter's equality fields
(thread_id, checkpoint_ns, checkpoint_id, and for writes task_id, task_path, idx) followed by the $set fields.
The order of those equality fields is NOT stable: it varies from document to document, even within one
collection. A byte-exact rebuild of the stored BSON must therefore keep each document's field order.

Usage: field_orders.py -> results/field_orders.json
"""
import collections
import glob
import json
import os
import struct

from bson.raw_bson import RawBSONDocument

import cells as C

HERE = os.path.dirname(os.path.abspath(__file__))
COLLS = ("checkpoints", "checkpoint_writes")


def main():
    out = {}
    total = [collections.Counter(), collections.Counter()]
    for path in sorted(glob.glob(os.path.join(C.DUMPS, "*.fxd"))):
        c = [collections.Counter(), collections.Counter()]
        with open(path, "rb") as f:
            data = f.read()
        p = 4
        while p < len(data):
            ci, tl = struct.unpack_from("<BH", data, p)
            p += 3 + tl
            (dl,) = struct.unpack_from("<I", data, p)
            p += 4
            c[ci][tuple(RawBSONDocument(data[p:p + dl]).keys())] += 1
            p += dl
        for ci in (0, 1):
            total[ci].update(c[ci])
        out[os.path.basename(path)[:-4]] = {COLLS[ci]: len(c[ci]) for ci in (0, 1)}
        print(os.path.basename(path), out[os.path.basename(path)[:-4]], flush=True)
    out["all_dumps"] = {COLLS[ci]: [{"docs": v, "order": list(k)} for k, v in total[ci].most_common()] for ci in (0, 1)}
    json.dump(out, open(os.path.join(HERE, "results", "field_orders.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
