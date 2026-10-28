#!/usr/bin/env python3
"""Mark the driver members that the founder allowed to be excluded as purely local (no request leaves the driver) in inventory.csv.

Only the rows listed here; each carries its one-sentence reason.  The other members of that list (GridIn.abort, GridIn.writelines,
GridFSBucketReadStream.abort/end/start) turned out to send requests (or to be exercised through requests) and are covered by tests
instead.  Idempotent.  usage: python3 compat/apply_local_exclusions.py"""
import csv, os

HERE = os.path.dirname(os.path.abspath(__file__))
INV = os.path.join(HERE, "inventory.csv")
LOCAL = {
    "node:MongoClient.connect-2": "purely local: calling connect() on a client that is already connected returns at once and sends no request to the server",
}
rows = list(csv.DictReader(open(INV, newline="")))
cols = list(rows[0].keys())
n = 0
for r in rows:
    if r["id"] in LOCAL:
        r["exclusion_proposed"], r["exclusion_reason"], r["exclusion_approved"] = "yes", LOCAL[r["id"]], "yes"
        n += 1
with open(INV, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
print(n, "rows marked as purely local (founder-allowed exclusion)")
