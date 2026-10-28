#!/usr/bin/env python3
"""Record the founder's decision on the proposed exclusions in inventory.csv (new column exclusion_approved).

Approved (exclusion_proposed=yes and exclusion_approved=yes):
  A1  client-only driver members, internal server-to-server commands, client-side encryption helpers
  A2  sharding, replication (except replSetGetStatus / replSetGetConfig, which stay in scope)
  A3  Enterprise/Atlas-only authentication mechanisms, audit commands, the removed/8.0-only cases, undocumented commands (except `features`),
      security.javascriptEnabled
  A4  $vectorSearch (not in MongoDB 7.0 Community), txn:UnknownTransactionCommitResult (a single node never produces it)
Everything else that was proposed is put back in scope (exclusion_proposed=no): the admin commands, the search-index commands and the driver
members that call them, `features`, replSetGetStatus / replSetGetConfig and their driver member.

Idempotent.  usage: python3 compat/apply_exclusions.py"""
import csv, os

HERE = os.path.dirname(os.path.abspath(__file__))
INV = os.path.join(HERE, "inventory.csv")
rows = list(csv.DictReader(open(INV, newline="")))
cols = list(rows[0].keys())
if "exclusion_approved" not in cols:
    cols.insert(cols.index("exclusion_reason") + 1, "exclusion_approved")
for r in rows:
    r.setdefault("exclusion_approved", "no")

KEEP_IN_SCOPE = {"cmd:replSetGetStatus", "cmd:replSetGetConfig", "cmd:features", "node:Admin.replSetGetStatus",
                 "cmd:createSearchIndexes", "cmd:dropSearchIndex", "cmd:updateSearchIndex", "cmd:listSearchIndexes"}
NEW = {
    "stage:$vectorSearch": "A4: $vectorSearch does not exist in MongoDB 7.0 Community (Atlas Vector Search only); mongod 7.0.45 rejects the stage",
    "txn:UnknownTransactionCommitResult": "A4: the error label is added by the server only when the outcome of a commit is unknown (needs a failover or a network partition); a single node never produces it",
}


def group(r):
    t = r["exclusion_reason"].lower(); first = t.split(";")[0]; i = r["id"]
    if i in NEW: return "A4"
    if "calls only commands" in t: return "B"
    if first.startswith(("client-side only", "internal server-to-server")) or "client-side field-level encryption" in first: return "A1"
    if "shard" in first or "replica-set" in first: return "A2"
    if first.startswith("auditing") or i.startswith("authmech") or "removed" in first or "8.0" in first or "undocumented" in first or "no page" in first or "javascriptenabled" in first: return "A3"
    return "B"


n = {"A1": 0, "A2": 0, "A3": 0, "A4": 0, "back_in_scope": 0}
for r in rows:
    i = r["id"]
    if i in NEW:
        r["exclusion_proposed"], r["exclusion_reason"], r["exclusion_approved"] = "yes", NEW[i], "yes"
        n["A4"] += 1
        continue
    if r["exclusion_proposed"] != "yes":
        continue
    g = group(r)
    if i in KEEP_IN_SCOPE or g == "B":
        r["exclusion_proposed"], r["exclusion_reason"], r["exclusion_approved"] = "no", "", "no"
        n["back_in_scope"] += 1
    else:
        r["exclusion_approved"] = "yes"
        n[g] += 1
with open(INV, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
    w.writeheader(); w.writerows(rows)
print(n)
