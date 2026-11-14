"""fix-l0-stall: an insert batch into a customer-keyed collection reads whether each document key exists once for the
whole batch (`coll::Writes::preread_ids`), not one point read per document in turn under the database's lock.

What must not change, seen from a client (each case with the batch read on, and with it off as the control
`FENVIX_INSERT_PREREAD=0`, which must give the same answers):
  - an `_id` already stored under the same customer is a duplicate key (11000), also when the stored document is in an
    SST of an earlier process (restart), not in the memtable;
  - the same `_id` under another customer is accepted (`_id` is unique per customer);
  - two documents of one batch with the same `_id` and customer: the second is the duplicate (the batch's own write is
    found first), ordered (the batch stops there) and unordered (the batch goes on);
  - a document deleted before the batch can be inserted again.
And the batch read ran (serverStatus `fenvix.insertPreread.keys` grows), or did not (control)."""
import pytest
from pymongo.errors import BulkWriteError

from lib import Server, make_bucket


def codes(e):
    return [(w["index"], w["code"]) for w in e.details["writeErrors"]]


def scenario(s):
    db = s.client().app
    c = db.messages
    c.insert_many([{"_id": i, "userId": i % 7, "x": "a" * 100} for i in range(2000)])
    c.delete_one({"_id": 5, "userId": 5})
    s.restart()  # the stored documents are now in SSTs of an earlier process
    db = s.client().app
    c = db.messages
    out = {}
    # 0: same customer, stored: duplicate; 1: same _id, other customer: accepted; 2: new; 3: deleted before: accepted
    batch = [{"_id": 10, "userId": 3}, {"_id": 10, "userId": 4}, {"_id": 5000, "userId": 1}, {"_id": 5, "userId": 5}]
    try:
        c.insert_many(batch, ordered=False)
        out["stored"] = []
    except BulkWriteError as e:
        out["stored"] = codes(e)
    # a duplicate inside one batch, unordered: the second of the pair fails, the rest goes in
    try:
        c.insert_many([{"_id": 6000, "userId": 2}, {"_id": 6001, "userId": 2}, {"_id": 6000, "userId": 2}, {"_id": 6002, "userId": 2}], ordered=False)
        out["in_batch_unordered"] = []
    except BulkWriteError as e:
        out["in_batch_unordered"] = codes(e)
    # ordered: the batch stops at the duplicate
    try:
        c.insert_many([{"_id": 7000, "userId": 2}, {"_id": 7000, "userId": 2}, {"_id": 7001, "userId": 2}], ordered=True)
        out["in_batch_ordered"] = []
    except BulkWriteError as e:
        out["in_batch_ordered"] = codes(e)
    out["present"] = sorted((d["_id"], d["userId"]) for d in c.find({"_id": {"$in": [5, 10, 5000, 6000, 6001, 6002, 7000, 7001]}}))
    out["count"] = c.count_documents({})
    out["preread"] = db.command("serverStatus")["fenvix"]["insertPreread"]["keys"]
    return out


EXPECTED = {
    "stored": [(0, 11000)],
    "in_batch_unordered": [(2, 11000)],
    "in_batch_ordered": [(1, 11000)],
    "present": [(5, 5), (10, 3), (10, 4), (5000, 1), (6000, 2), (6001, 2), (6002, 2), (7000, 2)],
    "count": 1999 + 3 + 3 + 1,
}


@pytest.mark.parametrize("preread", [True, False])
def test_insert_batch_read_keeps_duplicate_keys_exact(preread):
    env = {} if preread else {"FENVIX_INSERT_PREREAD": "0"}
    s = Server(make_bucket("preread"), "preread", idle=600, extra=["--customer-key", "messages=userId"], env=env).start()
    try:
        out = scenario(s)
    finally:
        s.term()
    got = {k: v for k, v in out.items() if k != "preread"}
    assert got == EXPECTED
    if preread:
        assert out["preread"] > 0, out
    else:
        assert out["preread"] == 0, out
