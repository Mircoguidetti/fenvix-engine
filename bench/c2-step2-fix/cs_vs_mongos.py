"""Fix round of compaction 2 step 2: the change-stream tests of the diff suites that update a document, replayed against
mongos 7.0.34 with the collection sharded on the customer field (the reference the founder's rule names: "like MongoDB
sharded collections") and against our server with the same field as customer key. The diff suites' reference is an
unsharded mongod, whose documentKey is `{_id}`. mongos refuses an update of the shard key whose filter does not name the
shard key (31025), so every update that changes `a` names the current `a` in its filter here, on both targets.

Usage: cs_vs_mongos.py MONGOS_URI OURS_URI   (ours started with `--customer-key c=a`). Prints each test and whether the
events (operationType, documentKey with its field order, fullDocument, updateDescription) are equal.
"""
import sys
import time

from pymongo import MongoClient

base = {"_id": 1, "a": 1, "s": {"x": 1, "y": 2}, "arr": [1, 2, 3], "t": "x"}
U = lambda f, u: ("update_one", f, u)
TESTS = {
    "insert": (False, [("insert_one", base)], None),
    "update_set": (True, [U({"_id": 1, "a": 1}, {"$set": {"a": 2}})], None),
    "update_inc": (True, [U({"_id": 1, "a": 1}, {"$inc": {"a": 5}})], None),
    "update_unset": (True, [U({"_id": 1}, {"$unset": {"t": ""}})], None),
    "update_push_append": (True, [U({"_id": 1}, {"$push": {"arr": 4}})], None),
    "replace": (True, [("replace_one", {"_id": 1}, {"_id": 1, "a": 1, "r": 1})], None),
    "delete": (True, [("delete_one", {"_id": 1})], None),
    "find_one_and_update": (True, [("find_one_and_update", {"_id": 1, "a": 1}, {"$inc": {"a": 1}})], None),
    "find_one_and_delete": (True, [("find_one_and_delete", {"_id": 1})], None),
    "update_lookup_returns_current_document": (True, [U({"_id": 1, "a": 1}, {"$set": {"a": 2}})], "updateLookup"),
    "update_lookup_null_when_deleted_later": (True, [U({"_id": 1, "a": 1}, {"$set": {"a": 2}}), ("delete_one", {"_id": 1})], "updateLookup"),
    "update_lookup_sees_later_update": (True, [U({"_id": 1, "a": 1}, {"$set": {"a": 2}}), U({"_id": 1, "a": 2}, {"$set": {"a": 3}})], "updateLookup"),
    "update_lookup_customer_unchanged": (True, [U({"_id": 1}, {"$set": {"t": "y"}})], "updateLookup"),
}


def run(uri, sharded, name):
    c = MongoClient(uri, directConnection=not sharded)
    db = c["csx"]
    db.drop_collection("c")
    if sharded:
        c.admin.command("enableSharding", "csx")
        c.admin.command("shardCollection", "csx.c", key={"a": 1})
    else:
        db.create_collection("c")
    pre, actions, full = TESTS[name]
    if pre:
        db.c.insert_one(dict(base))
    kw = {"full_document": full} if full else {}
    with db.c.watch(max_await_time_ms=500, **kw) as st:
        for op, *args in actions:
            getattr(db.c, op)(*args)
        out = []
        for _ in range(20):
            e = st.try_next()
            if e is not None:
                out.append((e["operationType"], list(e["documentKey"].items()), e.get("fullDocument"), e.get("updateDescription")))
                if len(out) >= len(actions):
                    break
    return out


same = 0
for name in TESTS:
    a, b = run(sys.argv[1], True, name), run(sys.argv[2], False, name)
    ok = a == b
    same += ok
    print(f"cs.{name}: {'SAME as mongos' if ok else 'DIFFERENT'}")
    if not ok:
        print("   mongos:", a)
        print("   ours:  ", b)
print(f"{same}/{len(TESTS)} the same as mongos with the collection sharded on the customer field")
