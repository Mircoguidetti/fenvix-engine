"""Change stream options and events beyond what the suite's cs.* tests cover (those cover insert/update/replace/delete/drop/invalidate, updateLookup,
resumeAfter/startAfter, pipelines, db-level streams). Real side: single-node replica set."""
import uuid
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from probe_diff import case


def strip(e):
    e = dict(e)
    for k in ("_id", "clusterTime", "wallTime", "lsid", "txnNumber", "collectionUUID"):
        e.pop(k, None)
    od = e.get("operationDescription")
    if isinstance(od, dict):
        od = {k: v for k, v in od.items() if k != "indexes" or True}
        od.pop("uuid", None)
        e["operationDescription"] = od
    if isinstance(e.get("ns"), dict):
        e["ns"] = {k: ("$DB" if k == "db" else v) for k, v in e["ns"].items()}
    if isinstance(e.get("to"), dict):
        e["to"] = {k: ("$DB" if k == "db" else v) for k, v in e["to"].items()}
    return e


def collect(st, n, tries=10):
    out = []
    for _ in range(tries):
        e = st.try_next()
        if e is not None:
            out.append(strip(e))
        if len(out) >= n:
            break
    return out


def watch_case(name, ids, target, opts, actions, n, rs=True, fail="partial", pre=None):
    def f(client, db):
        if pre:
            pre(client, db)
        t = {"coll": lambda: db.c, "db": lambda: db, "client": lambda: client}[target]()
        st = t.watch(max_await_time_ms=300, **opts)
        try:
            actions(client, db)
            return collect(st, n)
        finally:
            st.close()
    case(name, ids, rs=rs, fail=fail)(f)


def ins_upd(client, db):
    db.c.insert_one({"_id": 1, "a": 1})
    db.c.update_one({"_id": 1}, {"$set": {"a": 2}})


watch_case("compat.cs.fullDocument_whenAvailable", ["cso:$changeStream.fullDocument", "cs:whenAvailable"], "coll", {"full_document": "whenAvailable"}, ins_upd, 2)
watch_case("compat.cs.fullDocument_required", ["cs:required"], "coll", {"full_document": "required"}, ins_upd, 2)
watch_case("compat.cs.fullDocument_default", ["cs:updateLookup"], "coll", {"full_document": "updateLookup"}, ins_upd, 2)


def pre_images(client, db):
    db.create_collection("c", changeStreamPreAndPostImages={"enabled": True})


watch_case("compat.cs.fullDocumentBeforeChange_whenAvailable", ["cso:$changeStream.fullDocumentBeforeChange"], "coll",
           {"full_document_before_change": "whenAvailable", "full_document": "whenAvailable"}, ins_upd, 2, pre=pre_images)
watch_case("compat.cs.fullDocumentBeforeChange_required_without_images", [], "coll", {"full_document_before_change": "required"}, ins_upd, 2)
watch_case("compat.cs.pre_post_images_collection_option", ["cs:pre- and post-images (changeStreamPreAndPostImages)"], "coll",
           {"full_document_before_change": "required", "full_document": "required"}, ins_upd, 2, pre=pre_images)


def ddl(client, db):
    db.create_collection("c")
    db.c.create_index("a", name="a_1")
    db.command({"collMod": "c", "index": {"name": "a_1", "hidden": True}})
    db.c.drop_index("a_1")
    db.c.rename("c2")
    db.c2.drop()


for _ev, _n in (("create", 1), ("createIndexes", 2), ("modify", 3), ("dropIndexes", 4)):
    watch_case(f"compat.cs.expanded_{_ev}", [f"cse:{_ev}"] + (["cso:$changeStream.showExpandedEvents"] if _ev == "create" else []), "db",
               {"show_expanded_events": True}, ddl, _n, fail="missing")


def ddl_rename_drop(client, db):
    db.create_collection("c")
    db.c.rename("c2")
    db.c2.drop()


# rename and drop are standard events (no showExpandedEvents needed)
watch_case("compat.cs.rename_and_drop_events", ["cse:rename", "cse:drop"], "db", {}, ddl_rename_drop, 2)


def dropdb(client, db):
    db.c.insert_one({"_id": 1})
    client.drop_database(db.name)


watch_case("compat.cs.dropDatabase", ["cse:dropDatabase"], "db", {}, dropdb, 3)


def two_dbs(client, db):
    db.c.insert_one({"_id": 1})
    client[db.name + "_b"].c.insert_one({"_id": 2})
    client.drop_database(db.name + "_b")


watch_case("compat.cs.deployment_all_changes", ["cs:watch a deployment", "cso:$changeStream.allChangesForCluster"], "client", {}, two_dbs, 2)


@case("compat.cs.startAtOperationTime", ["cso:$changeStream.startAtOperationTime"], rs=True)
def _sat(client, db):
    with client.start_session() as s:
        db.c.insert_one({"_id": 0}, session=s)
        ts = s.operation_time
    db.c.insert_one({"_id": 1})
    if ts is None:
        return {"operation_time_reported": False}
    st = db.c.watch(start_at_operation_time=ts, max_await_time_ms=300)
    try:
        return {"operation_time_reported": True, "events": collect(st, 2)}
    finally:
        st.close()
