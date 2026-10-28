"""Item 10: change streams beyond the basics: fullDocument whenAvailable / required, pre- and post-images, expanded (DDL) events,
rename, invalidate, startAtOperationTime, option errors."""
import re
from bson import json_util
from gaps_py import case
from pymongo import IndexModel
from pymongo.errors import OperationFailure, PyMongoError


def strip(e):
    e = dict(e)
    had_uuid = "collectionUUID" in e
    for k in ("_id", "clusterTime", "wallTime", "lsid", "txnNumber", "collectionUUID"):
        e.pop(k, None)
    out = {"__keys": list(e.keys()), "__uuid": had_uuid}
    out.update(e)
    return norm(out)


def norm(x):
    if isinstance(x, dict):
        if x.get("subType") == 4 and "$binary" in x:
            return "<uuid>"
        return {k: ("<uuid>" if k in ("uuid", "dropTarget") and not isinstance(v, (dict, list)) else norm(v)) for k, v in x.items()}
    if isinstance(x, list):
        return [norm(v) for v in x]
    if isinstance(x, bytes):
        return "<bin>"
    if isinstance(x, str):
        return re.sub(r"Timestamp\(\d+, \d+\)", "Timestamp(T)", x)
    return x


def run(db, client, opts, actions, target="coll", n=20, pre=None):
    if pre:
        pre(db)
    t = {"coll": lambda: db.c, "db": lambda: db}[target]()
    try:
        st = t.watch(max_await_time_ms=200, **opts)
    except OperationFailure as e:
        return {"open_err": [e.code, e.details.get("codeName"), norm(e.details.get("errmsg"))]}
    out = []
    try:
        actions(db, client)
        for _ in range(n):
            try:
                e = st.try_next()
            except OperationFailure as ex:
                out.append({"err": [ex.code, ex.details.get("codeName"), norm(ex.details.get("errmsg"))]})
                break
            if e is not None:
                out.append(strip(e))
    finally:
        st.close()
    return out


def crud(db, client):
    db.c.insert_one({"_id": 1, "a": 1})
    db.c.update_one({"_id": 1}, {"$set": {"a": 2}})
    db.c.replace_one({"_id": 1}, {"a": 3})
    db.c.delete_one({"_id": 1})
    db.c.delete_one({"_id": 77})
    db.c.update_one({"_id": 5}, {"$set": {"x": 1}}, upsert=True)
    db.c.update_one({"_id": 5}, {"$set": {"x": 1}})


def images(db):
    db.create_collection("c", changeStreamPreAndPostImages={"enabled": True})


for _name, _opts in {
    "default": {}, "update_lookup": {"full_document": "updateLookup"}, "when_available": {"full_document": "whenAvailable"}, "required": {"full_document": "required"},
    "before_when": {"full_document_before_change": "whenAvailable"}, "before_required": {"full_document_before_change": "required"}, "both": {"full_document": "whenAvailable", "full_document_before_change": "whenAvailable"},
    "both_required": {"full_document": "required", "full_document_before_change": "required"}, "lookup_before": {"full_document": "updateLookup", "full_document_before_change": "whenAvailable"},
    "bad_full": {"full_document": "nope"}, "bad_before": {"full_document_before_change": "nope"}, "expanded": {"show_expanded_events": True},
}.items():
    case(f"g10.cs.opts_{_name}_plain", rs=True)(lambda client, db, o=_opts: run(db, client, o, crud))
    case(f"g10.cs.opts_{_name}_images", rs=True)(lambda client, db, o=_opts: run(db, client, o, crud, pre=images))


def update_forms(db, client):
    db.c.insert_one({"_id": 2, "arr": [1, 2, 3], "d": {"e": 1}})
    for u in ({"$push": {"arr": 4}}, {"$pop": {"arr": 1}}, [{"$set": {"p": 1}}], {"$rename": {"p": "q"}}, {"$inc": {"d.e": 1}}, {"$set": {"d": {"f": 1}}}, {"$set": {"n.m": 1}}, {"$unset": {"q": 1}}):
        db.c.update_one({"_id": 2}, u)


case("g10.cs.update_forms", rs=True)(lambda client, db: run(db, client, {}, update_forms))


def ddl(db, client):
    db.create_collection("e1")
    db.e1.create_indexes([IndexModel([("a", 1)], name="a_1", unique=True), IndexModel([("b", 1)], name="b_1")])
    db.command({"collMod": "e1", "index": {"name": "a_1", "hidden": True}})
    db.command({"collMod": "e1", "validator": {"a": {"$gt": 0}}})
    db.e1.drop_index("a_1")
    db.e2.insert_one({"_id": 1})
    db.e1.rename("e3")
    db.e2.rename("e3", dropTarget=True)
    db.e3.drop()


case("g10.cs.ddl_expanded", rs=True)(lambda client, db: run(db, client, {"show_expanded_events": True}, ddl, "db"))
case("g10.cs.ddl_standard", rs=True)(lambda client, db: run(db, client, {}, ddl, "db"))


def create_variants(db, client):
    db.create_collection("p1", capped=True, size=4096, max=10)
    db.create_collection("p2", validator={"a": {"$gt": 1}}, validationLevel="moderate")
    db.create_collection("p4", changeStreamPreAndPostImages={"enabled": True})
    db.command({"create": "p5", "viewOn": "p1", "pipeline": [{"$match": {}}]})
    db.p6.insert_one({"_id": 1})


case("g10.cs.create_variants", rs=True)(lambda client, db: run(db, client, {"show_expanded_events": True}, create_variants, "db"))


def rename_coll(db, client):
    db.c.insert_one({"_id": 1})
    db.c.rename("c2")
    db.c2.insert_one({"_id": 2})


case("g10.cs.rename_collection_stream", rs=True)(lambda client, db: run(db, client, {}, rename_coll))


def drop_db(db, client):
    db.z.insert_one({"_id": 1})
    client.drop_database(db.name)


case("g10.cs.drop_database_db_stream", rs=True)(lambda client, db: run(db, client, {}, drop_db, "db"))
case("g10.cs.drop_database_coll_stream", rs=True)(lambda client, db: run(db, client, {}, lambda d, c: (d.c.insert_one({"_id": 1}), c.drop_database(d.name))))


def image_toggle(db, client):
    db.create_collection("c")
    db.command({"collMod": "c", "changeStreamPreAndPostImages": {"enabled": True}})
    db.c.insert_one({"_id": 5})
    db.c.update_one({"_id": 5}, {"$set": {"z": 1}})
    db.command({"collMod": "c", "changeStreamPreAndPostImages": {"enabled": False}})
    db.c.update_one({"_id": 5}, {"$set": {"z": 2}})


case("g10.cs.image_toggle", rs=True)(lambda client, db: run(db, client, {"full_document_before_change": "whenAvailable", "full_document": "whenAvailable"}, image_toggle))


@case("g10.cs.image_option_errors", rs=True)
def _opt_err(client, db):
    out = {}
    for i, v in enumerate([{"enabled": True}, {}, True, {"enabled": "x"}, {"enabled": False}, {"enabled": True, "x": 1}]):
        try:
            db.command({"create": f"o{i}", "changeStreamPreAndPostImages": v})
            out[f"create{i}"] = "ok"
        except OperationFailure as e:
            out[f"create{i}"] = [e.code, e.details.get("codeName"), e.details.get("errmsg")]
    for i, v in enumerate([{"enabled": False}, 5, {}]):
        try:
            db.command({"collMod": "o0", "changeStreamPreAndPostImages": v})
            out[f"collMod{i}"] = "ok"
        except OperationFailure as e:
            out[f"collMod{i}"] = [e.code, e.details.get("codeName"), e.details.get("errmsg")]
    out["listed"] = sorted((x["name"], x["options"]) for x in db.command("listCollections")["cursor"]["firstBatch"])
    return out


@case("g10.cs.start_at_operation_time", rs=True)
def _sat(client, db):
    with client.start_session() as s:
        db.c.insert_one({"_id": 0}, session=s)
        ts = s.operation_time
    db.c.insert_one({"_id": 1})
    st = db.c.watch(start_at_operation_time=ts, max_await_time_ms=200)
    out = []
    for _ in range(6):
        e = st.try_next()
        if e is not None:
            out.append(strip(e))
    st.close()
    return out


@case("g10.cs.option_errors", rs=True)
def _opts(client, db):
    out = {}
    for name, o in {"unknown": {"bogus": 1}, "resume_bad": {"resumeAfter": {"x": 1}}, "start_after_bad": {"startAfter": 5}}.items():
        try:
            c = db.command({"aggregate": "c", "pipeline": [{"$changeStream": o}], "cursor": {}})
            out[name] = "ok"
        except OperationFailure as e:
            out[name] = [e.code, e.details.get("codeName"), e.details.get("errmsg")]
    return out
