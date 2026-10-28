"""Item 1: silent defects. createIndexes with several indexes; find without sort returns natural (insertion) order."""
from gaps_py import case
from pymongo import ASCENDING
from pymongo.errors import OperationFailure

SHUF = [30, 10, 20, 5, 25, 1, 99, 50]


def ids(cur):
    return [d["_id"] for d in cur]


@case("g01.createIndexes.several_in_one_call")
def _multi(client, db):
    db.t.insert_many([{"_id": i, "a": i, "b": i, "c": i} for i in range(4)])
    r = db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"b": -1}, "name": "b_1"}, {"key": {"c": 1, "a": 1}, "name": "ca"}]})
    return {"before": r["numIndexesBefore"], "after": r["numIndexesAfter"], "created": r.get("createdCollectionAutomatically"), "listed": sorted((i["name"], tuple(i["key"].items())) for i in db.t.list_indexes())}


@case("g01.createIndexes.several_on_new_collection")
def _multi_new(client, db):
    r = db.command({"createIndexes": "t", "indexes": [{"key": {"x": 1}, "name": "x_1"}, {"key": {"y": 1}, "name": "y_1", "unique": True}]})
    return {"r": {k: r[k] for k in ("createdCollectionAutomatically", "numIndexesBefore", "numIndexesAfter")}, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.second_spec_invalid_creates_none")
def _bad_second(client, db):
    db.t.insert_many([{"_id": i, "a": i} for i in range(3)])
    try:
        db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {}, "name": "bad"}]})
        err = None
    except OperationFailure as e:
        err = (e.code, e.details.get("codeName"))
    return {"err": err, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.duplicate_in_request")
def _dup_req(client, db):
    db.t.insert_one({"_id": 1, "a": 1})
    try:
        r = db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"a": 1}, "name": "a_1"}]})
        out = {k: r.get(k) for k in ("numIndexesBefore", "numIndexesAfter", "note")}
    except OperationFailure as e:
        out = {"err": (e.code, e.details.get("codeName"))}
    return {"out": out, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.same_key_two_names_in_request")
def _dup_key(client, db):
    db.t.insert_one({"_id": 1, "a": 1})
    try:
        db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "one"}, {"key": {"a": 1}, "name": "two"}]})
        out = "ok"
    except OperationFailure as e:
        out = (e.code, e.details.get("codeName"))
    return {"out": out, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.some_existing_some_new")
def _mix(client, db):
    db.t.insert_many([{"_id": i, "a": i, "b": i} for i in range(5)])
    db.t.create_index("a", name="a_1")
    r = db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"b": 1}, "name": "b_1"}]})
    return {"r": {k: r.get(k) for k in ("numIndexesBefore", "numIndexesAfter", "note")}, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.several_used_by_queries")
def _used(client, db):
    db.t.insert_many([{"_id": i, "a": i % 5, "b": i % 7} for i in range(60)])
    db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"b": 1}, "name": "b_1"}]})
    ex = db.command("explain", {"find": "t", "filter": {"b": 3}, "hint": {"b": 1}}, verbosity="queryPlanner")
    return {"plan": ex["queryPlanner"]["winningPlan"]["inputStage"]["indexName"], "n": db.t.count_documents({"b": 3})}


@case("g01.createIndexes.several_unique_violation_on_second")
def _uniq(client, db):
    db.t.insert_many([{"_id": 1, "a": 1, "b": 1}, {"_id": 2, "a": 2, "b": 1}])
    try:
        db.command({"createIndexes": "t", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"b": 1}, "name": "b_u", "unique": True}]})
        out = "ok"
    except OperationFailure as e:
        out = (e.code, e.details.get("codeName"))
    return {"out": out, "listed": sorted(i["name"] for i in db.t.list_indexes())}


@case("g01.createIndexes.pymongo_create_indexes_helper")
def _helper(client, db):
    from pymongo import IndexModel
    db.t.insert_one({"_id": 1})
    names = db.t.create_indexes([IndexModel([("a", 1)]), IndexModel([("b", -1)], unique=True), IndexModel([("c", 1), ("d", -1)], name="cd")])
    return {"names": names, "listed": sorted(i["name"] for i in db.t.list_indexes())}


# ------------------------------------------------------------------ natural order
def seeded(db, name="t", ids_=SHUF):
    db[name].insert_many([{"_id": i, "v": i % 3} for i in ids_])
    return db[name]


@case("g01.natural.find_all")
def _n1(client, db):
    return ids(seeded(db).find())


@case("g01.natural.find_filter_unindexed")
def _n2(client, db):
    return ids(seeded(db).find({"v": {"$in": [0, 1]}}))


@case("g01.natural.find_skip_limit")
def _n3(client, db):
    return ids(seeded(db).find().skip(2).limit(3))


@case("g01.natural.find_projection_only_id")
def _n3b(client, db):
    return ids(seeded(db).find({}, {"_id": 1}))


@case("g01.natural.after_update_keeps_position")
def _n4(client, db):
    t = seeded(db)
    t.update_one({"_id": 30}, {"$set": {"big": "x" * 5000}})
    t.replace_one({"_id": 10}, {"replaced": True})
    return ids(t.find())


@case("g01.natural.delete_then_reinsert_goes_last")
def _n5(client, db):
    t = seeded(db)
    t.delete_one({"_id": 20})
    t.insert_one({"_id": 20, "again": 1})
    t.insert_one({"_id": 2})
    return ids(t.find())


@case("g01.natural.upsert_appends")
def _n6(client, db):
    t = seeded(db)
    t.update_one({"_id": 3}, {"$set": {"n": 1}}, upsert=True)
    t.update_one({"_id": 7}, {"$set": {"n": 1}}, upsert=True)
    return ids(t.find())


@case("g01.natural.sort_natural_asc_desc")
def _n7(client, db):
    t = seeded(db)
    return {"asc": ids(t.find().sort("$natural", 1)), "desc": ids(t.find().sort("$natural", -1))}


@case("g01.natural.hint_natural")
def _n8(client, db):
    t = seeded(db)
    return {"fwd": ids(t.find().hint([("$natural", 1)])), "rev": ids(t.find().hint([("$natural", -1)])), "filtered": ids(t.find({"v": 1}).hint([("$natural", -1)]))}


@case("g01.natural.find_one_picks_first_inserted")
def _n9(client, db):
    t = seeded(db)
    return {"one": t.find_one({"v": 2}), "any": t.find_one()}


@case("g01.natural.update_one_and_delete_one_pick_first_inserted")
def _n10(client, db):
    t = seeded(db)
    t.update_one({"v": 1}, {"$set": {"hit": 1}})
    t.delete_one({"v": 2})
    return ids(t.find())


@case("g01.natural.find_one_and_update_no_sort")
def _n11(client, db):
    t = seeded(db)
    r = t.find_one_and_update({"v": 0}, {"$set": {"hit": 1}})
    r2 = t.find_one_and_delete({"v": 1})
    return {"r": r, "r2": r2}


@case("g01.natural.aggregate_match_and_project")
def _n12(client, db):
    t = seeded(db)
    return {"all": ids(t.aggregate([])), "match": ids(t.aggregate([{"$match": {"v": {"$ne": 0}}}])), "limit": ids(t.aggregate([{"$limit": 3}]))}


@case("g01.natural.aggregate_group_first_last")
def _n13(client, db):
    t = seeded(db)
    return sorted(t.aggregate([{"$group": {"_id": "$v", "first": {"$first": "$_id"}, "last": {"$last": "$_id"}, "all": {"$push": "$_id"}}}]), key=lambda d: d["_id"])


@case("g01.natural.aggregate_out_preserves_order")
def _n14(client, db):
    t = seeded(db)
    list(t.aggregate([{"$match": {}}, {"$out": "o"}]))
    return ids(db.o.find())


@case("g01.natural.distinct_and_unwind")
def _n15(client, db):
    t = seeded(db)
    t.insert_one({"_id": 400, "v": 9})
    return {"distinct": t.distinct("_id"), "v": t.distinct("v")}


@case("g01.natural.string_and_objectid_ids")
def _n16(client, db):
    t = db.t
    t.insert_many([{"_id": "zeta"}, {"_id": "alpha"}, {"_id": "mid"}, {"_id": 5}, {"_id": None}, {"_id": {"a": 1}}])
    return [d["_id"] for d in t.find()]


@case("g01.natural.insert_many_batches")
def _n17(client, db):
    t = db.t
    docs = [{"_id": (i * 7919) % 1000, "i": i} for i in range(700)]
    t.insert_many(docs)
    return [d["i"] for d in t.find()][:50] + [t.count_documents({})]


@case("g01.natural.bulk_write_mixed")
def _n18(client, db):
    from pymongo import InsertOne, UpdateOne, DeleteOne, ReplaceOne
    t = db.t
    t.insert_many([{"_id": 4}, {"_id": 2}, {"_id": 8}])
    t.bulk_write([InsertOne({"_id": 1}), DeleteOne({"_id": 2}), InsertOne({"_id": 2}), UpdateOne({"_id": 9}, {"$set": {"u": 1}}, upsert=True), ReplaceOne({"_id": 4}, {"r": 1})])
    return ids(t.find())


@case("g01.natural.cursor_getmore_batches")
def _n19(client, db):
    t = db.t
    t.insert_many([{"_id": (i * 37) % 301} for i in range(301)])
    return ids(t.find().batch_size(7))[:40]


@case("g01.natural.list_via_index_hint_uses_index_order")
def _n20(client, db):
    t = seeded(db)
    t.create_index("v")
    return {"filter": ids(t.find({"v": {"$gte": 0}}).hint("v_1")), "sort": ids(t.find().sort("v", 1).hint("v_1"))}


@case("g01.natural.transaction_inserts_append", rs=True)
def _n21(client, db):
    t = seeded(db)
    db.create_collection("keep") if False else None
    with client.start_session() as s:
        with s.start_transaction():
            t.insert_one({"_id": 1000}, session=s)
            t.delete_one({"_id": 5}, session=s)
            t.insert_one({"_id": 5, "re": 1}, session=s)
            inside = ids(t.find(session=s))
    return {"inside": inside, "after": ids(t.find())}


@case("g01.natural.rename_collection_keeps_order")
def _n22(client, db):
    t = seeded(db)
    t.rename("t2")
    db.t2.insert_one({"_id": 0})
    return ids(db.t2.find())


@case("g01.natural.collection_copy_via_out_after_deletes")
def _n23(client, db):
    t = seeded(db)
    t.delete_many({"v": 1})
    t.insert_many([{"_id": 2, "v": 1}, {"_id": 100}])
    return ids(t.find())
