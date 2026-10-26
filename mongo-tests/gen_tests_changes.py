"""Differential tests for change streams (results7); category "replset"."""
import datetime

from bson import Int64, ObjectId


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    G = "change streams"

    def t(name, steps, **flags):
        add("cs." + name, steps, G, category="replset", **flags)

    def w(actions, expect, coll="c", pipeline=None, **opts):
        st = {"op": "watch", "coll": coll, "actions": actions, "expect": expect, "pipeline": pipeline or []}
        if opts:
            st["options"] = opts
        return st

    def i1(doc, coll="c"):
        return {"op": "insert_one", "coll": coll, "doc": doc}

    def u(filter, update, coll="c", **kw):
        return upd(coll, filter, update, **kw)

    base = {"_id": 1, "a": 1, "s": {"x": 1, "y": 2}, "arr": [1, 2, 3], "t": "x"}
    pre = {"op": "create_collection", "coll": "c"}

    t("insert", [pre, w([i1(base)], 1)])
    t("insert_many_in_order", [pre, w([ins("c", [{"_id": i, "v": i} for i in range(1, 6)])], 5)])
    t("insert_default_has_no_full_document_on_update", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 2}})], 1)])
    t("update_set", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 2}})], 1)])
    t("update_inc", [pre, ins("c", [base]), w([u({"_id": 1}, {"$inc": {"a": 5}})], 1)])
    t("update_unset", [pre, ins("c", [base]), w([u({"_id": 1}, {"$unset": {"t": ""}})], 1)])
    t("update_set_nested_existing", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"s.x": 9}})], 1)])
    t("update_set_nested_new", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"new.deep": 1}})], 1)])
    t("update_set_whole_subdoc", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"s": {"z": 1}}})], 1)])
    t("update_push_append", [pre, ins("c", [base]), w([u({"_id": 1}, {"$push": {"arr": 4}})], 1)])
    t("update_push_each", [pre, ins("c", [base]), w([u({"_id": 1}, {"$push": {"arr": {"$each": [4, 5]}}})], 1)])
    t("update_pull", [pre, ins("c", [base]), w([u({"_id": 1}, {"$pull": {"arr": 1}})], 1)])
    t("update_pop", [pre, ins("c", [base]), w([u({"_id": 1}, {"$pop": {"arr": 1}})], 1)])
    t("update_addtoset", [pre, ins("c", [base]), w([u({"_id": 1}, {"$addToSet": {"arr": 9}})], 1)])
    t("update_array_index", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"arr.1": 7}})], 1)])
    t("update_rename", [pre, ins("c", [base]), w([u({"_id": 1}, {"$rename": {"a": "b"}})], 1)])
    t("update_noop_emits_nothing", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 1}}), u({"_id": 1}, {"$set": {"a": 2}})], 1)])
    t("update_many_one_event_per_doc", [pre, ins("c", [{"_id": i, "g": 1} for i in range(1, 4)]), w([u({"g": 1}, {"$set": {"h": 1}}, kind="update_many")], 3)])
    t("replace", [pre, ins("c", [base]), w([u({"_id": 1}, {"r": 1}, kind="replace_one")], 1)])
    t("delete", [pre, ins("c", [base]), w([{"op": "delete_one", "coll": "c", "filter": {"_id": 1}}], 1)])
    t("delete_many", [pre, ins("c", [{"_id": i} for i in range(1, 4)]), w([{"op": "delete_many", "coll": "c", "filter": {}}], 3)])
    t("upsert_is_insert", [pre, w([u({"_id": 5}, {"$set": {"x": 1}}, upsert=True)], 1)])
    t("find_one_and_update", [pre, ins("c", [base]), w([{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$inc": {"a": 1}}}], 1)])
    t("find_one_and_delete", [pre, ins("c", [base]), w([{"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 1}}], 1)])
    t("update_lookup_returns_current_document", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 2}})], 1, full_document="updateLookup")])
    t("update_lookup_null_when_deleted_later", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 2}}), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}], 2, full_document="updateLookup")])
    t("update_lookup_sees_later_update", [pre, ins("c", [base]), w([u({"_id": 1}, {"$set": {"a": 2}}), u({"_id": 1}, {"$set": {"a": 3}})], 2, full_document="updateLookup")])
    t("only_events_after_open", [pre, ins("c", [{"_id": 0}]), w([i1({"_id": 1})], 1)])
    t("other_collections_not_seen", [pre, w([i1({"_id": 1}, "other"), i1({"_id": 2})], 1)])
    t("match_operation_type", [pre, ins("c", [base]), w([i1({"_id": 2}), u({"_id": 1}, {"$set": {"a": 2}}), {"op": "delete_one", "coll": "c", "filter": {"_id": 2}}], 2, pipeline=[{"$match": {"operationType": {"$in": ["insert", "delete"]}}}])])
    t("match_full_document_field", [pre, w([i1({"_id": 1, "k": "a"}), i1({"_id": 2, "k": "b"}), i1({"_id": 3, "k": "a"})], 2, pipeline=[{"$match": {"fullDocument.k": "a"}}])])
    t("project_stage", [pre, w([i1({"_id": 1, "k": "a", "z": 1})], 1, pipeline=[{"$project": {"operationType": 1, "fullDocument.k": 1}}])])
    t("addfields_stage", [pre, w([i1({"_id": 1})], 1, pipeline=[{"$addFields": {"tag": "x"}}])])
    t("string_and_compound_ids", [pre, w([i1({"_id": "abc", "v": 1}), i1({"_id": {"a": 1, "b": 2}, "v": 2})], 2)])
    t("many_inserts_keep_order", [pre, w([ins("c", [{"_id": i} for i in range(200)])], 200)])
    t("database_level_sees_all_collections", [{"op": "create_collection", "coll": "x"}, {"op": "create_collection", "coll": "y"}, {"op": "watch", "actions": [i1({"_id": 1}, "x"), i1({"_id": 2}, "y")], "expect": 2, "pipeline": []}])
    t("database_level_drop_event", [{"op": "create_collection", "coll": "x"}, {"op": "watch", "actions": [{"op": "drop_collection", "coll": "x"}], "expect": 1, "pipeline": []}])
    t("collection_drop_then_invalidate", [pre, w([{"op": "drop_collection", "coll": "c"}], 2)])
    t("resume_after_token", [pre, {"op": "watch", "coll": "c", "actions": [i1({"_id": 1}), i1({"_id": 2})], "expect": 1, "resume_actions": [i1({"_id": 3})], "resume_expect": 2, "pipeline": []}])
    t("start_after_token", [pre, {"op": "watch", "coll": "c", "actions": [i1({"_id": 1})], "expect": 1, "resume_actions": [i1({"_id": 2})], "resume_expect": 1, "resume_with": "start_after", "pipeline": []}])
    t("resume_with_match", [pre, {"op": "watch", "coll": "c", "actions": [i1({"_id": 1, "k": 1}), i1({"_id": 2, "k": 2})], "expect": 1, "resume_actions": [i1({"_id": 3, "k": 1}), i1({"_id": 4, "k": 2})], "resume_expect": 2, "pipeline": [{"$match": {"fullDocument.k": {"$gte": 1}}}]}])
    t("transaction_events_at_commit", [pre, {"op": "watch", "coll": "c", "actions": [{"op": "with_txn", "steps": [i1({"_id": 1}), u({"_id": 1}, {"$set": {"a": 1}}), i1({"_id": 2})]}], "expect": 3, "pipeline": []}])
    t("aborted_transaction_emits_nothing", [pre, w([{"op": "txn_start", "name": "a"}, {"op": "insert_one", "coll": "c", "doc": {"_id": 1}, "session": "a"}, {"op": "txn_abort", "session": "a"}, i1({"_id": 2})], 1)])
    t("unordered_insert_partial_events", [pre, ins("c", [{"_id": 1}]), w([{"op": "command", "cmd": {"insert": "c", "ordered": False, "documents": [{"_id": 2}, {"_id": 1}, {"_id": 3}]}, "pick": ["n", "ok"]}], 2)])
    t("ttl_delete_event", [{"op": "create_index", "coll": "c", "keys": [["t", 1]], "opts": {"expireAfterSeconds": 0}}, w([i1({"_id": 1, "t": datetime.datetime(2001, 1, 1)}), {"op": "sleep", "secs": 3}], 2)])
    t("update_double_and_long_values", [pre, ins("c", [{"_id": 1, "d": 1.5, "l": Int64(1)}]), w([u({"_id": 1}, {"$inc": {"d": 1, "l": 1}})], 1)])
    t("date_and_oid_in_documents", [pre, w([i1({"_id": ObjectId("000000000000000000000001"), "d": datetime.datetime(2024, 1, 2)})], 1)])
