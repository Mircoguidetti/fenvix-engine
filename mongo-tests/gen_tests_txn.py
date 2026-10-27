"""Differential tests for multi-document transactions (results7). They need a replica-set reference
(single-node `mongod --replSet`), so they are category "replset"; both servers are reached with
directConnection=true."""


import datetime


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    G = "transactions"

    def t(name, steps, **flags):
        add("txn." + name, steps, G, category="replset", **flags)

    def S(name):
        return {"op": "txn_start", "name": name}

    def C(name):
        return {"op": "txn_commit", "session": name}

    def A(name):
        return {"op": "txn_abort", "session": name}

    def ins1(coll, doc, s=None):
        st = {"op": "insert_one", "coll": coll, "doc": doc}
        if s:
            st["session"] = s
        return st

    def fnd(coll, filter=None, s=None, **kw):
        st = find(coll, filter or {}, sort={"_id": 1}, **kw)
        if s:
            st["session"] = s
        return st

    def u1(coll, filter, update, s=None, **kw):
        st = upd(coll, filter, update, **kw)
        if s:
            st["session"] = s
        return st

    def cnt(coll, s=None, filter=None):
        st = {"op": "count_documents", "coll": coll, "filter": filter or {}}
        if s:
            st["session"] = s
        return st

    base = [{"_id": i, "n": i, "g": i % 3} for i in range(1, 7)]

    t("commit_makes_inserts_visible", [S("a"), ins1("c", {"_id": 1, "v": 1}, "a"), ins1("c", {"_id": 2, "v": 2}, "a"), C("a"), fnd("c")])
    t("abort_discards_inserts", [ins("c", [{"_id": 0}]), S("a"), ins1("c", {"_id": 1}, "a"), A("a"), fnd("c")])
    t("read_your_own_writes", [ins("c", base), S("a"), ins1("c", {"_id": 10, "n": 10}, "a"), fnd("c", {"n": {"$gte": 5}}, "a"), cnt("c", "a"), cnt("c"), A("a")])
    t("uncommitted_invisible_to_others_until_commit", [ins("c", base), S("a"), ins1("c", {"_id": 10}, "a"), u1("c", {"_id": 1}, {"$set": {"n": 100}}, "a"), fnd("c"), C("a"), fnd("c")])
    t("update_in_txn_commit", [ins("c", base), S("a"), u1("c", {"g": 1}, {"$inc": {"n": 10}}, "a", kind="update_many"), fnd("c", {"g": 1}, "a"), C("a"), fnd("c", {"g": 1})])
    t("update_in_txn_abort", [ins("c", base), S("a"), u1("c", {"g": 1}, {"$inc": {"n": 10}}, "a", kind="update_many"), A("a"), fnd("c")])
    t("delete_in_txn_commit", [ins("c", base), S("a"), {"op": "delete_many", "coll": "c", "filter": {"g": 0}, "session": "a"}, cnt("c", "a"), cnt("c"), C("a"), cnt("c")])
    t("delete_in_txn_abort", [ins("c", base), S("a"), {"op": "delete_many", "coll": "c", "filter": {}, "session": "a"}, cnt("c", "a"), A("a"), cnt("c")])
    t("replace_in_txn", [ins("c", base), S("a"), u1("c", {"_id": 2}, {"zz": 1}, "a", kind="replace_one"), fnd("c", {"_id": 2}, "a"), C("a"), fnd("c", {"_id": 2})])
    t("upsert_in_txn", [S("a"), u1("c", {"_id": 9}, {"$set": {"x": 1}}, "a", upsert=True), fnd("c", {}, "a"), C("a"), fnd("c")])
    t("find_one_and_update_in_txn", [ins("c", base), S("a"), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$inc": {"n": 5}}, "return_document": "after", "session": "a"}, C("a"), fnd("c", {"_id": 1})])
    t("find_one_and_delete_in_txn", [ins("c", base), S("a"), {"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 1}, "session": "a"}, cnt("c", "a"), A("a"), cnt("c")])
    t("multi_collection_atomic_commit", [S("a"), ins1("x", {"_id": 1}, "a"), ins1("y", {"_id": 1}, "a"), cnt("x"), C("a"), cnt("x"), cnt("y"), {"op": "list_collection_names"}])
    t("multi_collection_abort_creates_nothing", [ins("keep", [{"_id": 1}]), S("a"), ins1("x", {"_id": 1}, "a"), ins1("y", {"_id": 1}, "a"), A("a"), {"op": "list_collection_names"}])
    t("delete_then_reinsert_same_id", [ins("c", base), S("a"), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}, "session": "a"}, ins1("c", {"_id": 1, "again": True}, "a"), C("a"), fnd("c", {"_id": 1})])
    t("dup_id_in_txn_aborts_txn", [ins("c", base), S("a"), ins1("c", {"_id": 20}, "a"), ins1("c", {"_id": 1}, "a"), ins1("c", {"_id": 21}, "a"), C("a"), cnt("c")])
    t("unique_violation_aborts_txn", [{"op": "create_index", "coll": "c", "keys": [["k", 1]], "unique": True}, ins("c", [{"_id": 1, "k": 1}]), S("a"), ins1("c", {"_id": 2, "k": 2}, "a"), ins1("c", {"_id": 3, "k": 1}, "a"), cnt("c")])
    t("unique_within_txn", [{"op": "create_index", "coll": "c", "keys": [["k", 1]], "unique": True}, S("a"), ins1("c", {"_id": 1, "k": 1}, "a"), ins1("c", {"_id": 2, "k": 1}, "a"), cnt("c")])
    t("unique_key_freed_in_txn", [{"op": "create_index", "coll": "c", "keys": [["k", 1]], "unique": True}, ins("c", [{"_id": 1, "k": 1}]), S("a"), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}, "session": "a"}, ins1("c", {"_id": 2, "k": 1}, "a"), C("a"), fnd("c")])
    t("snapshot_isolation_reads", [ins("c", base), S("a"), cnt("c", "a"), ins1("c", {"_id": 50}), u1("c", {"_id": 1}, {"$set": {"n": -1}}), cnt("c", "a"), fnd("c", {"_id": 1}, "a"), C("a"), cnt("c"), fnd("c", {"_id": 1})])
    t("snapshot_isolation_index_reads", [{"op": "create_index", "coll": "c", "keys": [["g", 1]]}, ins("c", base), S("a"), cnt("c", "a", {"g": 1}), ins1("c", {"_id": 60, "g": 1}), cnt("c", "a", {"g": 1}), cnt("c", None, {"g": 1}), A("a")])
    t("write_conflict_second_writer", [ins("c", base), S("a"), S("b"), u1("c", {"_id": 1}, {"$set": {"n": 11}}, "a"), u1("c", {"_id": 1}, {"$set": {"n": 22}}, "b"), C("a"), {"op": "txn_abort", "session": "b"}, fnd("c", {"_id": 1})])
    t("write_conflict_after_concurrent_commit", [ins("c", base), S("a"), S("b"), cnt("c", "b"), u1("c", {"_id": 1}, {"$set": {"n": 11}}, "a"), C("a"), u1("c", {"_id": 1}, {"$set": {"n": 22}}, "b"), {"op": "txn_abort", "session": "b"}, fnd("c", {"_id": 1})])
    t("write_conflict_with_non_txn_commit_before_write", [ins("c", base), S("a"), cnt("c", "a"), u1("c", {"_id": 1}, {"$set": {"n": 7}}), u1("c", {"_id": 1}, {"$set": {"n": 8}}, "a"), {"op": "txn_abort", "session": "a"}, fnd("c", {"_id": 1})])
    t("no_conflict_different_documents", [ins("c", base), S("a"), S("b"), u1("c", {"_id": 1}, {"$set": {"n": 11}}, "a"), u1("c", {"_id": 2}, {"$set": {"n": 22}}, "b"), C("a"), C("b"), fnd("c")])
    t("concurrent_insert_same_id_conflicts", [{"op": "create_collection", "coll": "c"}, S("a"), S("b"), ins1("c", {"_id": 1, "by": "a"}, "a"), ins1("c", {"_id": 1, "by": "b"}, "b"), C("a"), {"op": "txn_abort", "session": "b"}, fnd("c")])
    t("increment_in_two_sequential_txns", [ins("c", [{"_id": 1, "n": 0}]), S("a"), u1("c", {"_id": 1}, {"$inc": {"n": 1}}, "a"), C("a"), S("b"), u1("c", {"_id": 1}, {"$inc": {"n": 1}}, "b"), C("b"), fnd("c")])
    t("with_transaction_commits", [ins("c", base), {"op": "with_txn", "steps": [u1("c", {"_id": 1}, {"$set": {"n": 99}}), ins1("c", {"_id": 77}), cnt("c")]}, fnd("c", {"_id": {"$in": [1, 77]}})])
    t("with_transaction_error_aborts", [ins("c", base), {"op": "with_txn", "steps": [ins1("c", {"_id": 77}), ins1("c", {"_id": 1})]}, cnt("c")])
    t("find_sort_limit_in_txn", [ins("c", base), S("a"), ins1("c", {"_id": 10, "n": 0}, "a"), {"op": "find", "coll": "c", "filter": {}, "sort": {"n": 1}, "limit": 2, "session": "a"}, A("a")])
    t("aggregate_sees_txn_writes", [ins("c", base), S("a"), ins1("c", {"_id": 10, "n": 10, "g": 1}, "a"), {"op": "aggregate", "coll": "c", "pipeline": [{"$group": {"_id": "$g", "t": {"$sum": "$n"}}}, {"$sort": {"_id": 1}}], "session": "a"}, A("a")])
    t("distinct_in_txn", [ins("c", base), S("a"), ins1("c", {"_id": 10, "g": 9}, "a"), {"op": "distinct", "coll": "c", "key": "g", "session": "a"}, A("a")])
    t("indexed_find_in_txn_sees_own_writes", [{"op": "create_index", "coll": "c", "keys": [["g", 1]]}, ins("c", base), S("a"), u1("c", {"_id": 1}, {"$set": {"g": 5}}, "a"), fnd("c", {"g": 5}, "a"), fnd("c", {"g": 1}, "a"), C("a"), fnd("c", {"g": 5}), fnd("c", {"g": 1})])
    t("index_entries_after_abort", [{"op": "create_index", "coll": "c", "keys": [["g", 1]]}, ins("c", base), S("a"), u1("c", {"g": 1}, {"$set": {"g": 8}}, "a", kind="update_many"), A("a"), fnd("c", {"g": 8}), fnd("c", {"g": 1}), cmd({"validate": "c"}, ["valid"])])
    t("index_entries_after_commit", [{"op": "create_index", "coll": "c", "keys": [["g", 1]]}, ins("c", base), S("a"), u1("c", {"g": 1}, {"$set": {"g": 8}}, "a", kind="update_many"), C("a"), fnd("c", {"g": 8}), fnd("c", {"g": 1}), cmd({"validate": "c"}, ["valid"])])
    t("large_txn_500_inserts", [S("a"), {"op": "insert_many", "coll": "c", "docs": [{"_id": i, "v": i} for i in range(500)], "session": "a"}, cnt("c", "a"), cnt("c"), C("a"), cnt("c")])
    t("unordered_insert_error_aborts_txn", [ins("c", [{"_id": 1}]), S("a"), {"op": "insert_many", "coll": "c", "docs": [{"_id": 2}, {"_id": 1}, {"_id": 3}], "ordered": False, "session": "a"}, cnt("c", "a"), cnt("c")])
    t("drop_not_allowed_in_txn", [ins("c", base), S("a"), cnt("c", "a"), {"op": "command", "cmd": {"drop": "c"}, "session": "a", "pick": None}, cnt("c")])
    t("commit_empty_txn", [S("a"), cnt("c", "a"), C("a"), cnt("c")])
    t("txn_after_failed_txn_works", [ins("c", base), S("a"), ins1("c", {"_id": 1}, "a"), S("b"), ins1("c", {"_id": 11}, "b"), C("b"), cnt("c")])
    t("aborted_txn_rejects_further_ops", [ins("c", base), S("a"), ins1("c", {"_id": 1}, "a"), cnt("c", "a"), ins1("c", {"_id": 30}, "a")])
    t("ttl_collection_in_txn", [{"op": "create_index", "coll": "c", "keys": [["t", 1]], "opts": {"expireAfterSeconds": 3600}}, S("a"), ins1("c", {"_id": 1, "t": datetime.datetime(2100, 1, 1)}, "a"), C("a"), cnt("c")])
