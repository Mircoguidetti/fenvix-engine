"""Differential tests for secondary indexes (results7, step 2): planner and explain, query results through
every index shape, index maintenance, unique / sparse / partial indexes, backfill, dropIndexes, persistence.
Registered into gen_tests.TESTS by gen_tests.py."""
import datetime

from bson import Int64, ObjectId


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    Dbl = g["Dbl"]
    G = "secondary indexes"

    def cix(coll, keys, **opts):
        step = {"op": "create_index", "coll": coll, "keys": keys}
        if opts:
            step["opts"] = opts
        return step

    def docs(n, coll_fields=None):
        out = []
        for i in range(1, n + 1):
            d = {"_id": i, "a": i % 10, "b": i % 7, "s": f"k{i % 13:02d}", "m": [i % 5, (i + 1) % 5], "o": {"x": i % 4, "y": f"y{i % 3}"}}
            out.append(d)
        return out

    D = docs(60)

    def explain(filter=None, plan_stats=False, coll="c", **kw):
        inner = {"find": coll, "filter": filter or {}}
        inner.update(kw)
        return cmd({"explain": inner, "verbosity": "executionStats" if plan_stats else "queryPlanner"}, None, reduce="explain_stats" if plan_stats else "explain")

    def fs(filter=None, sort=None, **kw):
        return find("c", filter or {}, sort=sort or {"_id": 1}, **kw)

    def t(name, steps, **flags):
        add("idx." + name, steps, G, **flags)

    def lst():
        return {"op": "list_indexes", "coll": "c", "full": True}

    # ---- plans and explain ------------------------------------------------------------------------
    t("plan_eq_ixscan", [ins("c", D), cix("c", [["a", 1]]), explain({"a": 3})])
    t("plan_no_index_collscan", [ins("c", D), explain({"a": 3})])
    t("plan_range_gt_lt", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$gt": 2, "$lt": 7}})])
    t("plan_range_gte_only", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$gte": 4}})])
    t("plan_range_lte_only", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$lte": 4}})])
    t("plan_in_list", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$in": [1, 3, 5]}})])
    t("plan_string_range", [ins("c", D), cix("c", [["s", 1]]), explain({"s": {"$gte": "k03", "$lt": "k07"}})])
    t("plan_compound_prefix", [ins("c", D), cix("c", [["a", 1], ["b", 1]]), explain({"a": 3})])
    t("plan_compound_eq_eq", [ins("c", D), cix("c", [["a", 1], ["b", 1]]), explain({"a": 3, "b": 2})])
    t("plan_compound_eq_range", [ins("c", D), cix("c", [["a", 1], ["b", 1]]), explain({"a": 3, "b": {"$gt": 2}})])
    t("plan_compound_non_prefix_collscan", [ins("c", D), cix("c", [["a", 1], ["b", 1]]), explain({"b": 2})])
    t("plan_compound_desc_bounds", [ins("c", D), cix("c", [["a", 1], ["b", -1]]), explain({"a": 3, "b": {"$gt": 2}})])
    t("plan_nested_path", [ins("c", D), cix("c", [["o.x", 1]]), explain({"o.x": 2})])
    t("plan_sort_by_index_limit", [ins("c", D), cix("c", [["a", 1]]), explain({}, sort={"a": 1}, limit=3)])
    t("plan_sort_desc_backward", [ins("c", D), cix("c", [["a", 1]]), explain({}, sort={"a": -1}, limit=3)])
    t("plan_sort_without_index_has_sort_stage", [ins("c", D), explain({}, sort={"a": 1}, limit=3)])
    t("plan_sort_after_equality_prefix", [ins("c", D), cix("c", [["a", 1], ["b", 1]]), explain({"a": 3}, sort={"b": 1})])
    t("plan_compound_mixed_directions_forward", [ins("c", D), cix("c", [["a", 1], ["b", -1]]), explain({}, sort={"a": 1, "b": -1}, limit=5)])
    t("plan_compound_mixed_directions_backward", [ins("c", D), cix("c", [["a", 1], ["b", -1]]), explain({}, sort={"a": -1, "b": 1}, limit=5)])
    t("plan_compound_mixed_directions_mismatch_sorts", [ins("c", D), cix("c", [["a", 1], ["b", -1]]), explain({}, sort={"a": 1, "b": 1}, limit=5)])
    t("plan_hint_by_name", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["b", 1]]), explain({"a": 3, "b": 2}, hint="b_1")])
    t("plan_hint_by_pattern", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["b", 1]]), explain({"a": 3, "b": 2}, hint={"b": 1})])
    t("plan_hint_natural", [ins("c", D), cix("c", [["a", 1]]), explain({"a": 3}, hint={"$natural": 1})])
    t("plan_hint_natural_reverse", [ins("c", D), cix("c", [["a", 1]]), explain({"a": 3}, hint={"$natural": -1})])
    t("plan_hint_unconstrained_field", [ins("c", D), cix("c", [["b", 1]]), explain({"a": 3}, hint={"b": 1})])
    t("plan_id_lookup", [ins("c", D), explain({"_id": 5})])
    t("plan_multikey_flag", [ins("c", D), cix("c", [["m", 1]]), explain({"m": 2})])
    t("plan_sparse_unconstrained_not_used", [ins("c", D), cix("c", [["z", 1]], sparse=True), explain({"a": 3}, sort={"z": 1}, limit=2)])
    t("plan_sparse_used_when_range", [ins("c", D), cix("c", [["a", 1]], sparse=True), explain({"a": {"$gt": 5}})])
    t("plan_partial_used_when_implied", [ins("c", D), cix("c", [["b", 1]], partialFilterExpression={"a": {"$gt": 4}}), explain({"b": 2, "a": {"$gt": 6}})])
    t("plan_partial_not_used_when_not_implied", [ins("c", D), cix("c", [["b", 1]], partialFilterExpression={"a": {"$gt": 4}}), explain({"b": 2})])
    t("stats_eq_keys_docs", [ins("c", D), cix("c", [["a", 1]]), explain({"a": 3}, plan_stats=True)])
    t("stats_range_keys_docs", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$gte": 8}}, plan_stats=True)])
    t("stats_residual_filter_docs", [ins("c", D), cix("c", [["a", 1]]), explain({"a": 3, "b": 3}, plan_stats=True)])
    t("stats_collscan_docs", [ins("c", D), explain({"a": 3}, plan_stats=True)])
    t("stats_sort_limit_early_stop", [ins("c", D), cix("c", [["a", 1]]), explain({}, sort={"a": 1}, limit=3, plan_stats=True)])
    t("stats_unique_point", [ins("c", D), cix("c", [["s", 1]]), explain({"s": "k05"}, plan_stats=True)])

    # ---- results through every index shape ----------------------------------------------------------------
    def results(name, filter, keys, data=None, sort=None, extra=None, **idxopts):
        steps = [ins("c", data if data is not None else D), cix("c", keys, **idxopts)] + (extra or []) + [fs(filter, sort=sort)]
        t("res_" + name, steps)

    results("eq", {"a": 3}, [["a", 1]])
    results("eq_missing_value", {"a": 99}, [["a", 1]])
    results("range", {"a": {"$gt": 2, "$lte": 6}}, [["a", 1]])
    results("range_desc_index", {"a": {"$gt": 2, "$lte": 6}}, [["a", -1]])
    results("in", {"a": {"$in": [1, 5, 99]}}, [["a", 1]])
    results("in_desc_index", {"a": {"$in": [1, 5, 99]}}, [["a", -1]])
    results("string_prefix_range", {"s": {"$gte": "k05", "$lt": "k09"}}, [["s", 1]])
    results("compound_eq_range", {"a": 3, "b": {"$gte": 1}}, [["a", 1], ["b", 1]])
    results("compound_desc_second", {"a": 3, "b": {"$lt": 5}}, [["a", 1], ["b", -1]])
    results("compound_desc_first", {"a": {"$gt": 6}, "b": 2}, [["a", -1], ["b", 1]])
    results("compound_in_then_range", {"a": {"$in": [2, 4]}, "b": {"$gt": 1}}, [["a", 1], ["b", 1]])
    results("nested", {"o.x": 1, "o.y": "y1"}, [["o.x", 1], ["o.y", 1]])
    results("multikey_eq", {"m": 3}, [["m", 1]])
    results("multikey_range_both_bounds", {"m": {"$gt": 1, "$lt": 3}}, [["m", 1]])
    results("multikey_elemmatch_range", {"m": {"$elemMatch": {"$gt": 1, "$lt": 3}}}, [["m", 1]])
    results("multikey_in", {"m": {"$in": [0, 4]}}, [["m", 1]])
    results("multikey_sort", None, [["m", 1]], sort={"m": 1, "_id": 1})
    results("multikey_whole_array_eq", {"m": [1, 2]}, [["m", 1]])
    results("multikey_empty_array", {"m": []}, [["m", 1]], data=[{"_id": 1, "m": []}, {"_id": 2, "m": [1]}, {"_id": 3}, {"_id": 4, "m": None}])
    results("null_matches_missing_and_null", {"k": None}, [["k", 1]], data=[{"_id": 1}, {"_id": 2, "k": None}, {"_id": 3, "k": 1}, {"_id": 4, "k": [None, 2]}])
    results("exists_true", {"k": {"$exists": True}}, [["k", 1]], data=[{"_id": 1}, {"_id": 2, "k": None}, {"_id": 3, "k": 1}])
    results("type_bracketing_numbers_only", {"k": {"$gt": 1}}, [["k", 1]], data=[{"_id": 1, "k": 0}, {"_id": 2, "k": 2}, {"_id": 3, "k": "x"}, {"_id": 4, "k": None}, {"_id": 5, "k": True}, {"_id": 6, "k": [3]}])
    results("type_bracketing_strings_only", {"k": {"$lt": "m"}}, [["k", 1]], data=[{"_id": 1, "k": 0}, {"_id": 2, "k": "a"}, {"_id": 3, "k": "z"}, {"_id": 4, "k": None}, {"_id": 5, "k": {"x": 1}}])
    results("mixed_numeric_types", {"k": 5}, [["k", 1]], data=[{"_id": 1, "k": 5}, {"_id": 2, "k": Dbl(5.0)}, {"_id": 3, "k": Int64(5)}, {"_id": 4, "k": 5.5}, {"_id": 5, "k": Int64(2 ** 60 + 1)}])
    results("big_int64_distinct", {"k": Int64(2 ** 60)}, [["k", 1]], data=[{"_id": 1, "k": Int64(2 ** 60)}, {"_id": 2, "k": Int64(2 ** 60 + 1)}, {"_id": 3, "k": Dbl(float(2 ** 60))}])
    results("negative_and_zero", {"k": {"$gte": -2, "$lte": 0}}, [["k", 1]], data=[{"_id": i, "k": v} for i, v in enumerate([-5, -2, -1.5, 0, Dbl(-0.0), 1, 2], 1)])
    results("date_range", {"d": {"$gte": datetime.datetime(2024, 1, 3), "$lt": datetime.datetime(2024, 1, 6)}}, [["d", 1]], data=[{"_id": i, "d": datetime.datetime(2024, 1, i)} for i in range(1, 10)])
    results("objectid_eq", {"k": ObjectId("000000000000000000000003")}, [["k", 1]], data=[{"_id": i, "k": ObjectId("00000000000000000000000%d" % i)} for i in range(1, 6)])
    results("bool_eq", {"k": True}, [["k", 1]], data=[{"_id": i, "k": i % 2 == 0} for i in range(1, 7)])
    results("embedded_document_eq", {"k": {"a": 1, "b": 2}}, [["k", 1]], data=[{"_id": 1, "k": {"a": 1, "b": 2}}, {"_id": 2, "k": {"b": 2, "a": 1}}, {"_id": 3, "k": {"a": 1}}])
    results("or_both_indexed", {"$or": [{"a": 1}, {"b": 2}]}, [["a", 1]], extra=[cix("c", [["b", 1]])])
    results("or_one_unindexed", {"$or": [{"a": 1}, {"s": "k02"}]}, [["a", 1]])
    results("and_with_in_and_ne", {"a": {"$in": [1, 2, 3], "$ne": 2}, "b": {"$ne": 0}}, [["a", 1]])
    results("sort_limit_skip_indexed", None, [["a", 1]], sort={"a": -1, "_id": 1})
    t("res_sort_limit_skip_early_stop", [ins("c", D), cix("c", [["a", 1]]), find("c", {}, sort={"a": 1}, skip=5, limit=7)], unordered=False)
    t("res_sort_with_filter_early_stop", [ins("c", D), cix("c", [["a", 1]]), find("c", {"b": {"$gte": 3}}, sort={"a": -1}, limit=6)])
    t("res_sparse_hint_hides_missing", [ins("c", [{"_id": 1, "k": 1}, {"_id": 2}, {"_id": 3, "k": None}]), cix("c", [["k", 1]], sparse=True), find("c", {}, hint={"k": 1}, sort={"_id": 1}), find("c", {}, sort={"_id": 1}), fs({"k": None})])
    t("res_partial_queries", [ins("c", D), cix("c", [["b", 1]], partialFilterExpression={"a": {"$gt": 4}}), fs({"b": 2, "a": {"$gt": 6}}), fs({"b": 2}), fs({"b": 2, "a": {"$gte": 5}})])
    t("res_partial_exists", [ins("c", [{"_id": 1, "k": 1, "p": 1}, {"_id": 2, "k": 1}, {"_id": 3, "k": 2, "p": None}]), cix("c", [["k", 1]], partialFilterExpression={"p": {"$exists": True}}), fs({"k": 1}), fs({"k": 1, "p": {"$exists": True}})])
    t("res_count_distinct_with_index", [ins("c", D), cix("c", [["a", 1]]), {"op": "count_documents", "coll": "c", "filter": {"a": {"$gt": 4}}}, {"op": "distinct", "coll": "c", "key": "a", "filter": {"b": 3}}, agg("c", [{"$match": {"a": 2}}, {"$group": {"_id": "$b", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])])

    # ---- maintenance across writes ---------------------------------------------------------------------
    q = lambda: fs({"a": {"$gte": 8}})
    t("maint_update_moves_entry", [ins("c", D), cix("c", [["a", 1]]), upd("c", {"_id": 1}, {"$set": {"a": 9}}), q(), fs({"a": 1}), upd("c", {"_id": 1}, {"$unset": {"a": ""}}), q(), fs({"a": None})])
    t("maint_update_many_inc", [ins("c", D), cix("c", [["a", 1]]), upd("c", {"b": 2}, {"$inc": {"a": 5}}, kind="update_many"), q(), fs({"a": 3})])
    t("maint_delete_removes_entry", [ins("c", D), cix("c", [["a", 1]]), {"op": "delete_many", "coll": "c", "filter": {"a": 9}}, q(), fs({"a": 9})])
    t("maint_replace_one", [ins("c", D), cix("c", [["a", 1]]), upd("c", {"_id": 2}, {"zzz": 1}, kind="replace_one"), fs({"a": 2}), fs({"zzz": 1})])
    t("maint_upsert_new_doc", [cix("c", [["a", 1]]), upd("c", {"a": 77}, {"$set": {"v": 1}}, upsert=True), fs({"a": 77})], keep_oids=False)
    t("maint_find_one_and_update", [ins("c", D), cix("c", [["a", 1]]), {"op": "find_one_and_update", "coll": "c", "filter": {"a": 9}, "update": {"$set": {"a": 100}}, "sort": {"_id": 1}, "return_document": "after"}, fs({"a": 100}), fs({"a": 9})])
    t("maint_find_one_and_delete", [ins("c", D), cix("c", [["a", 1]]), {"op": "find_one_and_delete", "coll": "c", "filter": {"a": 9}, "sort": {"_id": -1}}, fs({"a": 9})])
    t("maint_array_element_changes", [ins("c", D), cix("c", [["m", 1]]), upd("c", {"_id": 1}, {"$push": {"m": 42}}), fs({"m": 42}), upd("c", {"_id": 1}, {"$pull": {"m": 42}}), fs({"m": 42})])
    t("maint_unchanged_update_noop", [ins("c", D), cix("c", [["a", 1]]), upd("c", {"_id": 1}, {"$set": {"a": 1}}), fs({"a": 1})])
    t("maint_insert_after_index", [cix("c", [["a", 1]]), ins("c", D), fs({"a": 4})])
    t("maint_two_updates_same_doc_one_command", [ins("c", D), cix("c", [["a", 1]]), {"op": "command", "cmd": {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"a": 50}}}, {"q": {"a": 50}, "u": {"$set": {"a": 51}}}]}, "pick": ["n", "nModified", "ok"]}, fs({"a": {"$gte": 50}})])
    t("maint_drop_collection_clears_index_data", [ins("c", D), cix("c", [["a", 1]]), {"op": "drop_collection", "coll": "c"}, ins("c", [{"_id": 1, "a": 3}]), fs({"a": 3}), lst()])

    # ---- unique, sparse, partial semantics ---------------------------------------------------------------------
    t("unique_insert_dup_shape", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": "x"}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 2, "k": "x"}, "cmp_msg": True}])
    t("unique_dup_command_error_shape", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": "x"}]), cmd({"insert": "c", "documents": [{"_id": 2, "k": "x"}]}, ["n", "writeErrors", "ok"])])
    t("unique_update_dup", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": 1}, {"_id": 2, "k": 2}]), upd("c", {"_id": 2}, {"$set": {"k": 1}}, cmp_msg=True), fs()])
    t("unique_multikey_element_dup", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": [1, 2]}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 2, "k": [2, 3]}, "cmp_msg": True}])
    t("unique_multikey_same_doc_repeats_ok", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": [1, 1, 2]}]), fs()])
    t("unique_compound_dup", [cix("c", [["a", 1], ["b", -1]], unique=True), ins("c", [{"_id": 1, "a": 1, "b": 1}, {"_id": 2, "a": 1, "b": 2}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 3, "a": 1, "b": 2}, "cmp_msg": True}])
    t("unique_missing_twice_dup", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 2}, "cmp_msg": True}])
    t("unique_sparse_allows_many_missing", [cix("c", [["k", 1]], unique=True, sparse=True), ins("c", [{"_id": 1}, {"_id": 2}, {"_id": 3, "k": 1}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 4, "k": 1}, "cmp_msg": True}, {"op": "insert_one", "coll": "c", "doc": {"_id": 5, "k": None}}, {"op": "insert_one", "coll": "c", "doc": {"_id": 6, "k": None}, "cmp_msg": True}])
    t("unique_partial_scope", [cix("c", [["k", 1]], unique=True, partialFilterExpression={"on": True}), ins("c", [{"_id": 1, "k": 1, "on": True}, {"_id": 2, "k": 1, "on": False}, {"_id": 3, "k": 1}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 4, "k": 1, "on": True}, "cmp_msg": True}, upd("c", {"_id": 2}, {"$set": {"on": True}}, cmp_msg=True)])
    t("unique_upsert_dup", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": 1}]), upd("c", {"k": 1, "z": 1}, {"$set": {"q": 1}}, upsert=True), upd("c", {"_id": 9}, {"$set": {"k": 1}}, upsert=True, cmp_msg=True), fs()])
    t("unique_delete_frees_key", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": 1}]), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, ins("c", [{"_id": 2, "k": 1}]), fs()])
    t("unique_in_one_command_unordered", [cix("c", [["k", 1]], unique=True), {"op": "command", "cmd": {"insert": "c", "ordered": False, "documents": [{"_id": 1, "k": 1}, {"_id": 2, "k": 1}, {"_id": 3, "k": 2}, {"_id": 4, "k": 2}]}, "pick": ["n", "ok"]}, fs()])
    t("unique_numeric_types_collide", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": 1}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 2, "k": Int64(1)}, "cmp_msg": True}, {"op": "insert_one", "coll": "c", "doc": {"_id": 3, "k": Dbl(1.0)}, "cmp_msg": True}])
    t("unique_string_vs_number_distinct", [cix("c", [["k", 1]], unique=True), ins("c", [{"_id": 1, "k": 1}, {"_id": 2, "k": "1"}]), fs()])
    t("parallel_arrays_rejected", [cix("c", [["a", 1], ["b", 1]]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": [1], "b": [2]}, "cmp_msg": True}, ins("c", [{"_id": 2, "a": [1, 2], "b": 3}]), fs()])
    t("sparse_compound_any_field_present", [ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "b": 1}, {"_id": 3}]), cix("c", [["a", 1], ["b", 1]], sparse=True), fs({"a": 1}), find("c", {}, hint={"a": 1, "b": 1}, sort={"_id": 1})])

    # ---- createIndexes / listIndexes / dropIndexes ----------------------------------------------------------------
    t("list_options_full", [ins("c", [{"_id": 1}]), cix("c", [["a", 1]], sparse=True), cix("c", [["b", -1]], partialFilterExpression={"b": {"$gt": 1}}), cix("c", [["t", 1]], expireAfterSeconds=3600), cix("c", [["u", 1]], unique=True, name="uu"), lst()])
    t("create_conflict_same_name_different_options", [cix("c", [["a", 1]], name="n"), cix("c", [["a", 1]], name="n", unique=True)])
    t("create_conflict_same_key_different_options", [cix("c", [["a", 1]]), cix("c", [["a", 1]], unique=True)])
    t("create_same_spec_twice_is_noop", [cix("c", [["a", 1]], sparse=True), cix("c", [["a", 1]], sparse=True), lst()])
    t("create_partial_and_sparse_error", [cix("c", [["a", 1]], sparse=True, partialFilterExpression={"b": 1})])
    t("create_partial_unsupported_operator_error", [cix("c", [["a", 1]], partialFilterExpression={"b": {"$ne": 1}})])
    t("create_ttl_negative_error", [cix("c", [["a", 1]], expireAfterSeconds=-1)])
    t("create_ttl_string_error", [cix("c", [["a", 1]], expireAfterSeconds="x")])
    t("drop_index_by_name_then_query", [ins("c", D), cix("c", [["a", 1]]), {"op": "drop_index", "coll": "c", "index": "a_1"}, lst(), fs({"a": 3}), explain({"a": 3})])
    t("drop_index_by_key_pattern", [ins("c", D), cix("c", [["a", 1], ["b", -1]]), {"op": "drop_index", "coll": "c", "index": [["a", 1], ["b", -1]]}, lst()])
    t("drop_index_missing_error", [ins("c", D), {"op": "drop_index", "coll": "c", "index": "nope"}])
    t("drop_index_id_error", [ins("c", D), {"op": "drop_index", "coll": "c", "index": "_id_"}])
    t("drop_indexes_star", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["b", 1]]), cmd({"dropIndexes": "c", "index": "*"}, ["nIndexesWas", "msg", "ok"]), lst()])
    t("drop_then_recreate_unique", [ins("c", [{"_id": 1, "k": 1}, {"_id": 2, "k": 2}]), cix("c", [["k", 1]], unique=True), {"op": "drop_index", "coll": "c", "index": "k_1"}, ins("c", [{"_id": 3, "k": 1}]), cix("c", [["k", 1]], unique=True)])
    t("backfill_unique_fails_on_duplicates", [ins("c", [{"_id": 1, "k": 1}, {"_id": 2, "k": 2}, {"_id": 3, "k": 1}]), {"op": "create_index", "coll": "c", "keys": [["k", 1]], "opts": {"unique": True}}, lst()])
    t("backfill_3000_docs_then_query", [ins("c", [{"_id": i, "a": i % 97, "b": i % 11} for i in range(1, 3001)]), cix("c", [["a", 1], ["b", -1]]), explain({"a": 5, "b": {"$gt": 3}}), {"op": "count_documents", "coll": "c", "filter": {"a": 5, "b": {"$gt": 3}}}, fs({"a": 5, "b": {"$gt": 3}}, limit=5)])
    t("backfill_unique_3000_ok_then_dup_rejected", [ins("c", [{"_id": i, "k": i} for i in range(1, 3001)]), cix("c", [["k", 1]], unique=True), {"op": "insert_one", "coll": "c", "doc": {"_id": 5000, "k": 2999}, "cmp_msg": True}, {"op": "insert_one", "coll": "c", "doc": {"_id": 5001, "k": 3001}}])
    t("backfill_multikey", [ins("c", [{"_id": i, "m": [i % 5, (i + 2) % 5]} for i in range(1, 301)]), cix("c", [["m", 1]]), fs({"m": 3}, limit=10), explain({"m": 3})])
    t("backfill_sparse_and_partial", [ins("c", D), cix("c", [["z", 1]], sparse=True), cix("c", [["b", 1]], partialFilterExpression={"a": {"$gt": 4}}), fs({"z": {"$exists": True}}), fs({"b": 3, "a": {"$gt": 4}})])
    t("persist_index_survives_tenant_close", [ins("c", D), cix("c", [["a", 1]], unique=False), {"op": "tenant_close"}, explain({"a": 3}), fs({"a": 3}), {"op": "tenant_close"}, upd("c", {"_id": 3}, {"$set": {"a": 77}}), {"op": "tenant_close"}, fs({"a": 77})])
    t("collstats_lists_indexes", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["b", 1]], unique=False, name="bb"), cmd({"collStats": "c"}, None, reduce="collstats")])
    t("validate_ok_after_writes", [ins("c", D), cix("c", [["a", 1]], unique=False), upd("c", {"b": 1}, {"$inc": {"a": 1}}, kind="update_many"), {"op": "delete_many", "coll": "c", "filter": {"a": 3}}, cmd({"validate": "c"}, ["valid", "ok"])])

    # ---- more planner / error shapes -------------------------------------------------------------------------
    t("hint_nonexistent_error", [ins("c", D), find("c", {"a": 1}, hint="nope_1")])
    t("hint_nonexistent_pattern_error", [ins("c", D), cix("c", [["a", 1]]), find("c", {"a": 1}, hint={"zzz": 1})])
    t("hint_results_same_as_scan", [ins("c", D), cix("c", [["b", 1]]), find("c", {"a": 3}, hint={"b": 1}, sort={"_id": 1}), find("c", {"a": 3}, hint={"$natural": 1}, sort={"_id": 1})])
    t("plan_or_both_indexed", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["b", 1]]), explain({"$or": [{"a": 1}, {"b": 2}]})])
    t("plan_or_one_unindexed_collscan", [ins("c", D), cix("c", [["a", 1]]), explain({"$or": [{"a": 1}, {"s": "k01"}]})])
    t("plan_and_picks_more_selective_compound", [ins("c", D), cix("c", [["a", 1]]), cix("c", [["a", 1], ["b", 1]]), explain({"a": 3, "b": 2})])
    t("plan_unique_index_preferred", [ins("c", [{"_id": i, "a": i, "b": i} for i in range(1, 30)]), cix("c", [["a", 1]]), cix("c", [["b", 1]], unique=True), explain({"a": 3, "b": 3})])
    t("plan_eq_null", [ins("c", D), cix("c", [["zz", 1]]), explain({"zz": None})])
    t("plan_ne_not_indexed_bounds", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$ne": 3}})])
    t("plan_exists_false_collscan_or_ix", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$exists": False}})])
    t("plan_regex_prefix", [ins("c", D), cix("c", [["s", 1]]), explain({"s": {"$regex": "^k01"}})])
    t("plan_double_bounds", [ins("c", D), cix("c", [["a", 1]]), explain({"a": {"$gt": 2.5, "$lte": 6.5}})])
    t("plan_dotted_array_path_multikey", [ins("c", [{"_id": i, "items": [{"k": i % 4}, {"k": (i + 1) % 4}]} for i in range(1, 30)]), cix("c", [["items.k", 1]]), explain({"items.k": 2})])
    t("res_dotted_array_path", [ins("c", [{"_id": i, "items": [{"k": i % 4}, {"k": (i + 1) % 4}]} for i in range(1, 30)]), cix("c", [["items.k", 1]]), fs({"items.k": 2}), fs({"items.k": {"$gt": 2}}), fs({"items": {"$elemMatch": {"k": 3}}})])

    # ================================================================================================
    # TTL indexes (results7, step 3). The reference mongod runs its TTL monitor every second
    # (--setParameter ttlMonitorSleepSecs=1); tests sleep long enough for it to have run.
    # ================================================================================================
    GT = "TTL indexes"
    PAST = datetime.datetime(2001, 1, 1)
    FUTURE = datetime.datetime(2100, 1, 1)
    now = lambda ms: {"$nowOffsetMs": ms}

    def tt(name, steps, **flags):
        add("ttl." + name, steps, GT, **flags)

    def ids(filter=None, **kw):
        return find("c", filter or {}, projection={"t": 0, "u": 0, "d": 0}, sort={"_id": 1}, **kw)

    def wait(secs=4):
        return {"op": "sleep", "secs": secs}

    tt("expired_docs_removed", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": PAST}, {"_id": 2, "t": FUTURE}, {"_id": 3, "t": PAST}]), wait(), ids()])
    tt("expire_after_zero_seconds", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": PAST}, {"_id": 2, "t": FUTURE}]), wait(), ids()])
    tt("non_date_values_never_expire", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": "2001-01-01"}, {"_id": 2, "t": 5}, {"_id": 3}, {"_id": 4, "t": None}, {"_id": 5, "t": PAST}]), wait(), ids()])
    tt("array_uses_earliest_date", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": [FUTURE, PAST]}, {"_id": 2, "t": [FUTURE, FUTURE]}, {"_id": 3, "t": [PAST, "x"]}]), wait(), ids()])
    tt("nested_path", [cix("c", [["u.exp", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "u": {"exp": PAST}}, {"_id": 2, "u": {"exp": FUTURE}}, {"_id": 3, "u": [{"exp": PAST}]}]), wait(), ids()])
    tt("compound_index_ignores_ttl", [cix("c", [["t", 1], ["x", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": PAST, "x": 1}]), wait(), ids(), {"op": "list_indexes", "coll": "c", "full": True}])
    tt("partial_only_matching_docs_expire", [cix("c", [["t", 1]], expireAfterSeconds=0, partialFilterExpression={"kind": "tmp"}), ins("c", [{"_id": 1, "t": PAST, "kind": "tmp"}, {"_id": 2, "t": PAST, "kind": "keep"}, {"_id": 3, "t": PAST}]), wait(), ids()])
    tt("relative_expiry_visible_then_gone", [cix("c", [["t", 1]], expireAfterSeconds=3), ins("c", [{"_id": 1, "t": now(0)}, {"_id": 2, "t": now(60000)}]), ids(), wait(7), ids()])
    tt("inserted_after_index_expire", [cix("c", [["t", 1]], expireAfterSeconds=1), wait(1), ins("c", [{"_id": 1, "t": PAST}, {"_id": 2, "t": FUTURE}]), wait(), ids()])
    tt("index_created_on_existing_expired_docs", [ins("c", [{"_id": 1, "t": PAST}, {"_id": 2, "t": FUTURE}]), cix("c", [["t", 1]], expireAfterSeconds=60), wait(), ids()])
    tt("update_extends_lifetime", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": now(-50000)}, {"_id": 2, "t": now(-50000)}]), upd("c", {"_id": 1}, {"$set": {"t": FUTURE}}), wait(), ids()])
    tt("update_to_past_expires", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": FUTURE}]), upd("c", {"_id": 1}, {"$set": {"t": PAST}}), wait(), ids()])
    tt("replace_with_new_date", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": FUTURE}]), upd("c", {"_id": 1}, {"t": PAST, "v": 1}, kind="replace_one"), wait(), ids()])
    tt("count_distinct_aggregate_agree", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": i, "t": PAST if i % 2 else FUTURE, "g": i % 3} for i in range(1, 21)]), wait(), {"op": "count_documents", "coll": "c", "filter": {}}, {"op": "distinct", "coll": "c", "key": "g"}, agg("c", [{"$group": {"_id": "$g", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])])
    tt("listindexes_shape", [cix("c", [["t", 1]], expireAfterSeconds=3600), {"op": "list_indexes", "coll": "c", "full": True}])
    tt("survives_tenant_close", [cix("c", [["t", 1]], expireAfterSeconds=60), ins("c", [{"_id": 1, "t": PAST}, {"_id": 2, "t": FUTURE}]), {"op": "tenant_close"}, wait(), ids(), {"op": "tenant_close"}, ids()])
    tt("unique_key_freed_after_expiry", [cix("c", [["k", 1]], unique=True), cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "k": "x", "t": PAST}]), wait(), ins("c", [{"_id": 2, "k": "x", "t": FUTURE}]), ids()])
    tt("id_reusable_after_expiry", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": PAST}]), wait(), ins("c", [{"_id": 1, "t": FUTURE}]), ids()])
    tt("collmod_changes_expiry", [cix("c", [["t", 1]], expireAfterSeconds=3600), ins("c", [{"_id": 1, "t": now(-20000)}, {"_id": 2, "t": FUTURE}]), wait(2), ids(), cmd({"collMod": "c", "index": {"keyPattern": {"t": 1}, "expireAfterSeconds": 1}}, ["expireAfterSeconds_old", "expireAfterSeconds_new", "ok"]), wait(), ids(), {"op": "list_indexes", "coll": "c", "full": True}])
    tt("collmod_by_name", [cix("c", [["t", 1]], expireAfterSeconds=3600, name="tt"), cmd({"collMod": "c", "index": {"name": "tt", "expireAfterSeconds": 7200}}, ["expireAfterSeconds_old", "expireAfterSeconds_new", "ok"]), {"op": "list_indexes", "coll": "c", "full": True}])
    tt("collmod_non_ttl_index_becomes_ttl", [cix("c", [["t", 1]], name="plain"), cmd({"collMod": "c", "index": {"name": "plain", "expireAfterSeconds": 5}}, ["expireAfterSeconds_old", "expireAfterSeconds_new", "ok"]), {"op": "list_indexes", "coll": "c", "full": True}])
    tt("collmod_missing_index_error", [ins("c", [{"_id": 1}]), cmd({"collMod": "c", "index": {"name": "nope", "expireAfterSeconds": 5}}, None)])
    tt("drop_ttl_index_stops_expiry", [cix("c", [["t", 1]], expireAfterSeconds=3600), ins("c", [{"_id": 1, "t": now(-30000)}]), {"op": "drop_index", "coll": "c", "index": "t_1"}, cix("c", [["t", 1]], expireAfterSeconds=1), wait(), ids()])
    tt("delete_and_find_respect_expiry", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": PAST, "g": 1}, {"_id": 2, "t": FUTURE, "g": 1}]), wait(), {"op": "delete_many", "coll": "c", "filter": {"g": 1}}, ids()])
    tt("update_many_skips_expired", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": 1, "t": PAST, "g": 1}, {"_id": 2, "t": FUTURE, "g": 1}]), wait(), upd("c", {"g": 1}, {"$set": {"z": 1}}, kind="update_many"), ids()])
    tt("sort_limit_with_index_skips_expired", [cix("c", [["t", 1]], expireAfterSeconds=0), ins("c", [{"_id": i, "t": PAST if i <= 5 else FUTURE} for i in range(1, 11)]), wait(), find("c", {}, sort={"t": 1, "_id": 1}, limit=3, projection={"t": 0})])
    tt("lookup_foreign_respects_expiry", [cix("f", [["t", 1]], expireAfterSeconds=0), ins("f", [{"_id": 1, "t": PAST, "k": 1}, {"_id": 2, "t": FUTURE, "k": 1}]), ins("c", [{"_id": 1, "k": 1}]), wait(), agg("c", [{"$lookup": {"from": "f", "localField": "k", "foreignField": "k", "as": "m"}}, {"$project": {"m": {"_id": 1}}}])])
