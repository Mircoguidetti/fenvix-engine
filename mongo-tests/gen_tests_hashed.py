"""Differential tests for hashed and wildcard indexes (results9): specifications and their errors, which documents the
indexes accept, and that queries (with and without `hint`) return what MongoDB returns, including the cases where
MongoDB refuses a hinted wildcard scan."""


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]

    def th(name, steps, **flags):
        add("hashed." + name, steps, "hashed index", **flags)

    def tw(name, steps, **flags):
        add("wild." + name, steps, "wildcard index", **flags)

    def cix(coll, keys, **opts):
        step = {"op": "create_index", "coll": coll, "keys": keys}
        if opts:
            step["opts"] = opts
        return step

    full = {"op": "list_indexes", "coll": "c", "full": True}
    IDS = {"_id": 1}

    def ids(flt, coll="c", **kw):
        return find(coll, flt, sort=kw.pop("sort", {"_id": 1}), projection=IDS, **kw)

    # ============================================================================================================
    # hashed
    # ============================================================================================================
    H = [["a", "hashed"]]
    th("create", [cix("c", H), full])
    th("create_twice", [cix("c", H), cix("c", H), full])
    th("create_named", [cix("c", H, name="h"), full])
    th("create_sparse", [cix("c", H, sparse=True), full])
    th("create_partial", [cix("c", H, partialFilterExpression={"b": 1}), full])
    th("create_ttl", [cix("c", H, expireAfterSeconds=10), full])
    th("create_unique_error", [cix("c", H, unique=True), full])
    th("create_unique_false", [cix("c", H, unique=False), full])
    th("create_compound_prefix", [cix("c", [["a", "hashed"], ["b", 1]]), full])
    th("create_compound_suffix", [cix("c", [["b", 1], ["a", "hashed"]]), full])
    th("create_compound_desc", [cix("c", [["a", "hashed"], ["b", -1]]), full])
    th("create_compound_middle", [cix("c", [["b", 1], ["a", "hashed"], ["d", 1]]), full])
    th("create_two_hashed_error", [cix("c", [["a", "hashed"], ["b", "hashed"]]), full])
    th("create_dotted", [cix("c", [["a.b", "hashed"]]), full])
    th("create_on_id", [cix("c", [["_id", "hashed"]]), full])
    th("create_with_text_error", [cix("c", [["a", "hashed"], ["b", "text"]]), full])
    th("create_with_2dsphere_error", [cix("c", [["a", "hashed"], ["b", "2dsphere"]]), full])
    th("create_with_2d_error", [cix("c", [["a", "hashed"], ["b", "2d"]]), full])
    th("create_and_regular_same_field", [cix("c", [["a", 1]]), cix("c", H), full])
    th("create_unknown_plugin_error", [cix("c", [["a", "bogus"]]), full])
    th("create_options_conflict", [cix("c", H), cix("c", H, sparse=True), full])
    th("drop", [cix("c", H), {"op": "drop_index", "coll": "c", "index": "a_hashed"}, full])
    th("create_on_collection_with_docs", [ins("c", [{"_id": i, "a": i % 7} for i in range(1, 30)]), cix("c", H), ids({"a": 3})])
    th("create_on_collection_with_array_error", [ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "a": [1, 2]}]), cix("c", H), full])
    th("create_on_collection_with_missing_and_null", [ins("c", [{"_id": 1}, {"_id": 2, "a": None}, {"_id": 3, "a": 1}]), cix("c", H), ids({"a": None}), ids({"a": None}, hint="a_hashed")])

    # ---- which documents it accepts -----------------------------------------------------------------------------
    for nm, v in [("array", [1, 2]), ("empty_array", []), ("nested_array", [[1]]), ("array_of_docs", [{"x": 1}])]:
        th("insert_" + nm + "_error", [cix("c", H), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": v}}, ids({})])
    for nm, v in [("doc_with_array", {"x": [1, 2]}), ("empty_doc", {}), ("null", None), ("string", "x"), ("deep_doc", {"x": {"y": {"z": 1}}})]:
        th("insert_" + nm, [cix("c", H), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": v}}, ids({}), ids({"a": v})])
    th("insert_missing", [cix("c", H), {"op": "insert_one", "coll": "c", "doc": {"_id": 1}}, ids({"a": None})])
    th("dotted_through_array_error", [cix("c", [["a.b", "hashed"]]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": [{"b": 1}]}}, ids({})])
    th("dotted_leaf_array_error", [cix("c", [["a.b", "hashed"]]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": {"b": [1]}}}, ids({})])
    th("dotted_ok", [cix("c", [["a.b", "hashed"]]), ins("c", [{"_id": 1, "a": {"b": 1}}, {"_id": 2, "a": {"b": 2}}, {"_id": 3, "a": 5}, {"_id": 4}]), ids({"a.b": 1}), ids({"a.b": None})])
    th("compound_array_in_other_field_error", [cix("c", [["a", "hashed"], ["b", 1]]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": 1, "b": [1, 2]}}, ids({})])
    th("compound_array_in_hashed_field_error", [cix("c", [["a", "hashed"], ["b", 1]]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": [1], "b": 1}}, ids({})])
    th("update_to_array_error", [cix("c", H), ins("c", [{"_id": 1, "a": 1}]), upd("c", {"_id": 1}, {"$set": {"a": [1, 2]}}), ids({"a": 1})])
    th("push_error", [cix("c", H), ins("c", [{"_id": 1, "a": 1}]), upd("c", {"_id": 1}, {"$push": {"a": 2}}), ids({})])
    th("insert_many_ordered_stops", [cix("c", H), ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "a": [1]}, {"_id": 3, "a": 3}]), ids({})])
    th("insert_many_unordered_continues", [cix("c", H), ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "a": [1]}, {"_id": 3, "a": 3}], ordered=False), ids({})])

    # ---- queries ---------------------------------------------------------------------------------------------------
    D = [
        {"_id": 1, "a": 1}, {"_id": 2, "a": 1.0}, {"_id": 3, "a": 2.3}, {"_id": 4, "a": 2.2}, {"_id": 5, "a": "x"}, {"_id": 6, "a": True},
        {"_id": 7, "a": {"k": 1}}, {"_id": 8, "a": {"k": 1.0}}, {"_id": 9, "a": {"k": [1]}}, {"_id": 10, "a": None}, {"_id": 11, "a": "2"},
        {"_id": 12, "a": 2 ** 40}, {"_id": 13, "a": 1.5e300}, {"_id": 14, "a": {"$numberDouble": "NaN"}}, {"_id": 15, "a": {"$numberDouble": "Infinity"}},
        {"_id": 16, "a": {"$numberDouble": "-0.0"}}, {"_id": 17, "a": 0}, {"_id": 18, "a": {"$numberDecimal": "3.7"}}, {"_id": 19}, {"_id": 20, "a": {"$numberLong": "1"}},
        {"_id": 21, "a": {"k": 1, "j": 2}}, {"_id": 22, "a": {"j": 2, "k": 1}}, {"_id": 23, "a": ""}, {"_id": 24, "a": False}, {"_id": 25, "a": 3},
        {"_id": 26, "a": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 27, "a": {"$oid": "000000000000000000000001"}}, {"_id": 28, "a": {"$regularExpression": {"pattern": "^x", "options": ""}}},
    ]
    for hint in (None, "a_hashed"):
        sfx = "_hint" if hint else ""
        kw = {"hint": hint} if hint else {}
        pre = [cix("c", H), ins("c", D)]
        for nm, q in [
            ("eq_int", {"a": 1}), ("eq_double", {"a": 1.0}), ("eq_fraction", {"a": 2.3}), ("eq_fraction_collides", {"a": 2.2}), ("eq_trunc", {"a": 2}),
            ("eq_string", {"a": "x"}), ("eq_numeric_string", {"a": "2"}), ("eq_bool", {"a": True}), ("eq_doc", {"a": {"k": 1}}), ("eq_doc_double", {"a": {"k": 1.0}}),
            ("eq_doc_order", {"a": {"j": 2, "k": 1}}), ("eq_doc_array", {"a": {"k": [1]}}), ("eq_null", {"a": None}), ("eq_empty_string", {"a": ""}),
            ("eq_big", {"a": 2 ** 40}), ("eq_huge", {"a": 1.5e300}), ("eq_nan", {"a": {"$numberDouble": "NaN"}}), ("eq_inf", {"a": {"$numberDouble": "Infinity"}}),
            ("eq_neg_zero", {"a": {"$numberDouble": "-0.0"}}), ("eq_zero", {"a": 0}), ("eq_long", {"a": {"$numberLong": "1"}}), ("eq_decimal", {"a": {"$numberDecimal": "3.7"}}),
            ("eq_date", {"a": {"$date": "2020-01-01T00:00:00Z"}}), ("eq_oid", {"a": {"$oid": "000000000000000000000001"}}), ("eq_array", {"a": [1, 2]}),
            ("eq_op", {"a": {"$eq": 1}}), ("in_list", {"a": {"$in": [1, "x", None]}}), ("in_empty", {"a": {"$in": []}}), ("in_doc", {"a": {"$in": [{"k": 1}, 3]}}),
            ("in_array_member_error_free", {"a": {"$in": [[1, 2], 3]}}), ("in_regex", {"a": {"$in": [{"$regularExpression": {"pattern": "^x", "options": ""}}]}}),
            ("nin_list", {"a": {"$nin": [1, "x"]}}), ("ne", {"a": {"$ne": 1}}), ("gt", {"a": {"$gt": 1}}), ("range", {"a": {"$gte": 1, "$lt": 3}}),
            ("lt_string", {"a": {"$lt": "y"}}), ("exists_true", {"a": {"$exists": True}}), ("exists_false", {"a": {"$exists": False}}), ("regex", {"a": {"$regex": "^x"}}),
            ("type_string", {"a": {"$type": "string"}}), ("not_eq", {"a": {"$not": {"$eq": 1}}}), ("eq_and_range", {"a": 1, "_id": {"$gt": 1}}), ("or_two_eq", {"$or": [{"a": 1}, {"a": "x"}]}),
            ("or_eq_other", {"$or": [{"a": 1}, {"_id": 5}]}), ("all", {}), ("and_eq", {"$and": [{"a": 1}, {"a": {"$in": [1, 2]}}]}), ("eq_and_eq_conflict", {"a": 1, "$and": [{"a": 2}]}),
            ("elem_eq_on_scalar_field", {"a": {"$elemMatch": {"$eq": 1}}}), ("size", {"a": {"$size": 1}}), ("mod", {"a": {"$mod": [2, 1]}}),
        ]:
            th(nm + sfx, pre + [ids(q, **kw)])
        th("count" + sfx, pre + [{"op": "count_documents", "coll": "c", "filter": {"a": 1}, **kw}])
        th("count_in" + sfx, pre + [{"op": "count_documents", "coll": "c", "filter": {"a": {"$in": [1, "x", 3]}}, **kw}])
        th("sort_asc" + sfx, pre + [find("c", {"a": {"$in": [1, 2.3, "x", None]}}, sort={"a": 1, "_id": 1}, projection=IDS, **kw)])
        th("sort_desc_limit" + sfx, pre + [find("c", {}, sort={"a": -1, "_id": 1}, projection=IDS, limit=7, **kw)])
        th("distinct" + sfx, pre + [{"op": "distinct", "coll": "c", "key": "_id", "filter": {"a": {"$in": [1, "x"]}}, **kw}])
        th("projection_skip_limit" + sfx, pre + [find("c", {"a": {"$in": [1, "x", 3, None]}}, sort={"_id": 1}, projection={"_id": 1, "a": 1}, skip=1, limit=3, **kw)])
    th("hint_wrong_name_error", [cix("c", H), ins("c", D), ids({"a": 1}, hint="nope")])
    th("hint_by_key_pattern", [cix("c", H), ins("c", D), find("c", {"a": 1}, sort={"_id": 1}, projection=IDS, hint=[["a", "hashed"]])])
    th("hint_while_other_index_exists", [cix("c", H), cix("c", [["a", 1]]), ins("c", D), ids({"a": 1}, hint="a_hashed"), ids({"a": 1}, hint="a_1")])
    th("both_indexes_range_and_eq", [cix("c", H), cix("c", [["a", 1]]), ins("c", D), ids({"a": {"$gt": 0}}), ids({"a": 1})])

    # ---- sparse / partial / compound ---------------------------------------------------------------------------------
    th("sparse_eq_null", [cix("c", H, sparse=True), ins("c", D), ids({"a": None}), ids({"a": None}, hint="a_hashed")])
    th("sparse_eq", [cix("c", H, sparse=True), ins("c", D), ids({"a": 1}), ids({"a": 1}, hint="a_hashed")])
    th("sparse_all_hint", [cix("c", H, sparse=True), ins("c", D), ids({}, hint="a_hashed")])
    th("sparse_exists_false_hint", [cix("c", H, sparse=True), ins("c", D), ids({"a": {"$exists": False}}, hint="a_hashed")])
    th("partial_eq", [cix("c", H, partialFilterExpression={"b": {"$gt": 5}}), ins("c", [{"_id": i, "a": i % 4, "b": i} for i in range(1, 21)]), ids({"a": 1, "b": {"$gt": 5}}), ids({"a": 1}), ids({"a": 1}, hint="a_hashed")])
    th("partial_update_membership", [cix("c", H, partialFilterExpression={"b": 1}), ins("c", [{"_id": 1, "a": 1, "b": 0}, {"_id": 2, "a": 1, "b": 1}]), upd("c", {"_id": 1}, {"$set": {"b": 1}}), upd("c", {"_id": 2}, {"$set": {"b": 0}}), ids({"a": 1}, hint="a_hashed")])
    CP = [{"_id": i, "a": i % 5, "b": i % 3, "c": "v%d" % (i % 2)} for i in range(1, 61)]
    th("compound_eq_eq", [cix("c", [["a", "hashed"], ["b", 1]]), ins("c", CP), ids({"a": 2, "b": 1}), ids({"a": 2, "b": 1}, hint="a_hashed_b_1")])
    th("compound_eq_range", [cix("c", [["a", "hashed"], ["b", 1]]), ins("c", CP), ids({"a": 2, "b": {"$gte": 1}}), ids({"a": 2, "b": {"$gte": 1}}, hint="a_hashed_b_1")])
    th("compound_only_second", [cix("c", [["a", "hashed"], ["b", 1]]), ins("c", CP), ids({"b": 1}), ids({"b": 1}, hint="a_hashed_b_1")])
    th("compound_only_hashed", [cix("c", [["a", "hashed"], ["b", 1]]), ins("c", CP), ids({"a": 2}), ids({"a": {"$in": [1, 2]}}, hint="a_hashed_b_1")])
    th("compound_prefix_eq_hashed_range_ignored", [cix("c", [["b", 1], ["a", "hashed"]]), ins("c", CP), ids({"b": 1, "a": {"$gt": 1}}), ids({"b": 1, "a": 3}), ids({"b": 1, "a": 3}, hint="b_1_a_hashed")])
    th("compound_sort_by_prefix", [cix("c", [["b", 1], ["a", "hashed"]]), ins("c", CP), find("c", {"b": {"$gte": 1}}, sort={"b": 1, "_id": 1}, projection=IDS, limit=10)])
    th("compound_sort_hinted", [cix("c", [["b", 1], ["a", "hashed"]]), ins("c", CP), find("c", {}, sort={"b": 1, "a": 1, "_id": 1}, projection=IDS, limit=12, hint="b_1_a_hashed")])
    th("compound_three_fields", [cix("c", [["b", 1], ["a", "hashed"], ["c", -1]]), ins("c", CP), ids({"b": 2, "a": 4, "c": "v0"}), ids({"b": 2, "a": 4, "c": "v0"}, hint="b_1_a_hashed_c_-1")])

    # ---- maintenance -----------------------------------------------------------------------------------------------
    th("update_changes_lookup", [cix("c", H), ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "a": 2}]), upd("c", {"_id": 1}, {"$set": {"a": 5}}), ids({"a": 1}, hint="a_hashed"), ids({"a": 5}, hint="a_hashed")])
    th("unset_field", [cix("c", H), ins("c", [{"_id": 1, "a": 1}]), upd("c", {"_id": 1}, {"$unset": {"a": ""}}), ids({"a": 1}, hint="a_hashed"), ids({"a": None}, hint="a_hashed")])
    th("delete_removes", [cix("c", H), ins("c", [{"_id": 1, "a": 1}, {"_id": 2, "a": 1}]), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, ids({"a": 1}, hint="a_hashed")])
    th("upsert", [cix("c", H), upd("c", {"a": 7}, {"$set": {"x": 1}}, upsert=True), ids({"a": 7}, hint="a_hashed", projection=None) if False else ids({"a": 7}, hint="a_hashed")])
    th("replace", [cix("c", H), ins("c", [{"_id": 1, "a": 1}]), {"op": "replace_one", "coll": "c", "filter": {"_id": 1}, "update": {"a": 9}}, ids({"a": 9}, hint="a_hashed"), ids({"a": 1}, hint="a_hashed")])
    th("update_many_by_hashed", [cix("c", H), ins("c", [{"_id": i, "a": i % 3} for i in range(1, 13)]), {"op": "update_many", "coll": "c", "filter": {"a": 1}, "update": {"$set": {"z": 1}}}, ids({"z": 1})])
    th("delete_many_by_in", [cix("c", H), ins("c", [{"_id": i, "a": i % 5} for i in range(1, 31)]), {"op": "delete_many", "coll": "c", "filter": {"a": {"$in": [1, 2]}}}, ids({})])
    th("find_one_and_update_by_hashed", [cix("c", H), ins("c", [{"_id": i, "a": i} for i in range(1, 6)]), {"op": "find_one_and_update", "coll": "c", "filter": {"a": 3}, "update": {"$set": {"s": 1}}, "return_document": "after", "projection": IDS}])
    th("inc_other_field_keeps_entry", [cix("c", H), ins("c", [{"_id": 1, "a": 1, "n": 0}]), upd("c", {"_id": 1}, {"$inc": {"n": 1}}), ids({"a": 1}, hint="a_hashed")])
    th("many_documents", [cix("c", H), ins("c", [{"_id": i, "a": (i * 7) % 211, "g": i % 5} for i in range(1, 601)]), {"op": "count_documents", "coll": "c", "filter": {"a": 17}}, ids({"a": {"$in": [3, 5, 8]}}), {"op": "count_documents", "coll": "c", "filter": {"a": {"$gt": 200}}, "hint": "a_hashed"}])
    th("transaction_visibility", [cix("c", H), {"op": "with_txn", "steps": [{"op": "insert_one", "coll": "c", "doc": {"_id": 1, "a": 1}}, ids({"a": 1}), ids({"a": 1}, hint="a_hashed")]}]) if False else None
    th("drop_collection_recreate", [cix("c", H), ins("c", [{"_id": 1, "a": 1}]), {"op": "drop_collection", "coll": "c"}, ins("c", [{"_id": 2, "a": 1}]), ids({"a": 1})])

    # ============================================================================================================
    # wildcard
    # ============================================================================================================
    W = [["$**", 1]]
    tw("create", [cix("c", W), full])
    tw("create_twice", [cix("c", W), cix("c", W), full])
    tw("create_named", [cix("c", W, name="w"), full])
    tw("create_desc", [cix("c", [["$**", -1]]), full])
    tw("create_subtree", [cix("c", [["a.$**", 1]]), full])
    tw("create_deep_subtree", [cix("c", [["a.b.c.$**", 1]]), full])
    tw("create_projection_include", [cix("c", W, wildcardProjection={"a": 1, "b.c": 1}), full])
    tw("create_projection_exclude", [cix("c", W, wildcardProjection={"a": 0}), full])
    tw("create_projection_id_include", [cix("c", W, wildcardProjection={"a": 1, "_id": 1}), full])
    tw("create_projection_id_exclude", [cix("c", W, wildcardProjection={"a": 1, "_id": 0}), full])
    tw("create_projection_bool", [cix("c", W, wildcardProjection={"a": True}), full])
    tw("create_projection_mixed_error", [cix("c", W, wildcardProjection={"a": 1, "b": 0}), full])
    tw("create_projection_empty_error", [cix("c", W, wildcardProjection={}), full])
    tw("create_projection_bad_value_error", [cix("c", W, wildcardProjection={"a": "x"}), full])
    tw("create_projection_collision_error", [cix("c", W, wildcardProjection={"a.b": 1, "a": 1}), full])
    tw("create_projection_on_subtree_error", [cix("c", [["a.$**", 1]], wildcardProjection={"a.b": 1}), full])
    tw("create_projection_on_regular_error", [cix("c", [["a", 1]], wildcardProjection={"a": 1}), full])
    tw("create_projection_not_object_error", [cix("c", W, wildcardProjection=5), full])
    tw("create_unique_error", [cix("c", W, unique=True), full])
    tw("create_sparse_error", [cix("c", W, sparse=True), full])
    tw("create_ttl_error", [cix("c", W, expireAfterSeconds=10), full])
    tw("create_partial", [cix("c", W, partialFilterExpression={"a": 1}), full])
    tw("create_compound_without_projection_error", [cix("c", [["a", 1], ["$**", 1]]), full])
    tw("create_compound_with_projection", [cix("c", [["a", 1], ["$**", 1]], wildcardProjection={"b": 1}), full])
    tw("create_wildcard_first_without_projection_error", [cix("c", [["$**", 1], ["a", 1]]), full])
    tw("create_two_wildcards_error", [cix("c", [["a.$**", 1], ["b.$**", 1]]), full])
    tw("create_subtree_then_regular", [cix("c", [["a.$**", 1], ["b", 1]]), full])
    tw("create_hashed_error", [cix("c", [["$**", "hashed"]]), full])
    tw("create_text_wildcard", [cix("c", [["$**", "text"]]), full])
    tw("create_wildcard_in_middle_error", [cix("c", [["a.$**.b", 1]]), full])
    tw("create_dollar_field_error", [cix("c", [["a.$b", 1]]), full])
    tw("create_dollar_prefix_error", [cix("c", [["$a", 1]]), full])
    tw("create_wildcard_and_regular_same_collection", [cix("c", W), cix("c", [["a", 1]]), full])
    tw("create_options_conflict", [cix("c", W), cix("c", W, wildcardProjection={"a": 1}), full])
    tw("drop", [cix("c", W), {"op": "drop_index", "coll": "c", "index": "$**_1"}, full])
    tw("create_on_collection_with_docs", [ins("c", [{"_id": i, "a": {"x": i % 5}} for i in range(1, 25)]), cix("c", W), ids({"a.x": 2}, hint="$**_1")])

    WD = [
        {"_id": 1, "a": 1, "b": {"c": 2, "d": [1, 2, {"e": 3}]}, "arr": [1, [2, 3], {"x": 4}]},
        {"_id": 2, "a": "s", "b": {}, "arr": []},
        {"_id": 3, "a": None, "b": {"c": None}},
        {"_id": 4, "a": {"x": 1, "y": {"z": 2}}},
        {"_id": 5, "a": [{"x": 1}, {"x": 2}]},
        {"_id": 6, "b": []},
        {"_id": 7, "c": True, "d": {"$numberDecimal": "1.5"}},
        {"_id": 8, "a": 2.5, "e": {"f": {"g": {"h": 1}}}},
        {"_id": 9, "a": [1, 2, 3], "s": "hello world"},
        {"_id": 10, "a": [5, 6], "s": "help", "t": {"u": [{"v": 1}, {"v": [7, 8]}]}},
        {"_id": 11, "a": {"$date": "2021-01-01T00:00:00Z"}, "s": ""},
    ]
    QS = [
        ("eq_int", {"a": 1}), ("range_gt", {"a": {"$gt": 0}}), ("range_both", {"a": {"$gte": 1, "$lt": 3}}), ("eq_string", {"a": "s"}), ("eq_null", {"a": None}),
        ("eq_doc", {"a": {"x": 1}}), ("path_eq", {"a.x": 1}), ("path_range", {"a.x": {"$gte": 1}}), ("deep_path", {"a.y.z": 2}), ("nested_eq", {"b.c": 2}),
        ("path_into_array", {"b.d": 2}), ("path_into_array_docs", {"b.d.e": 3}), ("eq_array", {"arr": [2, 3]}), ("arr_scalar", {"arr": 2}), ("arr_doc_path", {"arr.x": 4}),
        ("eq_empty_doc", {"b": {}}), ("eq_empty_array", {"b": []}), ("exists_true", {"a": {"$exists": True}}), ("exists_false", {"a": {"$exists": False}}), ("exists_nested", {"e.f.g.h": {"$exists": True}}),
        ("deep_exists", {"e.f.g.h": 1}), ("in_list", {"a": {"$in": [1, "s"]}}), ("in_with_null", {"a": {"$in": [1, None]}}), ("eq_bool", {"c": True}), ("ne", {"a": {"$ne": 1}}),
        ("lt", {"a": {"$lt": 2}}), ("type_string", {"a": {"$type": "string"}}), ("or_indexed", {"$or": [{"a": 1}, {"c": True}]}), ("and_two", {"a": 1, "c": True}),
        ("id_eq", {"_id": 3}), ("regex_prefix", {"s": {"$regex": "^hel"}}), ("regex_any", {"s": {"$regex": "orl"}}), ("eq_date", {"a": {"$date": "2021-01-01T00:00:00Z"}}),
        ("array_element_range", {"a": {"$gt": 4}}), ("array_both_bounds_elementwise", {"a": {"$gt": 1, "$lt": 3}}), ("elem_match", {"a": {"$elemMatch": {"$gte": 5}}}),
        ("elem_match_doc", {"a": {"$elemMatch": {"x": 2}}}), ("nested_array_value", {"t.u.v": 7}), ("nested_array_value_range", {"t.u.v": {"$gt": 6}}), ("numeric_path", {"arr.0": 1}),
        ("numeric_path_doc", {"b.d.2.e": 3}), ("size", {"arr": {"$size": 3}}), ("not_gt", {"a": {"$not": {"$gt": 1}}}), ("nin", {"a": {"$nin": [1, 2]}}), ("all", {}),
        ("string_range", {"s": {"$gt": "he", "$lt": "hz"}}), ("eq_empty_string", {"s": ""}), ("eq_long_path_missing", {"x.y.z": 1}), ("mixed_types_range", {"a": {"$gte": "a"}}),
    ]
    for hint in (None, "$**_1"):
        sfx = "_hint" if hint else ""
        kw = {"hint": hint} if hint else {}
        for nm, q in QS:
            tw(nm + sfx, [cix("c", W), ins("c", WD), ids(q, **kw)])
    tw("count_hint", [cix("c", W), ins("c", WD), {"op": "count_documents", "coll": "c", "filter": {"a": 1}, "hint": "$**_1"}])
    tw("sort_hint_error", [cix("c", W), ins("c", WD), find("c", {}, sort={"a": 1}, projection=IDS, hint="$**_1")])
    tw("sort_with_filter_hint", [cix("c", W), ins("c", WD), find("c", {"a": {"$gt": 0}}, sort={"_id": -1}, projection=IDS, hint="$**_1")])
    tw("distinct_hint", [cix("c", W), ins("c", WD), {"op": "distinct", "coll": "c", "key": "_id", "filter": {"a": 1}, "hint": "$**_1"}])
    tw("hint_by_key_pattern", [cix("c", W), ins("c", WD), find("c", {"a": 1}, sort={"_id": 1}, projection=IDS, hint=[["$**", 1]])])

    # subtree and projection coverage
    SUB = [cix("c", [["a.$**", 1]]), ins("c", WD)]
    for nm, q in [("in_subtree", {"a.x": 1}), ("subtree_root_scalar", {"a": 1}), ("outside_subtree", {"b.c": 2}), ("subtree_deep", {"a.y.z": 2}), ("subtree_range", {"a.x": {"$gt": 0}})]:
        tw("subtree_" + nm, SUB + [ids(q)])
        tw("subtree_" + nm + "_hint", SUB + [ids(q, hint="a.$**_1")])
    PI = [cix("c", W, wildcardProjection={"a": 1, "b.c": 1}), ins("c", WD)]
    for nm, q in [("included", {"a": 1}), ("included_nested", {"b.c": 2}), ("not_included", {"c": True}), ("parent_of_included", {"b": {}}), ("under_included", {"a.x": 1}), ("sibling_of_included", {"b.d": 1})]:
        tw("projection_include_" + nm, PI + [ids(q)])
        tw("projection_include_" + nm + "_hint", PI + [ids(q, hint="$**_1")])
    PE = [cix("c", W, wildcardProjection={"a": 0, "b.d": 0}), ins("c", WD)]
    for nm, q in [("excluded", {"a": 1}), ("excluded_nested", {"b.d": 1}), ("other", {"c": True}), ("sibling", {"b.c": 2}), ("under_excluded", {"a.x": 1})]:
        tw("projection_exclude_" + nm, PE + [ids(q)])
        tw("projection_exclude_" + nm + "_hint", PE + [ids(q, hint="$**_1")])
    PID = [cix("c", W, wildcardProjection={"a": 1, "_id": 1}), ins("c", WD)]
    tw("projection_id_included_hint", PID + [ids({"_id": 3}, hint="$**_1")])
    tw("id_not_covered_hint", [cix("c", W), ins("c", WD), ids({"_id": 3}, hint="$**_1")])
    tw("partial_wildcard", [cix("c", W, partialFilterExpression={"c": True}), ins("c", WD), ids({"c": True}), ids({"a": 1}), ids({"c": True, "a": 1}, hint="$**_1")])

    # maintenance
    tw("update_changes_entries", [cix("c", W), ins("c", WD), upd("c", {"_id": 1}, {"$set": {"a": 99}}), ids({"a": 1}, hint="$**_1"), ids({"a": 99}, hint="$**_1")])
    tw("update_adds_new_path", [cix("c", W), ins("c", WD), upd("c", {"_id": 1}, {"$set": {"zzz": {"y": 5}}}), ids({"zzz.y": 5}, hint="$**_1")])
    tw("unset_path", [cix("c", W), ins("c", WD), upd("c", {"_id": 1}, {"$unset": {"b": ""}}), ids({"b.c": 2}, hint="$**_1")])
    tw("delete_removes", [cix("c", W), ins("c", WD), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, ids({"a": 1}, hint="$**_1")])
    tw("push_element", [cix("c", W), ins("c", WD), upd("c", {"_id": 9}, {"$push": {"a": 100}}), ids({"a": 100}, hint="$**_1")])
    tw("pull_element", [cix("c", W), ins("c", WD), upd("c", {"_id": 9}, {"$pull": {"a": 2}}), ids({"a": 2}, hint="$**_1")])
    tw("replace_doc", [cix("c", W), ins("c", WD), {"op": "replace_one", "coll": "c", "filter": {"_id": 1}, "update": {"q": {"r": 1}}}, ids({"q.r": 1}, hint="$**_1"), ids({"a": 1}, hint="$**_1")])
    tw("upsert", [cix("c", W), upd("c", {"k": 5}, {"$set": {"m": {"n": 6}}}, upsert=True), ids({"m.n": 6}, hint="$**_1")])
    tw("update_many_via_path", [cix("c", W), ins("c", WD), {"op": "update_many", "coll": "c", "filter": {"a.x": {"$gte": 1}}, "update": {"$set": {"hit": 1}}}, ids({"hit": 1})])
    tw("many_documents", [cix("c", W), ins("c", [{"_id": i, "p": {"q": i % 11, "r": [i % 7, i % 3]}, "s": "w%d" % (i % 13)} for i in range(1, 401)]),
                          {"op": "count_documents", "coll": "c", "filter": {"p.q": 4}}, {"op": "count_documents", "coll": "c", "filter": {"p.r": 5}}, {"op": "count_documents", "coll": "c", "filter": {"p.q": {"$gt": 8}}, "hint": "$**_1"},
                          ids({"s": {"$in": ["w1", "w2"]}, "p.q": 3}), {"op": "count_documents", "coll": "c", "filter": {"p.r": {"$gt": 1, "$lt": 3}}, "hint": "$**_1"}])
    tw("drop_collection_recreate", [cix("c", W), ins("c", [{"_id": 1, "a": 1}]), {"op": "drop_collection", "coll": "c"}, ins("c", [{"_id": 2, "a": 1}]), ids({"a": 1})])
