"""Differential tests for the update features added in results8: the positional operators `$`, `$[]` and
`$[<identifier>]` with `arrayFilters`, pipeline-style updates, and `renameCollection` (error cases included)."""
import datetime


def register(g):
    add, ins, find, agg, cmd = g["add"], g["ins"], g["find"], g["agg"], g["cmd"]
    upd = g["upd"]
    sorted_find = g["sorted_find"]
    G = "update: positional operators, pipeline updates, renameCollection"

    def t(name, steps, **flags):
        add("upd8." + name, steps, G, **flags)

    ids = lambda coll="c": sorted_find(coll)
    NUMS = [
        {"_id": 1, "a": [1, 2, 3, 2], "n": 0},
        {"_id": 2, "a": [5, 6], "n": 0},
        {"_id": 3, "a": [], "n": 0},
        {"_id": 4, "a": 7, "n": 0},
        {"_id": 5, "n": 0},
    ]
    ITEMS = [
        {"_id": 1, "items": [{"sku": "x", "qty": 1, "tags": ["a", "b"]}, {"sku": "y", "qty": 5, "tags": []}, {"sku": "x", "qty": 10, "tags": ["c"]}], "t": 0},
        {"_id": 2, "items": [{"sku": "z", "qty": 3}], "t": 0},
        {"_id": 3, "items": [], "t": 0},
    ]
    GRID = [{"_id": 1, "g": [[1, 2], [3, 4]], "grades": [{"m": 80, "std": [{"n": "a", "v": 1}, {"n": "b", "v": 9}]}, {"m": 70, "std": [{"n": "c", "v": 5}]}]}]

    # ---- positional $ ------------------------------------------------------------------------
    t("pos_set_first_match", [ins("c", NUMS), upd("c", {"_id": 1, "a": 2}, {"$set": {"a.$": 20}}), ids()])
    t("pos_set_range_query", [ins("c", NUMS), upd("c", {"_id": 2, "a": {"$gt": 5}}, {"$set": {"a.$": 60}}), ids()])
    t("pos_inc", [ins("c", NUMS), upd("c", {"_id": 1, "a": 3}, {"$inc": {"a.$": 100}}), ids()])
    t("pos_unset_sets_null", [ins("c", NUMS), upd("c", {"_id": 1, "a": 2}, {"$unset": {"a.$": 1}}), ids()])
    t("pos_elemmatch_subfield", [ins("c", ITEMS), upd("c", {"_id": 1, "items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}}, {"$set": {"items.$.qty": 99}}), ids()])
    t("pos_dotted_condition", [ins("c", ITEMS), upd("c", {"_id": 1, "items.sku": "y"}, {"$inc": {"items.$.qty": 1}, "$set": {"t": 1}}), ids()])
    t("pos_push_into_inner_array", [ins("c", ITEMS), upd("c", {"_id": 1, "items.sku": "y"}, {"$push": {"items.$.tags": "new"}}), ids()])
    t("pos_pull_from_inner_array", [ins("c", ITEMS), upd("c", {"_id": 1, "items.sku": "x"}, {"$pull": {"items.$.tags": "a"}}), ids()])
    t("pos_update_many_each_own_position", [ins("c", NUMS), upd("c", {"a": 6}, {"$set": {"a.$": "six"}}, kind="update_many"), ids()])
    t("pos_no_array_condition_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$": 1}}), ids()])
    t("pos_no_match_in_array_error", [ins("c", NUMS), upd("c", {"_id": 3}, {"$set": {"a.$": 1}}), ids()])
    t("pos_field_not_array_error", [ins("c", NUMS), upd("c", {"_id": 4, "a": 7}, {"$set": {"a.$": 1}}), ids()])
    t("pos_first_position_error", [ins("c", NUMS), upd("c", {"_id": 1, "a": 2}, {"$set": {"$": 1}}), ids()])
    t("pos_two_positional_error", [ins("c", GRID), upd("c", {"_id": 1, "g": 1}, {"$set": {"g.$.$": 1}}), ids()])
    t("pos_with_upsert_error", [ins("c", NUMS), upd("c", {"_id": 77, "a": 2}, {"$set": {"a.$": 1}}, upsert=True), ids()])
    t("pos_rename_error", [ins("c", NUMS), upd("c", {"_id": 1, "a": 2}, {"$rename": {"a.$": "b"}}), ids()])
    t("pos_find_one_and_update", [ins("c", NUMS), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1, "a": 3}, "update": {"$set": {"a.$": 30}}, "return_document": "after"}, ids()])
    t("pos_nested_array_dotted", [ins("c", GRID), upd("c", {"_id": 1, "grades.m": 70}, {"$set": {"grades.$.m": 75}}), ids()])
    t("pos_two_conditions_same_array", [ins("c", ITEMS), upd("c", {"_id": 1, "items.sku": "x", "items.qty": 10}, {"$set": {"items.$.mark": 1}}), ids()])

    # ---- $[] ---------------------------------------------------------------------------------
    t("all_set", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[]": 0}}), ids()])
    t("all_inc", [ins("c", NUMS), upd("c", {"_id": 2}, {"$inc": {"a.$[]": 1}}), ids()])
    t("all_empty_array_noop", [ins("c", NUMS), upd("c", {"_id": 3}, {"$set": {"a.$[]": 1}}), ids()])
    t("all_non_array_error", [ins("c", NUMS), upd("c", {"_id": 4}, {"$set": {"a.$[]": 1}}), ids()])
    t("all_missing_path_error", [ins("c", NUMS), upd("c", {"_id": 5}, {"$set": {"a.$[]": 1}}), ids()])
    t("all_subfield_set", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$set": {"items.$[].seen": True}}), ids()])
    t("all_subfield_inc", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$inc": {"items.$[].qty": 2}}), ids()])
    t("all_push_to_inner_arrays", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$push": {"items.$[].tags": "z"}}), ids()])
    t("all_unset_subfield", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$unset": {"items.$[].tags": 1}}), ids()])
    t("all_nested_all", [ins("c", GRID), upd("c", {"_id": 1}, {"$inc": {"g.$[].$[]": 10}}), ids()])
    t("all_update_many", [ins("c", NUMS), upd("c", {"a": {"$type": "array"}}, {"$inc": {"a.$[]": 1}}, kind="update_many"), ids()])
    t("all_mixed_with_other_operator", [ins("c", NUMS), upd("c", {"_id": 1}, {"$inc": {"a.$[]": 1, "n": 5}}), ids()])
    t("all_find_one_and_update", [ins("c", NUMS), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 2}, "update": {"$mul": {"a.$[]": 2}}, "return_document": "before"}, ids()])

    # ---- $[<identifier>] and arrayFilters ------------------------------------------------------
    t("filtered_scalar_gte", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 99}}, array_filters=[{"e": {"$gte": 2}}]), ids()])
    t("filtered_scalar_eq", [ins("c", NUMS), upd("c", {"_id": 1}, {"$inc": {"a.$[e]": 100}}, array_filters=[{"e": 2}]), ids()])
    t("filtered_no_element_matches", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 99}}, array_filters=[{"e": {"$gt": 1000}}]), ids()])
    t("filtered_doc_field", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$set": {"items.$[i].qty": 0}}, array_filters=[{"i.sku": "x"}]), ids()])
    t("filtered_doc_two_fields", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$inc": {"items.$[i].qty": 1}}, array_filters=[{"i.sku": "x", "i.qty": {"$gt": 5}}]), ids()])
    t("filtered_with_or", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$set": {"items.$[i].hit": 1}}, array_filters=[{"$or": [{"i.sku": "y"}, {"i.qty": 10}]}]), ids()])
    t("filtered_with_and", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$set": {"items.$[i].hit": 1}}, array_filters=[{"$and": [{"i.sku": "x"}, {"i.qty": {"$lt": 5}}]}]), ids()])
    t("filtered_two_identifiers", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$set": {"items.$[i].qty": 0, "items.$[j].qty": 1}}, array_filters=[{"i.sku": "x"}, {"j.sku": "y"}]), ids()])
    t("filtered_nested_arrays", [ins("c", GRID), upd("c", {"_id": 1}, {"$inc": {"grades.$[g].std.$[s].v": 100}}, array_filters=[{"g.m": {"$gte": 75}}, {"s.v": {"$gt": 5}}]), ids()])
    t("filtered_combined_with_all", [ins("c", GRID), upd("c", {"_id": 1}, {"$set": {"grades.$[].std.$[s].flag": True}}, array_filters=[{"s.v": {"$lt": 6}}]), ids()])
    t("filtered_update_many", [ins("c", ITEMS), upd("c", {}, {"$inc": {"items.$[i].qty": 1}}, array_filters=[{"i.qty": {"$gte": 3}}], kind="update_many"), ids()])
    t("filtered_find_one_and_update", [ins("c", ITEMS), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$set": {"items.$[i].qty": -1}}, "array_filters": [{"i.sku": "y"}], "return_document": "after"}, ids()])
    t("filtered_push_inner", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$push": {"items.$[i].tags": "n"}}, array_filters=[{"i.sku": "x"}]), ids()])
    t("filtered_unset", [ins("c", ITEMS), upd("c", {"_id": 1}, {"$unset": {"items.$[i].tags": 1}}, array_filters=[{"i.qty": {"$gt": 2}}]), ids()])
    t("filtered_with_positional_condition_on_other_field", [ins("c", ITEMS), upd("c", {"_id": 1, "t": 0}, {"$set": {"t": 1, "items.$[i].qty": 7}}, array_filters=[{"i.sku": "z"}]), ids()])
    t("filtered_unused_filter_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1}, {"f": 2}]), ids()])
    t("filtered_only_filter_without_use_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"n": 1}}, array_filters=[{"e": 1}]), ids()])
    t("filtered_missing_filter_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 1}}), ids()])
    t("filtered_missing_filter_other_id_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 1}}, array_filters=[{"x": 1}]), ids()])
    t("filtered_uppercase_identifier_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[E]": 1}}, array_filters=[{"E": 1}]), ids()])
    t("filtered_two_top_level_names_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1, "f": 2}]), ids()])
    t("filtered_duplicate_identifier_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1}, {"e": 2}]), ids()])
    t("filtered_non_array_error", [ins("c", NUMS), upd("c", {"_id": 4}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1}]), ids()])
    t("filtered_missing_path_error", [ins("c", NUMS), upd("c", {"_id": 5}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1}]), ids()])
    t("filtered_with_replacement_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"n": 1}, array_filters=[{"e": 1}]), ids()])
    t("filtered_with_pipeline_error", [ins("c", NUMS), upd("c", {"_id": 1}, [{"$set": {"n": 1}}], array_filters=[{"e": 1}]), ids()])
    t("filtered_empty_array_filters_ok", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"n": 9}}, array_filters=[]), ids()])
    t("filtered_upsert_missing_path_error", [ins("c", NUMS), upd("c", {"_id": 55}, {"$set": {"a.$[e]": 1}}, array_filters=[{"e": 1}], upsert=True), ids()])
    t("filtered_bad_identifier_in_path_error", [ins("c", NUMS), upd("c", {"_id": 1}, {"$set": {"a.$[1x]": 1}}, array_filters=[{"1x": 1}]), ids()])

    # ---- pipeline updates ------------------------------------------------------------------------
    PD = [{"_id": 1, "a": 1, "b": 2, "tags": ["x", "y"], "o": {"p": 1}}, {"_id": 2, "a": 10, "b": 20, "tags": [], "o": {"p": 2}}, {"_id": 3, "a": "s", "b": None}]
    t("pipe_set_expression", [ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"sum": {"$add": ["$a", "$b"]}}}]), ids()])
    t("pipe_addfields_stage", [ins("c", PD), upd("c", {"_id": 1}, [{"$addFields": {"z": "$o.p", "tagCount": {"$size": "$tags"}}}]), ids()])
    t("pipe_unset_stage", [ins("c", PD), upd("c", {"_id": 1}, [{"$unset": ["tags", "o"]}]), ids()])
    t("pipe_unset_string", [ins("c", PD), upd("c", {"_id": 1}, [{"$unset": "b"}]), ids()])
    t("pipe_project_stage", [ins("c", PD), upd("c", {"_id": 1}, [{"$project": {"a": 1, "dbl": {"$multiply": ["$a", 2]}}}]), ids()])
    t("pipe_replacewith", [ins("c", PD), upd("c", {"_id": 1}, [{"$replaceWith": {"_id": "$_id", "only": "$a"}}]), ids()])
    t("pipe_replaceroot", [ins("c", PD), upd("c", {"_id": 1}, [{"$replaceRoot": {"newRoot": {"$mergeObjects": [{"_id": "$_id"}, "$o"]}}}]), ids()])
    t("pipe_multi_stage", [ins("c", PD), upd("c", {"_id": 2}, [{"$set": {"t": {"$add": ["$a", "$b"]}}}, {"$set": {"u": {"$multiply": ["$t", 2]}}}, {"$unset": "tags"}]), ids()])
    t("pipe_conditional", [ins("c", PD), upd("c", {}, [{"$set": {"size": {"$cond": [{"$gt": ["$a", 5]}, "big", "small"]}}}], kind="update_many"), ids()])
    t("pipe_reference_missing_field", [ins("c", PD), upd("c", {"_id": 3}, [{"$set": {"x": "$nope", "y": {"$ifNull": ["$nope", "dflt"]}}}]), ids()])
    t("pipe_update_many", [ins("c", PD), upd("c", {"a": {"$type": "number"}}, [{"$set": {"a": {"$add": ["$a", 1]}}}], kind="update_many"), ids()])
    t("pipe_counts", [ins("c", PD), upd("c", {"a": {"$gt": 0}}, [{"$set": {"a": "$a"}}], kind="update_many"), ids()])
    t("pipe_find_one_and_update", [ins("c", PD), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": [{"$set": {"a": {"$add": ["$a", 100]}}}], "return_document": "after"}, ids()])
    t("pipe_upsert_from_filter", [ins("c", PD), upd("c", {"_id": 50, "k": 1}, [{"$set": {"made": True}}], upsert=True), ids()])
    t("pipe_upsert_generated_id_count", [ins("c", PD), upd("c", {"zz": 1}, [{"$set": {"made": "$zz"}}], upsert=True), {"op": "count_documents", "coll": "c", "filter": {"made": 1}}])
    t("pipe_change_id_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"_id": 99}}]), ids()])
    t("pipe_remove_id_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$unset": "_id"}]), ids()])
    t("pipe_replacewith_other_id_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$replaceWith": {"_id": 5, "v": 1}}]), ids()])
    t("pipe_disallowed_stage_match_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$match": {"a": 1}}]), ids()])
    t("pipe_disallowed_stage_group_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$group": {"_id": None}}]), ids()])
    t("pipe_empty_pipeline_error", [ins("c", PD), upd("c", {"_id": 1}, []), ids()])
    t("pipe_two_fields_stage_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"a": 1}, "$unset": "b"}]), ids()])
    t("pipe_bad_expression_error", [ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"x": {"$bogus": 1}}}]), ids()])
    t("pipe_no_change_modified_zero", [ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"a": 1}}]), ids()])
    t("pipe_unique_index_violation_error", [{"op": "create_index", "coll": "c", "keys": [["u", 1]], "unique": True}, ins("c", [{"_id": 1, "u": 1}, {"_id": 2, "u": 2}]), upd("c", {"_id": 2}, [{"$set": {"u": 1}}]), ids()])
    t("pipe_maintains_secondary_index", [{"op": "create_index", "coll": "c", "keys": [["a", 1]]}, ins("c", PD), upd("c", {"_id": 1}, [{"$set": {"a": 77}}]), find("c", {"a": 77}, hint="a_1", projection={"_id": 1}), find("c", {"a": 1}, hint="a_1", projection={"_id": 1})])
    t("pos_maintains_secondary_index", [{"op": "create_index", "coll": "c", "keys": [["a", 1]]}, ins("c", NUMS), upd("c", {"_id": 1, "a": 3}, {"$set": {"a.$": 33}}), find("c", {"a": 33}, hint="a_1", projection={"_id": 1}), find("c", {"a": 3}, hint="a_1", projection={"_id": 1})])
    t("filtered_maintains_unique_index", [{"op": "create_index", "coll": "c", "keys": [["a", 1]], "unique": True}, ins("c", [{"_id": 1, "a": [1, 2]}, {"_id": 2, "a": [3]}]), upd("c", {"_id": 1}, {"$set": {"a.$[e]": 3}}, array_filters=[{"e": 2}]), ids()])

    # ---- renameCollection -------------------------------------------------------------------------
    R = lambda frm, to, **kw: {"op": "admin_command", "cmd": {"renameCollection": "$DB." + frm, "to": "$DB." + to, **kw}}
    names = {"op": "list_collection_names"}
    t("rename_basic", [ins("a", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}]), R("a", "b"), names, ids("b"), {"op": "count_documents", "coll": "a", "filter": {}}])
    t("rename_keeps_indexes", [{"op": "create_index", "coll": "a", "keys": [["v", 1]]}, ins("a", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}]), R("a", "b"), {"op": "list_indexes", "coll": "b"}, find("b", {"v": 2}, hint="v_1", projection={"_id": 1})])
    t("rename_keeps_unique_enforced", [{"op": "create_index", "coll": "a", "keys": [["u", 1]], "unique": True}, ins("a", [{"_id": 1, "u": 1}]), R("a", "b"), ins("b", [{"_id": 2, "u": 1}]), ids("b")])
    t("rename_old_name_is_empty_afterwards", [ins("a", [{"_id": 1}]), R("a", "b"), ins("a", [{"_id": 9}]), ids("a"), ids("b")])
    t("rename_index_entries_not_left_behind", [{"op": "create_index", "coll": "a", "keys": [["v", 1]]}, ins("a", [{"_id": 1, "v": 1}]), R("a", "b"), ins("a", [{"_id": 2, "v": 1}]), find("a", {"v": 1}, sort={"_id": 1}, projection={"_id": 1}), {"op": "count_documents", "coll": "a", "filter": {"v": 1}}, {"op": "count_documents", "coll": "b", "filter": {"v": 1}}])
    t("rename_target_exists_error", [ins("a", [{"_id": 1}]), ins("b", [{"_id": 2}]), R("a", "b"), names, ids("a"), ids("b")])
    t("rename_drop_target", [ins("a", [{"_id": 1, "x": "a"}]), ins("b", [{"_id": 2, "x": "b"}]), R("a", "b", dropTarget=True), names, ids("b")])
    t("rename_drop_target_replaces_indexes", [{"op": "create_index", "coll": "b", "keys": [["old", 1]]}, ins("a", [{"_id": 1}]), ins("b", [{"_id": 2, "old": 1}]), R("a", "b", dropTarget=True), {"op": "list_indexes", "coll": "b"}])
    t("rename_drop_target_false_explicit_error", [ins("a", [{"_id": 1}]), ins("b", [{"_id": 2}]), R("a", "b", dropTarget=False), names])
    t("rename_source_missing_error", [ins("z", [{"_id": 1}]), R("a", "b"), names])
    t("rename_to_itself_error", [ins("a", [{"_id": 1}]), R("a", "a"), names])
    t("rename_target_with_dollar_error", [ins("a", [{"_id": 1}]), R("a", "b$c"), names])
    t("rename_to_system_prefix_error", [ins("a", [{"_id": 1}]), R("a", "system.x"), names])
    t("rename_empty_collection", [{"op": "create_collection", "coll": "a"}, R("a", "b"), names, ids("b")])
    t("rename_then_drop_new_name", [ins("a", [{"_id": 1}]), R("a", "b"), {"op": "drop_collection", "coll": "b"}, names])
    t("rename_chain", [ins("a", [{"_id": 1}]), R("a", "b"), R("b", "c"), R("c", "d"), names, ids("d")])
    t("rename_with_ttl_index_metadata", [{"op": "create_index", "coll": "a", "keys": [["t", 1]], "expireAfterSeconds": 3600}, ins("a", [{"_id": 1, "t": datetime.datetime(2100, 1, 1)}]), R("a", "b"), {"op": "list_indexes", "coll": "b"}])
    t("rename_many_docs", [ins("a", [{"_id": i, "v": i % 7} for i in range(1, 301)]), {"op": "create_index", "coll": "a", "keys": [["v", 1]]}, R("a", "b"), {"op": "count_documents", "coll": "b", "filter": {"v": 3}}, find("b", {"v": 3}, hint="v_1", projection={"_id": 1}, sort={"_id": 1}, limit=5)])
    t("rename_collection_stats_follow", [ins("a", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}]), R("a", "b"), {"op": "command_raw", "cmd": {"collStats": "b"}, "pick": ["ok", "count", "nindexes"]}])
    t("rename_name_prefix_does_not_move_other_collection", [ins("a", [{"_id": 1}]), ins("ab", [{"_id": 2}]), R("a", "z"), names, ids("ab"), ids("z")])
    t("rename_missing_to_field_error", [ins("a", [{"_id": 1}]), {"op": "admin_command", "cmd": {"renameCollection": "$DB.a"}}])
