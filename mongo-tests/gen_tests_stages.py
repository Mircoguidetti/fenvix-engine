"""Differential tests for aggregation stages and accumulators added in results7 ($facet, $collStats,
$replaceRoot, $unionWith, $lookup with pipeline, $merge, $out, $bucket, ...)."""
import datetime

from bson import Int64


def register(g):
    add, ins, find, agg, cmd = g["add"], g["ins"], g["find"], g["agg"], g["cmd"]
    G = "aggregation stages"

    def t(name, steps, **flags):
        add("stage7." + name, steps, G, **flags)

    P = [{"_id": i, "k": ["a", "b", "c"][i % 3], "n": i * 10 % 7, "v": i, "o": {"x": i, "y": {"z": i * 2}}, "tags": ["t%d" % (i % 2), "u"]} for i in range(1, 9)]
    O = [{"_id": 1, "cust": "a", "amt": 10}, {"_id": 2, "cust": "b", "amt": 20}, {"_id": 3, "cust": "a", "amt": 5}]
    C = [{"_id": "a", "name": "Alice", "tier": 1}, {"_id": "b", "name": "Bob", "tier": 2}, {"_id": "z", "name": "Zed", "tier": 3}]
    s = lambda coll, pipeline: agg(coll, pipeline)

    # replaceRoot / replaceWith / unset
    t("replaceroot_field", [ins("c", P), s("c", [{"$replaceRoot": {"newRoot": "$o"}}, {"$sort": {"x": 1}}])])
    t("replaceroot_expression", [ins("c", P), s("c", [{"$replaceRoot": {"newRoot": {"$mergeObjects": [{"id": "$_id"}, "$o.y"]}}}, {"$sort": {"id": 1}}])])
    t("replaceroot_non_object_error", [ins("c", P), s("c", [{"$replaceRoot": {"newRoot": "$v"}}])])
    t("replaceroot_missing_error", [ins("c", P), s("c", [{"$replaceRoot": {"newRoot": "$nope"}}])])
    t("replaceroot_no_newroot_error", [ins("c", P), s("c", [{"$replaceRoot": {}}])])
    t("replacewith_field", [ins("c", P), s("c", [{"$replaceWith": "$o"}, {"$sort": {"x": 1}}])])
    t("replacewith_literal_object", [ins("c", P), s("c", [{"$replaceWith": {"a": 1}}, {"$limit": 2}])])
    t("unset_string", [ins("c", P), s("c", [{"$unset": "o"}, {"$sort": {"_id": 1}}, {"$limit": 2}])])
    t("unset_array", [ins("c", P), s("c", [{"$unset": ["o", "tags", "n"]}, {"$sort": {"_id": 1}}, {"$limit": 2}])])
    t("unset_nested", [ins("c", P), s("c", [{"$unset": "o.y.z"}, {"$sort": {"_id": 1}}, {"$limit": 2}])])
    t("unset_bad_arg_error", [ins("c", P), s("c", [{"$unset": 5}])])
    t("unset_empty_array_error", [ins("c", P), s("c", [{"$unset": []}])])
    # sortByCount / bucket / sample
    t("sortbycount_field", [ins("c", P), s("c", [{"$sortByCount": "$k"}, {"$sort": {"count": -1, "_id": 1}}])])
    t("sortbycount_expression", [ins("c", P), s("c", [{"$sortByCount": {"$mod": ["$v", 2]}}, {"$sort": {"_id": 1}}])])
    t("sortbycount_unwind_tags", [ins("c", P), s("c", [{"$unwind": "$tags"}, {"$sortByCount": "$tags"}, {"$sort": {"_id": 1}}])])
    t("bucket_basic", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [0, 3, 6, 100]}}])])
    t("bucket_output", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [0, 4, 100], "output": {"total": {"$sum": "$v"}, "ks": {"$addToSet": "$k"}}}}, {"$project": {"total": 1, "n": {"$size": "$ks"}}}])])
    t("bucket_default", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [2, 5], "default": "other"}}])])
    t("bucket_no_default_error", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [2, 5]}}])])
    t("bucket_unsorted_boundaries_error", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [5, 2]}}])])
    t("bucket_too_few_boundaries_error", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$v", "boundaries": [1]}}])])
    t("bucket_by_string", [ins("c", P), s("c", [{"$bucket": {"groupBy": "$k", "boundaries": ["a", "b", "d"]}}])])
    t("sample_size", [ins("c", P), s("c", [{"$sample": {"size": 3}}, {"$count": "n"}])])
    t("sample_larger_than_input", [ins("c", P), s("c", [{"$sample": {"size": 100}}, {"$count": "n"}])])
    t("sample_negative_error", [ins("c", P), s("c", [{"$sample": {"size": -1}}])])
    # facet
    t("facet_two_pipelines", [ins("c", P), s("c", [{"$facet": {"byK": [{"$sortByCount": "$k"}, {"$sort": {"_id": 1}}], "total": [{"$count": "n"}]}}])])
    t("facet_with_limit_skip", [ins("c", P), s("c", [{"$facet": {"page": [{"$sort": {"_id": 1}}, {"$skip": 2}, {"$limit": 3}, {"$project": {"v": 1}}], "meta": [{"$count": "total"}]}}])])
    t("facet_empty_input", [{"op": "create_collection", "coll": "c"}, s("c", [{"$facet": {"a": [{"$count": "n"}], "b": []}}])])
    t("facet_after_match", [ins("c", P), s("c", [{"$match": {"k": "a"}}, {"$facet": {"ids": [{"$project": {"_id": 1}}, {"$sort": {"_id": 1}}], "cnt": [{"$group": {"_id": None, "n": {"$sum": 1}}}]}}])])
    t("facet_then_unwind", [ins("c", P), s("c", [{"$facet": {"a": [{"$match": {"v": {"$lte": 2}}}, {"$project": {"v": 1}}]}}, {"$unwind": "$a"}, {"$sort": {"a._id": 1}}])])
    t("facet_forbidden_stage_error", [ins("c", P), s("c", [{"$facet": {"a": [{"$out": "x"}]}}])])
    t("facet_nested_facet_error", [ins("c", P), s("c", [{"$facet": {"a": [{"$facet": {"b": []}}]}}])])
    t("facet_empty_spec_error", [ins("c", P), s("c", [{"$facet": {}}])])
    t("facet_non_array_error", [ins("c", P), s("c", [{"$facet": {"a": {"$count": "n"}}}])])
    t("facet_lookup_inside", [ins("c", O), ins("cust", C), s("c", [{"$facet": {"j": [{"$lookup": {"from": "cust", "localField": "cust", "foreignField": "_id", "as": "c"}}, {"$project": {"c.name": 1}}, {"$sort": {"_id": 1}}]}}])])
    # unionWith / lookup pipeline
    t("unionwith_string", [ins("c", O), ins("cust", C), s("c", [{"$unionWith": "cust"}, {"$sort": {"_id": 1}}])])
    t("unionwith_pipeline", [ins("c", O), ins("cust", C), s("c", [{"$unionWith": {"coll": "cust", "pipeline": [{"$match": {"tier": {"$gte": 2}}}, {"$project": {"name": 1}}]}}, {"$sort": {"_id": 1}}])])
    t("unionwith_missing_collection", [ins("c", O), s("c", [{"$unionWith": "nope"}, {"$count": "n"}])])
    t("unionwith_then_group", [ins("c", O), ins("d", O), s("c", [{"$unionWith": "d"}, {"$group": {"_id": "$cust", "t": {"$sum": "$amt"}}}, {"$sort": {"_id": 1}}])])
    t("lookup_pipeline_let", [ins("c", O), ins("cust", C), s("c", [{"$lookup": {"from": "cust", "let": {"c": "$cust"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$c"]}}}, {"$project": {"name": 1}}], "as": "who"}}, {"$sort": {"_id": 1}}])])
    t("lookup_pipeline_uncorrelated", [ins("c", O), ins("cust", C), s("c", [{"$lookup": {"from": "cust", "pipeline": [{"$match": {"tier": 3}}], "as": "z"}}, {"$project": {"z.name": 1}}, {"$limit": 1}])])
    t("lookup_pipeline_with_local_foreign", [ins("c", O), ins("cust", C), s("c", [{"$lookup": {"from": "cust", "localField": "cust", "foreignField": "_id", "pipeline": [{"$project": {"tier": 1}}], "as": "t"}}, {"$sort": {"_id": 1}}])])
    t("lookup_pipeline_let_arithmetic", [ins("c", O), ins("d", [{"_id": 1, "limit": 8}, {"_id": 2, "limit": 15}]), s("c", [{"$lookup": {"from": "d", "let": {"a": "$amt"}, "pipeline": [{"$match": {"$expr": {"$lt": ["$limit", "$$a"]}}}], "as": "m"}}, {"$project": {"n": {"$size": "$m"}}}, {"$sort": {"_id": 1}}])])
    t("lookup_pipeline_with_group", [ins("c", O), ins("cust", C), s("c", [{"$lookup": {"from": "c", "let": {"c": "$cust"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$cust", "$$c"]}}}, {"$group": {"_id": None, "t": {"$sum": "$amt"}}}], "as": "tot"}}, {"$project": {"tot.t": 1}}, {"$sort": {"_id": 1}}])])
    t("lookup_bad_let_error", [ins("c", O), s("c", [{"$lookup": {"from": "x", "let": 5, "pipeline": [], "as": "a"}}])])
    # $out / $merge
    t("out_creates_collection", [ins("c", O), s("c", [{"$match": {"cust": "a"}}, {"$out": "res"}]), find("res", {}, sort={"_id": 1}), {"op": "list_collection_names"}])
    t("out_replaces_existing", [ins("c", O), ins("res", [{"_id": 99}]), s("c", [{"$out": "res"}]), find("res", {}, sort={"_id": 1})])
    t("out_keeps_indexes", [ins("c", O), {"op": "create_index", "coll": "res", "keys": [["cust", 1]], "unique": False}, s("c", [{"$out": "res"}]), {"op": "list_indexes", "coll": "res"}])
    t("out_not_last_error", [ins("c", O), s("c", [{"$out": "res"}, {"$count": "n"}])])
    t("out_assigns_ids", [ins("c", O), s("c", [{"$project": {"_id": 0, "amt": 1}}, {"$out": "res"}]), {"op": "count_documents", "coll": "res", "filter": {}}])
    t("merge_insert_new", [ins("c", O), s("c", [{"$merge": {"into": "res"}}]), find("res", {}, sort={"_id": 1})])
    t("merge_merges_matching", [ins("c", O), ins("res", [{"_id": 1, "old": True, "amt": 0}]), s("c", [{"$merge": {"into": "res"}}]), find("res", {}, sort={"_id": 1})])
    t("merge_replace", [ins("c", O), ins("res", [{"_id": 1, "old": True}]), s("c", [{"$merge": {"into": "res", "whenMatched": "replace"}}]), find("res", {}, sort={"_id": 1})])
    t("merge_keep_existing", [ins("c", O), ins("res", [{"_id": 1, "old": True}]), s("c", [{"$merge": {"into": "res", "whenMatched": "keepExisting"}}]), find("res", {}, sort={"_id": 1})])
    t("merge_fail_on_match", [ins("c", O), ins("res", [{"_id": 1}]), s("c", [{"$merge": {"into": "res", "whenMatched": "fail"}}])])
    t("merge_discard_unmatched", [ins("c", O), ins("res", [{"_id": 1}]), s("c", [{"$merge": {"into": "res", "whenNotMatched": "discard"}}]), find("res", {}, sort={"_id": 1})])
    t("merge_fail_unmatched", [ins("c", O), ins("res", [{"_id": 1}]), s("c", [{"$merge": {"into": "res", "whenNotMatched": "fail"}}])])
    t("merge_pipeline_update", [ins("c", O), ins("res", [{"_id": 1, "amt": 100}]), s("c", [{"$merge": {"into": "res", "whenMatched": [{"$set": {"amt": {"$add": ["$amt", "$$new.amt"]}}}]}}]), find("res", {}, sort={"_id": 1})])
    t("merge_on_unique_field", [ins("c", [{"_id": 1, "k": "x", "a": 1}, {"_id": 2, "k": "y", "a": 2}]), ins("res", [{"_id": 10, "k": "x", "a": 0}]), {"op": "create_index", "coll": "res", "keys": [["k", 1]], "unique": True}, s("c", [{"$project": {"_id": 0, "k": 1, "a": 1}}, {"$merge": {"into": "res", "on": "k"}}]), find("res", {}, sort={"k": 1})])
    # $collStats / $indexStats
    t("collstats_count", [ins("c", P), s("c", [{"$collStats": {"count": {}}}, {"$project": {"_id": 0, "count": 1, "ns": 1}}])])
    t("collstats_storage_fields", [ins("c", P), {"op": "create_index", "coll": "c", "keys": [["k", 1]]}, s("c", [{"$collStats": {"storageStats": {}}}, {"$project": {"_id": 0, "ns": 1, "storageStats.count": 1, "storageStats.nindexes": 1, "idx": {"$objectToArray": "$storageStats.indexSizes"}}}, {"$project": {"ns": 1, "storageStats": 1, "names": {"$sortArray": {"input": {"$map": {"input": "$idx", "as": "i", "in": "$$i.k"}}, "sortBy": 1}}}}])])
    t("collstats_not_first_error", [ins("c", P), s("c", [{"$match": {}}, {"$collStats": {}}])])
    t("collstats_missing_collection_error", [s("nope", [{"$collStats": {"count": {}}}])])
    t("collstats_unknown_option_error", [ins("c", P), s("c", [{"$collStats": {"bogus": {}}}])])
    t("collstats_then_pipeline", [ins("c", P), s("c", [{"$collStats": {"count": {}}}, {"$addFields": {"double": {"$multiply": ["$count", 2]}}}, {"$project": {"_id": 0, "double": 1}}])])
    t("collstats_has_localtime_host", [ins("c", P), s("c", [{"$collStats": {}}, {"$project": {"_id": 0, "t": {"$type": "$localTime"}, "h": {"$type": "$host"}}}])])
    t("indexstats_names", [ins("c", P), {"op": "create_index", "coll": "c", "keys": [["k", 1]]}, s("c", [{"$indexStats": {}}, {"$project": {"name": 1, "key": 1}}, {"$sort": {"name": 1}}])])
    # accumulators
    t("acc_mergeobjects", [ins("c", P), s("c", [{"$group": {"_id": "$k", "m": {"$mergeObjects": "$o"}}}, {"$sort": {"_id": 1}}])])
    t("acc_mergeobjects_ignores_null", [ins("c", [{"_id": 1, "g": 1, "o": {"a": 1}}, {"_id": 2, "g": 1, "o": None}, {"_id": 3, "g": 1}]), s("c", [{"$group": {"_id": "$g", "m": {"$mergeObjects": "$o"}}}])])
    t("acc_stddev", [ins("c", P), s("c", [{"$group": {"_id": "$k", "p": {"$stdDevPop": "$v"}, "s": {"$stdDevSamp": "$v"}}}, {"$sort": {"_id": 1}}])])
    t("acc_stddev_single_value", [ins("c", [{"_id": 1, "g": 1, "v": 5}]), s("c", [{"$group": {"_id": "$g", "p": {"$stdDevPop": "$v"}, "s": {"$stdDevSamp": "$v"}}}])])
    t("acc_stddev_no_numbers", [ins("c", [{"_id": 1, "g": 1, "v": "x"}]), s("c", [{"$group": {"_id": "$g", "p": {"$stdDevPop": "$v"}, "s": {"$stdDevSamp": "$v"}}}])])
    t("acc_firstn_lastn", [ins("c", P), s("c", [{"$sort": {"_id": 1}}, {"$group": {"_id": "$k", "f": {"$firstN": {"input": "$v", "n": 2}}, "l": {"$lastN": {"input": "$v", "n": 2}}}}, {"$sort": {"_id": 1}}])])
    t("acc_maxn_minn", [ins("c", P), s("c", [{"$group": {"_id": "$k", "mx": {"$maxN": {"input": "$v", "n": 2}}, "mn": {"$minN": {"input": "$v", "n": 2}}}}, {"$sort": {"_id": 1}}])])
    t("acc_top_bottom", [ins("c", P), s("c", [{"$group": {"_id": "$k", "top": {"$top": {"sortBy": {"v": -1}, "output": "$v"}}, "bot": {"$bottom": {"sortBy": {"v": -1}, "output": "$v"}}}}, {"$sort": {"_id": 1}}])])
    t("acc_topn_bottomn", [ins("c", P), s("c", [{"$group": {"_id": "$k", "t": {"$topN": {"sortBy": {"v": 1}, "output": ["$_id", "$v"], "n": 2}}, "b": {"$bottomN": {"sortBy": {"v": 1}, "output": "$v", "n": 2}}}}, {"$sort": {"_id": 1}}])])
    t("acc_firstn_bad_n_error", [ins("c", P), s("c", [{"$group": {"_id": "$k", "f": {"$firstN": {"input": "$v", "n": 0}}}}])])
    t("acc_accumulator_js", [ins("c", P), s("c", [{"$group": {"_id": "$k", "r": {"$accumulator": {"init": "function() { return {sum: 0, n: 0}; }", "accumulate": "function(s, v) { return {sum: s.sum + v, n: s.n + 1}; }", "accumulateArgs": ["$v"], "merge": "function(a, b) { return {sum: a.sum + b.sum, n: a.n + b.n}; }", "finalize": "function(s) { return s.sum / s.n; }", "lang": "js"}}}}, {"$sort": {"_id": 1}}])])
    t("acc_unknown_error", [ins("c", P), s("c", [{"$group": {"_id": "$k", "x": {"$bogus": "$v"}}}])])
    # operators in stages
    t("expr_in_project_and_match", [ins("c", P), s("c", [{"$match": {"$expr": {"$gt": [{"$strLenCP": "$k"}, 0]}}}, {"$project": {"r": {"$toUpper": "$k"}}}, {"$limit": 1}])])
    t("expr_in_group_key", [ins("c", P), s("c", [{"$group": {"_id": {"$switch": {"branches": [{"case": {"$lt": ["$v", 3]}, "then": "low"}, {"case": {"$lt": ["$v", 6]}, "then": "mid"}], "default": "high"}}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])])
    t("expr_date_group", [ins("c", [{"_id": i, "d": datetime.datetime(2024, 1 + i % 3, 1 + i)} for i in range(1, 9)]), s("c", [{"$group": {"_id": {"$month": "$d"}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])])
    t("expr_let_in_addfields", [ins("c", P), s("c", [{"$addFields": {"r": {"$let": {"vars": {"a": "$v", "b": 2}, "in": {"$multiply": ["$$a", "$$b"]}}}}}, {"$project": {"r": 1}}, {"$sort": {"_id": 1}}, {"$limit": 3}])])
