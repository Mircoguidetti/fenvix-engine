"""Differential tests for the aggregation features added in results9: $bucketAuto, $densify, $fill, $setWindowFields,
$graphLookup, $redact, $documents, $planCacheStats, $median / $percentile, $rand, $sampleRate, $jsonSchema,
$toDecimal, $tsSecond / $tsIncrement."""


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    G = "aggregation tail"

    def t(name, steps, **flags):
        add("tail." + name, steps, G, **flags)

    P = lambda *stages: list(stages)  # noqa
    N = [{"_id": i, "k": i % 3, "v": i * 1.5, "s": "s%d" % (i % 2), "lvl": i % 4} for i in range(1, 11)]

    # ---- $bucketAuto ------------------------------------------------------------------------------------------
    for n in (7, 10, 11, 13):
        for b in (2, 3, 4, 5):
            t("bucket_auto_%d_in_%d" % (n, b), [ins("c", [{"_id": i, "v": i} for i in range(n)]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": b}}])])
    t("bucket_auto_ties", [ins("c", [{"_id": i, "v": v} for i, v in enumerate([1, 1, 1, 2, 2, 3, 4, 5])]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 3}}])])
    t("bucket_auto_all_equal", [ins("c", [{"_id": i, "v": 1} for i in range(6)]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 3}}])])
    t("bucket_auto_mixed_types", [ins("c", [{"_id": i, "v": v} for i, v in enumerate([5, "a", None, 2.5, True, [1], {"x": 1}])]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 3}}])])
    t("bucket_auto_missing_field", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2}, {"_id": 3, "v": 3}, {"_id": 4}]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2}}])])
    t("bucket_auto_output", [ins("c", N), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "output": {"n": {"$sum": 1}, "m": {"$max": "$k"}, "items": {"$push": "$_id"}}}}])])
    t("bucket_auto_output_empty", [ins("c", N), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "output": {}}}])])
    t("bucket_auto_expression_key", [ins("c", N), agg("c", [{"$bucketAuto": {"groupBy": {"$multiply": ["$k", 10]}, "buckets": 2}}])])
    t("bucket_auto_empty_collection", [{"op": "create_collection", "coll": "c"} if False else ins("c", [{"_id": 1}]), agg("c", [{"$match": {"_id": 9}}, {"$bucketAuto": {"groupBy": "$v", "buckets": 2}}])])
    t("bucket_auto_more_buckets_than_docs", [ins("c", [{"_id": i, "v": i} for i in range(3)]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 10}}])])
    t("bucket_auto_one_bucket", [ins("c", N), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 1}}])])
    t("bucket_auto_pow2", [ins("c", [{"_id": i, "v": i} for i in range(1, 12)]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 3, "granularity": "POWERSOF2"}}])])
    t("bucket_auto_pow2_zero", [ins("c", [{"_id": i, "v": i} for i in range(0, 4)]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": "POWERSOF2"}}])])
    t("bucket_auto_125", [ins("c", [{"_id": i, "v": v} for i, v in enumerate([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 100, 250])]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 3, "granularity": "1-2-5"}}])])
    t("bucket_auto_granularity_negative_error", [ins("c", [{"_id": 1, "v": -1}, {"_id": 2, "v": 1}]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": "POWERSOF2"}}])])
    t("bucket_auto_granularity_string_error", [ins("c", [{"_id": 1, "v": "a"}]), agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": "POWERSOF2"}}])])
    for nm, spec in [("zero_buckets", {"groupBy": "$v", "buckets": 0}), ("negative_buckets", {"groupBy": "$v", "buckets": -1}), ("string_buckets", {"groupBy": "$v", "buckets": "a"}), ("no_group_by", {"buckets": 2}),
                     ("no_buckets", {"groupBy": "$v"}), ("unknown_option", {"groupBy": "$v", "buckets": 2, "bogus": 1}), ("output_not_object", {"groupBy": "$v", "buckets": 2, "output": 5}), ("not_object", 5)]:
        t("bucket_auto_error_" + nm, [ins("c", N), agg("c", [{"$bucketAuto": spec}])])

    # ---- $densify -----------------------------------------------------------------------------------------------
    DD = [{"_id": 1, "v": 1, "g": "a"}, {"_id": 2, "v": 4, "g": "a"}, {"_id": 3, "v": 2, "g": "b"}, {"_id": 4, "v": 6, "g": "b"}]
    dn = lambda spec: [ins("c", DD), agg("c", [{"$densify": spec}])]  # noqa
    t("densify_full", dn({"field": "v", "range": {"step": 1, "bounds": "full"}}))
    t("densify_partition", dn({"field": "v", "partitionByFields": ["g"], "range": {"step": 2, "bounds": "partition"}}))
    t("densify_explicit", dn({"field": "v", "range": {"step": 2, "bounds": [0, 5]}}))
    t("densify_explicit_partitioned", dn({"field": "v", "partitionByFields": ["g"], "range": {"step": 2, "bounds": [0, 5]}}))
    t("densify_explicit_beyond", dn({"field": "v", "range": {"step": 3, "bounds": [-2, 9]}}))
    t("densify_full_partitioned", dn({"field": "v", "partitionByFields": ["g"], "range": {"step": 2, "bounds": "full"}}))
    t("densify_float_step", dn({"field": "v", "range": {"step": 1.5, "bounds": "full"}}))
    t("densify_big_step", dn({"field": "v", "range": {"step": 100, "bounds": "full"}}))
    t("densify_empty", [ins("c", [{"_id": 1}]), agg("c", [{"$match": {"_id": 5}}, {"$densify": {"field": "v", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_missing_field_docs", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "w": 2}, {"_id": 3, "v": 3}]), agg("c", [{"$densify": {"field": "v", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_dotted", [ins("c", [{"_id": 1, "a": {"b": 1}}, {"_id": 2, "a": {"b": 3}}]), agg("c", [{"$densify": {"field": "a.b", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_unsorted_input", [ins("c", [{"_id": 1, "v": 5}, {"_id": 2, "v": 1}, {"_id": 3, "v": 3}]), agg("c", [{"$densify": {"field": "v", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_duplicates", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": 1}, {"_id": 3, "v": 3}]), agg("c", [{"$densify": {"field": "v", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_off_grid_values", [ins("c", [{"_id": 1, "v": 0}, {"_id": 2, "v": 2.5}, {"_id": 3, "v": 6}]), agg("c", [{"$densify": {"field": "v", "range": {"step": 2, "bounds": "full"}}}])])
    import datetime as dt
    t("densify_dates_day", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2020-01-04T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "unit": "day", "bounds": "full"}}}])])
    t("densify_dates_hour_step", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2020-01-01T10:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 4, "unit": "hour", "bounds": "full"}}}])])
    t("densify_dates_month_clamp", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-31T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2020-05-01T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "unit": "month", "bounds": "full"}}}])])
    t("densify_dates_year", [ins("c", [{"_id": 1, "t": {"$date": "2018-06-01T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2021-06-01T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "unit": "year", "bounds": "full"}}}])])
    t("densify_dates_week", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2020-02-01T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "unit": "week", "bounds": "full"}}}])])
    t("densify_dates_explicit", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-02T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "unit": "day", "bounds": [{"$date": "2020-01-01T00:00:00Z"}, {"$date": "2020-01-04T00:00:00Z"}]}}}])])
    t("densify_date_without_unit_error", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 2, "t": {"$date": "2020-01-04T00:00:00Z"}}]), agg("c", [{"$densify": {"field": "t", "range": {"step": 1, "bounds": "full"}}}])])
    t("densify_number_with_unit_error", dn({"field": "v", "range": {"step": 1, "unit": "day", "bounds": "full"}}))
    t("densify_non_numeric_error", [ins("c", [{"_id": 1, "v": "x"}]), agg("c", [{"$densify": {"field": "v", "range": {"step": 1, "bounds": "full"}}}])])
    for nm, spec in [("step_zero", {"field": "v", "range": {"step": 0, "bounds": "full"}}), ("step_negative", {"field": "v", "range": {"step": -1, "bounds": "full"}}), ("bad_bounds_string", {"field": "v", "range": {"step": 1, "bounds": "x"}}),
                     ("no_range", {"field": "v"}), ("no_field", {"range": {"step": 1, "bounds": "full"}}), ("bounds_reversed", {"field": "v", "range": {"step": 1, "bounds": [5, 0]}}), ("bounds_len", {"field": "v", "range": {"step": 1, "bounds": [5]}}),
                     ("unknown_field", {"field": "v", "range": {"step": 1, "bounds": "full"}, "bogus": 1}), ("no_step", {"field": "v", "range": {"bounds": "full"}}), ("bad_unit", {"field": "t", "range": {"step": 1, "unit": "fortnight", "bounds": "full"}})]:
        t("densify_error_" + nm, dn(spec))

    # ---- $fill --------------------------------------------------------------------------------------------------
    F = [{"_id": 1, "v": None, "g": "a", "t": 1}, {"_id": 2, "v": 3, "g": "a", "t": 2}, {"_id": 3, "g": "a", "t": 3}, {"_id": 4, "v": 7, "g": "b", "t": 4}, {"_id": 5, "v": None, "g": "b", "t": 5}, {"_id": 6, "g": "b", "t": 6}, {"_id": 7, "v": 9, "g": "b", "t": 7}]
    fl = lambda spec: [ins("c", F), agg("c", [{"$fill": spec}])]  # noqa
    t("fill_locf", fl({"sortBy": {"t": 1}, "output": {"v": {"method": "locf"}}}))
    t("fill_linear", fl({"sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}}))
    t("fill_value", fl({"output": {"v": {"value": 0}}}))
    t("fill_value_expression", fl({"output": {"v": {"value": {"$add": ["$t", 100]}}}}))
    t("fill_partition_by_fields", fl({"partitionByFields": ["g"], "sortBy": {"t": 1}, "output": {"v": {"method": "locf"}}}))
    t("fill_partition_by_expr", fl({"partitionBy": "$g", "sortBy": {"t": 1}, "output": {"v": {"method": "locf"}}}))
    t("fill_linear_partitioned", fl({"partitionByFields": ["g"], "sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}}))
    t("fill_two_outputs", fl({"sortBy": {"t": 1}, "output": {"v": {"method": "locf"}, "z": {"value": 1}}}))
    t("fill_locf_without_sort", fl({"output": {"v": {"method": "locf"}}}))
    t("fill_sorted_descending", fl({"sortBy": {"t": -1}, "output": {"v": {"method": "locf"}}}))
    t("fill_new_field_value", fl({"output": {"nf": {"value": "x"}}}))
    t("fill_unsorted_input", [ins("c", [{"_id": 1, "t": 3, "v": 3}, {"_id": 2, "t": 1, "v": 1}, {"_id": 3, "t": 2}]), agg("c", [{"$fill": {"sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}}}])])
    t("fill_linear_dates", [ins("c", [{"_id": 1, "t": {"$date": "2020-01-01T00:00:00Z"}, "v": 0}, {"_id": 2, "t": {"$date": "2020-01-02T00:00:00Z"}}, {"_id": 3, "t": {"$date": "2020-01-05T00:00:00Z"}, "v": 8}]), agg("c", [{"$fill": {"sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}}}])])
    for nm, spec in [("bad_method", {"sortBy": {"t": 1}, "output": {"v": {"method": "bogus"}}}), ("both", {"sortBy": {"t": 1}, "output": {"v": {"method": "locf", "value": 1}}}), ("empty_output", {"sortBy": {"t": 1}, "output": {}}),
                     ("no_output", {"sortBy": {"t": 1}}), ("neither", {"output": {"v": {}}}), ("partition_both", {"partitionBy": "$g", "partitionByFields": ["g"], "output": {"v": {"value": 1}}}), ("linear_no_sort", {"output": {"v": {"method": "linear"}}}),
                     ("unknown_field", {"output": {"v": {"value": 1}}, "bogus": 1}), ("output_not_object", {"output": 5})]:
        t("fill_error_" + nm, fl(spec))

    # ---- $setWindowFields -------------------------------------------------------------------------------------------
    W = [{"_id": i, "g": "a" if i <= 5 else "b", "n": (i * 7) % 5, "v": float(i), "t": i * 10} for i in range(1, 11)]
    sw = lambda spec, tail=None: [ins("c", W), agg("c", [{"$setWindowFields": spec}] + (tail or []))]  # noqa
    t("window_running_sum", sw({"sortBy": {"_id": 1}, "output": {"run": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}}}}))
    t("window_whole_partition", sw({"partitionBy": "$g", "output": {"total": {"$sum": "$v"}, "avg": {"$avg": "$v"}, "c": {"$count": {}}}}))
    t("window_sliding", sw({"sortBy": {"_id": 1}, "output": {"m": {"$avg": "$v", "window": {"documents": [-1, 1]}}}}))
    t("window_following", sw({"sortBy": {"_id": 1}, "output": {"m": {"$max": "$v", "window": {"documents": ["current", 2]}}}}))
    t("window_partitioned_running", sw({"partitionBy": "$g", "sortBy": {"_id": 1}, "output": {"run": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}}}}))
    t("window_range", sw({"sortBy": {"t": 1}, "output": {"s": {"$sum": "$v", "window": {"range": [-20, 0]}}}}))
    t("window_range_unbounded", sw({"sortBy": {"t": 1}, "output": {"s": {"$sum": "$v", "window": {"range": ["unbounded", 10]}}}}))
    t("window_push_addtoset", sw({"sortBy": {"_id": 1}, "output": {"p": {"$push": "$n", "window": {"documents": [-1, 0]}}, "a": {"$addToSet": "$n", "window": {"documents": [-2, 0]}}}}))
    t("window_first_last", sw({"sortBy": {"_id": 1}, "output": {"f": {"$first": "$v", "window": {"documents": [-2, 2]}}, "l": {"$last": "$v", "window": {"documents": [-2, 2]}}}}))
    t("window_min_max", sw({"sortBy": {"_id": 1}, "output": {"mn": {"$min": "$n", "window": {"documents": [-2, 0]}}, "mx": {"$max": "$n", "window": {"documents": [-2, 0]}}}}))
    t("window_stddev", sw({"sortBy": {"_id": 1}, "output": {"sp": {"$stdDevPop": "$v", "window": {"documents": [-2, 0]}}, "ss": {"$stdDevSamp": "$v", "window": {"documents": [-2, 0]}}}}))
    t("window_rank", sw({"sortBy": {"n": 1}, "output": {"r": {"$rank": {}}, "d": {"$denseRank": {}}, "dn": {"$documentNumber": {}}}}))
    t("window_rank_partitioned", sw({"partitionBy": "$g", "sortBy": {"n": -1}, "output": {"r": {"$rank": {}}, "d": {"$denseRank": {}}}}))
    t("window_shift", sw({"sortBy": {"_id": 1}, "output": {"nx": {"$shift": {"output": "$v", "by": 1, "default": -1}}, "pv": {"$shift": {"output": "$v", "by": -1}}}}))
    t("window_ema", sw({"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$v", "N": 3}}}}))
    t("window_ema_alpha", sw({"sortBy": {"_id": 1}, "output": {"e": {"$expMovingAvg": {"input": "$v", "alpha": 0.5}}}}))
    t("window_derivative", sw({"sortBy": {"t": 1}, "output": {"d": {"$derivative": {"input": "$v"}, "window": {"documents": [-1, 0]}}}}))
    t("window_integral", sw({"sortBy": {"t": 1}, "output": {"i": {"$integral": {"input": "$v"}, "window": {"documents": ["unbounded", "current"]}}}}))
    t("window_median", sw({"sortBy": {"_id": 1}, "output": {"m": {"$median": {"input": "$v", "method": "approximate"}, "window": {"documents": [-2, 2]}}}}))
    t("window_percentile", sw({"sortBy": {"_id": 1}, "output": {"p": {"$percentile": {"input": "$v", "p": [0.25, 0.75], "method": "approximate"}, "window": {"documents": ["unbounded", "current"]}}}}))
    t("window_locf", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": None}, {"_id": 3}, {"_id": 4, "v": 4}]), agg("c", [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"f": {"$locf": "$v"}}}}])])
    t("window_linear_fill", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": None}, {"_id": 3}, {"_id": 4, "v": 4}]), agg("c", [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"f": {"$linearFill": "$v"}}}}])])
    t("window_multiple_outputs", sw({"partitionBy": "$g", "sortBy": {"_id": 1}, "output": {"a": {"$sum": 1, "window": {"documents": ["unbounded", "current"]}}, "b": {"$rank": {}}}}))
    t("window_then_match", sw({"sortBy": {"_id": 1}, "output": {"run": {"$sum": "$v", "window": {"documents": ["unbounded", "current"]}}}}, [{"$match": {"run": {"$gt": 20}}}, {"$project": {"run": 1}}]))
    t("window_no_sort_whole", sw({"output": {"mx": {"$max": "$v"}}}))
    t("window_expr_partition", sw({"partitionBy": {"$mod": ["$_id", 2]}, "sortBy": {"_id": 1}, "output": {"c": {"$sum": 1, "window": {"documents": ["unbounded", "current"]}}}}))
    t("window_empty_collection", [ins("c", [{"_id": 1}]), agg("c", [{"$match": {"_id": 5}}, {"$setWindowFields": {"output": {"x": {"$sum": 1}}}}])])
    for nm, spec in [("no_output", {"sortBy": {"_id": 1}}), ("unknown_field", {"output": {"x": {"$sum": 1}}, "bogus": 1}), ("output_not_object", {"output": 5}), ("rank_without_sort", {"output": {"r": {"$rank": {}}}}),
                     ("bad_window", {"sortBy": {"_id": 1}, "output": {"x": {"$sum": 1, "window": {"documents": [1]}}}}), ("bad_bound", {"sortBy": {"_id": 1}, "output": {"x": {"$sum": 1, "window": {"documents": ["x", 1]}}}}),
                     ("unknown_operator", {"sortBy": {"_id": 1}, "output": {"x": {"$bogus": 1}}}), ("range_without_sort", {"output": {"x": {"$sum": 1, "window": {"range": [-1, 1]}}}})]:
        t("window_error_" + nm, sw(spec))

    # ---- $graphLookup ----------------------------------------------------------------------------------------------
    TREE = [{"_id": 1, "name": "root", "parent": None}, {"_id": 2, "name": "a", "parent": 1}, {"_id": 3, "name": "b", "parent": 1}, {"_id": 4, "name": "a1", "parent": 2}, {"_id": 5, "name": "a2", "parent": 2}, {"_id": 6, "name": "a11", "parent": 4}, {"_id": 7, "name": "lonely", "parent": 99}]
    gl = lambda spec, coll="c", tail=None: [ins("c", TREE), agg(coll, [{"$graphLookup": spec}] + (tail or []))]  # noqa
    SRT = {"$project": {"name": 1, "up": {"$sortArray": {"input": "$up.name", "sortBy": 1}}}}
    t("graph_up", gl({"from": "c", "startWith": "$parent", "connectFromField": "parent", "connectToField": "_id", "as": "up"}, tail=[{"$sort": {"_id": 1}}, SRT]))
    t("graph_down", gl({"from": "c", "startWith": "$_id", "connectFromField": "_id", "connectToField": "parent", "as": "up"}, tail=[{"$sort": {"_id": 1}}, SRT]))
    t("graph_max_depth", gl({"from": "c", "startWith": "$_id", "connectFromField": "_id", "connectToField": "parent", "as": "up", "maxDepth": 1}, tail=[{"$sort": {"_id": 1}}, SRT]))
    t("graph_max_depth_zero", gl({"from": "c", "startWith": "$_id", "connectFromField": "_id", "connectToField": "parent", "as": "up", "maxDepth": 0}, tail=[{"$sort": {"_id": 1}}, SRT]))
    t("graph_depth_field", gl({"from": "c", "startWith": "$parent", "connectFromField": "parent", "connectToField": "_id", "as": "up", "depthField": "d"}, tail=[{"$match": {"_id": 6}}, {"$unwind": "$up"}, {"$sort": {"up.d": 1}}, {"$project": {"n": "$up.name", "d": "$up.d"}}]))
    t("graph_restrict", gl({"from": "c", "startWith": "$_id", "connectFromField": "_id", "connectToField": "parent", "as": "up", "restrictSearchWithMatch": {"name": {"$ne": "a1"}}}, tail=[{"$sort": {"_id": 1}}, SRT]))
    t("graph_start_array", gl({"from": "c", "startWith": [2, 3], "connectFromField": "_id", "connectToField": "parent", "as": "up"}, tail=[{"$match": {"_id": 1}}, SRT]))
    t("graph_start_missing", gl({"from": "c", "startWith": "$nope", "connectFromField": "_id", "connectToField": "parent", "as": "up"}, tail=[{"$match": {"_id": 1}}, SRT]))
    t("graph_cycle", [ins("c", [{"_id": 1, "next": 2}, {"_id": 2, "next": 3}, {"_id": 3, "next": 1}]), agg("c", [{"$graphLookup": {"from": "c", "startWith": "$next", "connectFromField": "next", "connectToField": "_id", "as": "r"}}, {"$project": {"n": {"$size": "$r"}}}, {"$sort": {"_id": 1}}])])
    t("graph_array_connect_field", [ins("c", [{"_id": 1, "to": [2, 3]}, {"_id": 2, "to": [4]}, {"_id": 3, "to": []}, {"_id": 4, "to": [1]}]), agg("c", [{"$match": {"_id": 1}}, {"$graphLookup": {"from": "c", "startWith": "$to", "connectFromField": "to", "connectToField": "_id", "as": "r"}}, {"$project": {"ids": {"$sortArray": {"input": "$r._id", "sortBy": 1}}}}])])
    t("graph_other_collection", [ins("c", [{"_id": 1, "s": "x"}]), ins("e", [{"_id": "x", "n": "y"}, {"_id": "y", "n": None}]), agg("c", [{"$graphLookup": {"from": "e", "startWith": "$s", "connectFromField": "n", "connectToField": "_id", "as": "r"}}, {"$project": {"ids": {"$sortArray": {"input": "$r._id", "sortBy": 1}}}}])])
    for nm, spec in [("missing_from", {"startWith": "$parent", "connectFromField": "parent", "connectToField": "_id", "as": "up"}), ("unknown_option", {"from": "c", "startWith": "$parent", "connectFromField": "parent", "connectToField": "_id", "as": "up", "bogus": 1}),
                     ("negative_depth", {"from": "c", "startWith": "$parent", "connectFromField": "parent", "connectToField": "_id", "as": "up", "maxDepth": -1}), ("not_object", 5)]:
        t("graph_error_" + nm, gl(spec))

    # ---- $redact --------------------------------------------------------------------------------------------------------
    RD = [{"_id": 1, "level": 1, "body": "a", "sub": {"level": 1, "x": 1}, "list": [{"level": 1, "y": 1}, {"level": 5, "y": 2}]}, {"_id": 2, "level": 5, "body": "b"}, {"_id": 3, "level": 3, "sub": {"level": 9, "x": 3}}]
    rc = lambda e: [ins("c", RD), agg("c", [{"$redact": e}])]  # noqa
    t("redact_keep_prune", rc({"$cond": [{"$gt": ["$level", 2]}, "$$PRUNE", "$$KEEP"]}))
    t("redact_descend", rc({"$cond": [{"$lte": ["$level", 3]}, "$$DESCEND", "$$PRUNE"]}))
    t("redact_always_keep", rc("$$KEEP"))
    t("redact_always_prune", rc("$$PRUNE"))
    t("redact_always_descend", rc("$$DESCEND"))
    t("redact_switch", rc({"$switch": {"branches": [{"case": {"$eq": ["$level", 1]}, "then": "$$DESCEND"}, {"case": {"$eq": ["$level", 3]}, "then": "$$KEEP"}], "default": "$$PRUNE"}}))
    t("redact_bad_value_error", rc("keep"))
    t("redact_number_error", rc(5))

    # ---- $documents, $planCacheStats --------------------------------------------------------------------------------------
    DB = lambda pipe: cmd({"aggregate": 1, "pipeline": pipe, "cursor": {}})  # noqa
    t("documents_basic", [DB([{"$documents": [{"a": 1}, {"a": 2}]}])])
    t("documents_pipeline", [DB([{"$documents": [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}]}, {"$match": {"a": {"$gt": 1}}}, {"$project": {"s": {"$add": ["$a", "$b"]}}}])])
    t("documents_empty", [DB([{"$documents": []}])])
    t("documents_expression", [DB([{"$documents": [{"a": {"$add": [1, 2]}}, {"a": {"$literal": "$x"}}]}])])
    t("documents_group", [DB([{"$documents": [{"k": 1, "v": 2}, {"k": 1, "v": 3}, {"k": 2, "v": 4}]}, {"$group": {"_id": "$k", "s": {"$sum": "$v"}}}, {"$sort": {"_id": 1}}])])
    t("documents_not_first_error", [DB([{"$match": {}}, {"$documents": [{"a": 1}]}])])
    t("documents_non_object_error", [DB([{"$documents": [1, 2]}])])
    t("documents_not_array_error", [DB([{"$documents": "x"}])])
    t("documents_on_collection_error", [ins("c", [{"_id": 1}]), agg("c", [{"$documents": [{"a": 1}]}])])
    t("database_aggregate_without_documents_error", [DB([{"$match": {}}])])
    t("plan_cache_stats_fresh", [ins("c", [{"_id": 1}]), agg("c", [{"$planCacheStats": {}}])])

    # ---- $median / $percentile ---------------------------------------------------------------------------------------------
    PV = [{"_id": i, "k": i % 2, "v": v} for i, v in enumerate([5, 1, 9, 3, 7, 2, 8, 4, 6, 10])]
    t("percentile_group", [ins("c", PV), agg("c", [{"$group": {"_id": None, "p": {"$percentile": {"input": "$v", "p": [0.5, 0.9], "method": "approximate"}}}}])])
    t("percentile_group_by", [ins("c", PV), agg("c", [{"$group": {"_id": "$k", "p": {"$percentile": {"input": "$v", "p": [0, 0.25, 0.5, 0.75, 1], "method": "approximate"}}}}, {"$sort": {"_id": 1}}])])
    t("median_group", [ins("c", PV), agg("c", [{"$group": {"_id": "$k", "m": {"$median": {"input": "$v", "method": "approximate"}}}}, {"$sort": {"_id": 1}}])])
    t("median_odd_count", [ins("c", PV[:9]), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "method": "approximate"}}}}])])
    t("median_floats", [ins("c", [{"_id": i, "v": v} for i, v in enumerate([1.5, 2.5, 0.5, 9.25])]), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "method": "approximate"}}}}])])
    t("median_ignores_non_numeric", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": "x"}, {"_id": 3, "v": None}, {"_id": 4}, {"_id": 5, "v": 3}]), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "method": "approximate"}}}}])])
    t("median_no_numbers", [ins("c", [{"_id": 1, "v": "x"}]), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "method": "approximate"}}, "p": {"$percentile": {"input": "$v", "p": [0.5], "method": "approximate"}}}}])])
    t("median_mixed_int_double", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2.5}, {"_id": 3, "v": {"$numberLong": "7"}}]), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "method": "approximate"}}, "p": {"$percentile": {"input": "$v", "p": [0.1, 0.99], "method": "approximate"}}}}])])
    t("percentile_expression", [ins("c", [{"_id": 1, "a": [3, 1, 2, 10, 7]}]), agg("c", [{"$project": {"p": {"$percentile": {"input": "$a", "p": [0.2, 0.8], "method": "approximate"}}, "m": {"$median": {"input": "$a", "method": "approximate"}}}}])])
    t("median_expression_empty_array", [ins("c", [{"_id": 1, "a": []}]), agg("c", [{"$project": {"m": {"$median": {"input": "$a", "method": "approximate"}}}}])])
    t("median_expression_scalar_input", [ins("c", [{"_id": 1, "a": 5}]), agg("c", [{"$project": {"m": {"$median": {"input": "$a", "method": "approximate"}}}}])])
    t("percentile_bad_p_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "p": {"$percentile": {"input": "$v", "p": [1.5], "method": "approximate"}}}}])])
    t("percentile_empty_p_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "p": {"$percentile": {"input": "$v", "p": [], "method": "approximate"}}}}])])
    t("percentile_no_p_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "p": {"$percentile": {"input": "$v", "method": "approximate"}}}}])])
    t("percentile_bad_method_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "p": {"$percentile": {"input": "$v", "p": [0.5], "method": "exact"}}}}])])
    t("median_no_method_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v"}}}}])])
    t("median_with_p_error", [ins("c", PV), agg("c", [{"$group": {"_id": None, "m": {"$median": {"input": "$v", "p": [0.5], "method": "approximate"}}}}])])

    # ---- $rand, $sampleRate, $jsonSchema, conversions --------------------------------------------------------------------------
    t("rand_range", [ins("c", N), agg("c", [{"$project": {"r": {"$rand": {}}}}, {"$match": {"r": {"$gte": 0, "$lt": 1}}}, {"$count": "n"}])])
    t("rand_in_match_expr", [ins("c", N), agg("c", [{"$match": {"$expr": {"$lt": [{"$rand": {}}, 2]}}}, {"$count": "n"}])])
    t("rand_varies", [ins("c", N), agg("c", [{"$project": {"r": {"$rand": {}}}}, {"$group": {"_id": None, "d": {"$addToSet": "$r"}}}, {"$project": {"many": {"$gt": [{"$size": "$d"}, 5]}}}])])
    t("rand_args_error", [ins("c", N), agg("c", [{"$project": {"r": {"$rand": 1}}}])])
    t("sample_rate_all", [ins("c", N), agg("c", [{"$match": {"$sampleRate": 1.0}}, {"$count": "n"}])])
    t("sample_rate_none", [ins("c", N), agg("c", [{"$match": {"$sampleRate": 0}}, {"$count": "n"}])])
    t("sample_rate_find_all", [ins("c", N), {"op": "find_count", "coll": "c", "filter": {"$sampleRate": 1}}])
    t("sample_rate_range_error", [ins("c", N), agg("c", [{"$match": {"$sampleRate": 1.5}}])])
    t("sample_rate_type_error", [ins("c", N), agg("c", [{"$match": {"$sampleRate": "x"}}])])
    JS = [{"_id": 1, "name": "a", "n": 5, "tags": ["x", "y"], "addr": {"zip": "123"}}, {"_id": 2, "name": "bb", "n": 15.5, "tags": [], "addr": {"zip": "x"}}, {"_id": 3, "n": "s"}, {"_id": 4, "name": None, "n": 2, "tags": ["x", "x"]},
          {"_id": 5, "name": "e", "n": {"$numberLong": "9"}, "extra": True}]
    js = lambda s: [ins("c", JS), find("c", {"$jsonSchema": s}, sort={"_id": 1}, projection={"_id": 1})]  # noqa
    for nm, s in [
        ("required", {"required": ["name"]}), ("bson_type", {"properties": {"n": {"bsonType": "double"}}}), ("bson_number", {"properties": {"n": {"bsonType": "number"}}}), ("bson_array_of_types", {"properties": {"n": {"bsonType": ["int", "long"]}}}),
        ("json_type", {"properties": {"n": {"type": "number"}}}), ("minimum", {"properties": {"n": {"bsonType": "number", "minimum": 5}}}), ("exclusive_minimum", {"properties": {"n": {"bsonType": "number", "minimum": 5, "exclusiveMinimum": True}}}),
        ("maximum", {"properties": {"n": {"bsonType": "number", "maximum": 9}}}), ("multiple_of", {"properties": {"n": {"bsonType": "number", "multipleOf": 5}}}), ("min_length", {"properties": {"name": {"minLength": 2}}}),
        ("max_length", {"properties": {"name": {"maxLength": 1}}}), ("pattern", {"properties": {"addr": {"properties": {"zip": {"pattern": "^[0-9]+$"}}}}}), ("enum", {"properties": {"name": {"enum": ["a", "e", None]}}}),
        ("min_items", {"properties": {"tags": {"minItems": 1}}}), ("max_items", {"properties": {"tags": {"maxItems": 1}}}), ("unique_items", {"properties": {"tags": {"uniqueItems": True}}}), ("items", {"properties": {"tags": {"items": {"enum": ["x"]}}}}),
        ("items_tuple", {"properties": {"tags": {"items": [{"enum": ["x"]}, {"enum": ["y"]}]}}}), ("additional_false", {"properties": {"_id": {}, "name": {}, "n": {}}, "additionalProperties": False}),
        ("additional_schema", {"properties": {"_id": {}}, "additionalProperties": {"bsonType": ["string", "double", "null", "array", "object", "long", "int"]}}), ("all_of", {"allOf": [{"required": ["n"]}, {"required": ["tags"]}]}),
        ("any_of", {"anyOf": [{"required": ["extra"]}, {"properties": {"n": {"bsonType": "int"}}, "required": ["n"]}]}), ("one_of", {"oneOf": [{"required": ["name"]}, {"required": ["tags"]}]}), ("not", {"not": {"required": ["name"]}}),
        ("min_properties", {"minProperties": 5}), ("max_properties", {"maxProperties": 3}), ("pattern_properties", {"patternProperties": {"^na": {"bsonType": "string"}}}), ("nested_properties", {"properties": {"addr": {"bsonType": "object", "required": ["zip"]}}}),
        ("title_description", {"title": "t", "description": "d", "required": ["n"]}), ("empty", {}), ("dependencies", {"dependencies": {"extra": ["name"]}}), ("type_object", {"type": "object"}),
    ]:
        t("json_schema_" + nm, js(s))
    for nm, s in [("unknown_keyword", {"bogus": 1}), ("bad_type_name", {"properties": {"n": {"bsonType": "nope"}}}), ("required_not_array", {"required": "name"}), ("properties_not_object", {"properties": 5}), ("all_of_empty", {"allOf": []}), ("enum_not_array", {"properties": {"n": {"enum": 5}}})]:
        t("json_schema_error_" + nm, js(s))
    t("json_schema_not_object_error", [ins("c", JS), find("c", {"$jsonSchema": 5})])
    t("json_schema_in_aggregate_match", [ins("c", JS), agg("c", [{"$match": {"$jsonSchema": {"required": ["name"]}}}, {"$count": "n"}])])
    t("json_schema_with_other_conditions", [ins("c", JS), find("c", {"n": {"$gt": 4}, "$jsonSchema": {"required": ["name"]}}, sort={"_id": 1}, projection={"_id": 1})])
    t("json_schema_in_or", [ins("c", JS), find("c", {"$or": [{"$jsonSchema": {"required": ["extra"]}}, {"_id": 1}]}, sort={"_id": 1}, projection={"_id": 1})])
    t("json_schema_update_filter", [ins("c", JS), {"op": "update_many", "coll": "c", "filter": {"$jsonSchema": {"required": ["tags"]}}, "update": {"$set": {"z": 1}}}, find("c", {"z": 1}, sort={"_id": 1}, projection={"_id": 1})])

    CV = [{"_id": 1, "d": 1.5}, {"_id": 2, "d": 3.0}, {"_id": 3, "d": 0.1}, {"_id": 4, "d": 123456.789}, {"_id": 5, "d": 1e20}, {"_id": 6, "d": -2.5}, {"_id": 7, "d": 0.0}, {"_id": 8, "d": 7}, {"_id": 9, "d": {"$numberLong": "12345678901234567"}},
          {"_id": 10, "d": "3.14159265358979323846"}, {"_id": 11, "d": True}, {"_id": 12, "d": {"$date": "1970-01-02T00:00:00Z"}}, {"_id": 13, "d": None}, {"_id": 14}, {"_id": 15, "d": {"$numberDecimal": "9.99"}}, {"_id": 16, "d": 1.0000000000000002}, {"_id": 17, "d": 2 ** 53 + 1.0}]
    t("to_decimal", [ins("c", CV), agg("c", [{"$project": {"x": {"$toDecimal": "$d"}}}, {"$sort": {"_id": 1}}])])
    t("to_decimal_type", [ins("c", CV), agg("c", [{"$project": {"x": {"$type": {"$toDecimal": "$d"}}}}, {"$sort": {"_id": 1}}])])
    t("to_decimal_back_to_double", [ins("c", CV[:8]), agg("c", [{"$project": {"x": {"$toDouble": {"$toDecimal": "$d"}}}}, {"$sort": {"_id": 1}}])])
    t("convert_to_decimal", [ins("c", CV[:8]), agg("c", [{"$project": {"x": {"$convert": {"input": "$d", "to": "decimal"}}}}, {"$sort": {"_id": 1}}])])
    t("decimal_compare_in_find", [ins("c", [{"_id": 1, "d": {"$numberDecimal": "1.5"}}, {"_id": 2, "d": 2}, {"_id": 3, "d": {"$numberDecimal": "2.5"}}, {"_id": 4, "d": 0.5}]), find("c", {"d": {"$gt": 1}}, sort={"_id": 1}, projection={"_id": 1}), find("c", {"d": 2}, projection={"_id": 1}), find("c", {}, sort={"d": 1}, projection={"_id": 1})])
    t("decimal_eq_across_types", [ins("c", [{"_id": 1, "d": {"$numberDecimal": "2"}}, {"_id": 2, "d": 2}, {"_id": 3, "d": 2.0}, {"_id": 4, "d": {"$numberLong": "2"}}]), find("c", {"d": 2}, sort={"_id": 1}, projection={"_id": 1}), find("c", {"d": {"$numberDecimal": "2.0"}}, sort={"_id": 1}, projection={"_id": 1})])
    t("decimal_group_sort", [ins("c", [{"_id": 1, "d": {"$numberDecimal": "1.5"}}, {"_id": 2, "d": {"$numberDecimal": "1.50"}}, {"_id": 3, "d": 1.5}]), agg("c", [{"$group": {"_id": "$d", "n": {"$sum": 1}}}, {"$count": "groups"}])])
    TS = [{"_id": 1, "t": {"$timestamp": {"t": 1000, "i": 7}}}, {"_id": 2, "t": {"$timestamp": {"t": 4294967295, "i": 4294967295}}}, {"_id": 3, "t": None}, {"_id": 4}, {"_id": 5, "t": 5}]
    t("ts_second_increment", [ins("c", TS[:2]), agg("c", [{"$project": {"s": {"$tsSecond": "$t"}, "i": {"$tsIncrement": "$t"}}}, {"$sort": {"_id": 1}}])])
    t("ts_null_missing", [ins("c", TS[2:4]), agg("c", [{"$project": {"s": {"$tsSecond": "$t"}, "i": {"$tsIncrement": "$t"}}}, {"$sort": {"_id": 1}}])])
    t("ts_wrong_type_error", [ins("c", TS[4:]), agg("c", [{"$project": {"s": {"$tsSecond": "$t"}}}])])
    t("ts_second_type", [ins("c", TS[:1]), agg("c", [{"$project": {"s": {"$type": {"$tsSecond": "$t"}}, "i": {"$type": {"$tsIncrement": "$t"}}}}])])
