"""Round 10 differential tests: $toHashedIndexKey, compound wildcard indexes, $bucketAuto granularities, $search
operators (the ones with a reference implementation here), server-side JavaScript."""


def register(g):
    add, ins, find, agg, upd = g["add"], g["ins"], g["find"], g["agg"], g["upd"]

    def t(name, steps, cat="aggregation", **flags):
        add("r10." + name, steps, cat, **flags)

    # ------------------------------------------------------------------ $toHashedIndexKey
    VALS = [
        {"_id": 1, "v": {"$numberInt": "1"}}, {"_id": 2, "v": {"$numberLong": "1"}}, {"_id": 3, "v": {"$numberDouble": "1.0"}},
        {"_id": 4, "v": {"$numberDouble": "2.5"}}, {"_id": 5, "v": {"$numberDouble": "-0.0"}}, {"_id": 6, "v": "abc"}, {"_id": 7, "v": ""},
        {"_id": 8, "v": None}, {"_id": 9, "v": True}, {"_id": 10, "v": False}, {"_id": 11, "v": {"a": 1}}, {"_id": 12, "v": {}},
        {"_id": 13, "v": [1, 2]}, {"_id": 14, "v": []}, {"_id": 15, "v": {"$oid": "507f1f77bcf86cd799439011"}},
        {"_id": 16, "v": {"$date": "2020-01-01T00:00:00Z"}}, {"_id": 17, "v": {"$numberDecimal": "1.5"}}, {"_id": 18, "v": {"$numberDecimal": "-1.7"}},
        {"_id": 19, "v": {"$binary": {"base64": "YWI=", "subType": "00"}}}, {"_id": 20, "v": {"$numberDouble": "NaN"}},
        {"_id": 21, "v": {"$numberDouble": "Infinity"}}, {"_id": 22, "v": {"$numberDouble": "-Infinity"}}, {"_id": 23, "v": {"$numberLong": "1099511627776"}},
        {"_id": 24, "v": {"$numberLong": "9223372036854775807"}}, {"_id": 25, "v": {"$numberDouble": "1.5e300"}}, {"_id": 26, "v": {"$numberDouble": "0.1"}},
        {"_id": 27, "v": {"$timestamp": {"t": 5, "i": 7}}}, {"_id": 28, "v": {"$regularExpression": {"pattern": "a.b", "options": "i"}}},
        {"_id": 29, "v": {"$minKey": 1}}, {"_id": 30, "v": {"$maxKey": 1}}, {"_id": 31, "v": {"$code": "x"}}, {"_id": 32, "v": {"a": [1, {"b": "c"}], "d": None}},
        {"_id": 33, "v": "héllo wörld"}, {"_id": 34, "v": {"$numberDecimal": "2.5"}}, {"_id": 35, "v": {"$numberDecimal": "1E+30"}},
        {"_id": 36, "v": {"$binary": {"base64": "AQID", "subType": "05"}}}, {"_id": 37, "v": [[1], []]},
    ]
    t("to_hashed_index_key_values", [ins("c", VALS), agg("c", [{"$sort": {"_id": 1}}, {"$project": {"h": {"$toHashedIndexKey": "$v"}}}])])
    t("to_hashed_index_key_missing", [ins("c", [{"_id": 1}]), agg("c", [{"$project": {"h": {"$toHashedIndexKey": "$nope"}, "l": {"$toHashedIndexKey": {"$literal": None}}}}])])
    t("to_hashed_index_key_array_arg", [ins("c", [{"_id": 1}]), agg("c", [{"$project": {"a": {"$toHashedIndexKey": [1]}, "b": {"$toHashedIndexKey": [1, 2]}, "c": {"$toHashedIndexKey": []}}}])])
    t("to_hashed_index_key_expr", [ins("c", [{"_id": 1, "a": 2, "b": 3}]), agg("c", [{"$project": {"h": {"$toHashedIndexKey": {"$add": ["$a", "$b"]}}, "s": {"$toHashedIndexKey": {"$concat": ["x", "y"]}}}}])])
    t("to_hashed_index_key_group", [ins("c", [{"_id": i, "k": i % 4} for i in range(20)]),
                                   agg("c", [{"$group": {"_id": {"$mod": [{"$abs": {"$toHashedIndexKey": "$k"}}, 3]}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])])
