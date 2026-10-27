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

    # ------------------------------------------------------------------ compound wildcard indexes
    def cix(coll, keys, **opts):
        step = {"op": "create_index", "coll": coll, "keys": keys}
        if opts:
            step["opts"] = opts
        return step

    full = {"op": "list_indexes", "coll": "c", "full": True}

    def tc(name, steps, **flags):
        add("cwild." + name, steps, "compound wildcard index", **flags)

    def ids(flt, **kw):
        return find("c", flt, sort={"_id": 1}, projection={"_id": 1}, **kw)

    CWD = [{"_id": 1, "a": 1, "b": 2, "c": {"x": 3}}, {"_id": 2, "a": 1, "b": 5, "c": {"x": 4}}, {"_id": 3, "a": 2, "b": 2, "c": {"x": 3}}, {"_id": 4, "b": 2},
           {"_id": 5, "a": 1}, {"_id": 6, "a": 3, "b": [2, 3], "c": [{"x": 3}, {"x": 9}]}, {"_id": 7, "a": 1, "d": 9}, {"_id": 8, "a": None, "b": 2},
           {"_id": 9, "a": {"k": 1}, "b": "s", "c": {"y": [1, 2]}}, {"_id": 10, "a": "z", "c": {}}]
    Q = [{"a": 1}, {"a": 1, "b": 2}, {"b": 2}, {"a": 1, "c.x": 3}, {"b": 2, "c.x": 3}, {"a": {"$gt": 0}, "b": 2}, {"a": {"$in": [1, 2]}, "b": {"$in": [2, 5]}},
         {}, {"a": 1, "b": {"$exists": True}}, {"a": 1, "d": 9}, {"c.x": {"$gte": 3}}, {"a": 2, "b": {"$gt": 1}}, {"a": None}, {"a": {"$exists": False}},
         {"a": {"$type": "string"}}, {"a": 1, "b": None}, {"a": {"$ne": 1}, "b": 2}, {"c.x": 3}, {"c.y": 1}, {"a": {"$in": [1, None]}}, {"a": {"$gte": 1, "$lte": 2}, "c.x": 3}]
    CONFIGS = {
        "wild_after": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"b": 1, "c": 1}}),
        "wild_before": ([["$**", 1], ["a", 1]], {"wildcardProjection": {"b": 1, "c": 1}}),
        "prefix_after": ([["a", 1], ["c.$**", 1]], {}),
        "prefix_before": ([["c.$**", 1], ["a", 1]], {}),
        "two_regular": ([["a", 1], ["b", 1], ["c.$**", -1]], {}),
        "wild_middle": ([["a", 1], ["c.$**", 1], ["b", 1]], {}),
        "descending": ([["a", -1], ["$**", -1]], {"wildcardProjection": {"b": 1, "c": 1}}),
        "partial": ([["a", 1], ["c.$**", 1]], {"partialFilterExpression": {"b": 2}}),
        "exclusion": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"a": 0, "d": 0}}),
    }
    for cname, (keys, opts) in CONFIGS.items():
        hint = "_".join(f"{k}_{v}" for k, v in keys)
        steps = [cix("c", keys, **opts), ins("c", [d for d in CWD if cname != "x"])]
        tc(cname + ".create_list", [cix("c", keys, **opts), full])
        # hinted: each query returns the same rows or the same refusal; unhinted: same rows
        tc(cname + ".hinted", steps + [ids(q, hint=hint) for q in Q])
        tc(cname + ".unhinted", steps + [ids(q) for q in Q])
        tc(cname + ".counts", steps + [{"op": "count_documents", "coll": "c", "filter": q} for q in Q[:12]])
    # array in a regular key field is refused on write
    for i, doc in enumerate([{"_id": 1, "a": []}, {"_id": 1, "a": [1]}, {"_id": 1, "a": [[1]]}, {"_id": 1, "a": [{"b": 1}]}, {"_id": 1, "a": 1, "c": [1, {"d": [2, 3]}]}]):
        tc(f"array_in_key_{i}", [cix("c", [["a", 1], ["c.$**", 1]]), ins("c", [doc]), ids({})])
    tc("array_in_key_update", [cix("c", [["a", 1], ["c.$**", 1]]), ins("c", [{"_id": 1, "a": 1, "c": {"x": 1}}]),
                               upd("c", {"_id": 1}, {"$set": {"a": [1, 2]}}), upd("c", {"_id": 1}, {"$set": {"a": 7}}), ids({"a": 7}, hint="a_1_c.$**_1")])
    tc("array_in_key_build", [ins("c", [{"_id": 1, "a": [1, 2]}, {"_id": 2, "a": 1}]), cix("c", [["a", 1], ["c.$**", 1]]), full])
    tc("dotted_key_field", [cix("c", [["a.b", 1], ["c.$**", 1]]), ins("c", [{"_id": 1, "a": {"b": 1}, "c": {"x": 1}}, {"_id": 2, "a": [{"b": 1}], "c": {"x": 1}}]), ids({})])
    tc("maintenance", [cix("c", [["a", 1], ["c.$**", 1]]), ins("c", CWD[:5]), upd("c", {"_id": 1}, {"$set": {"c.x": 100}}), upd("c", {"_id": 2}, {"$set": {"a": 50}}),
                       {"op": "delete_one", "coll": "c", "filter": {"_id": 3}}, ids({"a": 1, "c.x": 100}, hint="a_1_c.$**_1"), ids({"a": 50}, hint="a_1_c.$**_1"), ids({"a": 2}, hint="a_1_c.$**_1"),
                       upd("c", {"_id": 5}, {"$set": {"c": {"x": 3}}}), ids({"a": 1, "c.x": 3}, hint="a_1_c.$**_1")])
    # specifications
    SP = {
        "no_projection": ([["a", 1], ["$**", 1]], {}),
        "projection_overlap_key": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"a": 1}}),
        "projection_key_below_included": ([["a.x", 1], ["$**", 1]], {"wildcardProjection": {"a": 1}}),
        "projection_included_below_key": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"a.x": 1}}),
        "projection_with_prefix_form": ([["a", 1], ["c.$**", 1]], {"wildcardProjection": {"c.x": 1}}),
        "exclusion_missing_key": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"b": 0}}),
        "exclusion_key": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"a": 0}}),
        "exclusion_only_id": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"_id": 0}}),
        "inclusion_with_id": ([["a", 1], ["$**", 1]], {"wildcardProjection": {"_id": 1, "b": 1}}),
        "id_key": ([["_id", 1], ["$**", 1]], {"wildcardProjection": {"b": 1}}),
        "empty_projection": ([["a", 1], ["$**", 1]], {"wildcardProjection": {}}),
        "two_wildcards": ([["$**", 1], ["a.$**", 1]], {}),
        "overlap_same": ([["a", 1], ["a.$**", 1]], {}),
        "overlap_child_key": ([["a.b", 1], ["a.$**", 1]], {}),
        "overlap_parent_key": ([["a.$**", 1], ["a.b", 1]], {}),
        "disjoint": ([["c.$**", 1], ["a.b", 1]], {}),
        "unique": ([["a", 1], ["c.$**", 1]], {"unique": True}),
        "sparse": ([["a", 1], ["c.$**", 1]], {"sparse": True}),
        "ttl": ([["a", 1], ["$**", 1]], {"expireAfterSeconds": 10, "wildcardProjection": {"b": 1}}),
        "hashed_prefix": ([["a", "hashed"], ["$**", 1]], {"wildcardProjection": {"b": 1}}),
        "hashed_wild": ([["a", 1], ["c.$**", "hashed"]], {}),
        "text_prefix": ([["a", "text"], ["c.$**", 1]], {}),
        "geo_wild": ([["a", 1], ["$**", "2dsphere"]], {}),
        "named": ([["a", 1], ["c.$**", 1]], {"name": "custom"}),
        "v1": ([["a", 1], ["c.$**", 1]], {"v": 1}),
        "descending_wild": ([["a", 1], ["b", 1], ["c.$**", -1]], {}),
        "dir_both": ([["$**", -1], ["a", -1]], {"wildcardProjection": {"b": 1}}),
        "dotted_regular": ([["a.b", 1], ["$**", 1]], {"wildcardProjection": {"c": 1}}),
    }
    for sname, (keys, opts) in SP.items():
        tc("spec." + sname, [cix("c", keys, **opts), full])
    tc("spec.duplicate_other_name", [cix("c", [["a", 1], ["c.$**", 1]]), cix("c", [["a", 1], ["c.$**", 1]], name="again"), full])
    tc("spec.same_twice", [cix("c", [["a", 1], ["c.$**", 1]]), cix("c", [["a", 1], ["c.$**", 1]]), full])
    tc("drop_index", [cix("c", [["a", 1], ["c.$**", 1]]), ins("c", CWD), {"op": "drop_index", "coll": "c", "name": "a_1_c.$**_1"}, ids({"a": 1, "c.x": 3})])
    tc("many_documents", [cix("c", [["a", 1], ["p.$**", 1]]), ins("c", [{"_id": i, "a": i % 7, "p": {"q": i % 11, "r": [i % 5, i % 3]}} for i in range(1, 401)]),
                          {"op": "count_documents", "coll": "c", "filter": {"a": 3, "p.q": 4}, "hint": "a_1_p.$**_1"}, {"op": "count_documents", "coll": "c", "filter": {"a": 3}, "hint": "a_1_p.$**_1"},
                          {"op": "count_documents", "coll": "c", "filter": {"a": {"$gte": 2, "$lt": 5}, "p.r": 2}, "hint": "a_1_p.$**_1"}, {"op": "count_documents", "coll": "c", "filter": {"a": 5, "p.q": {"$gt": 8}}, "hint": "a_1_p.$**_1"}])

    # ------------------------------------------------------------------ $bucketAuto granularities
    def tb(name, steps, **flags):
        add("bucketauto." + name, steps, "$bucketAuto granularity", **flags)

    GR = ["R5", "R10", "R20", "R40", "R80", "1-2-5", "E6", "E12", "E24", "E48", "E96", "E192", "POWERSOF2"]

    def vals(vs):
        return [{"_id": i, "v": v} for i, v in enumerate(vs)]

    def ba(g, b, **kw):
        return agg("c", [{"$bucketAuto": {"groupBy": "$v", "buckets": b, "granularity": g, **kw}}])

    GRID = [round(0.9 + i * 0.037, 4) for i in range(250)]            # about 0.9 .. 10.1
    WIDE = [10 ** k * m for k in range(-4, 5) for m in (1.0, 1.7, 3.3, 6.4)]
    for g in GR:
        tb(f"{g}.grid_buckets", [ins("c", vals(GRID))] + [ba(g, b) for b in (1, 2, 7, 40, 250)])
        tb(f"{g}.wide_decades", [ins("c", vals(WIDE))] + [ba(g, b) for b in (1, 3, 10, 36)])
        tb(f"{g}.on_members", [ins("c", vals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000, 0.1, 0.01]))] + [ba(g, b) for b in (2, 5, 14)])
        tb(f"{g}.zeros", [ins("c", vals([0, 0, 5])), ba(g, 2), ba(g, 3), ba(g, 1)])
        tb(f"{g}.zero_mix", [ins("c", vals([0, 1, 2, 3])), ba(g, 4), ba(g, 2), ins("d", vals([0, 100])), agg("d", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": g}}])])
        tb(f"{g}.zero_variants", [ins("c", vals([0.0, 1.0])), ba(g, 2), ba(g, 1)])
        tb(f"{g}.single", [ins("c", vals([7])), ba(g, 1), ba(g, 3)])
        tb(f"{g}.equal_values", [ins("c", vals([2, 2, 2, 2])), ba(g, 4), ins("d", vals([1, 1, 2, 2])), agg("d", [{"$bucketAuto": {"groupBy": "$v", "buckets": 4, "granularity": g}}])])
        tb(f"{g}.integer_types", [ins("c", [{"_id": 1, "v": {"$numberLong": "3"}}, {"_id": 2, "v": {"$numberInt": "9"}}, {"_id": 3, "v": {"$numberDouble": "20.5"}}]), ba(g, 2), ba(g, 3)])
        tb(f"{g}.output", [ins("c", [{"_id": i, "v": i * 1.5, "w": i} for i in range(1, 30)]), ba(g, 4, output={"n": {"$sum": 1}, "top": {"$max": "$w"}, "ws": {"$push": "$w"}})])
        tb(f"{g}.negative", [ins("c", vals([-1, 2])), ba(g, 2)])
        tb(f"{g}.nan", [ins("c", [{"_id": 1, "v": {"$numberDouble": "NaN"}}, {"_id": 2, "v": 1}]), ba(g, 2)])
        tb(f"{g}.string", [ins("c", vals(["a", 1])), ba(g, 2)])
        tb(f"{g}.null_and_missing", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2}]), ba(g, 2), ins("d", [{"_id": 1, "v": None}]), agg("d", [{"$bucketAuto": {"groupBy": "$v", "buckets": 1, "granularity": g}}])])
        tb(f"{g}.empty", [{"op": "create_collection", "coll": "c"}, ba(g, 3)])
        tb(f"{g}.tiny_huge", [ins("c", vals([1e-12, 1e-9, 1e300, 1e305])), ba(g, 2), ba(g, 4)])
    DEC = lambda x: {"$numberDecimal": x}
    DVALS = ["0.37", "1.1", "3.3", "5.5", "12", "37", "155", "1234", "99999", "1E+7", "123456789", "1E+12", "1E+20", "1E+25", "1E+30", "1E-10", "1E-30", "7E-20", "6.3", "10", "1.6"]
    for g in GR:
        tb(f"{g}.decimal_each", [ins("c", [{"_id": i, "v": DEC(x)} for i, x in enumerate(DVALS)])] + [agg("c", [{"$match": {"_id": i}}, {"$bucketAuto": {"groupBy": "$v", "buckets": 1, "granularity": g}}]) for i in range(len(DVALS))])
        tb(f"{g}.decimal_buckets", [ins("c", [{"_id": i, "v": DEC(x)} for i, x in enumerate(DVALS)])] + [ba(g, b) for b in (2, 5, 21)])
        tb(f"{g}.decimal_special", [ins("c", [{"_id": 1, "v": DEC("0")}, {"_id": 2, "v": DEC("5")}]), ba(g, 2), ins("d", [{"_id": 1, "v": DEC("NaN")}, {"_id": 2, "v": 1}]), agg("d", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": g}}]),
                                    ins("e", [{"_id": 1, "v": DEC("Infinity")}, {"_id": 2, "v": 1}]), agg("e", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": g}}]),
                                    ins("f", [{"_id": 1, "v": DEC("-1.5")}, {"_id": 2, "v": 2}]), agg("f", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": g}}]),
                                    ins("h", [{"_id": 1, "v": DEC("1E+1000")}, {"_id": 2, "v": 1}]), agg("h", [{"$bucketAuto": {"groupBy": "$v", "buckets": 2, "granularity": g}}])])
        tb(f"{g}.decimal_mixed_types", [ins("c", [{"_id": 1, "v": DEC("1.5")}, {"_id": 2, "v": 2.5}, {"_id": 3, "v": 7}, {"_id": 4, "v": DEC("9.9")}]), ba(g, 2), ba(g, 4)])
    tb("unknown_granularity", [ins("c", vals([1, 2])), ba("R7", 2), ba("r5", 2), ba("e6", 2), ba("", 2)])
    tb("unknown_granularity_empty", [{"op": "create_collection", "coll": "c"}, ba("nope", 2)])
    tb("granularity_not_string", [ins("c", vals([1, 2])), ba(5, 2), ba(None, 2)])
    tb("infinity", [ins("c", vals([1, {"$numberDouble": "Infinity"}])), ba("POWERSOF2", 2), ba("R5", 2), ba("1-2-5", 2)])
    tb("pow2_large", [ins("c", vals([1, 2 ** 40])), ba("POWERSOF2", 1), ba("POWERSOF2", 2), ins("d", vals([2 ** 31, 2 ** 33])), agg("d", [{"$bucketAuto": {"groupBy": "$v", "buckets": 1, "granularity": "POWERSOF2"}}])])
    tb("computed_group_by", [ins("c", [{"_id": i, "a": i, "b": 2} for i in range(1, 20)]), agg("c", [{"$bucketAuto": {"groupBy": {"$multiply": ["$a", "$b"]}, "buckets": 5, "granularity": "E12"}}])])

    # ------------------------------------------------------------------ $near / $nearSphere without $maxDistance (rings)
    import random as _random
    rnd = _random.Random(5)

    def pts(n):
        out = []
        for i in range(n):
            lng, lat = round(rnd.uniform(-180, 180), 3), round(rnd.uniform(-89, 89), 3)
            out.append({"_id": i, "k": i % 5, "loc": {"type": "Point", "coordinates": [lng, lat]}, "xy": [round(lng / 2, 3), round(lat / 2, 3)]})
        # exact duplicates and near-duplicates test the tie order
        out.append({"_id": 1000, "k": 1, "loc": {"type": "Point", "coordinates": [10.0, 10.0]}, "xy": [5.0, 5.0]})
        out.append({"_id": 1001, "k": 2, "loc": {"type": "Point", "coordinates": [10.0, 10.0002]}, "xy": [5.0, 5.0002]})
        out.append({"_id": 1002, "k": 1, "loc": {"type": "Point", "coordinates": [10.0, 10.0001]}, "xy": [5.0, 5.0001]})
        out.append({"_id": 1003, "k": 3, "loc": {"type": "Point", "coordinates": [179.999, 0.0]}, "xy": [90.0, 0.0]})
        out.append({"_id": 1004, "k": 3, "loc": {"type": "Point", "coordinates": [-179.999, 0.0]}, "xy": [-90.0, 0.0]})
        out.append({"_id": 1005, "k": 4, "loc": {"type": "Point", "coordinates": [0.0, 89.9]}, "xy": [0.0, 44.95]})
        out.append({"_id": 1006, "k": 4, "loc": {"type": "Point", "coordinates": [0.0, -89.9]}, "xy": [0.0, -44.95]})
        return out

    PT = pts(300)
    IX = [{"op": "create_index", "coll": "c", "keys": [["loc", "2dsphere"]]}, {"op": "create_index", "coll": "c", "keys": [["xy", "2d"]]}]

    def nq(flt, sort=None, **kw):
        return find("c", flt, projection={"_id": 1}, **({"sort": sort} if sort else {}), **kw)

    def center(lng, lat):
        return {"type": "Point", "coordinates": [lng, lat]}

    for ci, (lng, lat) in enumerate([(10, 10), (0, 0), (180, 0), (-179.9, 10), (0, 89), (100, -60), (-100, 30)]):
        steps = [ins("c", PT)] + IX
        for lim in (1, 3, 10, 40):
            steps.append(nq({"loc": {"$nearSphere": {"$geometry": center(lng, lat)}}}, limit=lim))
        steps.append(nq({"loc": {"$near": {"$geometry": center(lng, lat)}}}, limit=7, skip=3))
        steps.append(nq({"loc": {"$near": {"$geometry": center(lng, lat), "$minDistance": 500000}}}, limit=5))
        steps.append(nq({"loc": {"$nearSphere": {"$geometry": center(lng, lat), "$maxDistance": 3000000}}}, limit=20))
        steps.append(nq({"loc": {"$nearSphere": [lng, lat]}}, limit=6))
        steps.append(nq({"k": 2, "loc": {"$nearSphere": {"$geometry": center(lng, lat)}}}, limit=5))
        steps.append(nq({"loc": {"$nearSphere": {"$geometry": center(lng, lat)}}, "k": {"$in": [1, 3]}}, limit=9))
        tc(f"near_rings_sphere_{ci}", steps) if False else add(f"geo.near_rings_sphere_{ci}", steps, "geo: nearest-first rings")
        steps2 = [ins("c", PT)] + IX
        for lim in (1, 4, 25):
            steps2.append(nq({"xy": {"$near": [lng / 2, lat / 2]}}, limit=lim))
        steps2.append(nq({"xy": {"$near": [lng / 2, lat / 2], "$maxDistance": 30}}, limit=10))
        steps2.append(nq({"xy": {"$nearSphere": [lng / 2, lat / 2]}}, limit=5))
        steps2.append(nq({"k": 4, "xy": {"$near": [lng / 2, lat / 2]}}, limit=3))
        add(f"geo.near_rings_flat_{ci}", steps2, "geo: nearest-first rings")
    add("geo.near_rings_no_limit", [ins("c", PT[:80])] + IX + [nq({"loc": {"$nearSphere": {"$geometry": center(10, 10)}}}), nq({"xy": {"$near": [5, 5]}})], "geo: nearest-first rings")
    add("geo.near_rings_sort_desc", [ins("c", PT[:80])] + IX + [nq({"loc": {"$nearSphere": {"$geometry": center(10, 10)}}}, sort={"_id": -1}, limit=5)], "geo: nearest-first rings")
    add("geo.near_rings_compound_prefix", [ins("c", PT), {"op": "create_index", "coll": "c", "keys": [["k", 1], ["loc", "2dsphere"]]},
                                          nq({"k": 2, "loc": {"$nearSphere": {"$geometry": center(10, 10)}}}, limit=6), nq({"k": {"$in": [1, 2]}, "loc": {"$nearSphere": {"$geometry": center(10, 10)}}}, limit=6)], "geo: nearest-first rings")
    add("geo.near_rings_polygons_indexed", [ins("c", [{"_id": i, "loc": {"type": "Polygon", "coordinates": [[[i, i], [i + 3, i], [i + 3, i + 3], [i, i + 3], [i, i]]]}} for i in range(0, 60, 2)]),
                                           {"op": "create_index", "coll": "c", "keys": [["loc", "2dsphere"]]}, nq({"loc": {"$nearSphere": {"$geometry": center(20, 22)}}}, limit=4), nq({"loc": {"$near": {"$geometry": center(-40, -40)}}}, limit=4)], "geo: nearest-first rings")
    add("geo.near_rings_multikey_points", [ins("c", [{"_id": i, "loc": [{"type": "Point", "coordinates": [i * 3 % 170, i * 7 % 80]}, {"type": "Point", "coordinates": [-i * 3 % 170, -i * 7 % 80]}]} for i in range(40)]),
                                          {"op": "create_index", "coll": "c", "keys": [["loc", "2dsphere"]]}, nq({"loc": {"$nearSphere": {"$geometry": center(30, 30)}}}, limit=6)], "geo: nearest-first rings")

    # Documents at exactly the same distance come out of mongod's search in the pop order of a binary heap; the order is
    # reproduced for a group that is alone in its search interval.
    TIES = [{"_id": i, "loc": {"type": "Point", "coordinates": [10.0, 10.0]}, "xy": [5.0, 5.0]} for i in range(1, 8)] + [{"_id": 20 + i, "loc": {"type": "Point", "coordinates": [10.0 + i, 10.0]}, "xy": [5.0 + i, 5.0]} for i in range(1, 6)]
    for lim in (1, 3, 4, 5, 7, 9):
        add(f"geo.near_exact_ties_limit_{lim}", [ins("c", TIES)] + IX + [nq({"loc": {"$nearSphere": {"$geometry": center(10, 10)}}}, limit=lim)], "geo: nearest-first, equal distances")
        add(f"geo.near_exact_ties_flat_limit_{lim}", [ins("c", TIES)] + IX + [nq({"xy": {"$near": [5, 5]}}, limit=lim)], "geo: nearest-first, equal distances")
    add("geo.near_exact_ties_no_limit", [ins("c", TIES)] + IX + [nq({"loc": {"$nearSphere": {"$geometry": center(10, 10)}}})], "geo: nearest-first, equal distances")

    # The same place written as longitude 180 and -180.
    add("geo.near_antimeridian_twins", [ins("c", [{"_id": 1, "loc": {"type": "Point", "coordinates": [180.0, 0.0]}}, {"_id": 2, "loc": {"type": "Point", "coordinates": [-180.0, 0.0]}}, {"_id": 3, "loc": {"type": "Point", "coordinates": [100.0, 0.0]}}]),
                                       {"op": "create_index", "coll": "c", "keys": [["loc", "2dsphere"]]}, nq({"loc": {"$nearSphere": {"$geometry": center(-179.9, 10)}}}, limit=3)], "geo: nearest-first, equal distances")


def register_knife(g):
    """Knife-edge geometry: the (offset, query, operator) answers of knife_cases.py on which mongo:7.0 and our server agree
    (knife_matched.json, written by geo_knife_battery.py). The other answers are listed in results10.md."""
    import json, os
    from knife_cases import OFFS, build
    add, ins, find = g["add"], g["ins"], g["find"]
    matched = set(json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "knife_matched.json"))))
    for (ox, oy, s) in OFFS:
        stored, queries = build(ox, oy, s)
        docs = [dict(_id=i + 1, name=n, g=stored[n]) for i, n in enumerate(stored)]
        steps = [ins("c", docs)]
        for qn, geom in queries.items():
            for op in ("$geoIntersects", "$geoWithin"):
                if f"{ox},{oy},{s}|{qn}|{op}" in matched:
                    steps.append(find("c", {"g": {op: {"$geometry": geom}}}, sort={"_id": 1}, projection={"name": 1}))
        add(f"geo.knife_edge_{ox}_{oy}_{s}".replace("-", "m"), steps, "geo: knife-edge shared boundaries")
    lines = [{"_id": 1, "g": {"type": "LineString", "coordinates": [[170, 0], [-170, 0]]}}, {"_id": 2, "g": {"type": "LineString", "coordinates": [[170, 10], [-170, 10]]}},
             {"_id": 3, "g": {"type": "LineString", "coordinates": [[80, 0], [100, 0]]}}, {"_id": 4, "g": {"type": "LineString", "coordinates": [[0, 70], [0, 90]]}},
             {"_id": 5, "g": {"type": "LineString", "coordinates": [[-100, 0], [-80, 0]]}}, {"_id": 6, "g": {"type": "LineString", "coordinates": [[0, -10], [0, 10]]}},
             {"_id": 7, "g": {"type": "LineString", "coordinates": [[40, 40], [50, 50]]}}]
    pts = [(180, 0), (175, 0), (-175, 0), (-90, 0), (0, 0), (0, 5), (0, -5), (0, 75), (0, 80), (45, 45), (45, 45.0001), (175, 10), (180, 10), (170, 0), (-170, 0), (100, 0), (80, 0), (85, 0), (90, 0)]
    add("geo.knife_point_on_stored_line", [ins("c", lines)] + [find("c", {"g": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [x, y]}}}}, sort={"_id": 1}, projection={"_id": 1}) for x, y in pts],
        "geo: points on stored lines on S2 face boundaries")
