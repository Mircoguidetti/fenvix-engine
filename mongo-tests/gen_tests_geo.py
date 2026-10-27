"""Differential tests for geospatial features (results9): 2dsphere and 2d indexes, `$geoWithin` (`$geometry`, `$box`,
`$polygon`, `$center`, `$centerSphere`), `$geoIntersects`, `$near` / `$nearSphere` (+ `$maxDistance` / `$minDistance`),
the `$geoNear` aggregation stage, GeoJSON types, big polygons, boundary cases, maintenance and the error cases."""


def register(g):
    add, ins, find, agg, upd, cmd = g["add"], g["ins"], g["find"], g["agg"], g["upd"], g["cmd"]
    G = "geo"

    def t(name, steps, **flags):
        add("geo." + name, steps, G, **flags)

    def cix(coll, keys, **opts):
        step = {"op": "create_index", "coll": coll, "keys": keys}
        if opts:
            step["opts"] = opts
        return step

    def P(x, y):
        return {"type": "Point", "coordinates": [x, y]}

    def ring(*pts):
        return [list(p) for p in pts] + [list(pts[0])]

    def poly(*pts):
        return {"type": "Polygon", "coordinates": [ring(*pts)]}

    SPH = [["loc", "2dsphere"]]
    PLN = [["loc", "2d"]]
    IDS = {"_id": 1}

    def ids(flt, coll="c", **kw):
        return find(coll, flt, sort=kw.pop("sort", {"_id": 1}), projection=IDS, **kw)

    def near_ids(flt, coll="c", **kw):
        return find(coll, flt, projection=IDS, **kw)

    def gnear(spec, tail=None, coll="c"):
        return agg(coll, [{"$geoNear": spec}] + (tail or []))

    CITIES = [
        ("paris", 2.35, 48.85), ("london", -0.12, 51.5), ("rome", 12.5, 41.9), ("berlin", 13.4, 52.5),
        ("madrid", -3.7, 40.4), ("nyc", -74.0, 40.7), ("tokyo", 139.7, 35.7), ("sydney", 151.2, -33.9),
        ("pole", 0.0, 90.0), ("east", 179.9, 0.0), ("west", -179.8, 0.0), ("origin", 0.0, 0.0), ("lisbon", -9.14, 38.7),
    ]
    D = [{"_id": i + 1, "name": n, "k": (i % 3), "loc": P(x, y)} for i, (n, x, y) in enumerate(CITIES)]
    DX = D + [{"_id": 20}, {"_id": 21, "loc": None}, {"_id": 22, "loc": 5}, {"_id": 23, "loc": "x"}, {"_id": 24, "loc": {"a": 1}}]
    # legacy coordinate pairs (planar data) -- also valid in a 2dsphere index
    L = [{"_id": i + 1, "name": n, "k": i % 3, "loc": [x, y]} for i, (n, x, y) in enumerate(CITIES)]
    LX = L + [{"_id": 20}, {"_id": 21, "loc": None}, {"_id": 22, "loc": [1]}, {"_id": 23, "loc": {"x": 1, "y": 2}}, {"_id": 24, "loc": {"lng": 3, "lat": 4}}]
    GRID = [{"_id": r * 10 + c + 1, "loc": [float(c), float(r)]} for r in range(6) for c in range(6)]
    GRIDG = [{"_id": r * 10 + c + 1, "loc": P(float(c), float(r))} for r in range(6) for c in range(6)]

    BOX_FR = poly((-5, 40), (15, 40), (15, 55), (-5, 55))
    WG = lambda geom, **kw: {"loc": {"$geoWithin": {"$geometry": geom}}}  # noqa

    # ---- index specs ----------------------------------------------------------------------------------------------
    full = {"op": "list_indexes", "coll": "c", "full": True}
    t("create_2dsphere", [cix("c", SPH), ins("c", D[:2]), full])
    t("create_2dsphere_version_2", [cix("c", SPH, **{"2dsphereIndexVersion": 2}), full])
    t("create_2dsphere_version_3", [cix("c", SPH, **{"2dsphereIndexVersion": 3}), full])
    t("create_2dsphere_version_bad", [cix("c", SPH, **{"2dsphereIndexVersion": 9}), full])
    t("create_2dsphere_named", [cix("c", SPH, name="geo_ix"), full])
    t("create_2dsphere_compound_prefix", [cix("c", [["k", 1], ["loc", "2dsphere"]]), full])
    t("create_2dsphere_compound_suffix", [cix("c", [["loc", "2dsphere"], ["k", -1]]), full])
    t("create_2dsphere_two_geo_fields", [cix("c", [["loc", "2dsphere"], ["other", "2dsphere"]]), full])
    t("create_2dsphere_sparse_partial", [cix("c", SPH, sparse=True), full])
    t("create_2dsphere_partial_filter", [cix("c", SPH, partialFilterExpression={"k": 1}), full])
    t("create_2dsphere_unique", [cix("c", SPH, unique=True), ins("c", [{"_id": 1, "loc": P(1, 1)}, {"_id": 2, "loc": P(1, 1)}])])
    t("create_2dsphere_unknown_option", [cix("c", SPH, bits=20), full])
    t("create_2d", [cix("c", PLN), full])
    t("create_2d_bits_min_max", [cix("c", PLN, bits=20, min=-10, max=10), full])
    t("create_2d_bits_zero", [cix("c", PLN, bits=0), full])
    t("create_2d_bits_33", [cix("c", PLN, bits=33), full])
    t("create_2d_bits_32", [cix("c", PLN, bits=32), full])
    t("create_2d_min_ge_max", [cix("c", PLN, min=5, max=5), full])
    t("create_2d_compound_suffix", [cix("c", [["loc", "2d"], ["k", 1]]), full])
    t("create_2d_compound_prefix", [cix("c", [["k", 1], ["loc", "2d"]]), full])
    t("create_two_2d_fields", [cix("c", [["loc", "2d"], ["b", "2d"]]), full])
    t("create_2d_and_2dsphere_same_index", [cix("c", [["loc", "2d"], ["b", "2dsphere"]]), full])
    t("create_2dsphere_with_text", [cix("c", [["loc", "2dsphere"], ["t", "text"]]), full])
    t("create_2dsphere_descending_error", [cix("c", [["loc", "2dsphere"], ["k", "hashed"]]), full])
    t("create_2dsphere_same_twice", [cix("c", SPH), cix("c", SPH), full])
    t("create_2dsphere_conflict_options", [cix("c", SPH), cix("c", SPH, **{"2dsphereIndexVersion": 2}), full])
    t("create_2d_then_2dsphere_same_field", [cix("c", PLN), cix("c", SPH), full])
    t("create_2dsphere_dotted_field", [cix("c", [["a.b", "2dsphere"]]), ins("c", [{"_id": 1, "a": {"b": P(1, 1)}}]), full])
    t("drop_2dsphere_index", [cix("c", SPH), {"op": "drop_index", "coll": "c", "index": "loc_2dsphere"}, full])

    # ---- documents that the index rejects ----------------------------------------------------------------------------
    for nm, v in [
        ("lng_out_of_range", P(200, 0)), ("lat_out_of_range", P(0, 95)), ("point_three_coords", {"type": "Point", "coordinates": [1, 2, 3]}),
        ("point_one_coord", {"type": "Point", "coordinates": [1]}), ("point_string_coords", {"type": "Point", "coordinates": ["a", "b"]}),
        ("point_no_coords", {"type": "Point"}), ("unknown_type", {"type": "Circle", "coordinates": [1, 2]}),
        ("legacy_out_of_range", [200, 0]), ("legacy_three", [1, 2, 3]), ("legacy_string", ["a", "b"]),
        ("polygon_unclosed", {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}),
        ("polygon_too_few", {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 0]]]}),
        ("polygon_self_intersecting", {"type": "Polygon", "coordinates": [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]]}),
        ("polygon_empty", {"type": "Polygon", "coordinates": []}),
        ("polygon_duplicate_vertex", {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 0], [1, 1], [0, 0]]]}),
        ("line_one_point", {"type": "LineString", "coordinates": [[0, 0]]}),
        ("line_duplicate_points", {"type": "LineString", "coordinates": [[0, 0], [0, 0]]}),
        ("multipoint_empty", {"type": "MultiPoint", "coordinates": []}),
        ("collection_empty", {"type": "GeometryCollection", "geometries": []}),
        ("collection_bad_member", {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [500, 1]}]}),
        ("nan_coordinate", {"type": "Point", "coordinates": [{"$numberDouble": "NaN"}, 1]}),
        ("bool_value", True), ("empty_doc", {}), ("empty_array", []),
    ]:
        t("invalid_2dsphere_doc_" + nm, [cix("c", SPH), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "loc": v}}, ids({})])
    for nm, v in [("out_of_range", [500, 0]), ("one_coord", [1]), ("string", ["a", 1]), ("geojson_point", P(1, 2)), ("doc_pair", {"x": 1, "y": 2}), ("nan", [{"$numberDouble": "NaN"}, 0]),
                  ("three", [1, 2, 3]), ("nested", [[1, 2], [3, 4]]), ("scalar", 5), ("string_value", "x"), ("bool", False)]:
        t("invalid_2d_doc_" + nm, [cix("c", PLN), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "loc": v}}, ids({})])
    t("2dsphere_accepts_missing_null_and_odd_when_not_indexable", [cix("c", SPH), {"op": "insert_one", "coll": "c", "doc": {"_id": 1}}, {"op": "insert_one", "coll": "c", "doc": {"_id": 2, "loc": None}}, ids({})])
    t("2dsphere_insert_many_fails_midway_ordered", [cix("c", SPH), ins("c", [{"_id": 1, "loc": P(1, 1)}, {"_id": 2, "loc": P(500, 1)}, {"_id": 3, "loc": P(2, 2)}]), ids({})])
    t("2dsphere_insert_many_unordered", [cix("c", SPH), ins("c", [{"_id": 1, "loc": P(1, 1)}, {"_id": 2, "loc": P(500, 1)}, {"_id": 3, "loc": P(2, 2)}], ordered=False), ids({})])
    t("2dsphere_backfill_invalid_doc", [ins("c", [{"_id": 1, "loc": P(1, 1)}, {"_id": 2, "loc": P(500, 1)}]), cix("c", SPH), {"op": "list_indexes", "coll": "c"}])

    # ---- $geoWithin ($geometry) ------------------------------------------------------------------------------------
    for with_index in (False, True):
        sfx = "_ix" if with_index else "_scan"
        pre = [cix("c", SPH)] if with_index else []
        t("within_polygon" + sfx, pre + [ins("c", DX), ids(WG(BOX_FR))])
        t("within_polygon_legacy_data" + sfx, pre + [ins("c", LX), ids(WG(BOX_FR))])
        t("within_polygon_hole" + sfx, pre + [ins("c", GRIDG), ids({"loc": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": [ring((0.5, 0.5), (4.5, 0.5), (4.5, 4.5), (0.5, 4.5)), ring((1.5, 1.5), (1.5, 3.5), (3.5, 3.5), (3.5, 1.5))]}}}})])
        t("within_multipolygon" + sfx, pre + [ins("c", GRIDG), ids(WG({"type": "MultiPolygon", "coordinates": [[ring((-0.5, -0.5), (1.5, -0.5), (1.5, 1.5), (-0.5, 1.5))], [ring((3.5, 3.5), (5.5, 3.5), (5.5, 5.5), (3.5, 5.5))]]}))])
        t("within_boundary_points" + sfx, pre + [ins("c", GRIDG), ids(WG(poly((1, 1), (3, 1), (3, 3), (1, 3))))])
        t("within_vertex_points" + sfx, pre + [ins("c", GRIDG), ids(WG(poly((0, 0), (5, 0), (5, 5), (0, 5))))])
        t("within_triangle_edge" + sfx, pre + [ins("c", GRIDG), ids(WG(poly((0, 0), (5, 0), (0, 5))))])
        t("within_world_ish" + sfx, pre + [ins("c", DX), ids(WG(poly((-80, -70), (80, -70), (80, 70), (-80, 70))))])
        t("within_antimeridian" + sfx, pre + [ins("c", DX), ids(WG(poly((170, -45), (-170, -45), (-170, 5), (170, 5))))])
        t("within_pole_polygon" + sfx, pre + [ins("c", DX), ids(WG(poly((-180, 85), (-90, 85), (0, 85), (90, 85), (180, 85), (90, 86), (0, 86), (-90, 86), (-180, 86))))])
        t("within_empty_result" + sfx, pre + [ins("c", DX), ids(WG(poly((100, -10), (101, -10), (101, -9), (100, -9))))])
        t("within_missing_and_mixed_fields" + sfx, pre + [ins("c", LX + [{"_id": 30, "loc": [[2, 48], [100, 80]]}, {"_id": 31, "loc": {"type": "Point", "coordinates": [2, 48]}}]), ids(WG(BOX_FR))])
    t("within_line_data", [cix("c", SPH), ins("c", [
        {"_id": 1, "loc": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}}, {"_id": 2, "loc": {"type": "LineString", "coordinates": [[0, 0], [10, 10]]}},
        {"_id": 3, "loc": {"type": "MultiPoint", "coordinates": [[1, 1], [2, 2]]}}, {"_id": 4, "loc": {"type": "MultiPoint", "coordinates": [[1, 1], [20, 2]]}},
        {"_id": 5, "loc": poly((0.2, 0.2), (3, 0.2), (3, 3), (0.2, 3))}, {"_id": 6, "loc": poly((0.2, 0.2), (30, 0.2), (30, 3), (0.2, 3))},
        {"_id": 7, "loc": {"type": "GeometryCollection", "geometries": [P(1, 1), {"type": "LineString", "coordinates": [[2, 2], [3, 3]]}]}},
        {"_id": 8, "loc": {"type": "GeometryCollection", "geometries": [P(1, 1), P(50, 50)]}},
        {"_id": 9, "loc": {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]}},
        {"_id": 10, "loc": {"type": "MultiPolygon", "coordinates": [[ring((0, 0), (1, 0), (1, 1), (0, 1))], [ring((2, 2), (3, 2), (3, 3), (2, 3))]]}},
    ]), ids(WG(poly((-1, -1), (4, -1), (4, 4), (-1, 4))))])
    t("within_geometry_must_be_polygon", [cix("c", SPH), ins("c", D), ids(WG(P(2, 48)))])
    t("within_geometry_linestring_error", [ins("c", D), ids(WG({"type": "LineString", "coordinates": [[0, 0], [1, 1]]}))])
    t("within_geometry_missing", [ins("c", D), ids({"loc": {"$geoWithin": {}}})])
    t("within_unknown_region", [ins("c", D), ids({"loc": {"$geoWithin": {"$circle": [[0, 0], 1]}}})])
    t("within_not_a_document", [ins("c", D), ids({"loc": {"$geoWithin": 5}})])
    t("within_two_regions_error", [ins("c", D), ids({"loc": {"$geoWithin": {"$geometry": BOX_FR, "$box": [[0, 0], [1, 1]]}}})])
    t("within_polygon_unclosed_error", [ins("c", D), ids(WG({"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}))])
    t("within_polygon_lat_out_of_range_error", [ins("c", D), ids(WG(poly((0, 0), (1, 0), (1, 100))))])
    t("within_polygon_self_intersect_error", [ins("c", D), ids(WG({"type": "Polygon", "coordinates": [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]]}))])
    t("within_not_operator", [ins("c", DX), ids({"loc": {"$not": {"$geoWithin": {"$geometry": BOX_FR}}}})])
    t("within_in_or", [ins("c", DX), ids({"$or": [WG(BOX_FR), {"name": "tokyo"}]})])
    t("within_in_nor", [ins("c", DX), ids({"$nor": [WG(BOX_FR)]})])
    t("within_with_other_condition", [cix("c", SPH), ins("c", DX), ids({"k": 1, **WG(BOX_FR)})])
    t("within_dotted_path", [cix("c", [["a.b", "2dsphere"]]), ins("c", [{"_id": i + 1, "a": {"b": P(x, y)}} for i, (n, x, y) in enumerate(CITIES)]), ids({"a.b": {"$geoWithin": {"$geometry": BOX_FR}}})])
    t("within_array_of_points", [cix("c", SPH), ins("c", [
        {"_id": 1, "loc": [P(2, 48), P(100, 10)]}, {"_id": 2, "loc": [P(2, 48), P(3, 49)]}, {"_id": 3, "loc": [P(100, 10)]}, {"_id": 4, "loc": []},
    ]), ids(WG(BOX_FR))])
    t("within_array_of_legacy_pairs", [ins("c", [
        {"_id": 1, "loc": [[2, 48], [100, 10]]}, {"_id": 2, "loc": [[2, 48], [3, 49]]}, {"_id": 3, "loc": [[100, 10]]},
    ]), ids({"loc": {"$geoWithin": {"$box": [[0, 40], [10, 50]]}}})])
    t("within_unique_docs_option", [ins("c", [{"_id": 1, "loc": [[2, 48], [3, 49]]}]), ids({"loc": {"$geoWithin": {"$box": [[0, 40], [10, 50]], "$uniqueDocs": True}}})])

    # big polygons (strictwinding CRS)
    EAST = [(-150, -30), (-90, -30), (-30, -30), (30, -30), (90, -30), (150, -30)]  # a ring around the south: east = interior to the north
    STRICT = {"type": "name", "properties": {"name": "urn:x-mongodb:crs:strictwinding:EPSG:4326"}}
    BIG = {"type": "Polygon", "coordinates": [ring(*EAST)], "crs": STRICT}
    BIG_CW = {"type": "Polygon", "coordinates": [ring(*reversed(EAST))], "crs": STRICT}
    NOCRS_CW = {"type": "Polygon", "coordinates": [ring(*reversed(EAST))]}
    NOCRS_CCW = {"type": "Polygon", "coordinates": [ring(*EAST)]}
    for nm, shape in [("ccw", BIG), ("cw", BIG_CW), ("no_crs_cw", NOCRS_CW), ("no_crs_ccw", NOCRS_CCW)]:
        t("big_polygon_query_" + nm, [ins("c", DX), ids(WG(shape))])
        t("big_polygon_query_indexed_" + nm, [cix("c", SPH), ins("c", DX), ids(WG(shape))])
    t("big_polygon_stored_geointersects", [cix("c", SPH), ins("c", [{"_id": 1, "area": BIG}, {"_id": 2, "area": BIG_CW}]), ids({"area": {"$geoIntersects": {"$geometry": P(-80, 0)}}}),
                                           ids({"area": {"$geoIntersects": {"$geometry": P(100, 0)}}}), ids({"area": {"$geoIntersects": {"$geometry": P(0, 80)}}})])
    t("big_polygon_stored_within_error", [cix("c", [["area", "2dsphere"]]), ins("c", [{"_id": 1, "area": BIG}])])
    t("big_polygon_bad_crs", [ins("c", D), ids(WG({**BIG, "crs": {"type": "name", "properties": {"name": "EPSG:9999"}}}))])
    t("big_polygon_crs_wrong_type", [ins("c", D), ids(WG({**BIG, "crs": {"type": "link", "properties": {"name": "x"}}}))])
    t("big_polygon_crs_epsg4326", [ins("c", DX), ids(WG({**NOCRS_CW, "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}}))])
    t("big_polygon_crs_on_point_error", [ins("c", D), ids(WG({**P(1, 1), "crs": {"type": "name", "properties": {"name": "urn:x-mongodb:crs:strictwinding:EPSG:4326"}}}))])
    t("large_polygon_without_hemisphere_check", [ins("c", DX), ids(WG(poly((-179, -89), (179, -89), (179, 89), (-179, 89))))])
    t("polygon_many_vertices", [cix("c", SPH), ins("c", DX), ids(WG({"type": "Polygon", "coordinates": [[[round(10 * __import__("math").cos(a / 20 * 2 * 3.141592653589793), 6), round(45 + 10 * __import__("math").sin(a / 20 * 2 * 3.141592653589793), 6)] for a in range(20)] + [[10.0, 45.0]]]}))])

    # ---- legacy shapes -------------------------------------------------------------------------------------------------
    for with_index in ("", "2d", "2dsphere"):
        sfx = "_" + (with_index or "scan")
        pre = [cix("c", PLN if with_index == "2d" else SPH)] if with_index else []
        t("box" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [[1, 1], [3, 3]]}}})])
        t("box_swapped_corners" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [[3, 3], [1, 1]]}}})])
        t("box_degenerate" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [[2, 2], [2, 2]]}}})])
        t("box_flat" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [[0, 2], [5, 2]]}}})])
        t("center" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$center": [[2, 2], 1.5]}}})])
        t("center_boundary" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$center": [[2, 2], 1]}}})])
        t("center_zero_radius" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$center": [[2, 2], 0]}}})])
        t("polygon" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [4, 0], [4, 4], [0, 4]]}}})])
        t("polygon_triangle" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [5, 0], [0, 5]]}}})])
        t("polygon_concave" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [5, 0], [5, 5], [3, 2], [0, 5]]}}})])
        t("polygon_unclosed_vs_closed" + sfx, pre + [ins("c", GRID), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]]}}})])
        t("center_sphere" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 0.1]}}})])
        t("center_sphere_geojson_data" + sfx, pre + [ins("c", D), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 0.1]}}})])
        t("center_sphere_zero" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 0]}}})])
        t("center_sphere_big" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 3.2]}}})])
        t("center_sphere_pole" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[0, 90], 0.8]}}})])
        t("center_sphere_antimeridian" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[179.9, 0], 0.01]}}})])
        t("center_sphere_km" + sfx, pre + [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 500 / 6378.1]}}})])
        t("center_sphere_missing_fields" + sfx, pre + [ins("c", LX), ids({"loc": {"$geoWithin": {"$centerSphere": [[2.35, 48.85], 3.2]}}})])
    t("box_on_geojson_points", [ins("c", D), ids({"loc": {"$geoWithin": {"$box": [[0, 40], [15, 55]]}}})])
    t("box_on_geojson_points_indexed", [cix("c", SPH), ins("c", D), ids({"loc": {"$geoWithin": {"$box": [[0, 40], [15, 55]]}}})])
    t("center_on_geojson_points", [ins("c", D), ids({"loc": {"$geoWithin": {"$center": [[2, 48], 12]}}})])
    t("box_not_array", [ins("c", L), ids({"loc": {"$geoWithin": {"$box": 5}}})])
    t("box_one_corner", [ins("c", L), ids({"loc": {"$geoWithin": {"$box": [[0, 0]]}}})])
    t("box_three_corners", [ins("c", L), ids({"loc": {"$geoWithin": {"$box": [[0, 0], [1, 1], [2, 2]]}}})])
    t("box_non_numeric", [ins("c", L), ids({"loc": {"$geoWithin": {"$box": [["a", 0], [1, 1]]}}})])
    t("center_negative_radius", [ins("c", L), ids({"loc": {"$geoWithin": {"$center": [[0, 0], -1]}}})])
    t("center_missing_radius", [ins("c", L), ids({"loc": {"$geoWithin": {"$center": [[0, 0]]}}})])
    t("center_string_radius", [ins("c", L), ids({"loc": {"$geoWithin": {"$center": [[0, 0], "a"]}}})])
    t("center_sphere_negative_radius", [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[0, 0], -1]}}})])
    t("center_sphere_bad_center", [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[400, 0], 1]}}})])
    t("center_sphere_missing_radius", [ins("c", L), ids({"loc": {"$geoWithin": {"$centerSphere": [[0, 0]]}}})])
    t("polygon_two_points", [ins("c", L), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [1, 1]]}}})])
    t("polygon_not_array", [ins("c", L), ids({"loc": {"$geoWithin": {"$polygon": 5}}})])
    t("polygon_bad_point", [ins("c", L), ids({"loc": {"$geoWithin": {"$polygon": [[0, 0], [1, 1], "a"]}}})])
    t("within_alias_$within", [ins("c", GRID), ids({"loc": {"$within": {"$box": [[1, 1], [3, 3]]}}})])
    t("box_with_point_as_document", [ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [{"x": 1, "y": 1}, {"x": 3, "y": 3}]}}})])
    t("box_float_precision", [ins("c", [{"_id": 1, "loc": [0.1 + 0.2, 1]}, {"_id": 2, "loc": [0.3, 1]}]), ids({"loc": {"$geoWithin": {"$box": [[0, 0], [0.3, 2]]}}})])
    t("center_2d_index_wrap_range", [cix("c", PLN, min=-10, max=10), ins("c", [{"_id": 1, "loc": [9, 9]}, {"_id": 2, "loc": [-9, 9]}, {"_id": 3, "loc": [0, 0]}]), ids({"loc": {"$geoWithin": {"$center": [[9.5, 9.5], 2]}}})])

    # ---- $geoIntersects ------------------------------------------------------------------------------------------------
    SH = [
        {"_id": 1, "loc": P(1, 1)}, {"_id": 2, "loc": {"type": "LineString", "coordinates": [[0, 0], [1.9, 1.9]]}},
        {"_id": 3, "loc": poly((0, 0), (2, 0), (2, 2), (0, 2))}, {"_id": 4, "loc": poly((5, 5), (6, 5), (6, 6), (5, 6))},
        {"_id": 5, "loc": {"type": "MultiPoint", "coordinates": [[1, 1], [9, 9.5]]}}, {"_id": 6, "loc": {"type": "LineString", "coordinates": [[3, 0], [3, 5]]}},
        {"_id": 7, "loc": {"type": "MultiPolygon", "coordinates": [[ring((0, 0), (1, 0), (1, 1), (0, 1))], [ring((8, 8), (9, 8), (9, 9), (8, 9))]]}},
        {"_id": 8, "loc": {"type": "GeometryCollection", "geometries": [P(1, 1), P(8.5, 8.5)]}}, {"_id": 9}, {"_id": 10, "loc": [1, 1]},
        {"_id": 11, "loc": poly((0, 0), (2.5, 0), (2.5, 2.5), (0, 2.5))}, {"_id": 12, "loc": poly((0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5))},
    ]
    # the same shapes with distinct nearest points, for ordering by distance (equal distances order arbitrarily)
    NSH = [dict(d) for d in SH]
    NSH[0] = {"_id": 1, "loc": P(1, 1)}
    NSH[4] = {"_id": 5, "loc": {"type": "MultiPoint", "coordinates": [[1.0002, 1], [9, 9.5]]}}
    NSH[7] = {"_id": 8, "loc": {"type": "GeometryCollection", "geometries": [P(1, 1.0006), P(8.5, 8.5)]}}
    NSH[9] = {"_id": 10, "loc": [1, 1.0008]}
    NSH[6] = {"_id": 7, "loc": {"type": "MultiPolygon", "coordinates": [[ring((0, 0), (1, 0), (1.001, 1.001), (0, 1))], [ring((8, 8), (9, 8), (9, 9), (8, 9))]]}}
    GI = lambda geom: {"loc": {"$geoIntersects": {"$geometry": geom}}}  # noqa
    for with_index in (False, True):
        sfx = "_ix" if with_index else "_scan"
        pre = [cix("c", SPH)] if with_index else []
        t("intersects_point" + sfx, pre + [ins("c", SH), ids(GI(P(1, 1)))])
        t("intersects_point_on_edge" + sfx, pre + [ins("c", SH), ids(GI(P(1, 0)))])
        t("intersects_point_on_vertex" + sfx, pre + [ins("c", SH), ids(GI(P(2, 2)))])
        t("intersects_line" + sfx, pre + [ins("c", SH), ids(GI({"type": "LineString", "coordinates": [[0, 2], [2, 0]]}))])
        t("intersects_line_crossing" + sfx, pre + [ins("c", SH), ids(GI({"type": "LineString", "coordinates": [[-1, 1], [4, 1]]}))])
        t("intersects_line_outside" + sfx, pre + [ins("c", SH), ids(GI({"type": "LineString", "coordinates": [[20, 20], [21, 21]]}))])
        t("intersects_polygon" + sfx, pre + [ins("c", SH), ids(GI(poly((1, 1), (4, 1), (4, 4), (1, 4))))])
        t("intersects_polygon_contains_all" + sfx, pre + [ins("c", SH), ids(GI(poly((-1, -1), (20, -1), (20, 20), (-1, 20))))])
        t("intersects_polygon_inside_stored" + sfx, pre + [ins("c", SH), ids(GI(poly((0.8, 0.8), (1.2, 0.8), (1.2, 1.2), (0.8, 1.2))))])
        t("intersects_polygon_touching_edge" + sfx, pre + [ins("c", SH), ids(GI(poly((2, 0), (3, 0), (3, 2), (2, 2))))])
        t("intersects_polygon_hole" + sfx, pre + [ins("c", SH), ids(GI({"type": "Polygon", "coordinates": [ring((-1, -1), (4, -1), (4, 4), (-1, 4)), ring((0.2, 0.2), (0.2, 1.8), (1.8, 1.8), (1.8, 0.2))]}))])
        t("intersects_multipoint" + sfx, pre + [ins("c", SH), ids(GI({"type": "MultiPoint", "coordinates": [[1, 1], [8.5, 8.5]]}))])
        t("intersects_multipolygon" + sfx, pre + [ins("c", SH), ids(GI({"type": "MultiPolygon", "coordinates": [[ring((5.5, 5.5), (7, 5.5), (7, 7), (5.5, 7))], [ring((8.5, 8.5), (10, 8.5), (10, 10), (8.5, 10))]]}))])
        t("intersects_collection" + sfx, pre + [ins("c", SH), ids(GI({"type": "GeometryCollection", "geometries": [P(1, 1), P(5.5, 5.5)]}))])
    t("intersects_big_polygon_query", [cix("c", SPH), ins("c", DX), ids(GI(BIG))])
    t("intersects_big_polygon_query_cw", [ins("c", DX), ids(GI(BIG_CW))])
    t("intersects_legacy_arg_error", [ins("c", SH), ids({"loc": {"$geoIntersects": {"$box": [[0, 0], [1, 1]]}}})])
    t("intersects_no_geometry_error", [ins("c", SH), ids({"loc": {"$geoIntersects": {}}})])
    t("intersects_not_document_error", [ins("c", SH), ids({"loc": {"$geoIntersects": 1}})])
    t("intersects_bad_geometry_error", [ins("c", SH), ids(GI({"type": "Point", "coordinates": [1]}))])
    t("intersects_unknown_type_error", [ins("c", SH), ids(GI({"type": "Foo", "coordinates": [1, 2]}))])
    t("intersects_in_or", [ins("c", SH), ids({"$or": [GI(P(1, 1)), {"_id": 4}]})])
    t("intersects_not", [ins("c", SH), ids({"loc": {"$not": {"$geoIntersects": {"$geometry": P(1, 1)}}}})])
    t("intersects_with_array_field", [cix("c", SPH), ins("c", [{"_id": 1, "loc": [P(1, 1), P(50, 50)]}, {"_id": 2, "loc": [P(7, 7)]}]), ids(GI(poly((0, 0), (3, 0), (3, 3), (0, 3))))])
    t("intersects_long_edge_great_circle", [cix("c", SPH), ins("c", [{"_id": 1, "loc": {"type": "LineString", "coordinates": [[-80, 0], [80, 60]]}}]), ids(GI(P(0, 40))), ids(GI(P(0, 30.7)))])

    # ---- $near / $nearSphere -------------------------------------------------------------------------------------------
    NG = lambda x, y, **kw: {"loc": {"$near": {"$geometry": P(x, y), **kw}}}  # noqa
    NS = lambda x, y, **kw: {"loc": {"$nearSphere": {"$geometry": P(x, y), **kw}}}  # noqa
    t("near_geojson", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85))])
    t("near_geojson_max", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": 1000000}))])
    t("near_geojson_min", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$minDistance": 1000000}))])
    t("near_geojson_min_max", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$minDistance": 500000, "$maxDistance": 1500000}))])
    t("near_geojson_max_zero", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": 0}))])
    t("near_geojson_max_exact", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": 343923.6}))])
    t("near_geojson_outside_query", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": P(2.35, 48.85)}, "$maxDistance": 1000000}})])
    t("near_geojson_min_outside_query", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": P(2.35, 48.85)}, "$minDistance": 1000000}})])
    t("near_sphere_geojson", [cix("c", SPH), ins("c", DX), near_ids(NS(2.35, 48.85))])
    t("near_sphere_geojson_max", [cix("c", SPH), ins("c", DX), near_ids(NS(2.35, 48.85, **{"$maxDistance": 1500000}))])
    t("near_legacy_on_2dsphere_error", [cix("c", SPH), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85]}})])
    t("near_sphere_legacy_on_2dsphere", [cix("c", SPH), ins("c", L), near_ids({"loc": {"$nearSphere": [2.35, 48.85]}})])
    t("near_sphere_legacy_max_radians", [cix("c", SPH), ins("c", L), near_ids({"loc": {"$nearSphere": [2.35, 48.85], "$maxDistance": 0.1}})])
    t("near_sphere_legacy_min_radians", [cix("c", SPH), ins("c", L), near_ids({"loc": {"$nearSphere": [2.35, 48.85], "$minDistance": 0.1}})])
    t("near_sphere_legacy_on_2d", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$nearSphere": [2.35, 48.85]}})])
    t("near_sphere_legacy_max_on_2d", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$nearSphere": [2.35, 48.85], "$maxDistance": 0.2}})])
    t("near_2d", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85]}})])
    t("near_2d_max", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85], "$maxDistance": 12}})])
    t("near_2d_min", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85], "$minDistance": 12}})])
    t("near_2d_min_max", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85], "$minDistance": 5, "$maxDistance": 20}})])
    t("near_2d_geometry_error", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": {"$geometry": P(2, 48)}}})])
    t("near_2d_document_point", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": {"x": 2.35, "y": 48.85}}})])
    t("near_2d_grid", [cix("c", PLN), ins("c", GRID), near_ids({"loc": {"$near": [2.37, 2.61]}}, limit=12)])
    t("near_2d_grid_limit_skip", [cix("c", PLN), ins("c", GRID), near_ids({"loc": {"$near": [2.37, 2.61]}}, limit=5, skip=3)])
    t("near_2d_default_limit_100", [cix("c", PLN), ins("c", [{"_id": i, "loc": [i * 0.01, 0]} for i in range(1, 151)]), {"op": "find_count", "coll": "c", "filter": {"loc": {"$near": [0, 0]}}}])
    t("near_2d_big_distance_all", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [0, 0], "$maxDistance": 1000}})])
    t("near_2d_with_filter", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [2.35, 48.85]}, "k": 1})])
    t("near_2d_with_filter_and_max", [cix("c", PLN), ins("c", L), near_ids({"k": {"$ne": 0}, "loc": {"$near": [2.35, 48.85], "$maxDistance": 30}})])
    t("near_2d_custom_range", [cix("c", PLN, min=-1000, max=1000), ins("c", [{"_id": 1, "loc": [500, 500]}, {"_id": 2, "loc": [-400, -400]}, {"_id": 3, "loc": [900, 0]}]), near_ids({"loc": {"$near": [0, 0]}})])
    t("near_2d_point_outside_range", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [500, 500]}})])
    t("near_2d_on_sphere_data_geojson", [cix("c", PLN), ins("c", D)])
    t("near_with_sort_override", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85), sort={"_id": -1})])
    t("near_with_sort_by_field", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85), sort={"k": 1, "_id": 1})])
    t("near_limit", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85), limit=3)])
    t("near_skip_limit", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85), skip=2, limit=3)])
    t("near_projection_meta_distance", [cix("c", SPH), ins("c", DX), find("c", NG(2.35, 48.85, **{"$maxDistance": 2000000}), projection={"d": {"$meta": "geoNearDistance"}}, limit=4)])
    t("near_projection_meta_point", [cix("c", SPH), ins("c", DX), find("c", NG(2.35, 48.85), projection={"p": {"$meta": "geoNearPoint"}, "_id": 0}, limit=3)])
    t("near_sort_by_meta_distance", [cix("c", SPH), ins("c", DX), find("c", NG(2.35, 48.85), sort={"d": {"$meta": "geoNearDistance"}}, projection={"_id": 1, "d": {"$meta": "geoNearDistance"}}, limit=5)])
    t("meta_distance_without_near_error", [cix("c", SPH), ins("c", DX), find("c", {}, projection={"d": {"$meta": "geoNearDistance"}})])
    t("near_with_other_filter", [cix("c", SPH), ins("c", DX), near_ids({"k": 1, **NG(2.35, 48.85)})])
    t("near_in_and", [cix("c", SPH), ins("c", DX), near_ids({"$and": [{"k": 1}, NG(2.35, 48.85)]})])
    t("near_with_name_filter_regex", [cix("c", SPH), ins("c", DX), near_ids({"name": {"$regex": "^[a-m]"}, **NG(2.35, 48.85)})])
    t("near_in_or_error", [cix("c", SPH), ins("c", DX), near_ids({"$or": [NG(2.35, 48.85), {"k": 1}]})])
    t("near_in_nor_error", [cix("c", SPH), ins("c", DX), near_ids({"$nor": [NG(2.35, 48.85)]})])
    t("near_in_not_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$not": {"$near": {"$geometry": P(0, 0)}}}})])
    t("near_in_elem_match_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$elemMatch": {"$near": {"$geometry": P(0, 0)}}}})])
    t("near_without_index_error", [ins("c", DX), near_ids(NG(2.35, 48.85))])
    t("near_sphere_without_index_error", [ins("c", DX), near_ids(NS(2.35, 48.85))])
    t("near_wrong_field_index_error", [cix("c", [["other", "2dsphere"]]), ins("c", DX), near_ids(NG(2.35, 48.85))])
    t("near_after_drop_index_error", [cix("c", SPH), ins("c", DX), {"op": "drop_index", "coll": "c", "index": "loc_2dsphere"}, near_ids(NG(2.35, 48.85))])
    t("near_two_near_error", [cix("c", SPH), ins("c", DX), near_ids({"$and": [NG(2.35, 48.85), NG(0, 0)]})])
    t("near_and_nearsphere_same_field_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": P(0, 0)}, "$nearSphere": {"$geometry": P(1, 1)}}})])
    t("near_with_within_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": P(0, 0)}, "$geoWithin": {"$geometry": BOX_FR}}})])
    t("near_with_text_error", [cix("c", SPH), cix("c", [["name", "text"]]), ins("c", DX), near_ids({"$text": {"$search": "paris"}, **NG(0, 0)})])
    t("near_count_documents_error", [cix("c", SPH), ins("c", DX), {"op": "count_documents", "coll": "c", "filter": NG(2.35, 48.85)}])
    t("near_count_command", [cix("c", SPH), ins("c", DX), cmd({"count": "c", "query": NG(2.35, 48.85, **{"$maxDistance": 1000000})}, pick=["n", "ok"])])
    t("near_distinct", [cix("c", SPH), ins("c", DX), {"op": "distinct", "coll": "c", "key": "name", "filter": NG(2.35, 48.85, **{"$maxDistance": 1000000})}])
    t("near_in_aggregate_match_error", [cix("c", SPH), ins("c", DX), agg("c", [{"$match": NG(2.35, 48.85)}])])
    t("near_in_lookup_pipeline_error", [cix("c", SPH), ins("c", DX), agg("c", [{"$limit": 1}, {"$lookup": {"from": "c", "pipeline": [{"$match": NG(0, 0)}], "as": "x"}}])])
    t("near_update_many_filter", [cix("c", SPH), ins("c", DX), {"op": "update_many", "coll": "c", "filter": NG(2.35, 48.85, **{"$maxDistance": 1000000}), "update": {"$set": {"hit": 1}}}, ids({"hit": 1})])
    t("near_delete_many_filter", [cix("c", SPH), ins("c", DX), {"op": "delete_many", "coll": "c", "filter": NG(2.35, 48.85, **{"$maxDistance": 1000000})}, ids({})])
    t("near_find_one_and_update", [cix("c", SPH), ins("c", DX), {"op": "find_one_and_update", "coll": "c", "filter": NG(2.35, 48.85), "update": {"$set": {"hit": 1}}, "projection": {"name": 1}}])
    t("near_find_one", [cix("c", SPH), ins("c", DX), {"op": "find_one", "coll": "c", "filter": NG(2.35, 48.85), "projection": {"name": 1}}])
    t("near_hint_index", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85), hint="loc_2dsphere", limit=3)])
    t("near_hint_other_index", [cix("c", SPH), cix("c", [["k", 1]]), ins("c", DX), near_ids(NG(2.35, 48.85), hint="k_1", limit=3)])
    t("near_max_negative_error", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": -1}))])
    t("near_min_negative_error", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$minDistance": -1}))])
    t("near_max_string_error", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": "a"}))])
    t("near_min_gt_max", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$minDistance": 100, "$maxDistance": 10}))])
    t("near_bad_geometry_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [1]}}}})])
    t("near_geometry_polygon_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$geometry": BOX_FR}}})])
    t("near_no_geometry_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": {"$maxDistance": 1}}})])
    t("near_scalar_arg_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": 5}})])
    t("near_string_arg_error", [cix("c", SPH), ins("c", DX), near_ids({"loc": {"$near": "x"}})])
    t("near_array_one_elem_error", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": [1]}})])
    t("near_array_non_numeric_error", [cix("c", PLN), ins("c", L), near_ids({"loc": {"$near": ["a", "b"]}})])
    t("near_legacy_out_of_range_2dsphere", [cix("c", SPH), ins("c", L), near_ids({"loc": {"$nearSphere": [500, 500]}})])
    t("near_point_at_pole", [cix("c", SPH), ins("c", DX), near_ids(NG(0, 90), limit=4)])
    t("near_point_antimeridian", [cix("c", SPH), ins("c", DX), near_ids(NG(179.9, 0), limit=4)])
    t("near_same_point_as_query", [cix("c", SPH), ins("c", DX), near_ids(NG(2.35, 48.85, **{"$maxDistance": 0}))])
    t("near_empty_collection", [cix("c", SPH), near_ids(NG(0, 0))])
    t("near_all_docs_without_loc", [cix("c", SPH), ins("c", [{"_id": 1}, {"_id": 2, "loc": None}]), near_ids(NG(0, 0))])
    t("near_compound_prefix_equality", [cix("c", [["k", 1], ["loc", "2dsphere"]]), ins("c", DX), near_ids({"k": 1, **NG(2.35, 48.85)})])
    t("near_compound_prefix_missing", [cix("c", [["k", 1], ["loc", "2dsphere"]]), ins("c", DX), near_ids(NG(2.35, 48.85))])
    t("near_compound_suffix", [cix("c", [["loc", "2dsphere"], ["k", 1]]), ins("c", DX), near_ids({"k": 2, **NG(2.35, 48.85)})])
    t("near_compound_2d_suffix", [cix("c", [["loc", "2d"], ["k", 1]]), ins("c", L), near_ids({"k": 2, "loc": {"$near": [2.35, 48.85]}})])
    t("near_2dsphere_with_partial_filter", [cix("c", SPH, partialFilterExpression={"k": 1}), ins("c", DX), near_ids({"k": 1, **NG(2.35, 48.85)})])
    t("near_2dsphere_sparse", [cix("c", SPH, sparse=True), ins("c", DX), near_ids(NG(2.35, 48.85), limit=3)])
    t("near_multikey_points", [cix("c", SPH), ins("c", [{"_id": 1, "loc": [P(0, 0), P(50, 50)]}, {"_id": 2, "loc": [P(10, 10)]}, {"_id": 3, "loc": [P(49, 49), P(60, 60)]}]), find("c", NG(50, 50), projection={"d": {"$meta": "geoNearDistance"}})])
    t("near_shapes", [cix("c", SPH), ins("c", NSH[:-1]), find("c", NG(1.01, 1.02), projection={"d": {"$meta": "geoNearDistance"}})])
    t("near_shapes_far", [cix("c", SPH), ins("c", NSH[:-1]), near_ids(NG(40, 40.2))])
    t("near_polygon_data_inside", [cix("c", SPH), ins("c", [{"_id": 1, "loc": poly((0, 0), (4, 0), (4, 4), (0, 4))}, {"_id": 2, "loc": P(5, 5)}]), find("c", NG(2, 2), projection={"d": {"$meta": "geoNearDistance"}})])
    t("near_dotted_path", [cix("c", [["a.b", "2dsphere"]]), ins("c", [{"_id": i + 1, "a": {"b": P(x, y)}} for i, (n, x, y) in enumerate(CITIES)]), near_ids({"a.b": {"$near": {"$geometry": P(2.35, 48.85)}}}, limit=4)])
    t("near_dotted_array_path", [cix("c", [["a.b", "2dsphere"]]), ins("c", [{"_id": 1, "a": [{"b": P(0, 0)}, {"b": P(30, 30)}]}, {"_id": 2, "a": [{"b": P(31, 31)}]}]), near_ids({"a.b": {"$near": {"$geometry": P(30, 30)}}})])
    t("near_after_update", [cix("c", SPH), ins("c", DX), upd("c", {"_id": 1}, {"$set": {"loc": P(100, 10)}}), near_ids(NG(2.35, 48.85), limit=3)])
    t("near_after_unset", [cix("c", SPH), ins("c", DX), upd("c", {"_id": 1}, {"$unset": {"loc": ""}}), near_ids(NG(2.35, 48.85), limit=3)])
    t("near_after_delete", [cix("c", SPH), ins("c", DX), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, near_ids(NG(2.35, 48.85), limit=3)])
    t("near_after_upsert", [cix("c", SPH), ins("c", DX), upd("c", {"_id": 100}, {"$set": {"loc": P(2.36, 48.86)}}, upsert=True), near_ids(NG(2.35, 48.85), limit=3)])
    t("near_backfill", [ins("c", DX), cix("c", SPH), near_ids(NG(2.35, 48.85), limit=4)])
    t("near_replace_doc", [cix("c", SPH), ins("c", DX), {"op": "replace_one", "coll": "c", "filter": {"_id": 1}, "update": {"name": "x", "loc": P(-120, 30)}}, near_ids(NG(-120, 30), limit=2)])
    t("near_large_set", [cix("c", SPH), ins("c", [{"_id": i, "loc": P(((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25)} for i in range(1, 301)]), near_ids(NG(0, 0), limit=15), near_ids(NG(0, 0, **{"$maxDistance": 3000000, "$minDistance": 1000000}), limit=200), {"op": "find_count", "coll": "c", "filter": NG(10, 10, **{"$maxDistance": 5000000})}])
    t("near_large_set_2d", [cix("c", PLN), ins("c", [{"_id": i, "loc": [((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25]} for i in range(1, 301)]), near_ids({"loc": {"$near": [0, 0]}}, limit=15), near_ids({"loc": {"$near": [0, 0], "$maxDistance": 60, "$minDistance": 20}}, limit=200)])

    # ---- $geoNear stage ----------------------------------------------------------------------------------------------
    PR = {"$project": {"_id": 1, "d": 1}}
    t("geonear_basic", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d"}, [PR])])
    t("geonear_spherical_flag", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d", "spherical": True}, [PR])])
    t("geonear_max_min", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d", "maxDistance": 1500000, "minDistance": 500000}, [PR])])
    t("geonear_query", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d", "query": {"k": 1}}, [PR])])
    t("geonear_distance_multiplier", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d", "distanceMultiplier": 0.001}, [PR, {"$limit": 4}])])
    t("geonear_include_locs", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d", "includeLocs": "l"}, [{"$project": {"_id": 1, "l": 1}}, {"$limit": 3}])])
    t("geonear_nested_fields", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "info.dist", "includeLocs": "info.loc"}, [{"$project": {"_id": 1, "info": 1}}, {"$limit": 3}])])
    t("geonear_distance_field_overwrites", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "name"}, [{"$project": {"_id": 1, "name": 1}}, {"$limit": 3}])])
    t("geonear_limit_stage", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d"}, [{"$limit": 3}, PR])])
    t("geonear_then_group", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d"}, [{"$group": {"_id": "$k", "n": {"$sum": 1}, "m": {"$min": "$d"}}}, {"$sort": {"_id": 1}}])])
    t("geonear_then_sort", [cix("c", SPH), ins("c", DX), gnear({"near": P(2.35, 48.85), "distanceField": "d"}, [{"$sort": {"name": 1}}, PR])])
    t("geonear_full_documents", [cix("c", SPH), ins("c", D[:4]), gnear({"near": P(2.35, 48.85), "distanceField": "d"})])
    t("geonear_legacy_spherical", [cix("c", SPH), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d", "spherical": True}, [PR, {"$limit": 4}])])
    t("geonear_legacy_spherical_max", [cix("c", SPH), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d", "spherical": True, "maxDistance": 0.1}, [PR])])
    t("geonear_legacy_on_2dsphere_error", [cix("c", SPH), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d"}, [PR])])
    t("geonear_2d_planar", [cix("c", PLN), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d"}, [PR, {"$limit": 5}])])
    t("geonear_2d_max_min", [cix("c", PLN), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d", "maxDistance": 20, "minDistance": 5}, [PR])])
    t("geonear_2d_spherical", [cix("c", PLN), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d", "spherical": True}, [PR, {"$limit": 5}])])
    t("geonear_2d_geojson_near_error", [cix("c", PLN), ins("c", L), gnear({"near": P(2.35, 48.85), "distanceField": "d"}, [PR])])
    t("geonear_2d_query", [cix("c", PLN), ins("c", L), gnear({"near": [2.35, 48.85], "distanceField": "d", "query": {"k": 2}}, [PR])])
    t("geonear_key", [cix("c", [["a", "2dsphere"]]), cix("c", [["b", "2dsphere"]]), ins("c", [{"_id": 1, "a": P(0, 0), "b": P(50, 50)}, {"_id": 2, "a": P(40, 40), "b": P(1, 1)}]), gnear({"near": P(0, 0), "distanceField": "d", "key": "b"}, [PR])])
    t("geonear_two_indexes_no_key_error", [cix("c", [["a", "2dsphere"]]), cix("c", [["b", "2dsphere"]]), ins("c", [{"_id": 1, "a": P(0, 0), "b": P(50, 50)}]), gnear({"near": P(0, 0), "distanceField": "d"}, [PR])])
    t("geonear_key_unknown_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "key": "nope"}, [PR])])
    t("geonear_key_not_string_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "key": 5}, [PR])])
    t("geonear_no_index_error", [ins("c", D), gnear({"near": P(0, 0), "distanceField": "d"}, [PR])])
    t("geonear_missing_collection_error", [gnear({"near": P(0, 0), "distanceField": "d"}, [PR], coll="nonexistent")])
    t("geonear_empty_result", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "maxDistance": 1}, [PR])])
    t("geonear_missing_near_error", [cix("c", SPH), ins("c", D), gnear({"distanceField": "d"})])
    t("geonear_missing_distance_field_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0)})])
    t("geonear_distance_field_not_string_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": 5})])
    t("geonear_near_bad_type_error", [cix("c", SPH), ins("c", D), gnear({"near": "x", "distanceField": "d"})])
    t("geonear_near_bad_coords_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 100), "distanceField": "d"})])
    t("geonear_limit_option_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "limit": 2})])
    t("geonear_num_option_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "num": 2})])
    t("geonear_negative_max_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "maxDistance": -1})])
    t("geonear_negative_min_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "minDistance": -1})])
    t("geonear_query_not_document_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "query": 5})])
    t("geonear_query_with_near_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "query": NG(1, 1)})])
    t("geonear_unknown_option_error", [cix("c", SPH), ins("c", D), gnear({"near": P(0, 0), "distanceField": "d", "bogus": 1})])
    t("geonear_not_first_error", [cix("c", SPH), ins("c", D), agg("c", [{"$match": {"k": 1}}, {"$geoNear": {"near": P(0, 0), "distanceField": "d"}}])])
    t("geonear_after_empty_match", [cix("c", SPH), ins("c", D), agg("c", [{"$match": {}}, {"$geoNear": {"near": P(0, 0), "distanceField": "d"}}, PR, {"$limit": 2}])])
    t("geonear_not_an_object_error", [cix("c", SPH), ins("c", D), agg("c", [{"$geoNear": 5}])])
    t("geonear_in_lookup_pipeline", [cix("c", SPH), ins("c", D[:3]), agg("c", [{"$limit": 1}, {"$lookup": {"from": "c", "pipeline": [{"$geoNear": {"near": P(0, 0), "distanceField": "d"}}, PR, {"$limit": 2}], "as": "x"}}, {"$project": {"x": 1}}])])
    t("geonear_in_facet_error", [cix("c", SPH), ins("c", D), agg("c", [{"$facet": {"a": [{"$geoNear": {"near": P(0, 0), "distanceField": "d"}}]}}])])
    t("geonear_with_hint", [cix("c", SPH), ins("c", D), agg("c", [{"$geoNear": {"near": P(0, 0), "distanceField": "d"}}, PR], hint="loc_2dsphere")])
    t("geonear_shapes_distance", [cix("c", SPH), ins("c", NSH[:-1]), gnear({"near": P(1.01, 1.02), "distanceField": "d"}, [PR])])
    t("geonear_multikey_locs", [cix("c", SPH), ins("c", [{"_id": 1, "loc": [P(0, 0), P(50, 50)]}, {"_id": 2, "loc": [P(10, 10)]}]), gnear({"near": P(50, 50), "distanceField": "d", "includeLocs": "l"}, [{"$project": {"_id": 1, "d": 1, "l": 1}}])])
    t("geonear_large", [cix("c", SPH), ins("c", [{"_id": i, "k": i % 7, "loc": P(((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25)} for i in range(1, 301)]), gnear({"near": P(0, 0), "distanceField": "d", "query": {"k": {"$in": [1, 2]}}, "maxDistance": 8000000}, [{"$limit": 20}, PR])])

    # ---- 2d index maintenance and edge cases ----------------------------------------------------------------------------
    t("2d_within_box_indexed_matches_scan", [cix("c", PLN), ins("c", GRID), ids({"loc": {"$geoWithin": {"$box": [[1, 1], [3, 3]]}}}), {"op": "find_count", "coll": "c", "filter": {"loc": {"$geoWithin": {"$center": [[2, 2], 2]}}}}])
    t("2d_array_values", [cix("c", PLN), ins("c", [{"_id": 1, "loc": [[1, 1], [8, 8]]}, {"_id": 2, "loc": [[3, 3]]}]), ids({"loc": {"$geoWithin": {"$box": [[0, 0], [2, 2]]}}}), near_ids({"loc": {"$near": [8, 8]}})])
    t("2d_document_points", [cix("c", PLN), ins("c", [{"_id": 1, "loc": {"x": 1, "y": 2}}, {"_id": 2, "loc": {"lng": 5, "lat": 6}}, {"_id": 3, "loc": {"a": 0, "b": 0}}]), near_ids({"loc": {"$near": [1, 2]}})])
    t("2d_update_loc", [cix("c", PLN), ins("c", GRID), upd("c", {"_id": 1}, {"$set": {"loc": [5, 5]}}), near_ids({"loc": {"$near": [5, 5]}}, limit=3)])
    t("2d_update_invalid_error", [cix("c", PLN), ins("c", GRID), upd("c", {"_id": 1}, {"$set": {"loc": [500, 5]}}), ids({"_id": 1})])
    t("2dsphere_update_invalid_error", [cix("c", SPH), ins("c", D), upd("c", {"_id": 1}, {"$set": {"loc": P(500, 5)}}), ids({"_id": 1}), near_ids(NG(0, 0), limit=2)])
    t("2dsphere_update_to_other_type", [cix("c", SPH), ins("c", D), upd("c", {"_id": 1}, {"$set": {"loc": poly((0, 0), (4, 0), (4, 4), (0, 4))}}), near_ids(NG(2, 2), limit=2)])
    t("2dsphere_bulk_update_set", [cix("c", SPH), ins("c", D), {"op": "update_many", "coll": "c", "filter": {}, "update": {"$set": {"z": 1}}}, near_ids(NG(2.35, 48.85), limit=2)])
    t("2dsphere_inc_other_field_keeps_index", [cix("c", SPH), ins("c", D), {"op": "update_many", "coll": "c", "filter": {"k": 1}, "update": {"$inc": {"n": 1}}}, near_ids(NG(2.35, 48.85), limit=3)])
    t("2dsphere_partial_filter_membership_changes", [cix("c", SPH, partialFilterExpression={"k": 1}), ins("c", DX), upd("c", {"_id": 1}, {"$set": {"k": 1}}), near_ids({"k": 1, **NG(2.35, 48.85)})])
    t("2dsphere_multi_collections", [cix("c", SPH), cix("d", SPH), ins("c", D[:3]), ins("d", D[3:6]), near_ids(NG(2.35, 48.85), coll="c"), near_ids(NG(2.35, 48.85), coll="d")])
    t("2dsphere_drop_collection_recreate", [cix("c", SPH), ins("c", D), {"op": "drop_collection", "coll": "c"}, ins("c", D[:2]), near_ids(NG(2.35, 48.85))])
    t("2dsphere_find_by_index_name_hint_wrong_name", [cix("c", SPH), ins("c", D), ids({}, hint="nope")])
    t("2dsphere_explain_free_count", [cix("c", SPH), ins("c", D), {"op": "count_documents", "coll": "c", "filter": WG(BOX_FR)}, {"op": "count_documents", "coll": "c", "filter": GI(P(2.35, 48.85))}])
    t("2dsphere_unique_ignored_for_geo_key", [cix("c", [["k", 1], ["loc", "2dsphere"]], unique=True), ins("c", [{"_id": 1, "k": 1, "loc": P(1, 1)}, {"_id": 2, "k": 1, "loc": P(2, 2)}, {"_id": 3, "k": 1, "loc": P(1, 1)}], ordered=False)])
    t("2dsphere_ttl_with_geo", [cix("c", [["loc", "2dsphere"]]), cix("c", [["at", 1]], expireAfterSeconds=3600), ins("c", [{"_id": 1, "loc": P(1, 1), "at": {"$nowOffsetMs": 0}}]), ids({})])

    # numeric edge cases in coordinates
    t("coords_int_double_mixed", [cix("c", SPH), ins("c", [{"_id": 1, "loc": {"type": "Point", "coordinates": [1, 2.5]}}, {"_id": 2, "loc": {"type": "Point", "coordinates": [{"$numberLong": "3"}, {"$numberDecimal": "4.5"}]}}]), near_ids(NG(2, 3))])
    t("coords_extreme", [cix("c", SPH), ins("c", [{"_id": 1, "loc": P(180, 89)}, {"_id": 2, "loc": P(-180, -80)}, {"_id": 3, "loc": P(0, 0)}]), near_ids(NG(0, 0))])
    t("coords_just_outside", [cix("c", SPH), {"op": "insert_one", "coll": "c", "doc": {"_id": 1, "loc": P(180.0000001, 0)}}, ids({})])
    t("coords_tiny_distances", [cix("c", SPH), ins("c", [{"_id": 1, "loc": P(0.000001, 0)}, {"_id": 2, "loc": P(0.0000001, 0)}, {"_id": 3, "loc": P(0, 0)}]), find("c", NG(0, 0), projection={"d": {"$meta": "geoNearDistance"}})])
    t("geometry_collection_stored_query", [cix("c", SPH), ins("c", [{"_id": 1, "loc": {"type": "GeometryCollection", "geometries": [P(1, 1), P(40, 40)]}}]), near_ids(NG(40, 40)), ids(WG(poly((39, 39), (41, 39), (41, 41), (39, 41))))])
    t("multipoint_near", [cix("c", SPH), ins("c", [{"_id": 1, "loc": {"type": "MultiPoint", "coordinates": [[0, 0], [30, 30]]}}, {"_id": 2, "loc": P(10, 10)}]), find("c", NG(30, 30), projection={"d": {"$meta": "geoNearDistance"}})])
    t("linestring_near", [cix("c", SPH), ins("c", [{"_id": 1, "loc": {"type": "LineString", "coordinates": [[0, 0], [10, 0]]}}, {"_id": 2, "loc": P(5, 1.2)}]), find("c", NG(5, 0.5), projection={"d": {"$meta": "geoNearDistance"}})])
    t("many_cities_within_polygon_index_vs_scan", [cix("c", SPH), ins("c", [{"_id": i, "loc": P(((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25)} for i in range(1, 401)]), {"op": "find_count", "coll": "c", "filter": WG(poly((-60, -40), (60, -40), (60, 40), (-60, 40)))}, {"op": "find_count", "coll": "c", "filter": WG(BIG)}, {"op": "find_count", "coll": "c", "filter": {"loc": {"$geoWithin": {"$centerSphere": [[0, 0], 1.0]}}}}, {"op": "find_count", "coll": "c", "filter": GI(poly((-10, -10), (10, -10), (10, 10), (-10, 10)))}])
    t("many_points_within_polygon_ids", [cix("c", SPH), ins("c", [{"_id": i, "loc": P(((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25)} for i in range(1, 401)]), ids(WG(poly((-60, -40), (60, -40), (60, 40), (-60, 40))))])
    t("many_points_2d_box_ids", [cix("c", PLN), ins("c", [{"_id": i, "loc": [((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25]} for i in range(1, 401)]), ids({"loc": {"$geoWithin": {"$box": [[-60, -40], [60, 40]]}}}), ids({"loc": {"$geoWithin": {"$center": [[0, 0], 50]}}}), ids({"loc": {"$geoWithin": {"$polygon": [[-60, -40], [60, -40], [0, 60]]}}})])
    t("many_points_geointersects_ids", [cix("c", SPH), ins("c", [{"_id": i, "loc": P(((i * 37) % 360) - 180 + 0.5, ((i * 53) % 170) - 85 + 0.25)} for i in range(1, 401)]), ids(GI(poly((-60, -40), (60, -40), (60, 40), (-60, 40)))), ids(GI({"type": "LineString", "coordinates": [[-170, -80], [170, 80]]}))])
