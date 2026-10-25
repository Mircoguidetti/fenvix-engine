#!/usr/bin/env python3
"""Generates tests.json: the operation scripts that both differential harnesses (pymongo and the
Node.js driver) run, unchanged, against real MongoDB and against mongo-server.

Values are written as Extended JSON where the BSON type matters: int64 as {"$numberLong"},
integral doubles as {"$numberDouble"}, dates as {"$date"}, ObjectIds as {"$oid"}. Plain JSON
integers are int32 and plain non-integral numbers are doubles in both drivers.
"""
import datetime
import json
import sys

from bson import Binary, Int64, ObjectId


class Dbl(float):
    """A double that must be sent as a double even if integral."""


def tag(x):
    if isinstance(x, dict):
        return {k: tag(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [tag(v) for v in x]
    if isinstance(x, Int64):
        return {"$numberLong": str(int(x))}
    if isinstance(x, Dbl):
        return {"$numberDouble": repr(float(x))}
    if isinstance(x, ObjectId):
        return {"$oid": str(x)}
    if isinstance(x, datetime.datetime):
        ms = int(x.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
        return {"$date": {"$numberLong": str(ms)}}
    if isinstance(x, Binary):
        import base64
        return {"$binary": {"base64": base64.b64encode(bytes(x)).decode(), "subType": "%02x" % x.subtype}}
    if isinstance(x, float):
        return x if x != int(x) else {"$numberDouble": repr(x)}
    return x


TESTS = []


def add(name, steps, group, category="core", **flags):
    TESTS.append({"name": name, "group": group, "category": category, "steps": [tag(s) for s in steps], **flags})


def ins(coll, docs, ordered=True):
    return {"op": "insert_many", "coll": coll, "docs": docs, "ordered": ordered}


def find(coll, filter=None, **kw):
    return {"op": "find", "coll": coll, "filter": filter or {}, **kw}


def agg(coll, pipeline, **kw):
    return {"op": "aggregate", "coll": coll, "pipeline": pipeline, **kw}


def upd(coll, filter, update, **kw):
    return {"op": kw.pop("kind", "update_one"), "coll": coll, "filter": filter, "update": update, **kw}


def cmd(command, pick=None, **kw):
    return {"op": "command", "cmd": command, "pick": pick, **kw}


# ---------------------------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------------------------
PEOPLE = [
    {"_id": 1, "name": "alice", "age": 30, "city": "Paris", "tags": ["a", "b"], "addr": {"zip": "75001", "geo": {"lat": 48.8}}, "scores": [10, 20, 30]},
    {"_id": 2, "name": "bob", "age": 25, "city": "Rome", "tags": ["b", "c"], "addr": {"zip": "00100"}, "scores": [5]},
    {"_id": 3, "name": "carol", "age": 35, "city": "Paris", "tags": [], "addr": None, "scores": []},
    {"_id": 4, "name": "dave", "age": None, "city": "Berlin", "tags": ["a"], "scores": [1, 2, 3, 4]},
    {"_id": 5, "name": "eve", "city": "Rome", "tags": ["c", "d", "a"], "addr": {"zip": "00100", "geo": {"lat": 41.9}}},
    {"_id": 6, "name": "frank", "age": "forty", "city": None, "tags": "a"},
    {"_id": 7, "name": "grace", "age": 25.5, "city": "Paris", "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 5}]},
    {"_id": 8, "name": "heidi", "age": Int64(30), "city": "paris", "items": [{"sku": "x", "qty": 10}]},
]
ORDERS = [
    {"_id": 1, "cust": "a", "amt": 10, "qty": 1},
    {"_id": 2, "cust": "a", "amt": 20.5, "qty": 2},
    {"_id": 3, "cust": "b", "amt": 5, "qty": 1},
    {"_id": 4, "cust": "b", "amt": Int64(7), "qty": 3},
    {"_id": 5, "cust": "c", "qty": 2},
    {"_id": 6, "cust": None, "amt": 1},
]
NUMS = [{"_id": i, "n": i, "g": i % 3, "s": f"s{i}"} for i in range(1, 11)]


def people_test(name, steps_after, group, **flags):
    add(name, [ins("people", PEOPLE)] + steps_after, group, **flags)


# ---------------------------------------------------------------------------------------------
# admin / handshake
# ---------------------------------------------------------------------------------------------
G = "handshake and admin"
add("admin.ping", [cmd({"ping": 1}, ["ok"])], G)
add("admin.hello", [{"op": "admin_command", "cmd": {"hello": 1}, "pick": ["isWritablePrimary", "maxBsonObjectSize", "maxMessageSizeBytes", "maxWriteBatchSize", "logicalSessionTimeoutMinutes", "minWireVersion", "maxWireVersion", "readOnly", "ok"]}], G)
add("admin.ismaster", [{"op": "admin_command", "cmd": {"isMaster": 1}, "pick": ["ismaster", "maxBsonObjectSize", "maxWireVersion", "ok"]}], G)
add("admin.buildinfo", [{"op": "admin_command", "cmd": {"buildInfo": 1}, "pick": ["ok", "bits", "maxBsonObjectSize"]}], G)
add("admin.getparameter_fcv", [{"op": "admin_command", "cmd": {"getParameter": 1, "featureCompatibilityVersion": 1}, "pick": ["featureCompatibilityVersion", "ok"]}], G)
add("admin.unknown_command", [cmd({"frobnicate": 1}, None, cmp_msg=True)], G)
add("admin.list_databases", [ins("c", [{"_id": 1}]), {"op": "db_in_list"}, {"op": "db_in_list_command"}], G)
add("admin.list_databases_namesonly_shape", [ins("c", [{"_id": 1}]), {"op": "admin_command", "cmd": {"listDatabases": 1, "nameOnly": True}, "pick": ["ok"]}], G)
add("admin.session_insert", [{"op": "session_insert", "coll": "c"}], G)
add("admin.server_api_client", [{"op": "ping_with_server_api"}], G)
add("admin.empty_db_reads", [find("nothing"), {"op": "count_documents", "coll": "nothing", "filter": {}}, {"op": "list_collection_names"}, agg("nothing", [{"$match": {}}])], G)

# ---------------------------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------------------------
G = "insert"
for label, _id in [("int", 1), ("string", "abc"), ("float", 1.5), ("double_integral", Dbl(7.0)), ("int64", Int64(5)), ("date", datetime.datetime(2024, 1, 2, 3, 4, 5)), ("bool", True), ("doc", {"a": 1, "b": "x"}), ("null", None)]:
    add(f"insert.one_id_{label}", [{"op": "insert_one", "coll": "c", "doc": {"_id": _id, "v": 1}}, find("c")], G)
add("insert.one_id_objectid", [{"op": "insert_one", "coll": "c", "doc": {"_id": ObjectId("65a1b2c3d4e5f60718293a4b"), "v": 1}}, find("c")], G, keep_oids=True)
add("insert.one_no_id_generated", [{"op": "insert_one", "coll": "c", "doc": {"v": 1}}, find("c"), {"op": "find_one", "coll": "c", "filter": {"v": 1}, "projection": {"_id": 0}}], G)
add("insert.id_first_when_generated", [{"op": "insert_one", "coll": "c", "doc": {"z": 1, "a": 2}}, {"op": "find_one", "coll": "c", "filter": {}, "keys_only": True}], G)
add("insert.id_moved_first_when_supplied_last", [{"op": "insert_one", "coll": "c", "doc": {"z": 1, "_id": 5, "a": 2}}, {"op": "find_one", "coll": "c", "filter": {}, "keys_only": True}, {"op": "insert_many", "coll": "c", "docs": [{"x": 1, "_id": 6}, {"y": 2, "_id": 7}]}, find("c", sort={"_id": 1})], G)
add("insert.id_numeric_equivalence", [{"op": "insert_one", "coll": "c", "doc": {"_id": 1}}, {"op": "insert_one", "coll": "c", "doc": {"_id": Dbl(1.0)}, "cmp_msg": True}, {"op": "insert_one", "coll": "c", "doc": {"_id": Int64(1)}, "cmp_msg": True}, find("c")], G)
add("insert.many_ordered", [ins("c", [{"_id": i} for i in range(1, 6)]), find("c", sort={"_id": 1})], G)
add("insert.many_unordered", [ins("c", [{"_id": i} for i in range(1, 6)], ordered=False), find("c", sort={"_id": 1})], G)
add("insert.many_generated_ids", [{"op": "insert_many", "coll": "c", "docs": [{"v": 1}, {"v": 2}, {"v": 3}], "ordered": True}, find("c", sort={"v": 1})], G)
add("insert.dup_id_int", [ins("c", [{"_id": 1}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1}, "cmp_msg": True}], G)
add("insert.dup_id_string", [ins("c", [{"_id": "abc"}]), {"op": "insert_one", "coll": "c", "doc": {"_id": "abc"}, "cmp_msg": True}], G)
add("insert.dup_id_objectid", [ins("c", [{"_id": ObjectId("65a1b2c3d4e5f60718293a4b")}]), {"op": "insert_one", "coll": "c", "doc": {"_id": ObjectId("65a1b2c3d4e5f60718293a4b")}, "cmp_msg": True}], G, keep_oids=True)
add("insert.dup_id_double", [ins("c", [{"_id": 1.5}]), {"op": "insert_one", "coll": "c", "doc": {"_id": 1.5}, "cmp_msg": True}], G)
add("insert.dup_id_document", [ins("c", [{"_id": {"a": 1}}]), {"op": "insert_one", "coll": "c", "doc": {"_id": {"a": 1}}, "cmp_msg": True}], G)
add("insert.dup_in_batch_ordered", [{"op": "insert_many", "coll": "c", "docs": [{"_id": 1}, {"_id": 2}, {"_id": 2}, {"_id": 3}], "ordered": True, "cmp_msg": True}, find("c", sort={"_id": 1})], G)
add("insert.dup_in_batch_unordered", [{"op": "insert_many", "coll": "c", "docs": [{"_id": 1}, {"_id": 2}, {"_id": 2}, {"_id": 3}, {"_id": 1}], "ordered": False, "cmp_msg": True}, find("c", sort={"_id": 1})], G)
add("insert.dup_against_existing_batch", [ins("c", [{"_id": 1}, {"_id": 2}]), {"op": "insert_many", "coll": "c", "docs": [{"_id": 0}, {"_id": 2}, {"_id": 9}], "ordered": False, "cmp_msg": True}, find("c", sort={"_id": 1})], G)
add("insert.array_id_rejected", [{"op": "insert_one", "coll": "c", "doc": {"_id": [1, 2]}}], G)
add("insert.1000_docs", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, {"op": "count_documents", "coll": "big", "filter": {}}, find("big", sort={"_id": 1}, limit=3), find("big", sort={"_id": -1}, limit=3), agg("big", [{"$group": {"_id": "$grp", "n": {"$sum": 1}, "s": {"$sum": "$n"}}}, {"$sort": {"_id": 1}}]), find("big", {"n": {"$gte": 990}}, sort={"n": 1}), find("big", {"_id": 777})], G)
add("insert.1000_docs_unordered", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic", "ordered": False}, {"op": "count_documents", "coll": "big", "filter": {"tag": "x"}}], G)
add("insert.types_roundtrip", [{"op": "insert_one", "coll": "t", "doc": {
    "_id": 1, "i32": 7, "i64": Int64(1 << 40), "dbl": 2.5, "dbl_int": Dbl(3.0), "neg": -5, "str": "héllo ✓ 日本", "empty": "", "t": True, "f": False, "nul": None,
    "date": datetime.datetime(2020, 2, 29, 12, 0, 0), "bin": Binary(b"\x00\x01\x02\xff"), "oid": ObjectId("65a1b2c3d4e5f60718293a4b"),
    "doc": {"a": {"b": {"c": [1, {"d": 2}]}}}, "arr": [1, "two", 3.5, None, [4, 5], {"six": 6}], "emptyDoc": {}, "emptyArr": [], "max64": Int64(9223372036854775807), "min64": Int64(-9223372036854775808)}}, find("t")], G, keep_oids=True)
add("insert.big_document", [{"op": "insert_one", "coll": "c", "doc": {"_id": 1, "blob": "x" * 1_000_000}}, {"op": "find_one", "coll": "c", "filter": {"_id": 1}, "projection": {"blob": 0}}, {"op": "count_documents", "coll": "c", "filter": {"blob": {"$exists": True}}}], G)
add("insert.field_order_preserved", [{"op": "insert_one", "coll": "c", "doc": {"_id": 1, "z": 1, "a": 2, "m": {"y": 1, "b": 2}}}, find("c")], G)
add("insert.unique_single", [{"op": "create_index", "coll": "u", "keys": [["email", 1]], "unique": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 1, "email": "a@x"}}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2, "email": "a@x"}, "cmp_msg": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 3, "email": "b@x"}}, find("u", sort={"_id": 1})], G)
add("insert.unique_compound", [{"op": "create_index", "coll": "u", "keys": [["a", 1], ["b", -1]], "unique": True}, ins("u", [{"_id": 1, "a": 1, "b": 1}, {"_id": 2, "a": 1, "b": 2}]), {"op": "insert_one", "coll": "u", "doc": {"_id": 3, "a": 1, "b": 2}, "cmp_msg": True}, find("u", sort={"_id": 1})], G)
add("insert.unique_missing_field_is_null", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 1}}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2}, "cmp_msg": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 3, "k": None}, "cmp_msg": True}], G)
add("insert.unique_in_batch", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, {"op": "insert_many", "coll": "u", "docs": [{"_id": 1, "k": 1}, {"_id": 2, "k": 2}, {"_id": 3, "k": 1}], "ordered": False, "cmp_msg": True}, find("u", sort={"_id": 1})], G)
add("insert.unique_numeric_equivalence", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 1, "k": 1}}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2, "k": Dbl(1.0)}, "cmp_msg": True}], G)
add("insert.unique_on_existing_dups_fails", [ins("u", [{"_id": 1, "k": 1}, {"_id": 2, "k": 1}]), {"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, {"op": "list_indexes", "coll": "u"}], G)
add("insert.unique_nested_path", [{"op": "create_index", "coll": "u", "keys": [["a.b", 1]], "unique": True}, {"op": "insert_one", "coll": "u", "doc": {"_id": 1, "a": {"b": 1}}}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2, "a": {"b": 1}}, "cmp_msg": True}], G)

# ---------------------------------------------------------------------------------------------
# find: filters (one test per filter)
# ---------------------------------------------------------------------------------------------
FILTERS = [
    ("empty", {}), ("eq_number", {"age": 30}), ("eq_int64_equiv", {"age": Int64(30)}), ("eq_double_equiv", {"age": Dbl(30.0)}),
    ("eq_double_fraction", {"age": 25.5}), ("eq_string", {"city": "Paris"}), ("eq_case_sensitive", {"city": "paris"}),
    ("eq_null", {"age": None}), ("eq_null_missing_field", {"nosuch": None}), ("eq_nested_doc", {"addr": {"zip": "00100"}}),
    ("eq_nested_doc_extra_field", {"addr": {"zip": "00100", "geo": {"lat": 41.9}}}), ("eq_array_whole", {"tags": ["a", "b"]}),
    ("eq_array_whole_wrong_order", {"tags": ["b", "a"]}), ("eq_array_element", {"tags": "a"}), ("eq_empty_array", {"tags": []}),
    ("eq_array_index", {"scores.0": 10}), ("eq_dotted", {"addr.zip": "00100"}), ("eq_dotted_deep", {"addr.geo.lat": 48.8}),
    ("eq_dotted_missing_parent", {"addr.zip": "75001"}), ("eq_dotted_array_of_docs", {"items.sku": "y"}), ("eq_dotted_array_of_docs_qty", {"items.qty": 5}),
    ("eq_dotted_array_index_doc", {"items.1.sku": "y"}), ("eq_id", {"_id": 3}), ("eq_id_missing", {"_id": 99}),
    ("op_eq", {"age": {"$eq": 25}}), ("op_eq_null", {"age": {"$eq": None}}), ("op_ne", {"age": {"$ne": 25}}), ("op_ne_null", {"age": {"$ne": None}}),
    ("op_ne_array_element", {"tags": {"$ne": "a"}}),
    ("op_gt_number", {"age": {"$gt": 25}}), ("op_gte_number", {"age": {"$gte": 25.5}}), ("op_lt_number", {"age": {"$lt": 30}}), ("op_lte_number", {"age": {"$lte": 30}}),
    ("op_gt_string", {"city": {"$gt": "Paris"}}), ("op_lt_string", {"city": {"$lt": "Paris"}}), ("op_range", {"age": {"$gt": 25, "$lt": 35}}),
    ("op_gt_on_array", {"scores": {"$gt": 25}}), ("op_lt_on_array", {"scores": {"$lt": 2}}), ("op_range_on_array_same_element", {"scores": {"$gt": 10, "$lt": 20}}),
    ("op_gt_null_nothing", {"age": {"$gt": None}}), ("op_gte_null", {"age": {"$gte": None}}), ("op_lt_string_vs_number_bracket", {"age": {"$lt": "zzz"}}),
    ("op_gt_dotted", {"addr.geo.lat": {"$gt": 45}}), ("op_gt_array_of_docs", {"items.qty": {"$gt": 5}}),
    ("op_in_numbers", {"age": {"$in": [25, 30]}}), ("op_in_with_null", {"age": {"$in": [None, 30]}}), ("op_in_strings", {"city": {"$in": ["Rome", "Berlin"]}}),
    ("op_in_array_field", {"tags": {"$in": ["c"]}}), ("op_in_empty", {"age": {"$in": []}}), ("op_in_numeric_types", {"age": {"$in": [Int64(25), Dbl(30.0)]}}),
    ("op_in_id", {"_id": {"$in": [1, 3, 99]}}),
    ("op_nin_numbers", {"age": {"$nin": [25, 30]}}), ("op_nin_includes_missing", {"age": {"$nin": [99]}}), ("op_nin_array_field", {"tags": {"$nin": ["a"]}}),
    ("op_exists_true", {"age": {"$exists": True}}), ("op_exists_false", {"age": {"$exists": False}}), ("op_exists_nested", {"addr.geo": {"$exists": True}}),
    ("op_exists_array_index", {"scores.2": {"$exists": True}}), ("op_exists_zero_int", {"age": {"$exists": 0}}),
    ("op_and_explicit", {"$and": [{"age": {"$gt": 20}}, {"city": "Paris"}]}), ("op_and_implicit", {"age": {"$gt": 20}, "city": "Paris"}),
    ("op_or", {"$or": [{"age": 25}, {"city": "Berlin"}]}), ("op_or_nested_and", {"$and": [{"$or": [{"age": 25}, {"age": 30}]}, {"city": {"$ne": "Rome"}}]}),
    ("op_nor", {"$nor": [{"age": 25}, {"city": "Paris"}]}),
    ("op_not_gt", {"age": {"$not": {"$gt": 25}}}), ("op_not_in", {"city": {"$not": {"$in": ["Paris"]}}}), ("op_not_exists", {"age": {"$not": {"$exists": True}}}),
    ("op_not_eq", {"age": {"$not": {"$eq": 25}}}), ("op_not_range", {"age": {"$not": {"$gt": 20, "$lt": 28}}}),
    ("multi_field_dotted_and_array", {"addr.zip": "00100", "tags": "c"}), ("mixed_types_eq_string_number", {"age": "forty"}),
]
for label, f in FILTERS:
    people_test(f"find.filter.{label}", [find("people", f)], "find: filters and operators", unordered=True)
add("find.filter.on_missing_collection", [find("nope", {"a": 1})], "find: filters and operators")
add("find.filter.id_types", [ins("c", [{"_id": 1}, {"_id": "1"}, {"_id": 1.0 + 0.5}, {"_id": True}, {"_id": None}]), find("c", {"_id": 1}), find("c", {"_id": "1"}), find("c", {"_id": 1.5}), find("c", {"_id": True}), find("c", {"_id": None})], "find: filters and operators")
add("find.filter.invalid_operator", [ins("c", [{"_id": 1}]), find("c", {"a": {"$frob": 1}})], "find: filters and operators")
add("find.filter.invalid_top_level_operator", [ins("c", [{"_id": 1}]), find("c", {"$frob": [1]})], "find: filters and operators")
add("find.filter.and_not_array", [ins("c", [{"_id": 1}]), find("c", {"$and": {"a": 1}})], "find: filters and operators")
add("find.filter.cross_type_comparison_order", [ins("c", [{"_id": 1, "v": None}, {"_id": 2, "v": 5}, {"_id": 3, "v": "s"}, {"_id": 4, "v": {"a": 1}}, {"_id": 5, "v": [1]}, {"_id": 6, "v": True}, {"_id": 7, "v": datetime.datetime(2024, 1, 1)}, {"_id": 8, "v": ObjectId("65a1b2c3d4e5f60718293a4b")}]), find("c", sort={"v": 1, "_id": 1}), find("c", sort={"v": -1, "_id": 1})], "find: filters and operators", keep_oids=True)

# extras: operators outside the supported set (expected to differ)
for label, f in [("regex", {"name": {"$regex": "^a"}}), ("elem_match", {"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}}), ("size", {"tags": {"$size": 2}}), ("all", {"tags": {"$all": ["a", "b"]}}), ("type", {"age": {"$type": "string"}}), ("mod", {"age": {"$mod": [5, 0]}}), ("expr", {"$expr": {"$gt": ["$age", 26]}})]:
    people_test(f"find.extra.{label}", [find("people", f)], "find: operators outside the supported set", category="extra", unordered=True)

# ---------------------------------------------------------------------------------------------
# find: projection, sort, skip/limit, cursors, count, distinct
# ---------------------------------------------------------------------------------------------
G = "find: projection"
for label, p in [("include", {"name": 1, "age": 1}), ("include_no_id", {"name": 1, "_id": 0}), ("exclude", {"tags": 0, "scores": 0, "addr": 0, "items": 0}), ("exclude_id_only", {"_id": 0}), ("include_nested", {"addr.zip": 1}), ("include_nested_deep", {"addr.geo.lat": 1}), ("exclude_nested", {"addr.geo": 0}), ("include_array_of_docs_field", {"items.sku": 1}), ("exclude_array_of_docs_field", {"items.qty": 0}), ("include_missing_field", {"nosuch": 1}), ("include_true_bool", {"name": True}), ("exclude_false_bool", {"tags": False, "scores": False, "addr": False, "items": False}), ("id_only_inclusion", {"_id": 1}), ("mixed_is_error", {"name": 1, "age": 0})]:
    people_test(f"find.projection.{label}", [find("people", {}, projection=p, sort={"_id": 1})], G)

G = "find: sort, skip, limit"
for label, s in [("asc", {"age": 1}), ("desc", {"age": -1}), ("string", {"name": -1}), ("multi", {"city": 1, "age": -1, "_id": 1}), ("mixed_types", {"age": 1, "_id": 1}), ("array_asc", {"scores": 1, "_id": 1}), ("array_desc", {"scores": -1, "_id": 1}), ("tags_array", {"tags": 1, "_id": 1}), ("nested", {"addr.zip": 1, "_id": 1}), ("missing_field", {"nosuch": 1, "_id": 1}), ("id_desc", {"_id": -1}), ("nested_geo_desc", {"addr.geo.lat": -1, "_id": 1})]:
    people_test(f"find.sort.{label}", [find("people", {}, sort=s)], G)
people_test("find.skip_limit", [find("people", {}, sort={"_id": 1}, skip=2, limit=3), find("people", {}, sort={"_id": 1}, skip=6), find("people", {}, sort={"_id": 1}, skip=100), find("people", {}, sort={"_id": 1}, limit=1), find("people", {}, sort={"_id": -1}, skip=1, limit=2)], G)
people_test("find.limit_without_sort_count", [{"op": "find_count", "coll": "people", "filter": {}, "limit": 3}, {"op": "find_count", "coll": "people", "filter": {"city": "Paris"}, "limit": 2}], G)
people_test("find.filter_sort_project_skip_limit", [find("people", {"age": {"$gt": 20}}, projection={"name": 1, "age": 1, "_id": 0}, sort={"age": -1, "name": 1}, skip=1, limit=3)], G)
people_test("find.find_one", [{"op": "find_one", "coll": "people", "filter": {"city": "Rome"}, "sort": {"_id": -1}}, {"op": "find_one", "coll": "people", "filter": {"nosuch": 1}}], G)

G = "cursors: batchSize, getMore, killCursors"
add("cursor.batch_size_small", [{"op": "insert_generated", "coll": "big", "n": 100, "kind": "basic"}, find("big", {}, sort={"_id": 1}, batch_size=7), find("big", {"n": {"$gt": 50}}, sort={"n": 1}, batch_size=1, limit=5), find("big", {}, sort={"_id": 1}, batch_size=100), find("big", {}, sort={"_id": 1}, batch_size=101), find("big", {}, sort={"_id": 1}, batch_size=2, skip=95)], G)
add("cursor.paging_1000", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, find("big", {}, sort={"_id": 1}, batch_size=100), find("big", {}, sort={"n": -1}), find("big", {"grp": 3}, sort={"_id": 1}, batch_size=13), {"op": "cursor_batches", "coll": "big", "sort": {"_id": 1}, "batch_size": 250}], G)
add("cursor.default_first_batch_101", [{"op": "insert_generated", "coll": "big", "n": 300, "kind": "basic"}, {"op": "cursor_batches", "coll": "big", "sort": {"_id": 1}}], G)
add("cursor.kill_midway", [{"op": "insert_generated", "coll": "big", "n": 200, "kind": "basic"}, {"op": "cursor_kill_after", "coll": "big", "batch_size": 10, "take": 15}, {"op": "getmore_bogus", "coll": "big"}], G)
add("cursor.getmore_unknown_id", [ins("c", [{"_id": 1}]), {"op": "getmore_bogus", "coll": "c"}], G)
add("cursor.aggregate_batches", [{"op": "insert_generated", "coll": "big", "n": 300, "kind": "basic"}, agg("big", [{"$sort": {"_id": 1}}], batch_size=50), {"op": "cursor_batches_agg", "coll": "big", "pipeline": [{"$sort": {"_id": 1}}], "batch_size": 120}], G)
add("cursor.list_collections_batches", [ins("a", [{"_id": 1}]), ins("b", [{"_id": 1}]), ins("c", [{"_id": 1}]), {"op": "list_collection_names"}], G)

G = "count and distinct"
people_test("count.count_documents", [{"op": "count_documents", "coll": "people", "filter": f} for f in [{}, {"city": "Paris"}, {"age": {"$gt": 25}}, {"nosuch": 1}, {"$or": [{"age": 25}, {"city": "Rome"}]}]] + [{"op": "count_documents", "coll": "people", "filter": {}, "skip": 3}, {"op": "count_documents", "coll": "people", "filter": {}, "limit": 3}, {"op": "count_documents", "coll": "people", "filter": {"city": "Paris"}, "skip": 1, "limit": 1}], G)
people_test("count.estimated", [{"op": "estimated_count", "coll": "people"}, {"op": "estimated_count", "coll": "missing"}], G)
people_test("count.legacy_command", [cmd({"count": "people", "query": {"city": "Paris"}}, ["n", "ok"]), cmd({"count": "people"}, ["n", "ok"]), cmd({"count": "people", "query": {}, "skip": 2, "limit": 3}, ["n", "ok"]), cmd({"count": "missing"}, ["n", "ok"])], G)
people_test("count.after_writes", [{"op": "count_documents", "coll": "people", "filter": {}}, {"op": "delete_many", "coll": "people", "filter": {"city": "Rome"}}, {"op": "count_documents", "coll": "people", "filter": {}}, {"op": "insert_one", "coll": "people", "doc": {"_id": 100}}, {"op": "count_documents", "coll": "people", "filter": {}}], G)
people_test("distinct.basic", [{"op": "distinct", "coll": "people", "key": "city"}, {"op": "distinct", "coll": "people", "key": "tags"}, {"op": "distinct", "coll": "people", "key": "addr.zip"}, {"op": "distinct", "coll": "people", "key": "age", "filter": {"city": "Paris"}}, {"op": "distinct", "coll": "people", "key": "items.sku"}, {"op": "distinct", "coll": "people", "key": "nosuch"}], G, category="extra", unordered=True)

# ---------------------------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------------------------
G = "aggregate"
PIPES = [
    ("match_only", "people", [{"$match": {"city": "Paris"}}]),
    ("match_group_count", "people", [{"$match": {"city": "Paris"}}, {"$group": {"_id": None, "n": {"$sum": 1}}}]),
    ("count_documents_shape", "people", [{"$match": {}}, {"$group": {"_id": 1, "n": {"$sum": 1}}}]),
    ("group_by_field_sum", "orders", [{"$group": {"_id": "$cust", "total": {"$sum": "$amt"}}}, {"$sort": {"_id": 1}}]),
    ("group_sum_avg_min_max", "orders", [{"$group": {"_id": "$cust", "s": {"$sum": "$amt"}, "a": {"$avg": "$amt"}, "mn": {"$min": "$amt"}, "mx": {"$max": "$amt"}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]),
    ("group_all", "orders", [{"$group": {"_id": None, "s": {"$sum": "$amt"}, "a": {"$avg": "$amt"}, "mn": {"$min": "$qty"}, "mx": {"$max": "$qty"}}}]),
    ("group_count_accumulator", "orders", [{"$group": {"_id": "$cust", "c": {"$count": {}}}}, {"$sort": {"_id": 1}}]),
    ("group_sum_qty_int_types", "orders", [{"$group": {"_id": None, "q": {"$sum": "$qty"}}}]),
    ("group_sum_constant", "orders", [{"$group": {"_id": "$cust", "n": {"$sum": 2}}}, {"$sort": {"_id": 1}}]),
    ("group_compound_id", "people", [{"$group": {"_id": {"city": "$city", "tagged": "$tags"}, "n": {"$sum": 1}}}, {"$sort": {"n": -1, "_id": 1}}]),
    ("group_missing_field_key", "people", [{"$group": {"_id": "$age", "n": {"$sum": 1}}}, {"$sort": {"n": -1, "_id": 1}}]),
    ("group_numeric_key_equivalence", "people", [{"$match": {"_id": {"$in": [1, 8]}}}, {"$group": {"_id": "$age", "n": {"$sum": 1}}}]),
    ("group_avg_ignores_non_numeric", "people", [{"$group": {"_id": None, "a": {"$avg": "$age"}, "s": {"$sum": "$age"}}}]),
    ("group_min_max_mixed_types", "people", [{"$group": {"_id": None, "mn": {"$min": "$age"}, "mx": {"$max": "$age"}}}]),
    ("group_min_max_strings", "people", [{"$group": {"_id": None, "mn": {"$min": "$name"}, "mx": {"$max": "$name"}}}]),
    ("group_dotted_field", "people", [{"$group": {"_id": "$addr.zip", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]),
    ("group_then_sort_limit", "orders", [{"$group": {"_id": "$cust", "s": {"$sum": "$amt"}}}, {"$sort": {"s": -1, "_id": 1}}, {"$limit": 2}]),
    ("sort_skip_limit", "people", [{"$sort": {"name": -1}}, {"$skip": 2}, {"$limit": 3}]),
    ("project_include", "people", [{"$sort": {"_id": 1}}, {"$project": {"name": 1, "city": 1}}]),
    ("project_exclude", "people", [{"$sort": {"_id": 1}}, {"$project": {"tags": 0, "scores": 0, "addr": 0, "items": 0}}]),
    ("project_no_id", "people", [{"$sort": {"_id": 1}}, {"$project": {"_id": 0, "name": 1}}]),
    ("project_field_ref", "people", [{"$sort": {"_id": 1}}, {"$project": {"who": "$name", "_id": 0}}]),
    ("count_stage", "people", [{"$match": {"city": "Paris"}}, {"$count": "n"}]),
    ("count_stage_zero", "people", [{"$match": {"city": "Mars"}}, {"$count": "n"}]),
    ("match_or_in", "people", [{"$match": {"$or": [{"city": {"$in": ["Rome", "Berlin"]}}, {"age": {"$gt": 34}}]}}, {"$sort": {"_id": 1}}]),
    ("match_after_group", "orders", [{"$group": {"_id": "$cust", "s": {"$sum": "$amt"}}}, {"$match": {"s": {"$gt": 6}}}, {"$sort": {"_id": 1}}]),
    ("empty_pipeline", "orders", []),
    ("limit_zero_error", "orders", [{"$limit": 0}]),
    ("skip_negative_error", "orders", [{"$skip": -1}]),
    ("unknown_stage_error", "orders", [{"$frobnicate": {}}]),
    ("group_without_id_error", "orders", [{"$group": {"n": {"$sum": 1}}}]),
    ("group_unknown_accumulator_error", "orders", [{"$group": {"_id": None, "n": {"$frob": 1}}}]),
    ("sum_all_missing", "orders", [{"$match": {"_id": 5}}, {"$group": {"_id": None, "s": {"$sum": "$amt"}, "a": {"$avg": "$amt"}, "mx": {"$max": "$amt"}}}]),
]
for label, coll, p in PIPES:
    add(f"aggregate.{label}", [ins("people", PEOPLE), ins("orders", ORDERS), agg(coll, p)], G, unordered=label in ("match_only", "match_group_count"))
add("aggregate.missing_collection", [agg("nope", [{"$group": {"_id": None, "n": {"$sum": 1}}}]), agg("nope", [{"$match": {}}])], G)
add("aggregate.cursor_option_required", [ins("c", [{"_id": 1}]), cmd({"aggregate": "c", "pipeline": []}, None)], G)
add("aggregate.1000_docs_group", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, agg("big", [{"$group": {"_id": "$tag", "n": {"$sum": 1}, "avg": {"$avg": "$n"}, "mn": {"$min": "$n"}, "mx": {"$max": "$n"}}}, {"$sort": {"_id": 1}}]), agg("big", [{"$match": {"nested.k": 3}}, {"$count": "n"}])], G)
for label, p in [("group_first_last", [{"$sort": {"_id": 1}}, {"$group": {"_id": "$cust", "f": {"$first": "$amt"}, "l": {"$last": "$amt"}}}, {"$sort": {"_id": 1}}]), ("group_push", [{"$sort": {"_id": 1}}, {"$group": {"_id": "$cust", "all": {"$push": "$qty"}}}, {"$sort": {"_id": 1}}]), ("group_add_to_set", [{"$group": {"_id": "$cust", "s": {"$addToSet": "$qty"}}}, {"$sort": {"_id": 1}}]), ("unwind", [{"$unwind": "$tags"}]), ("lookup", [{"$lookup": {"from": "orders", "localField": "_id", "foreignField": "_id", "as": "o"}}]), ("add_fields", [{"$addFields": {"x": 1}}]), ("project_arithmetic", [{"$project": {"t": {"$add": ["$qty", 1]}}}]), ("replace_root", [{"$replaceRoot": {"newRoot": "$$ROOT"}}]), ("facet", [{"$facet": {"a": [{"$count": "n"}]}}])]:
    add(f"aggregate.extra.{label}", [ins("orders", ORDERS), agg("orders", p)], G + " (outside the supported set)", category="extra")

# ---------------------------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------------------------
G = "update"
UPD = [
    ("set_basic", {"_id": 1}, {"$set": {"x": 1}}),
    ("set_multiple", {"_id": 1}, {"$set": {"x": 1, "y": "two", "z": [1, 2]}}),
    ("set_overwrite_existing", {"_id": 1}, {"$set": {"age": 99}}),
    ("set_nested_creates_subdoc", {"_id": 2}, {"$set": {"meta.created.by": "me"}}),
    ("set_nested_existing", {"_id": 1}, {"$set": {"addr.zip": "99999"}}),
    ("set_array_index", {"_id": 1}, {"$set": {"scores.1": 99}}),
    ("set_array_index_extends_with_null", {"_id": 1}, {"$set": {"scores.5": 7}}),
    ("set_in_array_of_docs", {"_id": 7}, {"$set": {"items.0.qty": 42}}),
    ("set_same_value_modifies_nothing", {"_id": 1}, {"$set": {"age": 30}}),
    ("set_type_change", {"_id": 1}, {"$set": {"age": "thirty"}}),
    ("unset_field", {"_id": 1}, {"$unset": {"age": ""}}),
    ("unset_nested", {"_id": 1}, {"$unset": {"addr.zip": ""}}),
    ("unset_missing_is_noop", {"_id": 1}, {"$unset": {"nosuch": ""}}),
    ("unset_array_element_makes_null", {"_id": 1}, {"$unset": {"scores.0": ""}}),
    ("inc_int32", {"_id": 1}, {"$inc": {"age": 5}}),
    ("inc_negative", {"_id": 1}, {"$inc": {"age": -40}}),
    ("inc_int64_operand", {"_id": 1}, {"$inc": {"age": Int64(1 << 40)}}),
    ("inc_double_operand", {"_id": 1}, {"$inc": {"age": 0.5}}),
    ("inc_double_field", {"_id": 7}, {"$inc": {"age": 1}}),
    ("inc_int64_field", {"_id": 8}, {"$inc": {"age": 1}}),
    ("inc_missing_field_sets", {"_id": 1}, {"$inc": {"counter": 3}}),
    ("inc_nested_missing", {"_id": 1}, {"$inc": {"stats.views": 1}}),
    ("inc_overflow_int32_to_int64", {"_id": 1}, {"$inc": {"age": 2147483647}}),
    ("inc_multiple_fields", {"_id": 1}, {"$inc": {"age": 1, "counter": 1}}),
    ("inc_non_numeric_field_error", {"_id": 6}, {"$inc": {"age": 1}}),
    ("inc_non_numeric_operand_error", {"_id": 1}, {"$inc": {"age": "x"}}),
    ("set_and_inc_and_unset", {"_id": 1}, {"$set": {"a": 1}, "$inc": {"age": 1}, "$unset": {"tags": ""}}),
    ("replace_whole_doc", {"_id": 1}, {"name": "replaced", "n": 1}),
    ("modify_id_via_set_error", {"_id": 1}, {"$set": {"_id": 2}}),
    ("modify_id_via_replacement_error", {"_id": 1}, {"_id": 2, "x": 1}),
    ("set_id_same_value_ok", {"_id": 1}, {"$set": {"_id": 1}}),
    ("unknown_modifier_error", {"_id": 1}, {"$frobnicate": {"a": 1}}),
    ("no_match_no_change", {"_id": 999}, {"$set": {"a": 1}}),
    ("filter_operator_match", {"age": {"$gt": 29}}, {"$set": {"senior": True}}),
]
for label, f, u in UPD:
    kind = "replace_one" if label.startswith("replace") or label == "modify_id_via_replacement_error" else "update_one"
    add(f"update.one.{label}", [ins("people", PEOPLE), upd("people", f, u, kind=kind, cmp_msg=label in ("modify_id_via_set_error", "modify_id_via_replacement_error")), find("people", sort={"_id": 1})], G)
add("update.one_picks_first_in_insertion_order", [ins("c", [{"_id": i, "g": 1} for i in range(1, 6)]), upd("c", {"g": 1}, {"$set": {"hit": True}}), find("c", sort={"_id": 1})], G)
add("update.many_filter", [ins("people", PEOPLE), upd("people", {"city": "Paris"}, {"$set": {"visited": True}, "$inc": {"visits": 1}}, kind="update_many"), find("people", sort={"_id": 1})], G)
add("update.many_all", [ins("c", [{"_id": i, "n": i} for i in range(1, 51)]), upd("c", {}, {"$inc": {"n": 100}}, kind="update_many"), agg("c", [{"$group": {"_id": None, "s": {"$sum": "$n"}, "mn": {"$min": "$n"}}}])], G)
add("update.many_no_match", [ins("c", [{"_id": 1}]), upd("c", {"x": 1}, {"$set": {"y": 1}}, kind="update_many")], G)
add("update.many_modified_count", [ins("c", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2}, {"_id": 3, "v": 1}]), upd("c", {}, {"$set": {"v": 1}}, kind="update_many")], G)
add("update.on_missing_collection", [upd("nope", {"_id": 1}, {"$set": {"a": 1}}), {"op": "list_collection_names"}], G)
add("update.upsert_inserts_with_set", [upd("c", {"k": "a"}, {"$set": {"v": 1}}, upsert=True), find("c", projection={"_id": 0})], G)
add("update.upsert_uses_filter_equalities", [upd("c", {"a": 1, "b": {"x": 2}, "c.d": 3, "e": {"$gt": 5}}, {"$set": {"z": 9}}, upsert=True), find("c", projection={"_id": 0})], G, sort_keys=True)
add("update.upsert_with_id_in_filter", [upd("c", {"_id": 42}, {"$set": {"v": 1}}, upsert=True), find("c")], G)
add("update.upsert_generated_id", [upd("c", {"k": 1}, {"$inc": {"n": 1}}, upsert=True), find("c", projection={"_id": 0})], G)
add("update.upsert_existing_acts_as_update", [ins("c", [{"_id": 1, "k": "a", "v": 1}]), upd("c", {"k": "a"}, {"$set": {"v": 2}}, upsert=True), find("c")], G)
add("update.upsert_replacement", [upd("c", {"_id": 5}, {"name": "new", "v": 1}, kind="replace_one", upsert=True), find("c")], G)
add("update.upsert_inc_creates", [upd("c", {"_id": 7}, {"$inc": {"n": 5}}, upsert=True), find("c")], G)
add("update.upsert_many", [ins("c", [{"_id": 1, "g": 1}]), upd("c", {"g": 2}, {"$set": {"seen": True}}, kind="update_many", upsert=True), find("c", projection={"_id": 0}, sort={"g": 1})], G)
add("update.upsert_creates_collection", [upd("fresh", {"a": 1}, {"$set": {"b": 1}}, upsert=True), {"op": "list_collection_names"}], G)
add("update.unique_violation", [{"op": "create_index", "coll": "u", "keys": [["email", 1]], "unique": True}, ins("u", [{"_id": 1, "email": "a"}, {"_id": 2, "email": "b"}]), upd("u", {"_id": 2}, {"$set": {"email": "a"}}, cmp_msg=True), upd("u", {"_id": 2}, {"$set": {"email": "c"}}), find("u", sort={"_id": 1})], G)
add("update.unique_violation_on_upsert", [{"op": "create_index", "coll": "u", "keys": [["email", 1]], "unique": True}, ins("u", [{"_id": 1, "email": "a"}]), upd("u", {"_id": 9}, {"$set": {"email": "a"}}, upsert=True, cmp_msg=True)], G)
add("update.many_unordered_error_continues", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, ins("u", [{"_id": i, "k": i} for i in range(1, 4)]), upd("u", {}, {"$set": {"k": 1}}, kind="update_many", cmp_msg=True), find("u", sort={"_id": 1})], G)
for label, u in [("push", {"$push": {"tags": "z"}}), ("pull", {"$pull": {"tags": "a"}}), ("add_to_set", {"$addToSet": {"tags": "z"}}), ("rename", {"$rename": {"name": "n2"}}), ("min", {"$min": {"age": 20}}), ("max", {"$max": {"age": 40}}), ("mul", {"$mul": {"age": 2}}), ("current_date", {"$currentDate": {"ts": True}}), ("pop", {"$pop": {"scores": 1}}), ("positional", {"$set": {"scores.$": 0}})]:
    add(f"update.extra.{label}", [ins("people", PEOPLE), upd("people", {"_id": 1}, u), find("people", {"_id": 1})], "update operators outside the supported set", category="extra")
add("update.extra.pipeline_update", [ins("c", [{"_id": 1, "a": 1}]), upd("c", {"_id": 1}, [{"$set": {"b": "$a"}}]), find("c")], "update operators outside the supported set", category="extra")
add("update.extra.find_one_and_update", [ins("c", [{"_id": 1, "a": 1}]), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$inc": {"a": 1}}}, find("c")], "update operators outside the supported set", category="extra")
add("update.extra.array_filters", [ins("c", [{"_id": 1, "a": [1, 2, 3]}]), upd("c", {"_id": 1}, {"$set": {"a.$[x]": 0}}, array_filters=[{"x": {"$gt": 1}}]), find("c")], "update operators outside the supported set", category="extra")

# ---------------------------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------------------------
G = "delete"
people_test("delete.one_first_match", [{"op": "delete_one", "coll": "people", "filter": {"city": "Paris"}}, find("people", sort={"_id": 1}, projection={"_id": 1})], G)
people_test("delete.many_filter", [{"op": "delete_many", "coll": "people", "filter": {"city": "Paris"}}, find("people", sort={"_id": 1}, projection={"_id": 1})], G)
people_test("delete.many_all", [{"op": "delete_many", "coll": "people", "filter": {}}, find("people"), {"op": "list_collection_names"}], G)
people_test("delete.no_match", [{"op": "delete_one", "coll": "people", "filter": {"nosuch": 1}}, {"op": "delete_many", "coll": "people", "filter": {"nosuch": 1}}, {"op": "count_documents", "coll": "people", "filter": {}}], G)
people_test("delete.by_id", [{"op": "delete_one", "coll": "people", "filter": {"_id": 3}}, {"op": "delete_one", "coll": "people", "filter": {"_id": 3}}, {"op": "count_documents", "coll": "people", "filter": {}}], G)
people_test("delete.by_operator", [{"op": "delete_many", "coll": "people", "filter": {"age": {"$gt": 25}}}, find("people", sort={"_id": 1}, projection={"_id": 1})], G)
add("delete.from_missing_collection", [{"op": "delete_many", "coll": "nope", "filter": {}}, {"op": "delete_one", "coll": "nope", "filter": {"a": 1}}], G)
add("delete.then_reinsert_same_id", [ins("c", [{"_id": 1, "v": 1}]), {"op": "delete_one", "coll": "c", "filter": {"_id": 1}}, ins("c", [{"_id": 1, "v": 2}]), find("c")], G)
add("delete.frees_unique_key", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True}, ins("u", [{"_id": 1, "k": 1}]), {"op": "delete_one", "coll": "u", "filter": {"_id": 1}}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2, "k": 1}}, find("u")], G)
add("delete.1000", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, {"op": "delete_many", "coll": "big", "filter": {"grp": {"$lt": 5}}}, {"op": "count_documents", "coll": "big", "filter": {}}, find("big", {}, sort={"_id": 1}, limit=3)], G)

# ---------------------------------------------------------------------------------------------
# collections and indexes
# ---------------------------------------------------------------------------------------------
G = "collections and indexes"
add("coll.create_and_list", [{"op": "create_collection", "coll": "a"}, {"op": "create_collection", "coll": "b"}, {"op": "list_collection_names"}, {"op": "list_collections"}, {"op": "list_collections", "filter": {"name": "a"}}, {"op": "list_collections", "name_only": True}], G)
add("coll.create_duplicate_error", [{"op": "create_collection", "coll": "a"}, {"op": "create_collection", "coll": "a", "cmp_msg": True}], G)
add("coll.create_existing_is_idempotent", [{"op": "command_raw", "cmd": {"create": "a"}}, {"op": "command_raw", "cmd": {"create": "a"}}, {"op": "command_raw", "cmd": {"create": "a", "capped": False}}, {"op": "list_collection_names"}], G)
add("coll.create_existing_with_different_options_error", [{"op": "command_raw", "cmd": {"create": "a"}}, {"op": "command_raw", "cmd": {"create": "a", "capped": True, "size": 4096}}], G)
add("coll.implicit_creation_by_insert", [ins("auto", [{"_id": 1}]), {"op": "list_collection_names"}], G)
add("coll.drop_existing", [ins("a", [{"_id": 1}]), ins("b", [{"_id": 1}]), {"op": "drop_collection", "coll": "a"}, {"op": "list_collection_names"}, find("a"), find("b")], G)
add("coll.drop_missing_is_ok", [{"op": "command_raw", "cmd": {"drop": "nope"}}], G)
add("coll.drop_then_recreate_empty", [ins("a", [{"_id": 1}, {"_id": 2}]), {"op": "drop_collection", "coll": "a"}, ins("a", [{"_id": 3}]), find("a")], G)
add("coll.drop_removes_indexes", [{"op": "create_index", "coll": "a", "keys": [["x", 1]], "unique": True}, ins("a", [{"_id": 1, "x": 1}]), {"op": "drop_collection", "coll": "a"}, ins("a", [{"_id": 1, "x": 1}, {"_id": 2, "x": 1}], ordered=False), {"op": "list_indexes", "coll": "a"}], G)
add("coll.special_names", [ins(n, [{"_id": 1, "from": n}]) for n in ["a.b", "a/b", "with space", "ünïcode", "UPPER", "x-y_z", "a%b"]] + [find(n) for n in ["a.b", "a/b", "with space", "ünïcode", "UPPER", "x-y_z", "a%b"]] + [{"op": "list_collection_names"}], G)
add("coll.prefix_collections_isolated", [ins("a", [{"_id": 1}]), ins("a/b", [{"_id": 2}]), ins("ab", [{"_id": 3}]), find("a"), find("a/b"), find("ab"), {"op": "delete_many", "coll": "a", "filter": {}}, find("a/b"), find("ab")], G)
add("coll.invalid_name_dollar", [{"op": "command_raw", "cmd": {"insert": "a$b", "documents": [{"_id": 1}]}}], G)
add("idx.list_default", [ins("a", [{"_id": 1}]), {"op": "list_indexes", "coll": "a"}], G)
add("idx.list_missing_collection_error", [{"op": "list_indexes", "coll": "nope", "cmp_msg": True}], G)
add("idx.create_single_default_name", [{"op": "create_index", "coll": "a", "keys": [["x", 1]]}, {"op": "list_indexes", "coll": "a"}, {"op": "list_collection_names"}], G)
add("idx.create_compound_and_desc", [{"op": "create_index", "coll": "a", "keys": [["x", 1], ["y", -1]]}, {"op": "list_indexes", "coll": "a"}], G)
add("idx.create_unique_flag_listed", [{"op": "create_index", "coll": "a", "keys": [["x", 1]], "unique": True, "name": "ux"}, {"op": "list_indexes", "coll": "a"}], G)
add("idx.create_twice_is_noop", [{"op": "create_index", "coll": "a", "keys": [["x", 1]]}, {"op": "create_index", "coll": "a", "keys": [["x", 1]]}, {"op": "list_indexes", "coll": "a"}], G)
add("idx.create_indexes_response", [ins("a", [{"_id": 1}]), cmd({"createIndexes": "a", "indexes": [{"key": {"x": 1}, "name": "x_1"}]}, ["numIndexesBefore", "numIndexesAfter", "createdCollectionAutomatically", "ok"]), cmd({"createIndexes": "a", "indexes": [{"key": {"x": 1}, "name": "x_1"}]}, ["numIndexesBefore", "numIndexesAfter", "note", "ok"]), cmd({"createIndexes": "fresh", "indexes": [{"key": {"y": 1}, "name": "y_1"}]}, ["numIndexesBefore", "numIndexesAfter", "createdCollectionAutomatically", "ok"])], G)
add("idx.same_name_different_key_error", [{"op": "create_index", "coll": "a", "keys": [["x", 1]], "name": "n"}, {"op": "create_index", "coll": "a", "keys": [["y", 1]], "name": "n"}], G)
add("idx.same_key_different_name_error", [{"op": "create_index", "coll": "a", "keys": [["x", 1]], "name": "n1"}, {"op": "create_index", "coll": "a", "keys": [["x", 1]], "name": "n2"}], G)
add("idx.queries_unaffected_by_index", [{"op": "create_index", "coll": "a", "keys": [["x", 1]]}, ins("a", [{"_id": i, "x": i % 5} for i in range(1, 21)]), find("a", {"x": 3}, sort={"_id": 1}), find("a", {"x": {"$gt": 2}}, sort={"x": 1, "_id": 1})], G)
add("idx.extra.drop_index", [{"op": "create_index", "coll": "a", "keys": [["x", 1]]}, {"op": "command_raw", "cmd": {"dropIndexes": "a", "index": "x_1"}}, {"op": "list_indexes", "coll": "a"}], G, category="extra")
add("coll.extra.rename", [ins("a", [{"_id": 1}]), {"op": "admin_command", "cmd": {"renameCollection": "$DB.a", "to": "$DB.b"}}, {"op": "list_collection_names"}], G, category="extra")
add("coll.extra.coll_stats", [ins("a", [{"_id": 1}]), {"op": "command_raw", "cmd": {"collStats": "a"}, "pick": ["ok", "count"]}], G, category="extra")
add("coll.extra.db_stats", [ins("a", [{"_id": 1}]), {"op": "command_raw", "cmd": {"dbStats": 1}, "pick": ["ok", "collections"]}], G, category="extra")

# ---------------------------------------------------------------------------------------------
# persistence across idle close / wake (and concurrency)
# ---------------------------------------------------------------------------------------------
G = "persistence: idle close and wake"
add("persist.idle_close_then_read", [ins("c", [{"_id": i, "v": i * i} for i in range(1, 11)]), {"op": "expect_mode", "mode": "writer"}, {"op": "sleep", "secs": 6}, {"op": "expect_mode", "mode": "closed"}, find("c", sort={"_id": 1}), {"op": "expect_mode", "mode": "reader"}], G, idle=True)
add("persist.idle_close_then_write_upgrades", [ins("c", [{"_id": 1}]), {"op": "sleep", "secs": 6}, {"op": "expect_mode", "mode": "closed"}, ins("c", [{"_id": 2}]), {"op": "expect_mode", "mode": "writer"}, find("c", sort={"_id": 1}), {"op": "sleep", "secs": 6}, {"op": "expect_mode", "mode": "closed"}, find("c", sort={"_id": 1})], G, idle=True)
add("persist.read_only_wake_stays_reader", [ins("c", [{"_id": 1}]), {"op": "tenant_close"}, {"op": "expect_mode", "mode": "closed"}, find("c"), find("c", {"_id": 1}), {"op": "count_documents", "coll": "c", "filter": {}}, {"op": "expect_mode", "mode": "reader"}, {"op": "insert_one", "coll": "c", "doc": {"_id": 2}}, {"op": "expect_mode", "mode": "writer"}], G)
add("persist.forced_close_cycles", sum([[ins("c", [{"_id": 10 * k + i, "k": k} for i in range(5)]), {"op": "tenant_close"}, find("c", {"k": k}, sort={"_id": 1}), upd("c", {"k": k}, {"$inc": {"touched": 1}}, kind="update_many"), {"op": "tenant_close"}, agg("c", [{"$group": {"_id": "$k", "n": {"$sum": 1}, "t": {"$sum": "$touched"}}}, {"$sort": {"_id": 1}}])] for k in range(4)], []), G)
add("persist.1000_docs_survive", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, {"op": "tenant_close"}, {"op": "count_documents", "coll": "big", "filter": {}}, find("big", {"n": {"$gte": 995}}, sort={"n": 1}), agg("big", [{"$group": {"_id": "$grp", "n": {"$sum": 1}, "s": {"$sum": "$n"}}}, {"$sort": {"_id": 1}}]), upd("big", {"grp": 4}, {"$set": {"flag": True}}, kind="update_many"), {"op": "tenant_close"}, {"op": "count_documents", "coll": "big", "filter": {"flag": True}}, {"op": "delete_many", "coll": "big", "filter": {"grp": 0}}, {"op": "tenant_close"}, {"op": "count_documents", "coll": "big", "filter": {}}, find("big", {"_id": 500})], G)
add("persist.indexes_and_collections_survive", [{"op": "create_index", "coll": "u", "keys": [["k", 1]], "unique": True, "name": "uk"}, ins("u", [{"_id": 1, "k": 1}]), {"op": "create_collection", "coll": "empty"}, {"op": "tenant_close"}, {"op": "list_collection_names"}, {"op": "list_indexes", "coll": "u"}, {"op": "insert_one", "coll": "u", "doc": {"_id": 2, "k": 1}, "cmp_msg": True}, {"op": "tenant_close"}, find("u")], G)
add("persist.drop_after_close", [ins("a", [{"_id": 1}]), ins("b", [{"_id": 1}]), {"op": "tenant_close"}, {"op": "drop_collection", "coll": "a"}, {"op": "tenant_close"}, {"op": "list_collection_names"}, find("a"), find("b")], G)
add("persist.cursor_survives_tenant_close", [{"op": "insert_generated", "coll": "big", "n": 300, "kind": "basic"}, {"op": "cursor_close_midway", "coll": "big", "batch_size": 50, "take": 60}], G)
add("persist.id_types_survive", [ins("c", [{"_id": 1}, {"_id": "s"}, {"_id": 2.5}, {"_id": Int64(1 << 50)}, {"_id": datetime.datetime(2024, 5, 6)}, {"_id": ObjectId("65a1b2c3d4e5f60718293a4b")}, {"_id": {"a": 1}}, {"_id": True}]), {"op": "tenant_close"}, find("c"), find("c", {"_id": 2.5}), find("c", {"_id": "s"}), find("c", {"_id": ObjectId("65a1b2c3d4e5f60718293a4b")}), find("c", {"_id": Int64(1 << 50)})], G, unordered=True, keep_oids=True)
add("persist.restart_server_graceful", [ins("c", [{"_id": i} for i in range(1, 6)]), {"op": "restart_server", "kill": False}, find("c", sort={"_id": 1}), ins("c", [{"_id": 6}]), {"op": "restart_server", "kill": False}, find("c", sort={"_id": 1})], G, restart=True)
add("persist.restart_server_sigkill", [ins("c", [{"_id": i} for i in range(1, 6)]), upd("c", {"_id": 1}, {"$set": {"x": 1}}), {"op": "restart_server", "kill": True}, find("c", sort={"_id": 1}), ins("c", [{"_id": 6}]), {"op": "restart_server", "kill": True}, find("c", sort={"_id": 1}), {"op": "count_documents", "coll": "c", "filter": {}}], G, restart=True)
add("persist.drop_database", [ins("a", [{"_id": 1}]), {"op": "drop_database"}, {"op": "db_in_list"}, find("a"), ins("a", [{"_id": 2}]), find("a")], G)
add("persist.many_collections", [ins(f"c{i}", [{"_id": 1, "i": i}]) for i in range(30)] + [{"op": "list_collection_names"}, {"op": "tenant_close"}, {"op": "list_collection_names"}, find("c17")], G)

G = "concurrency"
add("concurrency.parallel_inserts_same_db", [{"op": "parallel_inserts", "coll": "c", "workers": 8, "per_worker": 50}, {"op": "count_documents", "coll": "c", "filter": {}}, agg("c", [{"$group": {"_id": "$w", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])], G)
add("concurrency.parallel_inc_same_doc", [ins("c", [{"_id": 1, "n": 0}]), {"op": "parallel_inc", "coll": "c", "workers": 8, "per_worker": 25}, find("c")], G)
add("concurrency.parallel_dbs", [{"op": "parallel_dbs", "dbs": 6, "per_db": 20}], G)
add("concurrency.reads_during_writes", [ins("c", [{"_id": i} for i in range(100)]), {"op": "parallel_read_write", "coll": "c"}], G)

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "tests.json"
    names = [t["name"] for t in TESTS]
    assert len(names) == len(set(names)), [n for n in names if names.count(n) > 1]
    with open(out, "w") as f:
        json.dump(TESTS, f, indent=0)
    from collections import Counter
    print(f"{len(TESTS)} tests ({Counter(t['category'] for t in TESTS)}) -> {out}")
