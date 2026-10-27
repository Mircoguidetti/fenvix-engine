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
        # Legacy `$regex`/`$options` documents would be turned into regex values by the Extended JSON
        # parsers; the harnesses restore these two keys after parsing.
        return {("@@" + k[1:] if k in ("$regex", "$options") else k): tag(v) for k, v in x.items()}
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
add("admin.legacy_op_query_handshake", [{"op": "raw_op_query"}], G)
add("admin.unacknowledged_write_w0", [{"op": "insert_one_w0", "coll": "c", "doc": {"_id": 1}}, {"op": "sleep", "secs": 0.5}, find("c")], G)
add("admin.client_compression_requested", [{"op": "client_with_compression", "coll": "c"}], G)
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
_race = []
for _r in range(3):
    for _k in range(6):  # each closed writer session leaves an L0 SST; four or more queue a compaction
        _race += [{"op": "insert_one", "coll": "c", "doc": {"_id": _k, "r": _r}}, {"op": "tenant_close"}]
    _race += [{"op": "drop_database"}, ins("c", [{"_id": i} for i in range(3)]), {"op": "count_documents", "coll": "c", "filter": {}}, {"op": "drop_database"}]
add("persist.drop_database_races_background_compaction", _race, G)
add("persist.many_collections", [ins(f"c{i}", [{"_id": 1, "i": i}]) for i in range(30)] + [{"op": "list_collection_names"}, {"op": "tenant_close"}, {"op": "list_collection_names"}, find("c17")], G)

G = "concurrency"
add("concurrency.parallel_inserts_same_db", [{"op": "parallel_inserts", "coll": "c", "workers": 8, "per_worker": 50}, {"op": "count_documents", "coll": "c", "filter": {}}, agg("c", [{"$group": {"_id": "$w", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])], G)
add("concurrency.parallel_inc_same_doc", [ins("c", [{"_id": 1, "n": 0}]), {"op": "parallel_inc", "coll": "c", "workers": 8, "per_worker": 25}, find("c")], G)
add("concurrency.parallel_dbs", [{"op": "parallel_dbs", "dbs": 6, "per_db": 20}], G)
add("concurrency.reads_during_writes", [ins("c", [{"_id": i} for i in range(100)]), {"op": "parallel_read_write", "coll": "c"}], G)

# ---------------------------------------------------------------------------------------------
# v6: regex, findAndModify, update operators, query operators, expressions, aggregation stages
# ---------------------------------------------------------------------------------------------
def Rx(pattern, options=""):
    return {"$regularExpression": {"pattern": pattern, "options": options}}


def D(*a):
    return datetime.datetime(*a)


def sorted_find(coll, filter=None, **kw):
    return find(coll, filter or {}, sort={"_id": 1}, **kw)


# ---- regex ----------------------------------------------------------------------------------
RXD = [
    {"_id": 1, "s": "Hello World"}, {"_id": 2, "s": "hello"}, {"_id": 3, "s": "HELLO\nworld"},
    {"_id": 4, "s": "foo123"}, {"_id": 5, "s": ["abc", "xyz"]}, {"_id": 6, "s": None}, {"_id": 7},
    {"_id": 8, "s": 42}, {"_id": 9, "s": "héllo"}, {"_id": 10, "s": Rx("^stored", "i")},
    {"_id": 11, "s": "aabbcc"}, {"_id": 12, "s": "line1\nline2"}, {"_id": 13, "s": "a.b"}, {"_id": 14, "items": [{"sku": "xa"}, {"sku": "yb"}]},
    {"_id": 15, "s": "Ünï"}, {"_id": 16, "s": "price: 10 usd"},
]
G = "regex"


def rx_test(name, flt, **kw):
    add(f"regex.{name}", [ins("c", RXD), sorted_find("c", flt, projection={"_id": 1})], G, **kw)


def rx_err(name, flt):
    add(f"regex.{name}", [ins("c", RXD), sorted_find("c", flt)], G)


rx_test("string_basic", {"s": {"$regex": "ell"}})
rx_test("string_anchored", {"s": {"$regex": "^hello"}})
rx_test("string_end_anchor", {"s": {"$regex": "world$"}})
rx_test("options_i", {"s": {"$regex": "^hello", "$options": "i"}})
rx_test("options_before_regex", {"s": {"$options": "i", "$regex": "^hello"}})
rx_test("options_m", {"s": {"$regex": "^line2", "$options": "m"}})
rx_test("no_options_m_no_match", {"s": {"$regex": "^line2"}})
rx_test("options_s", {"s": {"$regex": "HELLO.world", "$options": "s"}})
rx_test("no_options_s_dot_newline", {"s": {"$regex": "HELLO.world"}})
rx_test("options_x", {"s": {"$regex": "foo  \\d+  # comment", "$options": "x"}})
rx_test("options_imsx", {"s": {"$regex": "^ hello . world $", "$options": "imsx"}})
rx_test("options_u_accepted", {"s": {"$regex": "^h", "$options": "iu"}})
rx_test("options_empty", {"s": {"$regex": "^h", "$options": ""}})
rx_test("bson_regex_operand", {"s": {"$regex": Rx("^hello", "i")}})
rx_test("bson_regex_operand_plus_options", {"s": {"$regex": Rx("^hello"), "$options": "i"}})
rx_test("plain_equality_regex", {"s": Rx("^hello", "i")})
rx_test("plain_equality_regex_array_elem", {"s": Rx("^x")})
rx_test("plain_equality_regex_matches_stored_regex", {"s": Rx("^stored", "i")})
rx_test("plain_equality_regex_stored_different_options", {"s": Rx("^stored", "")})
rx_test("regex_vs_array_elements", {"s": {"$regex": "^a"}})
rx_test("regex_nested_array_of_docs", {"items.sku": {"$regex": "^y"}})
rx_test("regex_nested_array_of_docs_plain", {"items.sku": Rx("^x")})
rx_test("regex_missing_and_null_never_match", {"s": {"$regex": ".*"}})
rx_test("in_with_regexes", {"s": {"$in": [Rx("^hello", "i"), "foo123", 42]}})
rx_test("in_only_regexes", {"s": {"$in": [Rx("^f"), Rx("c$")]}})
rx_test("in_regex_array_field", {"s": {"$in": [Rx("^xy")]}})
rx_test("nin_with_regexes", {"s": {"$nin": [Rx("^hello", "i"), 42]}})
rx_test("not_regex_literal", {"s": {"$not": Rx("^hello", "i")}})
rx_test("not_regex_operator_doc", {"s": {"$not": {"$regex": "^hello", "$options": "i"}}})
rx_test("eq_regex_is_equality", {"s": {"$eq": Rx("^stored", "i")}})
rx_test("lookahead", {"s": {"$regex": "foo(?=\\d{3})"}})
rx_test("negative_lookahead", {"s": {"$regex": "^hello(?!\\w)"}})
rx_test("lookbehind", {"s": {"$regex": "(?<=price: )\\d+"}})
rx_test("backreference", {"s": {"$regex": "(\\w)\\1"}})
rx_test("named_group_backreference", {"s": {"$regex": "(?<ch>b)\\k<ch>"}})
rx_test("lazy_quantifier", {"s": {"$regex": "a+?b"}})
rx_test("posix_class", {"s": {"$regex": "^[[:alpha:]]+$"}})
rx_test("unicode_property_L", {"s": {"$regex": "^\\p{L}+$"}})
rx_test("case_insensitive_non_ascii", {"s": {"$regex": "^ünï$", "$options": "i"}})
rx_test("escaped_dot", {"s": {"$regex": "^a\\.b$"}})
rx_test("alternation", {"s": {"$regex": "^(foo|aab)"}})
rx_test("word_boundary", {"s": {"$regex": "\\bworld\\b", "$options": "i"}})
rx_test("empty_pattern_matches_strings", {"s": {"$regex": ""}})
rx_test("combined_with_other_ops", {"s": {"$regex": "^h", "$options": "i", "$ne": "hello"}})
rx_test("top_level_or", {"$or": [{"s": Rx("^foo")}, {"s": {"$regex": "^aab"}}]})
rx_err("error_invalid_pattern_paren", {"s": {"$regex": "("}})
rx_err("error_invalid_pattern_quantifier", {"s": {"$regex": "*a"}})
rx_err("error_invalid_pattern_class", {"s": {"$regex": "[a"}})
rx_err("error_invalid_option_flag", {"s": {"$regex": "a", "$options": "z"}})
rx_err("error_options_without_regex", {"s": {"$options": "i"}})
rx_err("error_options_not_string", {"s": {"$regex": "a", "$options": 1}})
rx_err("error_regex_not_string", {"s": {"$regex": 5}})
rx_err("error_regex_both_options", {"s": {"$regex": Rx("a", "i"), "$options": "i"}})
rx_err("error_ne_regex", {"s": {"$ne": Rx("a")}})
rx_err("error_in_with_operator_doc", {"s": {"$in": [{"$regex": "a"}]}})
rx_err("error_embedded_null_byte", {"s": {"$regex": "a\u0000b"}})
add("regex.in_update_filter", [ins("c", RXD), upd("c", {"s": {"$regex": "^hello", "$options": "i"}}, {"$set": {"hit": True}}, kind="update_many"), sorted_find("c", {"hit": True}, projection={"_id": 1})], G)
add("regex.in_delete_filter", [ins("c", RXD), {"op": "delete_many", "coll": "c", "filter": {"s": Rx("^foo")}}, sorted_find("c", projection={"_id": 1})], G)
add("regex.in_count_and_distinct", [ins("c", RXD), {"op": "count_documents", "coll": "c", "filter": {"s": {"$regex": "^h", "$options": "i"}}}, {"op": "distinct", "coll": "c", "key": "s", "filter": {"s": Rx("^h", "i")}}], G, unordered=True)
add("regex.in_aggregate_match", [ins("c", RXD), agg("c", [{"$match": {"s": {"$regex": "^h", "$options": "i"}}}, {"$sort": {"_id": 1}}, {"$project": {"_id": 1}}])], G)
add("regex.in_pull_condition", [ins("c", [{"_id": 1, "a": ["apple", "banana", "avocado"]}]), upd("c", {"_id": 1}, {"$pull": {"a": Rx("^a")}}), sorted_find("c")], G)
add("regex.in_find_and_modify_query", [ins("c", RXD), {"op": "find_one_and_update", "coll": "c", "filter": {"s": Rx("^foo")}, "update": {"$set": {"x": 1}}, "return_document": "after"}], G)
add("regex.with_expr_in_same_filter", [ins("c", RXD), sorted_find("c", {"s": {"$regex": "^h", "$options": "i"}, "$expr": {"$eq": ["$_id", 2]}}, projection={"_id": 1})], G)

# ---- findAndModify --------------------------------------------------------------------------
G = "findAndModify"
FAM = [{"_id": 1, "k": "a", "n": 10, "tags": ["x"]}, {"_id": 2, "k": "a", "n": 30}, {"_id": 3, "k": "b", "n": 20}, {"_id": 4, "k": "a", "n": 20}]


def fam(name, steps, **kw):
    add(f"fam.{name}", [ins("c", FAM)] + steps, G, **kw)


def famc(command, **kw):
    c = {"findAndModify": "c"}
    c.update(command)
    return cmd(c, ["lastErrorObject", "value", "ok"], **kw)


fam("update_returns_before_by_default", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "a"}, "update": {"$inc": {"n": 1}}}, sorted_find("c")])
fam("update_returns_after", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "a"}, "update": {"$inc": {"n": 1}}, "return_document": "after"}, sorted_find("c")])
fam("update_sort_asc", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "a"}, "update": {"$set": {"hit": 1}}, "sort": {"n": 1}, "return_document": "after"}, sorted_find("c")])
fam("update_sort_desc", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "a"}, "update": {"$set": {"hit": 1}}, "sort": {"n": -1}, "return_document": "after"}, sorted_find("c")])
fam("update_sort_two_keys", [{"op": "find_one_and_update", "coll": "c", "filter": {}, "update": {"$set": {"hit": 1}}, "sort": {"k": -1, "n": -1}, "return_document": "after"}, sorted_find("c")])
fam("update_projection_include", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$set": {"z": 9}}, "projection": {"n": 1}, "return_document": "after"}])
fam("update_projection_exclude_id", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$set": {"z": 9}}, "projection": {"_id": 0, "tags": 0}, "return_document": "before"}])
fam("update_no_match", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "nope"}, "update": {"$set": {"z": 9}}}, sorted_find("c")])
fam("update_no_match_after", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "nope"}, "update": {"$set": {"z": 9}}, "return_document": "after"}])
fam("update_upsert_before_returns_null", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 99, "k": "new"}, "update": {"$set": {"z": 9}}, "upsert": True}, sorted_find("c", {"_id": 99})])
fam("update_upsert_after_returns_doc", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 99, "k": "new"}, "update": {"$set": {"z": 9}}, "upsert": True, "return_document": "after"}])
fam("update_upsert_generated_id", [{"op": "find_one_and_update", "coll": "c", "filter": {"k": "new"}, "update": {"$inc": {"n": 1}}, "upsert": True, "return_document": "after"}, {"op": "count_documents", "coll": "c", "filter": {}}])
fam("update_upsert_set_on_insert", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 50}, "update": {"$setOnInsert": {"made": True}, "$inc": {"n": 1}}, "upsert": True, "return_document": "after"}, {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 50}, "update": {"$setOnInsert": {"made": False}, "$inc": {"n": 1}}, "upsert": True, "return_document": "after"}])
fam("update_upsert_existing_is_update", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$set": {"z": 1}}, "upsert": True, "return_document": "after"}])
fam("update_upsert_creates_collection", [{"op": "find_one_and_update", "coll": "fresh", "filter": {"_id": 1}, "update": {"$set": {"a": 1}}, "upsert": True, "return_document": "after"}, {"op": "list_collection_names"}])
fam("update_on_missing_collection_no_upsert", [{"op": "find_one_and_update", "coll": "nothere", "filter": {"_id": 1}, "update": {"$set": {"a": 1}}}, {"op": "list_collection_names"}])
fam("replace_before", [{"op": "find_one_and_replace", "coll": "c", "filter": {"_id": 1}, "update": {"replaced": True}}, sorted_find("c", {"_id": 1})])
fam("replace_after_sorted", [{"op": "find_one_and_replace", "coll": "c", "filter": {"k": "a"}, "update": {"replaced": True}, "sort": {"n": -1}, "return_document": "after"}, sorted_find("c")])
fam("replace_upsert", [{"op": "find_one_and_replace", "coll": "c", "filter": {"_id": 77}, "update": {"r": 1}, "upsert": True, "return_document": "after"}])
fam("replace_changing_id_error", [{"op": "find_one_and_replace", "coll": "c", "filter": {"_id": 1}, "update": {"_id": 500, "r": 1}}, sorted_find("c", {"_id": 1})])
fam("delete_returns_deleted", [{"op": "find_one_and_delete", "coll": "c", "filter": {"k": "a"}}, sorted_find("c", projection={"_id": 1})])
fam("delete_sorted", [{"op": "find_one_and_delete", "coll": "c", "filter": {"k": "a"}, "sort": {"n": -1}}, sorted_find("c", projection={"_id": 1})])
fam("delete_projection", [{"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 3}, "projection": {"n": 1, "_id": 0}}])
fam("delete_no_match", [{"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 1000}}, {"op": "count_documents", "coll": "c", "filter": {}}])
fam("delete_then_gone", [{"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 1}}, {"op": "find_one_and_delete", "coll": "c", "filter": {"_id": 1}}])
fam("delete_on_missing_collection", [{"op": "find_one_and_delete", "coll": "nothere", "filter": {}}, {"op": "list_collection_names"}])
fam("raw_update_shape", [famc({"query": {"_id": 1}, "update": {"$set": {"z": 1}}})])
fam("raw_update_new_shape", [famc({"query": {"_id": 1}, "update": {"$set": {"z": 1}}, "new": True})])
fam("raw_update_noop_shape", [famc({"query": {"_id": 1}, "update": {"$set": {"k": "a"}}, "new": True})])
fam("raw_no_match_shape", [famc({"query": {"_id": 123}, "update": {"$set": {"z": 1}}})])
fam("raw_upsert_shape", [famc({"query": {"_id": 123}, "update": {"$set": {"z": 1}}, "upsert": True})])
fam("raw_upsert_new_shape", [famc({"query": {"_id": 123}, "update": {"$set": {"z": 1}}, "upsert": True, "new": True})])
fam("raw_upsert_replacement_shape", [famc({"query": {"_id": 123}, "update": {"z": 1}, "upsert": True, "new": True})])
fam("raw_remove_shape", [famc({"query": {"_id": 1}, "remove": True})])
fam("raw_remove_no_match_shape", [famc({"query": {"_id": 321}, "remove": True})])
fam("raw_fields_shape", [famc({"query": {"_id": 1}, "update": {"$inc": {"n": 1}}, "fields": {"n": 1, "_id": 0}, "new": True})])
fam("raw_sort_shape", [famc({"query": {"k": "a"}, "sort": {"n": -1}, "update": {"$inc": {"n": 100}}, "new": True})])
fam("raw_no_query_matches_first", [famc({"update": {"$set": {"z": 1}}, "new": True})])
fam("raw_lowercase_command_name", [cmd({"findandmodify": "c", "query": {"_id": 2}, "remove": True}, ["lastErrorObject", "value", "ok"])])
fam("raw_collection_created_by_upsert_response", [cmd({"findAndModify": "brandnew", "query": {"_id": 1}, "update": {"$set": {"a": 1}}, "upsert": True, "new": True}, ["lastErrorObject", "value", "ok"]), {"op": "list_collection_names"}])
fam("error_remove_and_update", [famc({"query": {}, "remove": True, "update": {"$set": {"a": 1}}})])
fam("error_neither_update_nor_remove", [famc({"query": {}})])
fam("error_remove_and_upsert", [famc({"query": {}, "remove": True, "upsert": True})])
fam("error_remove_and_new", [famc({"query": {}, "remove": True, "new": True})])
fam("error_invalid_operator_in_update", [famc({"query": {"_id": 1}, "update": {"$bogus": {"a": 1}}})])
fam("error_invalid_query_operator", [famc({"query": {"n": {"$bogus": 1}}, "update": {"$set": {"a": 1}}})])
fam("error_update_non_numeric_inc", [famc({"query": {"_id": 1}, "update": {"$inc": {"k": 1}}})])
fam("error_immutable_id", [famc({"query": {"_id": 1}, "update": {"$set": {"_id": 9}}})])
fam("error_duplicate_key_on_update", [{"op": "create_index", "coll": "c", "keys": [["n", 1]], "unique": True, "name": "un"}, famc({"query": {"_id": 1}, "update": {"$set": {"n": 30}}})], cmp_msg=False)
fam("error_duplicate_key_on_upsert", [{"op": "create_index", "coll": "c", "keys": [["n", 1]], "unique": True, "name": "un"}, famc({"query": {"_id": 1000}, "update": {"$set": {"n": 10}}, "upsert": True}), sorted_find("c", projection={"_id": 1})])
fam("error_duplicate_id_on_upsert_replacement", [famc({"query": {"_id": 1000}, "update": {"_id": 1, "x": 1}, "upsert": True})])
fam("error_duplicate_id_on_upsert_set", [famc({"query": {"k": "zzz"}, "update": {"$set": {"_id": 1}}, "upsert": True})])
fam("error_array_filters", [famc({"query": {"_id": 1}, "update": {"$set": {"tags.$[x]": "q"}}, "arrayFilters": [{"x": "x"}]})], category="extra")
fam("error_pipeline_update", [famc({"query": {"_id": 1}, "update": [{"$set": {"z": 1}}]})], category="extra")
fam("operators_push_pull", [{"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$push": {"tags": "y"}, "$inc": {"n": 1}}, "return_document": "after"}, {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$pull": {"tags": "x"}}, "return_document": "after"}])
fam("with_expr_query", [{"op": "find_one_and_update", "coll": "c", "filter": {"$expr": {"$gt": ["$n", 15]}}, "update": {"$set": {"big": True}}, "sort": {"n": 1}, "return_document": "after"}])
fam("concurrent_counter_style", [{"op": "find_one_and_update", "coll": "ctr", "filter": {"_id": "seq"}, "update": {"$inc": {"v": 1}}, "upsert": True, "return_document": "after"}] * 3 + [sorted_find("ctr")])

# ---- update operators -----------------------------------------------------------------------
G = "update operators"
UD = [
    {"_id": 1, "a": [1, 2, 3], "s": "x", "n": 5, "d": {"x": 1, "arr": [{"k": 1}, {"k": 2}]}, "e": []},
    {"_id": 2},
    {"_id": 3, "a": "str", "n": "s"},
    {"_id": 4, "a": None},
    {"_id": 5, "a": [3, 1, 2, 1, Dbl(2.0), Int64(1)], "n": 2.5},
    {"_id": 6, "a": [[1, 2], [3]], "d": {"arr": [{"k": 1, "v": [1, 2]}, {"k": 2}]}},
    {"_id": 7, "a": [{"k": 2, "n": "b"}, {"k": 1, "n": "a"}, {"k": 3, "n": "c"}], "n": Int64(7)},
]


def ut(name, idv, update, upsert=False, kind="update_one", **kw):
    flt = {"_id": idv} if idv is not None else {}
    steps = [ins("c", UD), upd("c", flt, update, kind=kind, upsert=upsert), sorted_find("c")]
    add(f"upd.{name}", steps, G, **kw)


# $push
ut("push_existing", 1, {"$push": {"a": 4}})
ut("push_missing_creates", 2, {"$push": {"a": 4}})
ut("push_null_error", 4, {"$push": {"a": 1}})
ut("push_string_error", 3, {"$push": {"a": 1}})
ut("push_nested_path", 1, {"$push": {"d.arr": {"k": 3}}})
ut("push_nested_creates", 2, {"$push": {"x.y": 1}})
ut("push_array_value_nests", 1, {"$push": {"a": [9, 9]}})
ut("push_document_value", 1, {"$push": {"a": {"z": 1}}})
ut("push_each", 1, {"$push": {"a": {"$each": [7, 8, 9]}}})
ut("push_each_empty", 1, {"$push": {"a": {"$each": []}}})
ut("push_each_missing_creates", 2, {"$push": {"a": {"$each": [1, 2]}}})
ut("push_each_empty_missing_creates", 2, {"$push": {"a": {"$each": []}}})
ut("push_each_slice_positive", 1, {"$push": {"a": {"$each": [4, 5], "$slice": 4}}})
ut("push_each_slice_negative", 1, {"$push": {"a": {"$each": [4, 5], "$slice": -2}}})
ut("push_each_slice_zero", 1, {"$push": {"a": {"$each": [4], "$slice": 0}}})
ut("push_each_slice_larger_than_array", 1, {"$push": {"a": {"$each": [4], "$slice": 100}}})
ut("push_each_slice_double_integral", 1, {"$push": {"a": {"$each": [4], "$slice": Dbl(2.0)}}})
ut("push_each_slice_fraction_error", 1, {"$push": {"a": {"$each": [4], "$slice": 1.5}}})
ut("push_each_slice_string_error", 1, {"$push": {"a": {"$each": [4], "$slice": "x"}}})
ut("push_each_sort_asc", 5, {"$push": {"a": {"$each": [0], "$sort": 1}}})
ut("push_each_sort_desc", 5, {"$push": {"a": {"$each": [0], "$sort": -1}}})
ut("push_each_sort_mixed_types", 1, {"$push": {"a": {"$each": ["b", None, True], "$sort": 1}}})
ut("push_each_sort_subdoc_field", 7, {"$push": {"a": {"$each": [{"k": 0, "n": "z"}], "$sort": {"k": 1}}}})
ut("push_each_sort_subdoc_desc", 7, {"$push": {"a": {"$each": [], "$sort": {"n": -1}}}})
ut("push_each_sort_subdoc_multi_key", 7, {"$push": {"a": {"$each": [{"k": 2, "n": "a"}], "$sort": {"k": 1, "n": -1}}}})
ut("push_each_sort_doc_on_scalars_error", 1, {"$push": {"a": {"$each": [4], "$sort": {"k": 1}}}})
ut("push_each_sort_invalid_direction_error", 1, {"$push": {"a": {"$each": [4], "$sort": 2}}})
ut("push_each_sort_empty_doc_error", 7, {"$push": {"a": {"$each": [], "$sort": {}}}})
ut("push_sort_then_slice", 5, {"$push": {"a": {"$each": [0, 10], "$sort": -1, "$slice": 3}}})
ut("push_sort_then_slice_negative", 5, {"$push": {"a": {"$each": [0, 10], "$sort": 1, "$slice": -3}}})
ut("push_position_front", 1, {"$push": {"a": {"$each": [0], "$position": 0}}})
ut("push_position_middle", 1, {"$push": {"a": {"$each": [8, 9], "$position": 1}}})
ut("push_position_negative", 1, {"$push": {"a": {"$each": [8], "$position": -1}}})
ut("push_position_negative_beyond_start", 1, {"$push": {"a": {"$each": [8], "$position": -10}}})
ut("push_position_beyond_end", 1, {"$push": {"a": {"$each": [8], "$position": 99}}})
ut("push_position_slice_sort_together", 5, {"$push": {"a": {"$each": [9], "$position": 0, "$sort": 1, "$slice": 4}}})
ut("push_position_string_error", 1, {"$push": {"a": {"$each": [8], "$position": "x"}}})
ut("push_each_not_array_error", 1, {"$push": {"a": {"$each": 5}}})
ut("push_unknown_modifier_error", 1, {"$push": {"a": {"$each": [1], "$bogus": 1}}})
ut("push_two_fields", 1, {"$push": {"a": 4, "e": 1}})
ut("push_many_docs", None, {"$push": {"a": 0}}, kind="update_many", category="core")
ut("push_upsert_creates_array", 99, {"$push": {"a": 1}}, upsert=True)
ut("push_into_array_element_path", 6, {"$push": {"a.0": 3}})
ut("push_into_array_of_docs_numeric_path", 1, {"$push": {"d.arr.0.vals": 1}})

# $pull
ut("pull_value", 5, {"$pull": {"a": 1}})
ut("pull_value_numeric_equivalence", 5, {"$pull": {"a": Dbl(2.0)}})
ut("pull_absent_value", 1, {"$pull": {"a": 99}})
ut("pull_missing_field", 2, {"$pull": {"a": 1}})
ut("pull_null_field", 4, {"$pull": {"a": 1}})
ut("pull_non_array_error", 3, {"$pull": {"a": 1}})
ut("pull_condition_gt", 1, {"$pull": {"a": {"$gt": 1}}})
ut("pull_condition_range", 5, {"$pull": {"a": {"$gte": 2, "$lt": 3}}})
ut("pull_condition_in", 5, {"$pull": {"a": {"$in": [1, 3]}}})
ut("pull_condition_ne", 1, {"$pull": {"a": {"$ne": 2}}})
ut("pull_subdocument_exact_fields", 7, {"$pull": {"a": {"k": 1}}})
ut("pull_subdocument_condition", 7, {"$pull": {"a": {"k": {"$gt": 1}}}})
ut("pull_subdocument_two_fields", 7, {"$pull": {"a": {"k": 2, "n": "b"}}})
ut("pull_subdocument_or", 7, {"$pull": {"a": {"$or": [{"k": 1}, {"k": 3}]}}})
ut("pull_nested_path_condition", 1, {"$pull": {"d.arr": {"k": 2}}})
ut("pull_nested_arrays_by_equality", 6, {"$pull": {"a": [1, 2]}})
ut("pull_all_elements_leaves_empty", 1, {"$pull": {"a": {"$gte": 0}}})
ut("pull_two_fields", 1, {"$pull": {"a": 2, "e": 1}})
ut("pull_many_docs", None, {"$pull": {"a": 1}}, kind="update_many")
ut("pull_elemmatch_condition", 6, {"$pull": {"d.arr": {"v": {"$elemMatch": {"$gt": 1}}}}})

# $pullAll
ut("pullall_values", 5, {"$pullAll": {"a": [1, 3]}})
ut("pullall_numeric_equivalence", 5, {"$pullAll": {"a": [Dbl(1.0)]}})
ut("pullall_empty_list", 1, {"$pullAll": {"a": []}})
ut("pullall_missing_field", 2, {"$pullAll": {"a": [1]}})
ut("pullall_non_array_field_error", 3, {"$pullAll": {"a": [1]}})
ut("pullall_non_array_arg_error", 1, {"$pullAll": {"a": 1}})
ut("pullall_docs", 7, {"$pullAll": {"a": [{"k": 1, "n": "a"}]}})
ut("pullall_docs_wrong_field_order_no_match", 7, {"$pullAll": {"a": [{"n": "a", "k": 1}]}})

# $addToSet
ut("addtoset_new_value", 1, {"$addToSet": {"a": 4}})
ut("addtoset_existing_value", 1, {"$addToSet": {"a": 2}})
ut("addtoset_numeric_equivalence", 1, {"$addToSet": {"a": Dbl(2.0)}})
ut("addtoset_missing_creates", 2, {"$addToSet": {"a": 1}})
ut("addtoset_null_error", 4, {"$addToSet": {"a": 1}})
ut("addtoset_string_error", 3, {"$addToSet": {"a": 1}})
ut("addtoset_each", 1, {"$addToSet": {"a": {"$each": [3, 4, 5]}}})
ut("addtoset_each_duplicates_in_list", 1, {"$addToSet": {"a": {"$each": [8, 8, Dbl(8.0), 9]}}})
ut("addtoset_each_empty", 1, {"$addToSet": {"a": {"$each": []}}})
ut("addtoset_each_missing_creates", 2, {"$addToSet": {"a": {"$each": [1, 1, 2]}}})
ut("addtoset_each_not_array_error", 1, {"$addToSet": {"a": {"$each": 3}}})
ut("addtoset_document_value_dedupes", 7, {"$addToSet": {"a": {"k": 1, "n": "a"}}})
ut("addtoset_document_value_field_order_matters", 7, {"$addToSet": {"a": {"n": "a", "k": 1}}})
ut("addtoset_array_value", 6, {"$addToSet": {"a": [3]}})
ut("addtoset_array_value_new", 6, {"$addToSet": {"a": [4]}})
ut("addtoset_nested_path", 1, {"$addToSet": {"d.tags": "t"}})
ut("addtoset_upsert", 99, {"$addToSet": {"a": 1}}, upsert=True)
ut("addtoset_extra_field_after_each_error", 1, {"$addToSet": {"a": {"$each": [1], "$position": 0}}})

# $pop
ut("pop_last", 1, {"$pop": {"a": 1}})
ut("pop_first", 1, {"$pop": {"a": -1}})
ut("pop_empty_array", 1, {"$pop": {"e": 1}})
ut("pop_missing_field", 2, {"$pop": {"a": 1}})
ut("pop_string_error", 3, {"$pop": {"a": 1}})
ut("pop_null_error", 4, {"$pop": {"a": 1}})
ut("pop_invalid_direction_error", 1, {"$pop": {"a": 2}})
ut("pop_zero_error", 1, {"$pop": {"a": 0}})
ut("pop_string_direction_error", 1, {"$pop": {"a": "x"}})
ut("pop_double_direction", 1, {"$pop": {"a": Dbl(-1.0)}})
ut("pop_nested", 1, {"$pop": {"d.arr": 1}})
ut("pop_until_empty", 1, {"$pop": {"a": 1, "e": -1}})

# $setOnInsert
ut("setoninsert_ignored_on_update", 1, {"$setOnInsert": {"made": 1}, "$set": {"x": 1}})
ut("setoninsert_applied_on_upsert", 99, {"$setOnInsert": {"made": 1}, "$set": {"x": 1}}, upsert=True)
ut("setoninsert_only_on_upsert", 99, {"$setOnInsert": {"made": 1}}, upsert=True)
ut("setoninsert_upsert_matches_existing", 1, {"$setOnInsert": {"made": 1}}, upsert=True)
ut("setoninsert_nested_on_upsert", 99, {"$setOnInsert": {"a.b.c": 1}}, upsert=True)
ut("setoninsert_with_filter_fields", None, {"$setOnInsert": {"made": 1}}, upsert=True, kind="update_one")
add("upd.setoninsert_upsert_filter_fields_merged", [upd("c", {"k": "q", "v": {"$gt": 1}}, {"$setOnInsert": {"made": 1}, "$inc": {"n": 1}}, upsert=True), find("c", projection={"_id": 0})], G)
add("upd.setoninsert_conflicts_with_filter_value", [upd("c", {"k": 1}, {"$setOnInsert": {"k": 2}}, upsert=True), find("c", projection={"_id": 0})], G)

# $rename
ut("rename_simple", 1, {"$rename": {"s": "t"}})
ut("rename_missing_source_noop", 2, {"$rename": {"s": "t"}})
ut("rename_to_nested", 1, {"$rename": {"s": "d.moved"}})
ut("rename_from_nested", 1, {"$rename": {"d.x": "top"}})
ut("rename_overwrites_existing", 1, {"$rename": {"s": "n"}})
ut("rename_same_name_error", 1, {"$rename": {"s": "s"}})
ut("rename_to_non_string_error", 1, {"$rename": {"s": 1}})
ut("rename_multiple", 1, {"$rename": {"s": "s2", "n": "n2"}})
ut("rename_swap_conflict", 1, {"$rename": {"s": "n", "n": "s"}})
ut("rename_with_set_same_target_conflict", 1, {"$rename": {"s": "t"}, "$set": {"t": 1}})
ut("rename_with_set_source_conflict", 1, {"$rename": {"s": "t"}, "$set": {"s": 1}})
ut("rename_array_value_moves", 1, {"$rename": {"a": "b"}})
ut("rename_id_error", 1, {"$rename": {"_id": "x"}})
ut("rename_to_id_error", 1, {"$rename": {"s": "_id"}})
ut("rename_moves_to_end_of_document", 1, {"$rename": {"a": "zz"}})
ut("rename_many_docs", None, {"$rename": {"n": "num"}}, kind="update_many")
ut("rename_upsert_missing_noop", 99, {"$rename": {"a": "b"}, "$set": {"c": 1}}, upsert=True)

# $min / $max
ut("min_lower_applies", 1, {"$min": {"n": 3}})
ut("min_higher_ignored", 1, {"$min": {"n": 30}})
ut("min_equal_keeps_type", 1, {"$min": {"n": Dbl(5.0)}})
ut("min_missing_sets", 2, {"$min": {"n": 3}})
ut("min_string_vs_number", 1, {"$min": {"n": "abc"}})
ut("min_number_vs_string", 3, {"$min": {"n": 5}})
ut("min_null_vs_number", 1, {"$min": {"n": None}})
ut("min_date", 1, {"$min": {"when": D(2024, 1, 1)}})
ut("min_nested_creates", 2, {"$min": {"x.y": 4}})
ut("min_document_order", 1, {"$min": {"d": {"x": 0}}})
ut("min_array_value", 1, {"$min": {"a": [0]}})
ut("min_mixed_numeric_types", 7, {"$min": {"n": 6.5}})
ut("min_two_fields", 1, {"$min": {"n": 1, "z": 2}})
ut("min_upsert", 99, {"$min": {"n": 3}}, upsert=True)
ut("max_higher_applies", 1, {"$max": {"n": 30}})
ut("max_lower_ignored", 1, {"$max": {"n": 3}})
ut("max_equal_keeps_type", 1, {"$max": {"n": Dbl(5.0)}})
ut("max_missing_sets", 2, {"$max": {"n": 3}})
ut("max_string_vs_number", 1, {"$max": {"n": "abc"}})
ut("max_number_vs_string", 3, {"$max": {"n": 5}})
ut("max_null_vs_number", 1, {"$max": {"n": None}})
ut("max_date", 1, {"$max": {"when": D(2024, 1, 1)}})
ut("max_array_value", 1, {"$max": {"a": [9]}})
ut("max_int64_vs_double", 7, {"$max": {"n": 7.5}})
ut("max_upsert", 99, {"$max": {"n": 3}}, upsert=True)
ut("max_many_docs", None, {"$max": {"n": 6}}, kind="update_many")

# $mul
ut("mul_int", 1, {"$mul": {"n": 3}})
ut("mul_double", 1, {"$mul": {"n": 1.5}})
ut("mul_double_field_by_int", 5, {"$mul": {"n": 2}})
ut("mul_int64_field", 7, {"$mul": {"n": 2}})
ut("mul_int64_arg", 1, {"$mul": {"n": Int64(2)}})
ut("mul_missing_sets_zero_int", 2, {"$mul": {"n": 5}})
ut("mul_missing_sets_zero_double", 2, {"$mul": {"n": 2.5}})
ut("mul_missing_sets_zero_long", 2, {"$mul": {"n": Int64(5)}})
ut("mul_non_numeric_field_error", 3, {"$mul": {"n": 2}})
ut("mul_non_numeric_arg_error", 1, {"$mul": {"n": "x"}})
ut("mul_null_field_error", 4, {"$mul": {"a": 2}})
ut("mul_by_zero", 1, {"$mul": {"n": 0}})
ut("mul_negative", 1, {"$mul": {"n": -2}})
ut("mul_nested_creates", 2, {"$mul": {"x.y": 4}})
ut("mul_many_docs", None, {"$mul": {"n": 2}}, kind="update_many")
add("upd.mul_int_overflow_long", [ins("c", [{"_id": 1, "n": 2147483647}, {"_id": 2, "n": Int64(9223372036854775807)}]), upd("c", {}, {"$mul": {"n": 2}}, kind="update_many"), sorted_find("c")], G)

# $currentDate
for label, spec, flt in [("true_is_date", {"$currentDate": {"ts": True}}, "date"), ("type_date", {"$currentDate": {"ts": {"$type": "date"}}}, "date"), ("type_timestamp", {"$currentDate": {"ts": {"$type": "timestamp"}}}, "timestamp"), ("nested", {"$currentDate": {"a.b.c": True}}, None), ("false_is_date", {"$currentDate": {"ts": False}}, "date"), ("two_fields", {"$currentDate": {"x": True, "y": {"$type": "timestamp"}}}, None)]:
    add(f"upd.currentdate_{label}", [ins("c", [{"_id": 1}]), upd("c", {"_id": 1}, spec), {"op": "find_one", "coll": "c", "filter": {"_id": 1}, "keys_only": True}] + ([{"op": "count_documents", "coll": "c", "filter": {"ts": {"$type": flt}}}] if flt else [{"op": "count_documents", "coll": "c", "filter": {"x": {"$type": "date"}, "y": {"$type": "timestamp"}}}] if "y" in spec["$currentDate"] else [{"op": "count_documents", "coll": "c", "filter": {"a.b.c": {"$type": "date"}}}]), G)
add("upd.currentdate_is_recent", [ins("c", [{"_id": 1}]), upd("c", {"_id": 1}, {"$currentDate": {"ts": True}}), {"op": "count_documents", "coll": "c", "filter": {"ts": {"$gt": D(2020, 1, 1), "$lt": D(2100, 1, 1)}}}], G)
add("upd.currentdate_invalid_spec_error", [ins("c", [{"_id": 1}]), upd("c", {"_id": 1}, {"$currentDate": {"ts": "yes"}}), upd("c", {"_id": 1}, {"$currentDate": {"ts": {"$type": "bogus"}}}), upd("c", {"_id": 1}, {"$currentDate": {"ts": 1}}), find("c")], G)
add("upd.currentdate_upsert", [upd("c", {"_id": 9}, {"$currentDate": {"ts": True}}, upsert=True), {"op": "count_documents", "coll": "c", "filter": {"ts": {"$type": "date"}}}], G)

# conflicts and misc errors
for label, u in [("set_and_inc_same", {"$set": {"n": 1}, "$inc": {"n": 1}}), ("set_parent_and_child", {"$set": {"d": 1, "d.x": 2}}), ("set_child_and_parent", {"$set": {"d.x": 2, "d": 1}}), ("push_and_pull_same", {"$push": {"a": 1}, "$pull": {"a": 2}}), ("set_and_unset_same", {"$set": {"s": 1}, "$unset": {"s": ""}}), ("inc_child_unset_parent", {"$inc": {"d.x": 1}, "$unset": {"d": ""}}), ("min_and_max_same", {"$min": {"n": 1}, "$max": {"n": 9}}), ("addtoset_and_push_same", {"$addToSet": {"a": 1}, "$push": {"a": 2}}), ("setoninsert_and_set_same", {"$setOnInsert": {"n": 1}, "$set": {"n": 2}}), ("same_path_twice_in_one_operator_is_ok_for_different_paths", {"$set": {"p": 1, "q": 2}}), ("array_index_and_whole_array", {"$set": {"a.0": 1, "a": [5]}}), ("dotted_prefix_not_conflict", {"$set": {"ab": 1, "a": [0]}})]:
    ut(f"conflict_{label}", 1, u)
ut("error_unknown_operator", 1, {"$bogus": {"a": 1}})
ut("error_operator_arg_not_object", 1, {"$push": 5})
ut("error_mixed_operator_and_field", 1, {"$set": {"a": 1}, "b": 2}, category="core")
ut("noop_update_reports_no_modification", 1, {"$push": {"e": {"$each": []}}})
ut("update_many_continues_through_docs", None, {"$addToSet": {"tagset": "t"}}, kind="update_many")
add("upd.error_in_update_many_reports_write_error", [ins("c", UD), upd("c", {}, {"$push": {"a": 1}}, kind="update_many"), sorted_find("c")], G)
add("upd.operators_counts_matched_modified", [ins("c", UD), upd("c", {"a": {"$type": "array"}}, {"$addToSet": {"a": 1}}, kind="update_many"), upd("c", {"a": {"$type": "array"}}, {"$pull": {"a": 12345}}, kind="update_many")], G)
add("upd.positional_all_extra", [ins("c", UD), upd("c", {"_id": 1}, {"$set": {"a.$[]": 0}}), sorted_find("c", {"_id": 1})], "update operators outside the supported set", category="extra")
add("upd.positional_filtered_extra", [ins("c", UD), upd("c", {"_id": 1}, {"$set": {"a.$[x]": 0}}, array_filters=[{"x": 2}]), sorted_find("c", {"_id": 1})], "update operators outside the supported set", category="extra")
add("upd.positional_first_extra", [ins("c", UD), upd("c", {"_id": 1, "a": 2}, {"$set": {"a.$": 0}}), sorted_find("c", {"_id": 1})], "update operators outside the supported set", category="extra")
add("upd.pipeline_update_extra", [ins("c", UD), upd("c", {"_id": 1}, [{"$set": {"z": "$n"}}]), sorted_find("c", {"_id": 1})], "update operators outside the supported set", category="extra")
add("upd.bit_operator_extra", [ins("c", UD), upd("c", {"_id": 1}, {"$bit": {"n": {"or": 2}}}), sorted_find("c", {"_id": 1})], "update operators outside the supported set", category="extra")

# ---- query operators ------------------------------------------------------------------------
G = "query operators"
QD = [
    {"_id": 1, "a": [1, 2, 3], "t": "str", "n": 5, "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 5}], "e": [], "d": {"x": [1, 2]}},
    {"_id": 2, "a": [1], "n": 5.5, "items": [{"sku": "x", "qty": 10}], "e": [[]], "d": {"x": []}},
    {"_id": 3, "a": 1, "t": None, "n": Int64(6), "items": [], "d": None},
    {"_id": 4, "a": [[1, 2], [3, 4]], "n": "five", "items": [{"sku": "z", "tags": ["p", "q"]}], "d": {"x": 5}},
    {"_id": 5, "a": [5, 6, 7, 8], "n": True, "items": [1, 2, {"sku": "x"}], "dt": D(2024, 1, 1)},
    {"_id": 6, "n": None, "a": [None, 1], "items": {"sku": "x"}, "t": ["s", 1]},
    {"_id": 7, "a": [], "n": [10, 20], "tags": ["red", "blue", "green"], "oid": ObjectId("65a1b2c3d4e5f60718293a4b"), "dt": D(2023, 5, 5), "re": Rx("^a", "i")},
    {"_id": 8, "a": [2, 3], "n": [3, [4]], "tags": ["red", "Blue"], "b": Binary(b"\x01\x02"), "dbl": 1.5, "nan": {"$numberDouble": "NaN"}},
]


def qt(name, flt, **kw):
    add(f"query.{name}", [ins("c", QD), sorted_find("c", flt, projection={"_id": 1})], G, keep_oids=True, **kw)


# $size
for label, v in [("0", 0), ("1", 1), ("2", 2), ("3", 3), ("4", 4), ("double_integral", Dbl(3.0)), ("int64", Int64(2)), ("negative", -1), ("large", 100)]:
    qt(f"size_{label}", {"a": {"$size": v}})
qt("size_on_nested_array_field", {"d.x": {"$size": 2}})
qt("size_on_missing_field", {"zzz": {"$size": 0}})
qt("size_on_non_array_field", {"t": {"$size": 3}})
qt("size_on_array_of_arrays", {"a": {"$size": 2}})
qt("size_path_through_array_of_docs", {"items.tags": {"$size": 2}})
qt("size_with_other_conditions", {"a": {"$size": 3, "$gt": 2}})
qt("size_not", {"a": {"$not": {"$size": 3}}})
qt("size_nin_in_or", {"$or": [{"a": {"$size": 0}}, {"a": {"$size": 4}}]})
qt("size_fraction_error", {"a": {"$size": 1.5}})
qt("size_string_error", {"a": {"$size": "2"}})
qt("size_null_error", {"a": {"$size": None}})
qt("size_array_arg_error", {"a": {"$size": [1]}})
# $all
qt("all_basic", {"a": {"$all": [1, 2]}})
qt("all_single", {"a": {"$all": [1]}})
qt("all_order_irrelevant", {"a": {"$all": [3, 1]}})
qt("all_empty_matches_nothing", {"a": {"$all": []}})
qt("all_missing_element", {"a": {"$all": [1, 99]}})
qt("all_on_scalar_field", {"a": {"$all": [1]}})
qt("all_duplicates", {"a": {"$all": [1, 1]}})
qt("all_nested_array_value", {"a": {"$all": [[1, 2]]}})
qt("all_nested_array_value_and_scalar", {"a": {"$all": [[1, 2], [3, 4]]}})
qt("all_numeric_equivalence", {"a": {"$all": [Dbl(1.0), Int64(2)]}})
qt("all_strings", {"tags": {"$all": ["red", "blue"]}})
qt("all_regexes", {"tags": {"$all": [Rx("^r"), Rx("^b", "i")]}})
qt("all_regex_and_string", {"tags": {"$all": [Rx("^r"), "green"]}})
qt("all_null", {"a": {"$all": [None]}})
qt("all_missing_field_null", {"zzz": {"$all": [None]}})
qt("all_elemmatch", {"items": {"$all": [{"$elemMatch": {"sku": "x"}}, {"$elemMatch": {"qty": {"$gt": 2}}}]}})
qt("all_elemmatch_single", {"items": {"$all": [{"$elemMatch": {"sku": "x", "qty": {"$gte": 10}}}]}})
qt("all_nested_path", {"items.sku": {"$all": ["x", "y"]}})
qt("all_not", {"a": {"$not": {"$all": [1, 2]}}})
qt("all_with_size", {"a": {"$all": [1], "$size": 1}})
qt("all_non_array_arg_error", {"a": {"$all": 1}})
qt("all_operator_doc_element_error", {"a": {"$all": [{"$gt": 1}]}})
qt("all_mixed_elemmatch_and_value_error", {"items": {"$all": [{"$elemMatch": {"sku": "x"}}, 1]}})
# $elemMatch
qt("elemmatch_doc_form", {"items": {"$elemMatch": {"sku": "x"}}})
qt("elemmatch_doc_form_two_conditions_same_element", {"items": {"$elemMatch": {"sku": "x", "qty": {"$gt": 5}}}})
qt("elemmatch_doc_form_conditions_across_elements_no_match", {"items": {"$elemMatch": {"sku": "y", "qty": {"$lt": 2}}}})
qt("elemmatch_vs_dotted_semantics", {"items.sku": "y", "items.qty": {"$lt": 2}})
qt("elemmatch_scalar_range", {"a": {"$elemMatch": {"$gt": 1, "$lt": 3}}})
qt("elemmatch_scalar_gte", {"a": {"$elemMatch": {"$gte": 8}}})
qt("elemmatch_scalar_eq", {"a": {"$elemMatch": {"$eq": 1}}})
qt("elemmatch_scalar_in", {"a": {"$elemMatch": {"$in": [4, 8]}}})
qt("elemmatch_scalar_nested_array_element", {"a": {"$elemMatch": {"$elemMatch": {"$gt": 3}}}})
qt("elemmatch_empty_doc_matches_non_empty_arrays_with_docs", {"items": {"$elemMatch": {}}})
qt("elemmatch_on_non_array_never_matches", {"t": {"$elemMatch": {"$eq": "str"}}})
qt("elemmatch_on_scalar_field", {"a": {"$elemMatch": {"$eq": 1}}, "_id": 3})
qt("elemmatch_on_missing", {"zzz": {"$elemMatch": {"a": 1}}})
qt("elemmatch_nested_array_in_element", {"items": {"$elemMatch": {"tags": {"$elemMatch": {"$eq": "q"}}}}})
qt("elemmatch_nested_field_path", {"items": {"$elemMatch": {"tags": "p"}}})
qt("elemmatch_with_or", {"items": {"$elemMatch": {"$or": [{"sku": "z"}, {"qty": 10}]}}})
qt("elemmatch_with_and", {"items": {"$elemMatch": {"$and": [{"sku": "x"}, {"qty": {"$lt": 5}}]}}})
qt("elemmatch_with_nor", {"items": {"$elemMatch": {"$nor": [{"sku": "x"}]}}})
qt("elemmatch_with_exists", {"items": {"$elemMatch": {"qty": {"$exists": False}}}})
qt("elemmatch_with_in", {"items": {"$elemMatch": {"sku": {"$in": ["y", "z"]}}}})
qt("elemmatch_with_regex", {"items": {"$elemMatch": {"sku": Rx("^y")}}})
qt("elemmatch_with_not", {"items": {"$elemMatch": {"sku": {"$not": {"$eq": "x"}}}}})
qt("elemmatch_with_ne", {"items": {"$elemMatch": {"sku": {"$ne": "x"}}}})
qt("elemmatch_with_null", {"items": {"$elemMatch": {"qty": None}}})
qt("elemmatch_inside_not", {"items": {"$not": {"$elemMatch": {"sku": "x"}}}})
qt("elemmatch_mixed_element_types_doc_form", {"items": {"$elemMatch": {"sku": "x"}}, "_id": 5})
qt("elemmatch_non_object_error", {"items": {"$elemMatch": 5}})
qt("elemmatch_array_arg_error", {"items": {"$elemMatch": [1]}})
qt("elemmatch_unknown_operator_error", {"items": {"$elemMatch": {"$bogus": 1}}})
# $type
for label, t in [("double", "double"), ("string", "string"), ("object", "object"), ("array", "array"), ("bindata", "binData"), ("objectid", "objectId"), ("bool", "bool"), ("date", "date"), ("null", "null"), ("regex", "regex"), ("int", "int"), ("long", "long"), ("number", "number"), ("timestamp", "timestamp"), ("decimal", "decimal"), ("minkey", "minKey"), ("maxkey", "maxKey")]:
    qt(f"type_alias_{label}_on_n", {"n": {"$type": t}})
for code in [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 16, 18, 19, -1, 127]:
    qt(f"type_code_{code}_on_n", {"n": {"$type": code}})
qt("type_on_array_field_array_alias", {"a": {"$type": "array"}})
qt("type_on_array_field_element_types", {"a": {"$type": "int"}})
qt("type_array_matches_nested_array_element_only_top_level", {"a": {"$type": "array"}, "_id": 4})
qt("type_list_of_types", {"n": {"$type": ["string", "bool"]}})
qt("type_list_with_number", {"n": {"$type": ["number", "null"]}})
qt("type_list_numeric_codes", {"n": {"$type": [2, 8]}})
qt("type_missing_field_no_match", {"zzz": {"$type": "null"}})
qt("type_null_field", {"t": {"$type": "null"}})
qt("type_object_nested", {"d": {"$type": "object"}})
qt("type_object_nested_path", {"d.x": {"$type": "array"}})
qt("type_double_value", {"dbl": {"$type": "double"}})
qt("type_nan_is_double", {"nan": {"$type": "double"}})
qt("type_bindata_field", {"b": {"$type": "binData"}})
qt("type_number_alias_with_int64", {"n": {"$type": "number"}, "_id": 3})
qt("type_not", {"n": {"$not": {"$type": "number"}}})
qt("type_double_code_arg", {"n": {"$type": Dbl(2.0)}})
qt("type_unknown_alias_error", {"n": {"$type": "bogus"}})
qt("type_invalid_code_error", {"n": {"$type": 99}})
qt("type_zero_code_error", {"n": {"$type": 0}})
qt("type_fraction_code_error", {"n": {"$type": 2.5}})
qt("type_empty_list_error", {"n": {"$type": []}})
qt("type_bool_arg_error", {"n": {"$type": True}})
qt("type_null_arg_error", {"n": {"$type": None}})
qt("type_in_aggregate_match", {"n": {"$type": "number"}})
# $mod
qt("mod_basic", {"n": {"$mod": [2, 1]}})
qt("mod_zero_remainder", {"n": {"$mod": [5, 0]}})
qt("mod_negative_divisor", {"n": {"$mod": [-4, 1]}})
qt("mod_double_args_truncated", {"n": {"$mod": [Dbl(2.9), Dbl(1.9)]}})
qt("mod_on_array_elements", {"a": {"$mod": [2, 0]}})
qt("mod_non_numeric_values_ignored", {"t": {"$mod": [2, 0]}})
qt("mod_not", {"n": {"$not": {"$mod": [2, 1]}}})
qt("mod_nan_ignored", {"nan": {"$mod": [2, 0]}})
qt("mod_zero_divisor_error", {"n": {"$mod": [0, 1]}})
qt("mod_missing_remainder_error", {"n": {"$mod": [2]}})
qt("mod_too_many_error", {"n": {"$mod": [2, 1, 0]}})
qt("mod_not_array_error", {"n": {"$mod": 2}})
qt("mod_non_numeric_divisor_error", {"n": {"$mod": ["a", 1]}})
qt("mod_non_numeric_remainder_error", {"n": {"$mod": [2, "a"]}})

# $expr in find / count / delete / update / aggregate
G = "$expr"
XD = [
    {"_id": 1, "a": 5, "b": 3, "s": "x", "arr": [1, 2, 3], "d": {"p": 1}},
    {"_id": 2, "a": 3, "b": 3, "s": "y", "arr": []},
    {"_id": 3, "a": 1, "b": 9, "s": "x", "arr": [5]},
    {"_id": 4, "a": None, "b": 1},
    {"_id": 5, "b": 2, "s": "z"},
    {"_id": 6, "a": "str", "b": "str"},
    {"_id": 7, "a": [1, 2], "b": [1, 2]},
    {"_id": 8, "a": Int64(5), "b": Dbl(5.0)},
]


def xt(name, flt, **kw):
    add(f"expr.{name}", [ins("c", XD), sorted_find("c", flt, projection={"_id": 1})], G, **kw)


xt("gt_fields", {"$expr": {"$gt": ["$a", "$b"]}})
xt("lt_fields", {"$expr": {"$lt": ["$a", "$b"]}})
xt("eq_fields", {"$expr": {"$eq": ["$a", "$b"]}})
xt("ne_fields", {"$expr": {"$ne": ["$a", "$b"]}})
xt("gte_fields", {"$expr": {"$gte": ["$a", "$b"]}})
xt("lte_fields", {"$expr": {"$lte": ["$a", "$b"]}})
xt("gt_literal", {"$expr": {"$gt": ["$a", 2]}})
xt("eq_missing_vs_missing", {"$expr": {"$eq": ["$zz", "$yy"]}})
xt("eq_missing_vs_null", {"$expr": {"$eq": ["$zz", None]}})
xt("compare_arrays_whole", {"$expr": {"$eq": ["$a", "$b"]}, "_id": 7})
xt("and", {"$expr": {"$and": [{"$gt": ["$a", 0]}, {"$lt": ["$b", 5]}]}})
xt("or", {"$expr": {"$or": [{"$gt": ["$a", 4]}, {"$eq": ["$s", "z"]}]}})
xt("not", {"$expr": {"$not": [{"$eq": ["$s", "x"]}]}})
xt("literal_true", {"$expr": True})
xt("literal_false", {"$expr": False})
xt("literal_number_truthy", {"$expr": 1})
xt("literal_zero_falsy", {"$expr": 0})
xt("literal_null_falsy", {"$expr": None})
xt("literal_string_truthy", {"$expr": "abc"})
xt("cond_inside", {"$expr": {"$cond": [{"$gt": ["$a", 2]}, True, False]}})
xt("cond_object_form", {"$expr": {"$cond": {"if": {"$eq": ["$s", "x"]}, "then": {"$gt": ["$a", 2]}, "else": {"$lt": ["$a", 2]}}}})
xt("in_array_field", {"$expr": {"$in": [5, "$arr"]}})
xt("size_of_array", {"$expr": {"$gt": [{"$size": "$arr"}, 1]}, "_id": {"$in": [1, 2, 3]}})
xt("arithmetic", {"$expr": {"$gt": [{"$add": ["$a", "$b"]}, 7]}, "_id": {"$in": [1, 2, 3, 4, 5]}})
xt("field_path_nested", {"$expr": {"$eq": ["$d.p", 1]}})
xt("combined_with_normal_conditions", {"s": "x", "$expr": {"$gt": ["$a", "$b"]}})
xt("inside_or", {"$or": [{"$expr": {"$eq": ["$a", "$b"]}}, {"s": "z"}]})
xt("inside_and", {"$and": [{"$expr": {"$gt": ["$b", 1]}}, {"s": "x"}]})
xt("inside_nor", {"$nor": [{"$expr": {"$gt": ["$a", 2]}}]})
xt("ifnull_in_compare", {"$expr": {"$gt": [{"$ifNull": ["$a", 0]}, 2]}, "_id": {"$lte": 5}})
xt("variable_root", {"$expr": {"$eq": ["$$ROOT.s", "x"]}})
xt("variable_current", {"$expr": {"$eq": ["$$CURRENT.s", "x"]}})
xt("numeric_types_equal", {"$expr": {"$eq": ["$a", "$b"]}, "_id": 8})
xt("string_compare", {"$expr": {"$lt": ["$s", "y"]}})
xt("unknown_operator_error", {"$expr": {"$bogus": [1]}})
xt("wrong_arity_error", {"$expr": {"$eq": [1]}})
xt("invalid_field_path_error", {"$expr": {"$eq": ["$", 1]}})
xt("non_dollar_string_is_literal_truthy", {"$expr": {"$eq": ["a", "a"]}})
add("expr.in_count", [ins("c", XD), {"op": "count_documents", "coll": "c", "filter": {"$expr": {"$gt": ["$a", "$b"]}}}], G)
add("expr.in_delete", [ins("c", XD), {"op": "delete_many", "coll": "c", "filter": {"$expr": {"$eq": ["$a", "$b"]}}}, sorted_find("c", projection={"_id": 1})], G)
add("expr.in_update_filter", [ins("c", XD), upd("c", {"$expr": {"$gt": ["$a", "$b"]}}, {"$set": {"hit": 1}}, kind="update_many"), sorted_find("c", {"hit": 1}, projection={"_id": 1})], G)
add("expr.in_aggregate_match", [ins("c", XD), agg("c", [{"$match": {"$expr": {"$gt": ["$a", "$b"]}}}, {"$sort": {"_id": 1}}, {"$project": {"_id": 1}}])], G)
add("expr.in_find_one_and_update", [ins("c", XD), {"op": "find_one_and_update", "coll": "c", "filter": {"$expr": {"$lt": ["$a", "$b"]}}, "update": {"$set": {"hit": 1}}, "sort": {"_id": 1}, "return_document": "after"}], G)
add("expr.in_distinct", [ins("c", XD), {"op": "distinct", "coll": "c", "key": "s", "filter": {"$expr": {"$gt": ["$a", 2]}}}], G, unordered=True)

# ---- projection $slice ----------------------------------------------------------------------
G = "find: projection"
SLD = [{"_id": 1, "a": [1, 2, 3, 4, 5], "n": 1, "d": {"x": [1, 2, 3]}}, {"_id": 2, "a": [], "n": 2}, {"_id": 3, "a": "str", "n": 3}, {"_id": 4, "n": 4}, {"_id": 5, "a": [[1, 2], [3]], "docs": [{"v": [1, 2, 3]}, {"v": [4]}]}]
for label, p in [("positive", {"a": {"$slice": 2}}), ("negative", {"a": {"$slice": -2}}), ("zero", {"a": {"$slice": 0}}), ("larger_than_array", {"a": {"$slice": 100}}), ("skip_limit", {"a": {"$slice": [1, 2]}}), ("negative_skip_limit", {"a": {"$slice": [-3, 2]}}), ("skip_beyond_end", {"a": {"$slice": [10, 2]}}), ("with_inclusion", {"a": {"$slice": 2}, "n": 1}), ("with_exclusion", {"a": {"$slice": 2}, "n": 0}), ("nested_path", {"d.x": {"$slice": 2}}), ("array_of_docs_nested", {"docs.v": {"$slice": 1}}), ("with_id_excluded", {"a": {"$slice": 1}, "_id": 0}), ("double_arg", {"a": {"$slice": Dbl(2.0)}}), ("limit_zero_error", {"a": {"$slice": [1, 0]}}), ("limit_negative_error", {"a": {"$slice": [1, -1]}}), ("array_of_three_error", {"a": {"$slice": [1, 2, 3]}}), ("string_error", {"a": {"$slice": "x"}})]:
    add(f"find.projection_slice_{label}", [ins("c", SLD), sorted_find("c", projection=p)], G)
add("find.projection_slice_find_one", [ins("c", SLD), {"op": "find_one", "coll": "c", "filter": {"_id": 1}, "projection": {"a": {"$slice": [1, 2]}}}], G)
add("find.projection_slice_in_find_and_modify", [ins("c", SLD), {"op": "find_one_and_update", "coll": "c", "filter": {"_id": 1}, "update": {"$set": {"z": 1}}, "projection": {"a": {"$slice": 2}, "z": 1}, "return_document": "after"}], G)


# ---- expressions (evaluated through $project, one aggregate per document) -----------------------
G = "aggregation expressions"
EXD = [
    {"_id": 1, "a": 5, "b": 3, "s": "Hello", "t": "World", "arr": [1, 2, 3], "d": {"x": 1}, "f": 2.5, "dt": D(2024, 3, 5, 14, 7, 9, 123000), "n": None, "l": Int64(10)},
    {"_id": 2, "a": -7, "b": 2, "s": "", "t": None, "arr": [], "f": -1.5, "dt": D(2023, 12, 31, 23, 59, 59, 1000)},
    {"_id": 3, "a": "x", "b": 1, "arr": "notarray", "d": None, "f": 0.0, "dt": "not a date"},
    {"_id": 4, "b": 0, "arr": [[1], [2, 3]], "s": 5, "t": "ünï"},
    {"_id": 5, "a": 2147483647, "b": 2147483647, "l": Int64(9223372036854775807), "arr": [{"k": 1}, {"k": 2}, {"k": 3}], "s": "MiXeD"},
    {"_id": 6, "a": Dbl(7.0), "b": Int64(2), "arr": [1, "a", None, [2], {"z": 1}], "f": 1e300, "dt": D(1999, 1, 1)},
]


def et(name, exprs, ids=None, **kw):
    """One test per operator: every expression is evaluated against every document (one aggregate each)."""
    steps = [ins("c", EXD)]
    for e in exprs:
        for i in (ids or [d["_id"] for d in EXD]):
            steps.append(agg("c", [{"$match": {"_id": i}}, {"$project": {"_id": 0, "r": e}}]))
    add(f"expr_op.{name}", steps, G, **kw)


# $cond / $ifNull / $literal
et("cond_array_form", [{"$cond": [{"$gt": ["$a", 4]}, "big", "small"]}, {"$cond": ["$zz", 1, 2]}, {"$cond": [{"$eq": ["$n", None]}, "nullish", "set"]}])
et("cond_object_form", [{"$cond": {"if": {"$gt": ["$b", 2]}, "then": "hi", "else": "lo"}}, {"$cond": {"else": 0, "then": 1, "if": "$arr"}}])
et("cond_missing_branches_error", [{"$cond": {"if": True, "then": 1}}, {"$cond": [True, 1]}, {"$cond": {"if": True, "then": 1, "else": 2, "x": 3}}])
et("cond_branch_missing_field", [{"$cond": [True, "$zz", 1]}, {"$cond": [False, 1, "$zz"]}])
et("cond_nested", [{"$cond": [{"$gt": ["$a", 0]}, {"$cond": [{"$gt": ["$a", 6]}, "huge", "pos"]}, "neg"]}])
et("ifnull", [{"$ifNull": ["$n", "dflt"]}, {"$ifNull": ["$zz", "dflt"]}, {"$ifNull": ["$a", "dflt"]}, {"$ifNull": ["$zz", "$n", "last"]}, {"$ifNull": ["$zz", "$yy"]}, {"$ifNull": ["$a"]}])
et("literal", [{"$literal": "$a"}, {"$literal": {"$gt": 1}}, {"$literal": 5}, {"$literal": [1, "$a"]}, {"$literal": None}], ids=[1])
et("array_and_object_literals", [["$a", "$zz", 5], {"x": "$a", "y": {"z": "$b"}}, {"x": "$zz"}, [[]], {}], ids=[1, 2])
# comparison & boolean
et("comparison_numbers", [{"$eq": ["$a", "$b"]}, {"$ne": ["$a", "$b"]}, {"$gt": ["$a", "$b"]}, {"$gte": ["$a", "$b"]}, {"$lt": ["$a", "$b"]}, {"$lte": ["$a", "$b"]}])
et("comparison_cross_type", [{"$gt": ["$s", "$a"]}, {"$lt": ["$a", "$s"]}, {"$eq": [1, 1.0]}, {"$eq": [Int64(1), 1]}, {"$gt": [None, "$zz"]}, {"$eq": ["$zz", None]}, {"$lt": ["$zz", None]}, {"$gt": [[], None]}, {"$gt": [{"a": 1}, [1]]}], ids=[1])
et("comparison_arrays_docs", [{"$eq": [[1, 2], [1, 2]]}, {"$lt": [[1, 2], [1, 3]]}, {"$eq": [{"a": 1, "b": 2}, {"b": 2, "a": 1}]}, {"$eq": [{"a": 1}, {"a": 1.0}]}, {"$eq": ["$arr", [1, 2, 3]]}], ids=[1, 2])
et("comparison_dates_strings", [{"$gt": ["$dt", D(2024, 1, 1)]}, {"$lt": ["$s", "I"]}, {"$eq": ["$s", "hello"]}])
et("comparison_wrong_arity_error", [{"$eq": [1]}, {"$gt": [1, 2, 3]}, {"$lt": 5}, {"$eq": []}], ids=[1])
et("and_or_not", [{"$and": ["$a", "$b"]}, {"$or": ["$n", "$zz"]}, {"$and": []}, {"$or": []}, {"$not": ["$a"]}, {"$not": ["$zz"]}, {"$and": [True, 1, "x"]}, {"$or": [0, None, False]}, {"$not": [0]}, {"$not": ["$arr"]}])
et("and_or_not_single_non_array_argument", [{"$and": True}, {"$or": 0}, {"$not": True}, {"$not": "$a"}], ids=[1, 2])
et("not_wrong_arity_error", [{"$not": [1, 2]}, {"$not": []}], ids=[1])
et("and_short_circuit_hides_errors", [{"$and": [False, {"$divide": [1, 0]}]}, {"$or": [True, {"$divide": [1, 0]}]}], ids=[1])
et("in_array", [{"$in": ["$a", "$arr"]}, {"$in": [2, "$arr"]}, {"$in": ["$zz", [1, None]]}, {"$in": [None, [None]]}, {"$in": [[2], "$arr"]}, {"$in": [{"k": 2}, "$arr"]}, {"$in": [1, [1.0]]}])
et("in_non_array_error", [{"$in": [1, "notarray"]}, {"$in": [1, "$zz"]}, {"$in": [1, None]}, {"$in": [1]}], ids=[1])
# arithmetic
et("add", [{"$add": ["$a", "$b"]}, {"$add": ["$a", 1]}, {"$add": [1, 2.5]}, {"$add": ["$l", 1]}, {"$add": []}, {"$add": ["$a"]}, {"$add": ["$a", None]}, {"$add": ["$a", "$zz"]}, {"$add": ["$dt", 1000]}, {"$add": [1, 2, 3, 4]}])
et("add_type_errors", [{"$add": ["$s", 1]}, {"$add": [1, "x"]}, {"$add": [[1], 1]}, {"$add": [True, 1]}], ids=[1])
et("add_overflow_promotes", [{"$add": [2147483647, 1]}, {"$add": [Int64(9223372036854775807), 1]}, {"$add": [Int64(1), 1]}, {"$add": [1, Dbl(1.0)]}, {"$add": [2147483647, 2147483647]}], ids=[1])
et("add_date_and_numbers", [{"$add": [D(2024, 1, 1), 86400000]}, {"$add": [86400000, D(2024, 1, 1)]}, {"$add": [D(2024, 1, 1), D(2024, 1, 1)]}], ids=[1])
et("subtract", [{"$subtract": ["$a", "$b"]}, {"$subtract": ["$a", 1.5]}, {"$subtract": ["$a", None]}, {"$subtract": [None, 1]}, {"$subtract": ["$zz", 1]}, {"$subtract": ["$l", 1]}, {"$subtract": ["$dt", "$dt"]}, {"$subtract": ["$dt", 1000]}])
et("subtract_errors", [{"$subtract": [1]}, {"$subtract": [1, 2, 3]}, {"$subtract": ["a", 1]}, {"$subtract": [1, D(2024, 1, 1)]}], ids=[1])
et("subtract_overflow", [{"$subtract": [-2147483648, 1]}, {"$subtract": [Int64(-9223372036854775807), 10]}], ids=[1])
et("multiply", [{"$multiply": ["$a", "$b"]}, {"$multiply": ["$a", 2.5]}, {"$multiply": ["$a", None]}, {"$multiply": ["$a", "$zz"]}, {"$multiply": []}, {"$multiply": ["$a"]}, {"$multiply": [2, 3, 4]}, {"$multiply": ["$l", 2]}])
et("multiply_overflow", [{"$multiply": [65536, 65536]}, {"$multiply": [Int64(4294967296), Int64(4294967296)]}, {"$multiply": [1.5, 2]}], ids=[1])
et("multiply_errors", [{"$multiply": ["x", 2]}, {"$multiply": [2, True]}], ids=[1])
et("divide", [{"$divide": ["$a", "$b"]}, {"$divide": ["$a", 2]}, {"$divide": ["$a", None]}, {"$divide": [None, 2]}, {"$divide": ["$zz", 2]}, {"$divide": [10, 4]}, {"$divide": [10, 5]}, {"$divide": [Int64(10), 4]}])
et("divide_by_zero_and_errors", [{"$divide": [1, 0]}, {"$divide": [1, 0.0]}, {"$divide": [0, 0]}, {"$divide": ["a", 1]}, {"$divide": [1]}, {"$divide": [1, 2, 3]}], ids=[1])
et("mod", [{"$mod": ["$a", "$b"]}, {"$mod": ["$a", 3]}, {"$mod": [-7, 3]}, {"$mod": [7, -3]}, {"$mod": [7.5, 2]}, {"$mod": ["$a", None]}, {"$mod": [None, 2]}, {"$mod": ["$zz", 2]}, {"$mod": [Int64(10), 3]}, {"$mod": [10, Int64(3)]}])
et("mod_errors", [{"$mod": [1, 0]}, {"$mod": [1, 0.0]}, {"$mod": ["a", 1]}, {"$mod": [1]}], ids=[1])
et("math_mixed_nested", [{"$add": [{"$multiply": ["$a", "$b"]}, {"$divide": ["$b", 2]}, {"$subtract": ["$a", 1]}]}], ids=[1, 2, 5])
# strings
et("concat", [{"$concat": ["$s", " ", "$t"]}, {"$concat": ["$s", "$zz"]}, {"$concat": ["$s", None]}, {"$concat": []}, {"$concat": ["$s"]}, {"$concat": ["a", "b", "c"]}, {"$concat": ["é", "ü"]}])
et("concat_type_errors", [{"$concat": ["a", 1]}, {"$concat": [1]}, {"$concat": ["a", True]}, {"$concat": ["a", ["b"]]}], ids=[1])
et("tolower_toupper", [{"$toLower": "$s"}, {"$toUpper": "$s"}, {"$toLower": "$zz"}, {"$toUpper": "$n"}, {"$toLower": ["$s"]}, {"$toUpper": ["$s"]}, {"$toLower": "$t"}, {"$toUpper": "$t"}, {"$toLower": ""}])
et("tolower_toupper_non_strings", [{"$toLower": "$a"}, {"$toUpper": "$b"}, {"$toLower": "$f"}, {"$toUpper": "$d"}, {"$toLower": "$dt"}, {"$toUpper": True}], ids=[1])
et("tolower_toupper_arity_error", [{"$toLower": ["a", "b"]}, {"$toUpper": []}], ids=[1])
# arrays
et("size", [{"$size": "$arr"}, {"$size": []}, {"$size": [[1, 2]]}, {"$size": ["$arr"]}])
et("size_non_array_error", [{"$size": "$s"}, {"$size": "$zz"}, {"$size": None}, {"$size": 5}], ids=[1])
et("isarray", [{"$isArray": "$arr"}, {"$isArray": ["$arr"]}, {"$isArray": "$zz"}, {"$isArray": "$s"}, {"$isArray": [[1]]}, {"$isArray": [1, 2]}])
et("arrayelemat", [{"$arrayElemAt": ["$arr", 0]}, {"$arrayElemAt": ["$arr", -1]}, {"$arrayElemAt": ["$arr", 1]}, {"$arrayElemAt": ["$arr", 10]}, {"$arrayElemAt": ["$arr", -10]}, {"$arrayElemAt": ["$zz", 0]}, {"$arrayElemAt": [None, 0]}, {"$arrayElemAt": ["$arr", None]}, {"$arrayElemAt": ["$arr", "$zz"]}, {"$arrayElemAt": ["$arr", Dbl(1.0)]}, {"$arrayElemAt": ["$arr", Int64(1)]}])
et("arrayelemat_errors", [{"$arrayElemAt": ["$s", 0]}, {"$arrayElemAt": [[1, 2], "x"]}, {"$arrayElemAt": [[1, 2], 0.5]}, {"$arrayElemAt": [[1]]}, {"$arrayElemAt": [[1], 0, 1]}, {"$arrayElemAt": [[1], Int64(9223372036854775807)]}], ids=[1])
et("concatarrays", [{"$concatArrays": ["$arr", [9]]}, {"$concatArrays": ["$arr", "$arr"]}, {"$concatArrays": []}, {"$concatArrays": ["$arr"]}, {"$concatArrays": ["$arr", None]}, {"$concatArrays": ["$arr", "$zz"]}, {"$concatArrays": [[], []]}, {"$concatArrays": [[1], [[2]], [{"a": 1}]]}])
et("concatarrays_non_array_error", [{"$concatArrays": ["$s", [1]]}, {"$concatArrays": [[1], 5]}, {"$concatArrays": [[1], "x"]}], ids=[1])
et("setunion", [{"$setUnion": ["$arr", [2, 5]]}, {"$setUnion": [[1, 1, 2], [2, 3]]}, {"$setUnion": []}, {"$setUnion": [[3, 1]]}, {"$setUnion": ["$arr", None]}, {"$setUnion": ["$arr", "$zz"]}, {"$setUnion": [[1, Dbl(1.0), Int64(1)], [2]]}, {"$setUnion": [["b", "a"], ["c", "a"]]}, {"$setUnion": [[[1], [1]], [[2]]]}, {"$setUnion": [[{"a": 1}, {"a": 1}], []]}])
et("setunion_non_array_error", [{"$setUnion": ["$s", [1]]}, {"$setUnion": [[1], 5]}], ids=[1])
et("slice", [{"$slice": ["$arr", 2]}, {"$slice": ["$arr", -2]}, {"$slice": ["$arr", 0]}, {"$slice": ["$arr", 10]}, {"$slice": ["$arr", -10]}, {"$slice": ["$arr", 1, 2]}, {"$slice": ["$arr", -2, 1]}, {"$slice": ["$arr", -10, 2]}, {"$slice": ["$arr", 10, 2]}, {"$slice": [[1, 2, 3, 4, 5], Dbl(2.0)]}, {"$slice": [[1, 2, 3, 4, 5], -2147483648]}, {"$slice": ["$zz", 1]}, {"$slice": [None, 1]}, {"$slice": [[1, 2], None]}, {"$slice": [[1, 2], "$zz"]}])
et("slice_errors", [{"$slice": [[1, 2], 1, 0]}, {"$slice": [[1, 2], 1, -1]}, {"$slice": ["abc", 1]}, {"$slice": [[1], "x"]}, {"$slice": [[1], 1.5]}, {"$slice": [[1], Int64(4294967296)]}, {"$slice": [[1], 1, "x"]}, {"$slice": [[1], 1, 1.5]}, {"$slice": [[1]]}, {"$slice": [[1], 1, 2, 3]}, {"$slice": "$arr"}], ids=[1])
et("filter", [{"$filter": {"input": "$arr", "as": "x", "cond": {"$gt": ["$$x", 1]}}}, {"$filter": {"input": "$arr", "cond": {"$gt": ["$$this", 1]}}}, {"$filter": {"input": "$arr", "as": "x", "cond": True}}, {"$filter": {"input": "$arr", "as": "x", "cond": False}}, {"$filter": {"input": "$zz", "as": "x", "cond": True}}, {"$filter": {"input": None, "as": "x", "cond": True}}, {"$filter": {"input": [{"k": 1}, {"k": 2}], "as": "o", "cond": {"$eq": ["$$o.k", 2]}}}, {"$filter": {"input": [1, 2, 3, 4], "as": "n", "cond": {"$gt": ["$$n", 1]}, "limit": 2}}])
et("filter_errors", [{"$filter": {"input": "$s", "as": "x", "cond": True}}, {"$filter": {"as": "x", "cond": True}}, {"$filter": {"input": [1], "as": "x"}}, {"$filter": {"input": [1], "as": "x", "cond": True, "bogus": 1}}, {"$filter": [1]}, {"$filter": {"input": [1], "as": "x", "cond": True, "limit": 0}}], ids=[1])
et("filter_uses_outer_fields", [{"$filter": {"input": "$arr", "as": "x", "cond": {"$gt": ["$$x", "$b"]}}}], ids=[1, 5])
et("filter_variable_shadowing_root", [{"$filter": {"input": [1, 2], "as": "ROOTLIKE", "cond": {"$eq": ["$$ROOTLIKE", "$a"]}}}], ids=[1])
et("map", [{"$map": {"input": "$arr", "as": "x", "in": {"$multiply": ["$$x", 2]}}}, {"$map": {"input": "$arr", "in": {"$add": ["$$this", 1]}}}, {"$map": {"input": "$arr", "as": "x", "in": "$$x"}}, {"$map": {"input": "$zz", "as": "x", "in": 1}}, {"$map": {"input": None, "as": "x", "in": 1}}, {"$map": {"input": [{"k": 1}, {"k": 2}], "as": "o", "in": "$$o.k"}}, {"$map": {"input": [1, 2], "as": "x", "in": {"v": "$$x", "b": "$b"}}}, {"$map": {"input": [1, 2], "as": "x", "in": "$zz"}}, {"$map": {"input": [], "as": "x", "in": 1}}])
et("map_errors", [{"$map": {"input": "$s", "as": "x", "in": 1}}, {"$map": {"as": "x", "in": 1}}, {"$map": {"input": [1], "as": "x"}}, {"$map": {"input": [1], "as": "x", "in": 1, "bogus": 1}}, {"$map": 5}], ids=[1])
et("map_nested", [{"$map": {"input": [[1, 2], [3]], "as": "a", "in": {"$size": "$$a"}}}, {"$map": {"input": [1, 2], "as": "x", "in": {"$map": {"input": [10, 20], "as": "y", "in": {"$add": ["$$x", "$$y"]}}}}}], ids=[1])
et("variables", [{"$literal": 1}, "$$ROOT", "$$CURRENT", "$$ROOT.a", "$$CURRENT.b", {"$eq": ["$$ROOT.a", "$a"]}, "$$REMOVE"], ids=[1, 2])
et("variable_errors", ["$$nosuchvar", "$$", "$$1abc", "$$NOW.x"], ids=[1])
et("field_paths", ["$a", "$d.x", "$d.zz", "$zz.yy", "$arr.k", "$arr.0", "$dt", "$n", "$$ROOT.d.x"], ids=[1, 3, 5])
et("field_path_array_of_docs", ["$arr.k", "$arr.z", "$arr.k.x"], ids=[5, 6])
et("field_path_errors", ["$", "$.a", "$a.", "$a..b"], ids=[1])
# dates
et("datetostring_default_format", [{"$dateToString": {"date": "$dt"}}, {"$dateToString": {"date": "$dt", "timezone": "UTC"}}, {"$dateToString": {"date": "$zz"}}, {"$dateToString": {"date": None}}], ids=[1, 2, 6])
et("datetostring_formats", [{"$dateToString": {"format": f, "date": "$dt"}} for f in ["%Y-%m-%d", "%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%LZ", "%j", "%w", "%u", "%d/%m/%Y", "%%", "year %Y", "%Z", "%z", "%M", "%L", "%m", "%Y%m%d%H%M%S"]], ids=[1, 2, 6])
et("datetostring_week_formats", [{"$dateToString": {"format": f, "date": "$dt"}} for f in ["%U", "%V", "%G"]], ids=[1, 2, 6], category="extra")
et("datetostring_timezone_utc_variants", [{"$dateToString": {"date": "$dt", "timezone": z}} for z in ["UTC", "GMT", "Etc/UTC", "+00:00", "Z"]], ids=[1])
et("datetostring_onnull", [{"$dateToString": {"date": "$zz", "onNull": "none"}}, {"$dateToString": {"date": "$dt", "onNull": "none"}}], ids=[1, 4])
et("datetostring_from_objectid_and_timestamp", [{"$dateToString": {"date": {"$toDate": 0}}}], ids=[1], category="extra")
et("datetostring_unknown_timezone_name_extra", [{"$dateToString": {"date": "$dt", "timezone": "Not/AZone"}}, {"$dateToString": {"date": "$dt", "timezone": "utc"}}], ids=[1], category="extra")
et("datetostring_errors", [{"$dateToString": {"date": "$s"}}, {"$dateToString": {"date": "$a"}}, {"$dateToString": {"format": "%Q", "date": "$dt"}}, {"$dateToString": {"format": 5, "date": "$dt"}}, {"$dateToString": {"format": "%Y"}}, {"$dateToString": {"date": "$dt", "bogus": 1}}, {"$dateToString": "$dt"}], ids=[1])
et("datetostring_other_timezones", [{"$dateToString": {"date": "$dt", "timezone": "Europe/Rome"}}, {"$dateToString": {"date": "$dt", "timezone": "+02:00"}}, {"$dateToString": {"date": "$dt", "timezone": "America/New_York", "format": "%H:%M"}}], ids=[1], category="extra")
et("datetostring_range", [{"$dateToString": {"date": d}} for d in [D(1970, 1, 1), D(1969, 12, 31, 23, 59, 59), D(2000, 2, 29, 12), D(9999, 12, 31, 23, 59, 59, 999000), D(1600, 1, 1), D(2024, 12, 31), D(2023, 12, 31)]], ids=[1])
et("datetostring_julian_and_weekday", [{"$dateToString": {"format": "%j %w %u", "date": d}} for d in [D(2024, 1, 1), D(2024, 12, 31), D(2023, 12, 31), D(2024, 2, 29), D(2024, 3, 1)]], ids=[1])
# unsupported expression operators stay unsupported (documented)
et("unsupported_operators_extra", [{"$switch": {"branches": [{"case": True, "then": 1}], "default": 2}}, {"$reduce": {"input": [1], "initialValue": 0, "in": "$$value"}}, {"$let": {"vars": {"x": 1}, "in": "$$x"}}, {"$substr": ["abc", 0, 1]}, {"$toString": 1}, {"$objectToArray": {"a": 1}}, {"$round": [1.5]}, {"$abs": -1}, {"$regexMatch": {"input": "abc", "regex": "a"}}, {"$max": [1, 2]}, {"$year": D(2024, 1, 1)}], ids=[1], category="extra")

# ---- aggregation stages ---------------------------------------------------------------------
G = "aggregate: stages"
PD = [
    {"_id": 1, "name": "a", "x": 1, "y": {"z": 10, "w": 5}, "arr": [1, 2], "docs": [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}]},
    {"_id": 2, "name": "b", "x": 2, "y": {"z": 20}, "arr": [], "docs": []},
    {"_id": 3, "name": "c", "x": None, "y": None, "arr": None},
    {"_id": 4, "name": "d", "y": 5, "arr": "scalar", "docs": {"k": 9}},
    {"_id": 5, "name": "e", "x": 5, "y": [{"z": 1}, {"z": 2, "q": 3}], "arr": [[1], 2], "docs": [1, {"k": 3}]},
]


def pt(name, pipeline, docs=None, coll="c", **kw):
    add(f"stage.{name}", [ins("c", docs or PD), agg("c", pipeline + [{"$sort": {"_id": 1}}] if kw.pop("sorted", True) else pipeline)], G, **kw)


def pe(name, pipeline, docs=None, **kw):
    """Error / no-sort test: the pipeline is run as given."""
    add(f"stage.{name}", [ins("c", docs or PD), agg("c", pipeline)], G, **kw)


# $project with expressions
pt("project_rename_field", [{"$project": {"renamed": "$name"}}])
pt("project_include_and_computed", [{"$project": {"name": 1, "double": {"$multiply": ["$x", 2]}}}])
pt("project_computed_order_follows_spec_after_included", [{"$project": {"c2": "$x", "name": 1, "c1": "$name"}}])
pt("project_computed_replaces_existing_in_place", [{"$project": {"x": {"$add": ["$x", 100]}, "name": 1}}])
pt("project_id_excluded", [{"$project": {"_id": 0, "n": "$name"}}])
pt("project_id_computed", [{"$project": {"_id": "$name", "x": 1}}], sorted=False)
pt("project_id_included_explicit", [{"$project": {"_id": 1, "n": "$name"}}])
pt("project_nested_computed_dotted", [{"$project": {"out.a": "$name", "out.b": "$x"}}])
pt("project_nested_computed_object", [{"$project": {"out": {"a": "$name", "b": {"$add": ["$x", 1]}}}}])
pt("project_nested_include_dotted", [{"$project": {"y.z": 1}}])
pt("project_nested_include_object_form", [{"$project": {"y": {"z": 1}}}])
pt("project_nested_include_and_computed_same_parent", [{"$project": {"y.z": 1, "y.n": "$name"}}])
pt("project_computed_missing_field_omitted", [{"$project": {"v": "$nosuch", "name": 1}}])
pt("project_computed_null_kept", [{"$project": {"v": "$x"}}])
pt("project_literal_string_constant", [{"$project": {"c": "constant", "name": 1}}])
pt("project_literal_operator", [{"$project": {"c": {"$literal": "$name"}, "one": {"$literal": 1}}}])
pt("project_number_constant_is_inclusion", [{"$project": {"name": 1, "x": 5}}])
pt("project_boolean_true_inclusion", [{"$project": {"name": True}}])
pt("project_array_expression", [{"$project": {"arr2": ["$name", "$x", 1]}}])
pt("project_array_of_docs_field_path", [{"$project": {"ks": "$docs.k"}}])
pt("project_array_of_docs_nested_include", [{"$project": {"docs.k": 1}}])
pt("project_array_of_docs_nested_computed", [{"$project": {"docs.n": "$name"}}])
pt("project_exclusion_still_works", [{"$project": {"arr": 0, "docs": 0, "y": 0}}])
pt("project_exclusion_nested", [{"$project": {"y.z": 0, "docs": 0}}])
pt("project_cond_expression", [{"$project": {"label": {"$cond": [{"$gt": ["$x", 1]}, "big", "small"]}}}])
pt("project_size_of_array", [{"$project": {"n": {"$cond": [{"$isArray": "$arr"}, {"$size": "$arr"}, -1]}}}])
pt("project_ifnull", [{"$project": {"v": {"$ifNull": ["$x", "none"]}}}])
pt("project_concat", [{"$project": {"v": {"$concat": ["$name", "-", {"$toUpper": "$name"}]}}}])
pt("project_remove_variable", [{"$project": {"v": "$$REMOVE", "name": 1}}])
pt("project_root_variable", [{"$project": {"copy": "$$ROOT"}}])
pt("project_exclusion_and_inclusion_mix_error", [{"$project": {"name": 1, "x": 0}}], sorted=False)
pt("project_computed_and_exclusion_mix_error", [{"$project": {"a": "$name", "x": 0}}], sorted=False)
pt("project_empty_error", [{"$project": {}}], sorted=False)
pt("project_non_object_error", [{"$project": 5}], sorted=False)
pt("project_unknown_expression_operator_error", [{"$project": {"a": {"$bogus": 1}}}], sorted=False)
pt("project_path_collision_error", [{"$project": {"a": "$name", "a.b": "$x"}}], sorted=False)
pt("project_path_collision_reverse_error", [{"$project": {"a.b": "$x", "a": "$name"}}], sorted=False)
pt("project_path_collision_include_error", [{"$project": {"a": 1, "a.b": 1}}], sorted=False)
pt("project_unknown_operator_nested_in_args_error", [{"$project": {"a": {"$add": [{"$bogus": 1}]}}}], sorted=False)
pt("project_unknown_operator_in_subdocument_error", [{"$project": {"a": {"x": {"$bogus": 1}}}}], sorted=False)
pt("project_computed_after_included_even_if_earlier_in_document", [{"$project": {"name": 1, "x": {"$add": ["$x", 1]}}}], docs=[{"_id": 1, "x": 1, "name": "n"}])
pt("project_computed_before_included_in_spec", [{"$project": {"x": {"$add": ["$x", 1]}, "name": 1}}], docs=[{"_id": 1, "x": 1, "name": "n"}])
pt("project_id_computed_after_included", [{"$project": {"_id": {"$concat": ["$name", "!"]}, "x": 1}}], docs=[{"_id": 1, "x": 1, "name": "n"}], sorted=False)
pt("project_id_computed_between_other_computed", [{"$project": {"a": "$x", "_id": "$name", "b": "$x"}}], docs=[{"_id": 1, "x": 1, "name": "n"}], sorted=False)
pt("project_dotted_computed_and_included_same_parent_order", [{"$project": {"y.n": "$name", "y.z": 1}}])
pt("addfields_two_operators_in_expression_error", [{"$addFields": {"a": {"$add": [1], "$subtract": [1, 2]}}}], sorted=False)
pt("addfields_nested_into_nested_arrays", [{"$addFields": {"arr.t": "v"}}], docs=[{"_id": 1, "arr": [1, {"a": 1}, [2]]}])
pt("addfields_object_form_into_arrays", [{"$addFields": {"arr": {"t": "v"}}}], docs=[{"_id": 1, "arr": [1, {"a": 1}, [2]]}])
pt("addfields_missing_source_removes_existing_field_check", [{"$addFields": {"x": "$nosuch"}}], docs=[{"_id": 1, "x": 5, "k": 1}])
pt("project_two_operators_in_expression_error", [{"$project": {"a": {"$add": [1], "$subtract": [1, 2]}}}], sorted=False)
pt("project_then_group", [{"$project": {"k": {"$cond": [{"$gt": ["$x", 1]}, "hi", "lo"]}, "x": 1}}, {"$group": {"_id": "$k", "n": {"$sum": 1}}}])
# $addFields / $set
pt("addfields_new_field", [{"$addFields": {"z": 1}}])
pt("addfields_computed", [{"$addFields": {"sum": {"$add": ["$x", 1]}}}])
pt("addfields_overwrite_in_place", [{"$addFields": {"name": {"$toUpper": "$name"}}}])
pt("addfields_multiple_see_original_values", [{"$addFields": {"x": {"$add": ["$x", 1]}, "x2": "$x"}}])
pt("addfields_nested_dotted_creates", [{"$addFields": {"n.a.b": "$name"}}])
pt("addfields_nested_dotted_into_existing_subdoc", [{"$addFields": {"y.extra": "$name"}}])
pt("addfields_nested_object_form_merges", [{"$addFields": {"y": {"extra": "$name"}}}])
pt("addfields_nested_over_non_document_replaces", [{"$addFields": {"x.sub": "v"}}])
pt("addfields_nested_into_array_of_docs", [{"$addFields": {"docs.tag": "t"}}])
pt("addfields_array_literal", [{"$addFields": {"l": [1, "$name"]}}])
pt("addfields_literal_values_kept_as_is", [{"$addFields": {"a": 1, "b": "str", "c": True, "d": None, "e": {"$literal": "$name"}}}])
pt("addfields_missing_source_removes_nothing_new", [{"$addFields": {"v": "$nosuch"}}])
pt("addfields_missing_source_over_existing_field", [{"$addFields": {"name": "$nosuch"}}])
pt("addfields_remove_variable_deletes_field", [{"$addFields": {"name": "$$REMOVE"}}])
pt("addfields_id_overwrite", [{"$addFields": {"_id": {"$add": ["$_id", 10]}}}], sorted=False)
pt("addfields_root_copy", [{"$addFields": {"snapshot": "$$ROOT"}}])
pt("addfields_then_match_on_new_field", [{"$addFields": {"dbl": {"$multiply": ["$x", 2]}}}, {"$match": {"dbl": {"$gt": 2}}}])
pt("addfields_then_group", [{"$addFields": {"k": {"$cond": [{"$gt": ["$_id", 2]}, "hi", "lo"]}}}, {"$group": {"_id": "$k", "n": {"$sum": 1}}}])
pt("set_alias", [{"$set": {"z": {"$add": ["$x", 1]}}}])
pt("set_alias_overwrite", [{"$set": {"x": "replaced"}}])
pt("addfields_empty_spec_is_noop", [{"$addFields": {}}])
pt("set_empty_spec_is_noop", [{"$set": {}}])
pt("addfields_non_object_error", [{"$addFields": 5}], sorted=False)
pt("set_non_object_error", [{"$set": "x"}], sorted=False)
pt("addfields_path_collision_error", [{"$addFields": {"a": 1, "a.b": 2}}], sorted=False)
pt("addfields_unknown_operator_error", [{"$addFields": {"a": {"$nope": 1}}}], sorted=False)
# $unwind
pt("unwind_string_path", [{"$unwind": "$arr"}], sorted=False)
pt("unwind_object_path", [{"$unwind": {"path": "$arr"}}], sorted=False)
pt("unwind_include_array_index", [{"$unwind": {"path": "$arr", "includeArrayIndex": "i"}}], sorted=False)
pt("unwind_index_type_is_long", [{"$unwind": {"path": "$arr", "includeArrayIndex": "i"}}, {"$project": {"t": {"$cond": [{"$eq": ["$i", 0]}, "zero", "other"]}, "i": 1}}], sorted=False)
pt("unwind_preserve_null_and_empty", [{"$unwind": {"path": "$arr", "preserveNullAndEmptyArrays": True}}], sorted=False)
pt("unwind_preserve_with_index", [{"$unwind": {"path": "$arr", "includeArrayIndex": "idx", "preserveNullAndEmptyArrays": True}}], sorted=False)
pt("unwind_preserve_false_explicit", [{"$unwind": {"path": "$arr", "preserveNullAndEmptyArrays": False}}], sorted=False)
pt("unwind_array_of_docs", [{"$unwind": "$docs"}], sorted=False)
pt("unwind_array_of_docs_preserve", [{"$unwind": {"path": "$docs", "preserveNullAndEmptyArrays": True}}], sorted=False)
pt("unwind_then_group_sum", [{"$unwind": "$arr"}, {"$group": {"_id": "$_id", "n": {"$sum": 1}}}])
pt("unwind_nested_path", [{"$unwind": "$y"}], sorted=False)
pt("unwind_nested_field_path_in_subdoc", [{"$unwind": "$y.z"}], sorted=False)
pt("unwind_nonexistent_field_drops_all", [{"$unwind": "$nosuch"}], sorted=False)
pt("unwind_nonexistent_field_preserved", [{"$unwind": {"path": "$nosuch", "preserveNullAndEmptyArrays": True}}], sorted=False)
pt("unwind_scalar_is_single_element", [{"$unwind": "$name"}], sorted=False)
pt("unwind_index_field_nested_path", [{"$unwind": {"path": "$arr", "includeArrayIndex": "meta.i"}}], sorted=False)
pt("unwind_index_overwrites_existing_field", [{"$unwind": {"path": "$arr", "includeArrayIndex": "name"}}], sorted=False)
pt("unwind_twice", [{"$unwind": "$docs"}, {"$unwind": "$docs.k"}], sorted=False)
pt("unwind_after_unwind_array_of_arrays", [{"$unwind": "$arr"}, {"$unwind": "$arr"}], sorted=False)
pt("unwind_then_project_field_from_element", [{"$unwind": "$docs"}, {"$project": {"k": "$docs.k"}}], sorted=False)
pt("unwind_then_sort_by_element", [{"$unwind": "$arr"}, {"$match": {"arr": {"$type": "number"}}}, {"$sort": {"arr": -1, "_id": 1}}], sorted=False)
pt("unwind_preserves_order_and_duplicates", [{"$unwind": "$a"}], docs=[{"_id": 1, "a": [3, 1, 3, 2]}], sorted=False)
pt("unwind_error_no_dollar", [{"$unwind": "arr"}], sorted=False)
pt("unwind_error_object_no_dollar", [{"$unwind": {"path": "arr"}}], sorted=False)
pt("unwind_error_missing_path", [{"$unwind": {"includeArrayIndex": "i"}}], sorted=False)
pt("unwind_error_path_not_string", [{"$unwind": {"path": 5}}], sorted=False)
pt("unwind_error_index_not_string", [{"$unwind": {"path": "$arr", "includeArrayIndex": 5}}], sorted=False)
pt("unwind_error_index_dollar_prefixed", [{"$unwind": {"path": "$arr", "includeArrayIndex": "$i"}}], sorted=False)
pt("unwind_error_preserve_not_bool", [{"$unwind": {"path": "$arr", "preserveNullAndEmptyArrays": "yes"}}], sorted=False)
pt("unwind_error_unknown_option", [{"$unwind": {"path": "$arr", "bogus": 1}}], sorted=False)
pt("unwind_error_number_spec", [{"$unwind": 5}], sorted=False)
pt("unwind_error_empty_path", [{"$unwind": "$"}], sorted=False)
pt("unwind_error_trailing_dot", [{"$unwind": "$a."}], sorted=False)
pt("unwind_error_double_dot", [{"$unwind": "$a..b"}], sorted=False)
# $lookup
LA = [{"_id": 1, "cust": "a", "n": 1}, {"_id": 2, "cust": "b", "n": 2}, {"_id": 3, "cust": "zz", "n": 3}, {"_id": 4, "n": 4}, {"_id": 5, "cust": None, "n": 5}, {"_id": 6, "cust": ["a", "b"], "n": 6}, {"_id": 7, "cust": Dbl(1.0), "n": 7}, {"_id": 8, "cust": {"k": 1}, "n": 8}]
# Inserted in _id order: this server's natural order is _id order, MongoDB's is insertion order.
LB = [{"_id": 1, "name": "One", "k": 1}, {"_id": 10, "name": "NoKey"}, {"_id": 11, "name": "NullKey", "k": None}, {"_id": 12, "name": "Doc", "k": {"k": 1}}, {"_id": 13, "name": "DupA", "k": "a"}, {"_id": "a", "name": "Alice", "k": "a"}, {"_id": "b", "name": "Bob", "k": "b"}, {"_id": "c", "name": "Carl", "k": ["a", "c"]}]


def lt(name, stage, sort=True, **kw):
    pipeline = [{"$lookup": stage}] + ([{"$sort": {"_id": 1}}] if sort else [])
    add(f"lookup.{name}", [ins("orders", LA), ins("people", LB), agg("orders", pipeline)], "aggregate: $lookup", **kw)


lt("basic", {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"})
lt("by_foreign_id", {"from": "people", "localField": "cust", "foreignField": "_id", "as": "who"})
lt("no_match_gives_empty_array", {"from": "people", "localField": "n", "foreignField": "k", "as": "who"})
lt("local_array_matches_any_element", {"from": "people", "localField": "cust", "foreignField": "_id", "as": "who"})
lt("foreign_array_matches_any_element", {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"})
lt("null_and_missing_match_null_and_missing_foreign", {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"})
lt("missing_local_matches_missing_foreign", {"from": "people", "localField": "nosuch", "foreignField": "nosuch2", "as": "who"})
lt("numeric_equivalence", {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"})
lt("embedded_document_equality", {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"})
lt("nested_as_path", {"from": "people", "localField": "cust", "foreignField": "k", "as": "joined.who"})
lt("as_overwrites_existing_field", {"from": "people", "localField": "cust", "foreignField": "k", "as": "cust"})
lt("as_overwrites_id", {"from": "people", "localField": "cust", "foreignField": "k", "as": "n"})
lt("nested_local_and_foreign_fields", {"from": "people", "localField": "cust.k", "foreignField": "k.k", "as": "who"})
lt("from_missing_collection", {"from": "nowhere", "localField": "cust", "foreignField": "k", "as": "who"})
lt("from_same_collection", {"from": "orders", "localField": "n", "foreignField": "_id", "as": "self"})
add("lookup.then_unwind", [ins("orders", LA), ins("people", LB), agg("orders", [{"$lookup": {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"}}, {"$unwind": "$who"}, {"$project": {"n": 1, "who": "$who.name"}}, {"$sort": {"n": 1, "who": 1}}])], "aggregate: $lookup")
add("lookup.then_unwind_preserve", [ins("orders", LA), ins("people", LB), agg("orders", [{"$lookup": {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"}}, {"$unwind": {"path": "$who", "preserveNullAndEmptyArrays": True}}, {"$project": {"n": 1, "who": "$who.name"}}, {"$sort": {"n": 1, "who": 1}}])], "aggregate: $lookup")
add("lookup.then_match_on_joined", [ins("orders", LA), ins("people", LB), agg("orders", [{"$lookup": {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"}}, {"$match": {"who.name": "Alice"}}, {"$project": {"n": 1}}, {"$sort": {"n": 1}}])], "aggregate: $lookup")
add("lookup.then_group_count", [ins("orders", LA), ins("people", LB), agg("orders", [{"$lookup": {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"}}, {"$project": {"n": 1, "c": {"$size": "$who"}}}, {"$group": {"_id": "$c", "ids": {"$sum": 1}}}, {"$sort": {"_id": 1}}])], "aggregate: $lookup")
add("lookup.match_before_lookup", [ins("orders", LA), ins("people", LB), agg("orders", [{"$match": {"n": {"$lte": 2}}}, {"$lookup": {"from": "people", "localField": "cust", "foreignField": "_id", "as": "who"}}, {"$sort": {"_id": 1}}])], "aggregate: $lookup")
add("lookup.two_lookups", [ins("orders", LA), ins("people", LB), agg("orders", [{"$match": {"n": {"$lte": 2}}}, {"$lookup": {"from": "people", "localField": "cust", "foreignField": "_id", "as": "p1"}}, {"$lookup": {"from": "orders", "localField": "n", "foreignField": "n", "as": "p2"}}, {"$sort": {"_id": 1}}])], "aggregate: $lookup")
add("lookup.empty_local_collection", [ins("people", LB), agg("orders", [{"$lookup": {"from": "people", "localField": "cust", "foreignField": "k", "as": "who"}}])], "aggregate: $lookup")
add("lookup.1000_docs_join", [{"op": "insert_generated", "coll": "big", "n": 1000, "kind": "basic"}, ins("grp", [{"_id": i, "label": f"g{i}"} for i in range(10)]), agg("big", [{"$lookup": {"from": "grp", "localField": "grp", "foreignField": "_id", "as": "g"}}, {"$unwind": "$g"}, {"$group": {"_id": "$g.label", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])], "aggregate: $lookup")
for label, stage in [("error_missing_as", {"from": "people", "localField": "cust", "foreignField": "k"}), ("error_missing_from", {"localField": "cust", "foreignField": "k", "as": "w"}), ("error_local_without_foreign", {"from": "people", "localField": "cust", "as": "w"}), ("error_foreign_without_local", {"from": "people", "foreignField": "k", "as": "w"}), ("error_neither_fields_nor_pipeline", {"from": "people", "as": "w"}), ("error_unknown_option", {"from": "people", "localField": "cust", "foreignField": "k", "as": "w", "bogus": 1}), ("error_from_not_string", {"from": 5, "localField": "cust", "foreignField": "k", "as": "w"}), ("error_as_not_string", {"from": "people", "localField": "cust", "foreignField": "k", "as": 5})]:
    lt(label, stage, sort=False)
add("lookup.error_not_an_object", [ins("orders", LA), agg("orders", [{"$lookup": "people"}])], "aggregate: $lookup")
lt("extra_pipeline_form", {"from": "people", "pipeline": [{"$match": {}}], "as": "w"}, category="extra")
lt("extra_let_pipeline_form", {"from": "people", "let": {"c": "$cust"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$k", "$$c"]}}}], "as": "w"}, category="extra")
lt("extra_local_foreign_with_pipeline", {"from": "people", "localField": "cust", "foreignField": "k", "pipeline": [{"$match": {"name": "Alice"}}], "as": "w"}, category="extra")
# $group with expressions and $addToSet
GD = [{"_id": 1, "k": "a", "v": 10, "tags": ["x", "y"], "o": {"c": "p"}}, {"_id": 2, "k": "a", "v": 20, "tags": ["y"], "o": {"c": "q"}}, {"_id": 3, "k": "b", "v": 5, "tags": [], "o": {"c": "p"}}, {"_id": 4, "k": "b", "tags": ["z"]}, {"_id": 5, "v": 7, "o": {"c": "p"}}, {"_id": 6, "k": None, "v": 1}]


def gt(name, group, post=None, docs=None, **kw):
    add(f"group.{name}", [ins("c", docs or GD), agg("c", [{"$group": group}] + (post if post is not None else [{"$sort": {"_id": 1}}]))], "aggregate: $group with expressions", **kw)


gt("key_expression_cond", {"_id": {"$cond": [{"$gt": ["$v", 6]}, "big", "small"]}, "n": {"$sum": 1}})
gt("key_compound_document", {"_id": {"k": "$k", "c": "$o.c"}, "n": {"$sum": 1}})
gt("key_compound_missing_fields_omitted", {"_id": {"k": "$k", "zz": "$zz"}, "n": {"$sum": 1}})
gt("key_nested_path", {"_id": "$o.c", "n": {"$sum": 1}})
gt("key_constant", {"_id": 1, "n": {"$sum": 1}})
gt("key_null_literal", {"_id": None, "n": {"$sum": "$v"}})
gt("key_array_value_groups_by_whole_array", {"_id": "$tags", "n": {"$sum": 1}}, post=[], unordered=True)
gt("key_expression_concat", {"_id": {"$concat": [{"$ifNull": ["$k", "none"]}, "!"]}, "n": {"$sum": 1}})
gt("key_numeric_equivalence", {"_id": "$v", "n": {"$sum": 1}}, docs=[{"_id": 1, "v": 1}, {"_id": 2, "v": Dbl(1.0)}, {"_id": 3, "v": Int64(1)}])
gt("sum_expression_multiply", {"_id": "$k", "s": {"$sum": {"$multiply": ["$v", 2]}}})
gt("sum_conditional_count", {"_id": "$k", "big": {"$sum": {"$cond": [{"$gt": ["$v", 6]}, 1, 0]}}})
gt("sum_literal_one_counts", {"_id": "$k", "n": {"$sum": 1}})
gt("sum_missing_and_non_numeric_ignored", {"_id": None, "s": {"$sum": "$k"}})
gt("sum_of_array_field_is_ignored_as_non_numeric", {"_id": None, "s": {"$sum": "$tags"}})
gt("sum_types_int_long_double", {"_id": None, "s": {"$sum": "$v"}}, docs=[{"_id": 1, "v": 1}, {"_id": 2, "v": Int64(2)}, {"_id": 3, "v": 0.5}])
gt("sum_int_overflow_to_long", {"_id": None, "s": {"$sum": "$v"}}, docs=[{"_id": 1, "v": 2147483647}, {"_id": 2, "v": 1}])
gt("avg_expression", {"_id": "$k", "a": {"$avg": {"$add": ["$v", 1]}}})
gt("min_max_expression", {"_id": "$k", "mn": {"$min": {"$subtract": ["$v", 1]}}, "mx": {"$max": {"$add": ["$v", 1]}}})
gt("first_last_expression", {"_id": "$k", "f": {"$first": {"$toUpper": "$k"}}, "l": {"$last": {"$ifNull": ["$v", -1]}}}, post=[{"$sort": {"_id": 1}}])
gt("push_expression_object", {"_id": "$k", "items": {"$push": {"id": "$_id", "dbl": {"$multiply": ["$v", 2]}}}})
gt("push_missing_skipped", {"_id": "$k", "vs": {"$push": "$v"}})
gt("push_arrays_nested", {"_id": "$k", "ts": {"$push": "$tags"}})
gt("addtoset_scalar_size", {"_id": "$k", "s": {"$addToSet": "$k"}})
gt("addtoset_size_after_dedupe", {"_id": "$k", "s": {"$addToSet": "$o.c"}}, post=[{"$project": {"n": {"$size": "$s"}}}, {"$sort": {"_id": 1}}])
gt("addtoset_numeric_equivalence_size", {"_id": None, "s": {"$addToSet": "$v"}}, post=[{"$project": {"n": {"$size": "$s"}}}], docs=[{"_id": 1, "v": 1}, {"_id": 2, "v": Dbl(1.0)}, {"_id": 3, "v": Int64(1)}, {"_id": 4, "v": 2}])
gt("addtoset_documents_size", {"_id": None, "s": {"$addToSet": "$o"}}, post=[{"$project": {"n": {"$size": "$s"}}}])
gt("addtoset_missing_skipped_size", {"_id": None, "s": {"$addToSet": "$v"}}, post=[{"$project": {"n": {"$size": "$s"}}}])
gt("addtoset_arrays_whole_value_size", {"_id": None, "s": {"$addToSet": "$tags"}}, post=[{"$project": {"n": {"$size": "$s"}}}])
gt("addtoset_expression_size", {"_id": None, "s": {"$addToSet": {"$toUpper": "$k"}}}, post=[{"$project": {"n": {"$size": "$s"}}}])
gt("addtoset_single_value_exact", {"_id": "$k", "s": {"$addToSet": "$v"}}, docs=[{"_id": 1, "k": "a", "v": 1}, {"_id": 2, "k": "b", "v": 2}])
gt("count_accumulator", {"_id": "$k", "n": {"$count": {}}})
gt("multiple_accumulators_with_expressions", {"_id": "$k", "n": {"$sum": 1}, "tot": {"$sum": "$v"}, "mx": {"$max": "$v"}, "tags": {"$push": {"$size": {"$ifNull": ["$tags", []]}}}})
gt("then_project_computed", {"_id": "$k", "tot": {"$sum": "$v"}, "n": {"$sum": 1}}, post=[{"$project": {"avg": {"$divide": ["$tot", "$n"]}}}, {"$sort": {"_id": 1}}])
gt("unknown_accumulator_error", {"_id": "$k", "x": {"$bogus": "$v"}}, post=[])
gt("accumulator_not_object_error", {"_id": "$k", "x": 5}, post=[])
gt("missing_id_error", {"n": {"$sum": 1}}, post=[])
gt("dotted_accumulator_name_error", {"_id": 1, "a.b": {"$sum": 1}}, post=[])
gt("accumulator_two_keys_error", {"_id": "$k", "x": {"$sum": 1, "$max": 1}}, post=[])
add("group.then_unwind_then_group", [ins("c", GD), agg("c", [{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}, "ks": {"$push": "$k"}}}, {"$sort": {"_id": 1}}])], "aggregate: $group with expressions")
add("group.pipeline_orders_report", [ins("orders", ORDERS), agg("orders", [{"$addFields": {"total": {"$multiply": [{"$ifNull": ["$amt", 0]}, "$qty"]}}}, {"$group": {"_id": "$cust", "revenue": {"$sum": "$total"}, "n": {"$sum": 1}}}, {"$sort": {"revenue": -1, "_id": 1}}, {"$project": {"_id": 0, "customer": "$_id", "revenue": 1, "n": 1}}])], "aggregate: $group with expressions")

import gen_tests_indexes
gen_tests_indexes.register(globals())
import gen_tests_txn
gen_tests_txn.register(globals())
import gen_tests_changes
gen_tests_changes.register(globals())
import gen_tests_where
gen_tests_where.register(globals())
import gen_tests_ops
gen_tests_ops.register(globals())
import gen_tests_stages
gen_tests_stages.register(globals())
import gen_tests_updates
gen_tests_updates.register(globals())
import gen_tests_text
gen_tests_text.register(globals())
import gen_tests_geo
gen_tests_geo.register(globals())

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "tests.json"
    names = [t["name"] for t in TESTS]
    assert len(names) == len(set(names)), [n for n in names if names.count(n) > 1]
    with open(out, "w") as f:
        json.dump(TESTS, f, indent=0)
    from collections import Counter
    print(f"{len(TESTS)} tests ({Counter(t['category'] for t in TESTS)}) -> {out}")
