"""Differential tests for $where (server-side JavaScript on QuickJS)."""
import datetime

from bson import Int64


def register(g):
    add, ins, find, agg = g["add"], g["ins"], g["find"], g["agg"]
    G = "$where"

    def t(name, steps, **flags):
        add("where." + name, steps, G, **flags)

    D = [{"_id": 1, "a": 1, "b": 2, "s": "hello", "arr": [1, 2, 3], "o": {"x": 5}}, {"_id": 2, "a": 5, "b": 5, "s": "World", "arr": [], "o": {"x": 1}}, {"_id": 3, "a": 9, "b": 1, "s": "", "arr": [9], "o": None}, {"_id": 4, "a": None, "s": "abc"}]

    def w(code, **kw):
        return find("c", {"$where": code}, sort={"_id": 1}, **kw)

    t("expression_string", [ins("c", D), w("this.a > 2")])
    t("expression_compares_fields", [ins("c", D), w("this.a == this.b")])
    t("function_string", [ins("c", D), w("function() { return this.a < this.b; }")])
    t("return_statement_string", [ins("c", D), w("return this.a > 4")])
    t("array_length", [ins("c", D), w("this.arr.length > 0")])
    t("array_methods", [ins("c", D), w("this.arr.indexOf(9) >= 0")])
    t("string_methods", [ins("c", D), w("this.s.toLowerCase() == 'world'")])
    t("string_length", [ins("c", D), w("this.s.length === 0")])
    t("nested_field", [ins("c", D), w("this.o && this.o.x > 3")])
    t("math_functions", [ins("c", D), w("Math.max(this.a, this.b) == 5")])
    t("regex_test", [ins("c", D), w("/^h/.test(this.s)")])
    t("missing_field_is_undefined", [ins("c", D), w("this.b === undefined")])
    t("null_field", [ins("c", D), w("this.a === null")])
    t("truthy_number_result", [ins("c", D), w("this.a")])
    t("combined_with_other_conditions", [ins("c", D), find("c", {"a": {"$gte": 1}, "$where": "this.b > 1"}, sort={"_id": 1})])
    t("inside_or", [ins("c", D), find("c", {"$or": [{"a": 1}, {"$where": "this.a == 9"}]}, sort={"_id": 1})])
    t("count_documents", [ins("c", D), {"op": "count_documents", "coll": "c", "filter": {"$where": "this.a > 1"}}])
    t("delete_many", [ins("c", D), {"op": "delete_many", "coll": "c", "filter": {"$where": "this.a >= 5"}}, find("c", {}, sort={"_id": 1})])
    t("update_many", [ins("c", D), {"op": "update_many", "coll": "c", "filter": {"$where": "this.a >= 5"}, "update": {"$set": {"big": True}}}, find("c", {}, sort={"_id": 1})])
    t("find_one_and_update", [ins("c", D), {"op": "find_one_and_update", "coll": "c", "filter": {"$where": "this.a == 9"}, "update": {"$inc": {"a": 1}}, "return_document": "after"}])
    t("syntax_error", [ins("c", D), w("this.a >>>>> 1")])
    t("runtime_error", [ins("c", D), w("this.a.b.c.d")])
    t("throws", [ins("c", D), w("throw 'boom'")])
    t("not_a_string_error", [ins("c", D), find("c", {"$where": 5})])
    t("not_allowed_in_aggregate_match", [ins("c", D), agg("c", [{"$match": {"$where": "this.a > 1"}}])])
    t("empty_collection", [{"op": "create_collection", "coll": "c"}, w("this.a > 1")])
    t("int64_and_double_fields", [ins("c", [{"_id": 1, "l": Int64(5), "d": 2.5}, {"_id": 2, "l": Int64(1), "d": 9.5}]), w("this.l > 2 && this.d < 3")])
    t("date_field", [ins("c", [{"_id": 1, "d": datetime.datetime(2020, 1, 1)}, {"_id": 2, "d": datetime.datetime(2030, 1, 1)}]), w("this.d.getFullYear() > 2025")])
    t("boolean_field", [ins("c", [{"_id": 1, "f": True}, {"_id": 2, "f": False}]), w("this.f")])
    t("uses_index_candidates_then_where", [{"op": "create_index", "coll": "c", "keys": [["a", 1]]}, ins("c", D), find("c", {"a": {"$gt": 0}, "$where": "this.b == 5"}, sort={"_id": 1})])
