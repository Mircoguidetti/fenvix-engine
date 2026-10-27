"""Item 8: $out and $merge into another database, renameCollection between databases (and the validation of their arguments)."""
from gaps_py import case
from pymongo import ASCENDING
from pymongo.collation import Collation
from pymongo.errors import OperationFailure
import re

UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
PLAN = "PlanExecutor error during aggregation :: caused by :: "


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        m = e.details.get("errmsg") or ""
        m = m[len(PLAN):] if m.startswith(PLAN) else m
        return {"err": [e.code, e.details.get("codeName"), UUID_RE.sub("<uuid>", m)]}


def other(db):
    return db.client[db.name + "_x"]


def state(*dbs):
    out = {}
    for db in dbs:
        for c in sorted(db.list_collection_names()):
            info = next(db.list_collections(filter={"name": c}))
            out[f"{db.name.split('_')[-1] if db.name.endswith('_x') else 'main'}.{c}"] = {"docs": list(db[c].find().sort("_id", 1)), "indexes": [{k: v for k, v in i.items() if k != "v"} for i in db[c].list_indexes()], "options": info.get("options")}
    return out


def seed(db, n=6):
    db.src.insert_many([{"_id": i, "v": i, "g": i % 3, "s": f"s{i}"} for i in range(n)])
    db.src.create_index("v", name="v_1")


@case("g08.out.creates_in_the_other_database")
def _out_new(client, db):
    seed(db)
    o = other(db)
    r = tr(lambda: list(db.src.aggregate([{"$match": {"v": {"$gt": 1}}}, {"$out": {"db": o.name, "coll": "dst"}}])))
    res = {"result": r, "state": state(db, o), "dbs": sorted(n for n in client.list_database_names() if n in (db.name, o.name))}
    client.drop_database(o.name)
    return res


@case("g08.out.replaces_and_keeps_indexes_and_options")
def _out_replace(client, db):
    seed(db)
    o = other(db)
    o.create_collection("dst", collation=Collation("en", strength=2))
    o.dst.insert_many([{"_id": "a", "v": 99}, {"_id": "b", "v": 98}])
    o.dst.create_index("g", name="g_1")
    o.dst.create_index("v", name="v_u", unique=True)
    r1 = tr(lambda: list(db.src.aggregate([{"$match": {"v": {"$lt": 2}}}, {"$out": {"db": o.name, "coll": "dst"}}])))
    s1 = state(o)
    r2 = tr(lambda: list(db.src.aggregate([{"$project": {"v": {"$literal": 1}}}, {"$out": {"db": o.name, "coll": "dst"}}])))
    s2 = state(o)
    client.drop_database(o.name)
    return {"r1": r1, "s1": s1, "violating": r2, "after_violation": s2}


@case("g08.out.same_database_forms")
def _out_same(client, db):
    seed(db)
    out = {}
    out["explicit_db"] = tr(lambda: list(db.src.aggregate([{"$out": {"db": db.name, "coll": "same"}}])))
    out["onto_source"] = tr(lambda: list(db.src.aggregate([{"$match": {"v": {"$lt": 3}}}, {"$out": "src"}])))
    out["state"] = state(db)
    return out


@case("g08.out.argument_errors")
def _out_errors(client, db):
    seed(db, 2)
    specs = [{"$out": ""}, {"$out": "a$b"}, {"$out": 5}, {"$out": None}, {"$out": {}}, {"$out": {"db": "x"}}, {"$out": {"coll": "x"}}, {"$out": {"db": 5, "coll": "x"}}, {"$out": {"db": "x", "coll": 5}}, {"$out": {"db": "x", "coll": ""}}, {"$out": {"db": "x", "coll": "a$b"}},
             {"$out": {"db": "local", "coll": "x"}}, {"$out": {"db": "config", "coll": "x"}}, {"$out": {"db": "admin", "coll": "x"}}, {"$out": "system.x"}, {"$out": {"db": "x" * 70, "coll": "x"}}, {"$out": {"db": "a.b", "coll": "x"}}, {"$out": {"db": "", "coll": "x"}},
             {"$out": {"db": "xx", "coll": "y", "foo": 1}}, {"$out": []}, {"$out": "a\0b"}]
    out = []
    for s in specs:
        out.append(tr(lambda: list(db.src.aggregate([s]))))
    out.append(tr(lambda: list(db.src.aggregate([{"$out": {"db": other(db).name, "coll": "q"}}, {"$limit": 1}]))))
    client.drop_database(other(db).name)
    return out


@case("g08.merge.into_the_other_database")
def _merge(client, db):
    seed(db)
    o = other(db)
    out = {}
    out["first"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "mrg"}}}])))
    out["again_merge"] = tr(lambda: list(db.src.aggregate([{"$addFields": {"w": 1}}, {"$merge": {"into": {"db": o.name, "coll": "mrg"}, "whenMatched": "merge"}}])))
    out["replace"] = tr(lambda: list(db.src.aggregate([{"$match": {"v": 2}}, {"$project": {"only": {"$literal": 1}}}, {"$merge": {"into": {"db": o.name, "coll": "mrg"}, "whenMatched": "replace"}}])))
    out["keep"] = tr(lambda: list(db.src.aggregate([{"$addFields": {"k": 1}}, {"$merge": {"into": {"db": o.name, "coll": "mrg"}, "whenMatched": "keepExisting"}}])))
    out["pipeline_let"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "mrg"}, "let": {"x": "$v"}, "whenMatched": [{"$set": {"q": "$$x", "n": "$$new.v"}}]}}])))
    out["fail"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "mrg"}, "whenMatched": "fail"}}])))
    out["discard_new"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "mrg2"}, "whenNotMatched": "discard"}}])))
    out["fail_missing"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "mrg2"}, "whenNotMatched": "fail"}}])))
    out["same_db_only_coll"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"coll": "mrg3"}}}])))
    out["empty_db_is_current"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": "", "coll": "mrg4"}}}])))
    out["state"] = state(db, o)
    client.drop_database(o.name)
    return out


@case("g08.merge.on_fields_and_unique_indexes")
def _merge_on(client, db):
    seed(db)
    o = other(db)
    o.t2.insert_many([{"_id": 50, "k": 1, "v": 1}, {"_id": 51, "k": 2, "v": 2}])
    o.t2.create_index("k", unique=True, name="k_u")
    o.t2.create_index([("v", 1), ("g", 1)], unique=True, name="vg_u")
    o.t2.create_index("s", name="s_nu")
    o.t2.create_index("w", unique=True, name="w_u", sparse=True)
    o.t2.create_index("z", unique=True, name="z_p", partialFilterExpression={"z": {"$gt": 0}})
    o.t2.create_index("c", unique=True, name="c_ci", collation=Collation("en", strength=2))
    out = {}
    for name, on in (("k", "k"), ("v_g", ["v", "g"]), ("g_v", ["g", "v"]), ("v", "v"), ("s", "s"), ("w", "w"), ("z", "z"), ("c", "c"), ("id", "_id"), ("k_id", ["k", "_id"]), ("missing_field", "nosuch"), ("empty_array", []), ("non_string", 5), ("bad_elem", ["k", 5])):
        out[name] = tr(lambda: list(db.src.aggregate([{"$project": {"k": "$v", "v": 1, "g": 1}}, {"$merge": {"into": {"db": o.name, "coll": "t2"}, "on": on, "whenMatched": "merge"}}])))
    out["missing_target_on_id"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "fresh1"}, "on": "_id"}}])))
    out["missing_target_on_other"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "fresh2"}, "on": "v"}}])))
    out["missing_target_on_two"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": {"db": o.name, "coll": "fresh3"}, "on": ["v", "_id"]}}])))
    out["state"] = state(o)
    client.drop_database(o.name)
    return out


@case("g08.merge.argument_errors")
def _merge_errors(client, db):
    seed(db, 2)
    specs = [{"$merge": 5}, {"$merge": ""}, {"$merge": {}}, {"$merge": {"into": 5}}, {"$merge": {"into": {}}}, {"$merge": {"into": {"db": "x"}}}, {"$merge": {"into": {"coll": ""}}}, {"$merge": {"into": {"db": "a.b", "coll": "c"}}}, {"$merge": {"into": {"db": "admin", "coll": "c"}}},
             {"$merge": {"into": {"db": "local", "coll": "c"}}}, {"$merge": {"into": {"db": "config", "coll": "c"}}}, {"$merge": {"into": "system.q"}}, {"$merge": {"into": "t1", "whenMatched": "x"}}, {"$merge": {"into": "t1", "whenNotMatched": "x"}},
             {"$merge": {"into": "t1", "let": 5}}, {"$merge": {"into": "t1", "foo": 1}}, {"$merge": {"into": "t1", "whenMatched": "fail", "whenNotMatched": "fail"}}, {"$merge": {"into": "t1", "whenMatched": 5}}, {"$merge": {"into": {"db": 5, "coll": "c"}}},
             {"$merge": {"into": "src", "whenMatched": "replace"}}, {"$merge": {"into": {"db": db.name, "coll": "src"}, "whenMatched": "merge"}}]
    out = []
    for s in specs:
        out.append(tr(lambda: list(db.src.aggregate([s]))))
    out.append(tr(lambda: list(db.src.aggregate([{"$merge": {"into": "q"}}, {"$limit": 1}]))))
    return out


@case("g08.merge.source_missing_on_field_and_dup_in_target")
def _merge_runtime(client, db):
    o = other(db)
    db.src.insert_many([{"_id": 1, "k": 1}, {"_id": 2, "k": None}, {"_id": 3}, {"_id": 4, "k": [1]}])
    o.t2.create_index("k", unique=True, name="k_u")
    out = {}
    for name, m in (("null", {"$match": {"_id": 2}}), ("missing", {"$match": {"_id": 3}}), ("array", {"$match": {"_id": 4}}), ("ok", {"$match": {"_id": 1}})):
        out[name] = tr(lambda: list(db.src.aggregate([m, {"$merge": {"into": {"db": o.name, "coll": "t2"}, "on": "k"}}])))
    o.t3.insert_one({"_id": 9, "k": 5})
    o.t3.create_index("k", unique=True, name="k_u")
    out["dup_k"] = tr(lambda: list(db.src.aggregate([{"$match": {"_id": 1}}, {"$project": {"_id": 0, "x": {"$literal": 1}}}, {"$merge": {"into": {"db": o.name, "coll": "t3"}, "on": "_id"}}])))
    out["state"] = state(o)
    client.drop_database(o.name)
    return out


@case("g08.rename.across_databases")
def _rename(client, db):
    o = other(db)
    db.src.insert_many([{"_id": i, "v": i, "s": f"s{i}", "arr": [i, i + 1]} for i in range(8)])
    db.src.create_index("v", unique=True, name="v_u")
    db.src.create_index([("s", 1), ("v", -1)], name="sv")
    db.src.create_index("arr", name="arr_1")
    db.src.create_index("s", name="s_partial", partialFilterExpression={"v": {"$gt": 3}})
    out = {}
    out["rename"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.dst"}))
    out["state"] = state(db, o)
    out["find_after"] = {"unique_enforced": o.command({"insert": "dst", "documents": [{"_id": 100, "v": 3}]}).get("writeErrors", [{}])[0].get("code"), "index_used": [d["_id"] for d in o.dst.find({"v": 5})], "arr": [d["_id"] for d in o.dst.find({"arr": 3}).sort("_id", 1)]}
    out["natural_order"] = [d["_id"] for d in o.dst.find()]
    client.drop_database(o.name)
    return out


@case("g08.rename.keeps_collection_options_and_natural_order")
def _rename_opts(client, db):
    o = other(db)
    db.create_collection("c", collation=Collation("en", strength=2))
    db.c.insert_many([{"_id": s, "n": i} for i, s in enumerate(["z", "m", "a", "Q", "b"])])
    db.c.create_index("n", name="n_1", collation=Collation("fr"))
    out = {}
    out["rename"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.c", "to": f"{o.name}.c2"}))
    out["state"] = state(db, o)
    out["order"] = [d["_id"] for d in o.c2.find()]
    out["id_unique_ci"] = o.command({"insert": "c2", "documents": [{"_id": "A"}]}).get("writeErrors", [{}])[0].get("code")
    out["find_ci"] = [d["_id"] for d in o.c2.find({"_id": "q"})]
    client.drop_database(o.name)
    return out


@case("g08.rename.target_exists_and_dropTarget")
def _rename_target(client, db):
    o = other(db)
    db.src.insert_many([{"_id": 1, "a": 1}, {"_id": 2, "a": 2}])
    db.src.create_index("a", name="a_1")
    o.dst.insert_many([{"_id": 9, "b": 1}])
    o.dst.create_index("b", name="b_1")
    out = {}
    out["no_drop"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.dst"}))
    out["state_after_refusal"] = state(db, o)
    out["drop_false"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.dst", "dropTarget": False}))
    out["drop_true"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.dst", "dropTarget": True}))
    out["state"] = state(db, o)
    out["source_gone"] = db.src.count_documents({})
    client.drop_database(o.name)
    return out


@case("g08.rename.errors")
def _rename_errors(client, db):
    o = other(db)
    db.src.insert_one({"_id": 1})
    out = {}
    cases = {"missing_source": {"renameCollection": f"{db.name}.nope", "to": f"{o.name}.d"}, "same": {"renameCollection": f"{db.name}.src", "to": f"{db.name}.src"}, "same_cross_name": {"renameCollection": f"{db.name}.src", "to": f"{o.name}.src"},
             "bad_source": {"renameCollection": "nodot", "to": f"{o.name}.d"}, "bad_target": {"renameCollection": f"{db.name}.src", "to": "nodot"}, "system_target": {"renameCollection": f"{db.name}.src", "to": f"{o.name}.system.x"},
             "dollar_target": {"renameCollection": f"{db.name}.src", "to": f"{o.name}.a$b"}, "dropTarget_type": {"renameCollection": f"{db.name}.src", "to": f"{o.name}.d", "dropTarget": "x"}, "source_type": {"renameCollection": 5, "to": f"{o.name}.d"},
             "target_type": {"renameCollection": f"{db.name}.src", "to": 5}, "no_to": {"renameCollection": f"{db.name}.src"}, "system_source": {"renameCollection": f"{db.name}.system.js", "to": f"{o.name}.d"}, "empty_coll": {"renameCollection": f"{db.name}.", "to": f"{o.name}.d"}}
    for name, cmd in cases.items():
        out[name] = tr(lambda: client.admin.command(cmd))
    out["non_admin_db"] = tr(lambda: db.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.d"}))
    out["state"] = state(db)
    client.drop_database(o.name)
    return out


@case("g08.rename.same_database")
def _rename_same(client, db):
    db.src.insert_many([{"_id": i, "v": i} for i in range(4)])
    db.src.create_index("v", name="v_1")
    db.other.insert_one({"_id": 1})
    out = {}
    out["target_exists"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{db.name}.other"}))
    out["ok"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{db.name}.moved"}))
    out["drop"] = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.moved", "to": f"{db.name}.other", "dropTarget": True}))
    out["state"] = state(db)
    return out


@case("g08.rename.larger_collection")
def _rename_large(client, db):
    o = other(db)
    db.src.insert_many([{"_id": i, "v": i % 50, "s": "x" * (i % 17)} for i in range(3000)])
    db.src.create_index("v", name="v_1")
    r = tr(lambda: client.admin.command({"renameCollection": f"{db.name}.src", "to": f"{o.name}.big"}))
    res = {"rename": r, "count": o.big.count_documents({}), "src_count": db.src.count_documents({}), "idx": o.big.count_documents({"v": 7}), "first": [d["_id"] for d in o.big.find().limit(5)], "sum": next(o.big.aggregate([{"$group": {"_id": None, "s": {"$sum": "$v"}}}]))["s"]}
    client.drop_database(o.name)
    return res
