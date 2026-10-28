"""Item 7: collation. The collation document (validation, errors, expanded form), the locales and options of mongod 7.0,
and every place where a collation changes a result: filters, sort, count, distinct, aggregation (group, lookup, expressions),
update/delete/findAndModify statements, collection default collation, index collation."""
import csv
import itertools
import json
import os

from gaps_py import case
from pymongo import ASCENDING
from pymongo.collation import Collation
from pymongo.errors import OperationFailure

HERE = os.path.dirname(os.path.abspath(__file__))
WORDS = ["b", "B", "a", "A", "á", "Á", "z", "Z", "10", "9", "a b", "ab", "a-b", "résumé", "resume", "Resume", "ä", "ö", "o", "æ", "ß", "ss", "ñ", "n", "ç", "c",
         "ı", "i", "İ", "I", "ŝ", "s", "Ａ", "é", "é", "ll", "l", "ch", "c", "cz", "sz", "ø", "å", "ij", "y"]
CI = {"locale": "en", "strength": 2}


PLAN = "PlanExecutor error during aggregation :: caused by :: "


UUID_RE = __import__("re").compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


KEY_RE = __import__("re").compile(r'CollationKey\(0x[0-9a-f]+\)')


def err(e):
    # (mongod prefixes errors raised while an aggregation runs with PLAN; the engine does not: the code and name are what is compared)
    msg = e.details.get("errmsg") or ""
    msg = msg[len(PLAN):] if msg.startswith(PLAN) else msg
    return {"err": [e.code, e.details.get("codeName"), KEY_RE.sub('CollationKey(0x..)', UUID_RE.sub('<uuid>', msg))]}


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        return err(e)


def ids(cur):
    return [d["_id"] for d in cur]


def load(db, name="t", words=WORDS):
    db[name].insert_many([{"_id": i, "s": s} for i, s in enumerate(words)])
    return db[name]


# ---- validation of the collation document --------------------------------------------------------------------------

SPECS = [
    {}, {"locale": "en"}, {"locale": "xx"}, {"locale": "en", "strength": 0}, {"locale": "en", "strength": 6}, {"locale": "en", "strength": -1},
    {"locale": "en", "strength": 4}, {"locale": "en", "strength": 5}, {"locale": "en", "strength": "1"}, {"locale": "en", "strength": 2.0}, {"locale": "en", "strength": 2.5},
    {"locale": "en", "strength": True}, {"locale": "en", "caseFirst": "x"}, {"locale": "en", "caseFirst": 1}, {"locale": "en", "alternate": "x"}, {"locale": "en", "maxVariable": "x"},
    {"locale": "en", "foo": 1}, {"locale": 5}, {"locale": ""}, {"locale": "EN"}, {"locale": "en_US"}, {"locale": "en-US"}, {"locale": "en_US_POSIX"}, {"locale": "simple", "strength": 2},
    {"locale": "simple"}, {"locale": "simple", "foo": 1}, {"locale": "en", "caseLevel": "x"}, {"locale": "en", "numericOrdering": 1}, {"locale": "en", "backwards": "x"},
    {"locale": "en", "normalization": 1}, {"locale": "fr_CA"}, {"locale": "zh@collation=pinyin"}, {"locale": "zh@collation=stroke"}, {"locale": "zh-Hans"}, {"locale": "zh_Hant"},
    {"locale": "en@colStrength=primary"}, {"locale": "en-u-co-phonebk"}, {"locale": "sr_Latn"}, {"locale": "xxxxx"}, {"locale": "de@collation=phonebook"},
    {"locale": "de@collation=nosuch"}, {"locale": "aa"}, {"locale": "af_ZA"}, {"locale": "root"}, {"locale": "und"}, {"locale": "es@collation=traditional"},
    {"strength": 2}, {"locale": "en", "locale2": "x"}, {"locale": "en", "version": "57.1"}, {"locale": "en", "caseLevel": True, "strength": 1}, "x", [], 5, None,
]


@case("g07.validation.find")
def _validation(client, db):
    db.t.insert_one({"_id": 1, "s": "a"})
    out = []
    for spec in SPECS:
        out.append(tr(lambda: (db.command({"find": "t", "filter": {}, "collation": spec}), "ok")[1]))
    return out


@case("g07.validation.other_commands")
def _validation_cmds(client, db):
    db.t.insert_one({"_id": 1, "s": "a"})
    bad = {"locale": "xx"}
    cmds = {
        "aggregate": {"aggregate": "t", "pipeline": [], "cursor": {}, "collation": bad},
        "count": {"count": "t", "collation": bad},
        "distinct": {"distinct": "t", "key": "s", "collation": bad},
        "findAndModify": {"findAndModify": "t", "query": {}, "update": {"$set": {"x": 1}}, "collation": bad},
        "update": {"update": "t", "updates": [{"q": {}, "u": {"$set": {"x": 1}}, "collation": bad}]},
        "delete": {"delete": "t", "deletes": [{"q": {}, "limit": 1, "collation": bad}]},
        "createIndexes": {"createIndexes": "t", "indexes": [{"key": {"s": 1}, "name": "s_1", "collation": bad}]},
        "create": {"create": "c1", "collation": bad},
        "update_notobject": {"update": "t", "updates": [{"q": {}, "u": {"$set": {"x": 1}}, "collation": "x"}]},
        "find_notobject": {"find": "t", "collation": "x"},
        "aggregate_notobject": {"aggregate": "t", "pipeline": [], "cursor": {}, "collation": 5},
    }
    out = {}
    for name, cmd in cmds.items():
        r = tr(lambda: (lambda x: x.get("writeErrors", "ok"))(db.command(cmd)))
        out[name] = r
    return out


# ---- the locales ---------------------------------------------------------------------------------------------------

def _locales():
    rows = csv.DictReader(open(os.path.join(HERE, "..", "..", "compat", "inventory_base.csv")))
    return [r["name"] for r in rows if r["id"].startswith("locale:")]


def _sorted_by(db, spec):
    db.t.drop()
    t = load(db)
    col = Collation(**spec)
    return {"sort": ids(t.find().sort([("s", 1), ("_id", 1)]).collation(col)),
            "eq_a": ids(t.find({"s": "a"}).sort("_id", 1).collation(col)),
            "eq_e": ids(t.find({"s": "é"}).sort("_id", 1).collation(col)),
            "gt_b": ids(t.find({"s": {"$gt": "b"}}).sort("_id", 1).collation(col))}


def _mk_locale(loc):
    def f(client, db):
        return _sorted_by(db, {"locale": loc})
    return f


for _l in _locales():
    case(f"g07.locale.{_l}")(_mk_locale(_l))


# ---- options -------------------------------------------------------------------------------------------------------

OPT_LOCALES = ["en", "da", "th", "de@collation=phonebook", "sv", "es@collation=traditional", "fr_CA", "tr", "ja", "ko", "ru", "pl", "cs", "el", "ar", "he", "hi", "zh", "zh@collation=stroke"]


def _mk_opts(loc, opts):
    def f(client, db):
        return _sorted_by(db, {"locale": loc, **opts})
    return f


_OPTS = ([{"strength": s} for s in (1, 2, 3, 4, 5)] + [{"caseLevel": True, "strength": s} for s in (1, 2)] + [{"caseFirst": "upper"}, {"caseFirst": "lower"}, {"caseFirst": "off"},
         {"caseFirst": "upper", "strength": 2, "caseLevel": True}, {"numericOrdering": True}, {"alternate": "shifted"}, {"alternate": "shifted", "maxVariable": "space"},
         {"alternate": "shifted", "strength": 4}, {"alternate": "shifted", "maxVariable": "punct", "strength": 1}, {"backwards": False}, {"normalization": True}, {"normalization": False}])
for _loc in OPT_LOCALES[:6]:
    for _o in _OPTS:
        case(f"g07.options.{_loc}.{json.dumps(_o, sort_keys=True)}")(_mk_opts(_loc, _o))
for _loc in OPT_LOCALES[6:]:
    for _o in ({"strength": 1}, {"strength": 2}, {"caseFirst": "upper"}, {"numericOrdering": True}, {"alternate": "shifted", "strength": 1}):
        case(f"g07.options.{_loc}.{json.dumps(_o, sort_keys=True)}")(_mk_opts(_loc, _o))


@case("g07.options.backwards_french")
def _backwards(client, db):
    t = load(db, "t", ["cote", "côte", "coté", "côté", "Cote", "cotE", "coté", "cótè", "côtè"])
    out = {}
    for spec in ({"locale": "fr"}, {"locale": "fr", "backwards": True}, {"locale": "fr_CA"}, {"locale": "fr_CA", "backwards": False}):
        out[json.dumps(spec)] = tr(lambda: ids(t.find().sort([("s", 1), ("_id", 1)]).collation(Collation(**spec))))
    return out


@case("g07.options.numericOrdering")
def _numeric(client, db):
    t = load(db, "t", ["a10", "a9", "a2", "a1", "a01", "a001", "10", "9", "1.5", "-5", "a-3", "b", "a 1", "a  1", "A1", "a1b", "a1a", "x007", "x7", "x07"])
    return {str(o): ids(t.find().sort([("s", 1), ("_id", 1)]).collation(Collation("en", **o))) for o in ({"numericOrdering": True}, {"numericOrdering": False}, {"numericOrdering": True, "strength": 1})}


@case("g07.options.alternate_shifted")
def _shifted(client, db):
    t = load(db, "t", ["a b", "ab", "a-b", "a_b", "a.b", "a,b", "a b c", "abc", "A B", "a  b", " ab", "-ab", "ab ", "a\tb", "a*b", "$a", "a$", "a€b", "a©b"])
    out = {}
    for o in ({"alternate": "shifted"}, {"alternate": "shifted", "maxVariable": "space"}, {"alternate": "non-ignorable"}, {"alternate": "shifted", "strength": 1}, {"alternate": "shifted", "strength": 4}):
        out[json.dumps(o, sort_keys=True)] = ids(t.find().sort([("s", 1), ("_id", 1)]).collation(Collation("en", **o)))
    return out


# ---- filters -------------------------------------------------------------------------------------------------------

def _seed(db):
    t = db.t
    t.insert_many([
        {"_id": 1, "s": "apple", "arr": ["Red", "green"], "d": {"c": "Blue"}, "n": 1},
        {"_id": 2, "s": "Apple", "arr": ["red"], "d": {"c": "blue"}, "n": 2},
        {"_id": 3, "s": "APPLE", "arr": [], "d": {"c": "BLUE"}, "n": 3},
        {"_id": 4, "s": "banana", "arr": ["GREEN", "Red"], "d": {"c": "yellow"}, "n": 4},
        {"_id": 5, "s": "Banana", "arr": [["Red"]], "d": {"c": "Yellow"}, "n": 5},
        {"_id": 6, "s": "cherry", "arr": ["red", "RED"], "d": "BLUE", "n": 6},
        {"_id": 7, "s": "ÉCLAIR", "arr": ["éclair"], "d": None, "n": 7},
        {"_id": 8, "s": "eclair", "arr": ["Eclair"], "d": {"c": ["BLUE", "x"]}, "n": 8},
        {"_id": 9, "s": 5, "arr": [5, "5"], "d": {"c": 5}, "n": 9},
        {"_id": 10, "s": None, "arr": None, "d": {}, "n": 10},
        {"_id": 11, "n": 11},
    ])
    return t


FILTERS = [
    {"s": "apple"}, {"s": {"$eq": "APPLE"}}, {"s": {"$ne": "apple"}}, {"s": {"$gt": "apple"}}, {"s": {"$gte": "apple"}}, {"s": {"$lt": "banana"}}, {"s": {"$lte": "BANANA"}},
    {"s": {"$in": ["APPLE", "cherry"]}}, {"s": {"$nin": ["APPLE", "cherry"]}}, {"s": {"$in": ["APPLE", 5, None]}}, {"s": {"$not": {"$eq": "apple"}}}, {"s": {"$not": {"$gt": "b"}}},
    {"arr": "red"}, {"arr": {"$in": ["RED"]}}, {"arr": {"$all": ["red", "green"]}}, {"arr": {"$elemMatch": {"$eq": "RED"}}}, {"arr": {"$elemMatch": {"$gt": "q"}}}, {"arr": ["red"]}, {"arr": ["RED", "red"]},
    {"arr": [["red"]]}, {"arr.0": "red"}, {"arr": {"$ne": "red"}}, {"arr": {"$nin": ["red"]}}, {"arr": {"$size": 2}}, {"d.c": "blue"}, {"d.c": {"$in": ["BLUE"]}}, {"d": {"c": "blue"}},
    {"d": "blue"}, {"d": {"$eq": {"c": "BLUE"}}}, {"d": {"$in": [{"c": "blue"}, "blue"]}}, {"d.c": {"$gt": "a", "$lt": "c"}}, {"$or": [{"s": "APPLE"}, {"d.c": "YELLOW"}]},
    {"$and": [{"s": {"$gt": "A"}}, {"s": {"$lt": "b"}}]}, {"$nor": [{"s": "apple"}]}, {"s": {"$regex": "^apple"}}, {"s": {"$regex": "^apple", "$options": "i"}}, {"s": {"$type": "string"}},
    {"s": {"$exists": True}}, {"s": {"$expr": 1}} if False else {"$expr": {"$eq": ["$s", "APPLE"]}}, {"$expr": {"$gt": ["$s", "b"]}}, {"$expr": {"$in": ["$s", ["APPLE"]]}},
    {"$expr": {"$eq": ["$arr", ["RED"]]}}, {"$expr": {"$lt": [{"$toLower": "$s"}, "b"]}}, {"s": {"$gt": 5}}, {"s": {"$gt": None}}, {"n": {"$gt": 2}}, {"s": {"$in": []}},
    {"s": {"$all": ["apple"]}}, {"s": {"$mod": [2, 1]}}, {"arr": {"$elemMatch": {"$regex": "^r"}}}, {"d.c": {"$all": ["BLUE"]}}, {"d.c": "x"},
]


def _mk_filter(i, f):
    def run(client, db):
        t = _seed(db)
        out = {"ci": tr(lambda: ids(t.find(f).sort("_id", 1).collation(Collation(**CI)))),
               "tert": tr(lambda: ids(t.find(f).sort("_id", 1).collation(Collation("en")))),
               "prim": tr(lambda: ids(t.find(f).sort("_id", 1).collation(Collation("en", strength=1)))),
               "none": tr(lambda: ids(t.find(f).sort("_id", 1)))}
        return out
    return run


for _i, _f in enumerate(FILTERS):
    case(f"g07.filter.{_i:02d}.{json.dumps(_f, sort_keys=True, ensure_ascii=False)[:60]}")(_mk_filter(_i, _f))


@case("g07.sort.compound_and_arrays")
def _sort(client, db):
    t = _seed(db)
    out = {}
    for name, srt in (("s", [("s", 1)]), ("s_desc", [("s", -1)]), ("arr", [("arr", 1)]), ("arr_desc", [("arr", -1)]), ("d", [("d", 1)]), ("dc", [("d.c", 1)]), ("dc_desc", [("d.c", -1)]), ("s_n", [("s", 1), ("n", -1)]), ("n_s", [("n", 1), ("s", 1)])):
        for col, spec in (("ci", CI), ("en", {"locale": "en"}), ("simple", None)):
            c = t.find().sort(srt + ([("_id", 1)] if name not in ("s_n", "n_s") else []))
            if spec:
                c = c.collation(Collation(**spec))
            out[f"{name}.{col}"] = tr(lambda: ids(c))
    out["limit_skip"] = ids(t.find().sort([("s", 1), ("_id", 1)]).skip(2).limit(4).collation(Collation(**CI)))
    return out


@case("g07.count_distinct")
def _count_distinct(client, db):
    t = _seed(db)
    out = {}
    for name, spec in (("ci", CI), ("en", {"locale": "en"})):
        col = Collation(**spec)
        out[name] = {"countDocuments": t.count_documents({"s": "apple"}, collation=col), "count": db.command({"count": "t", "query": {"s": "apple"}, "collation": spec})["n"],
                     "distinct": t.distinct("s", collation=col), "distinct_q": t.distinct("s", {"n": {"$lt": 4}}, collation=col), "distinct_arr": t.distinct("arr", collation=col),
                     "distinct_dc": t.distinct("d.c", collation=col)}
    out["simple"] = {"distinct": t.distinct("s"), "distinct_arr": t.distinct("arr")}
    return out


@case("g07.cursor.getmore_keeps_collation")
def _getmore(client, db):
    t = _seed(db)
    return ids(t.find({"s": {"$in": ["APPLE", "BANANA", "CHERRY"]}}).sort([("s", 1), ("_id", 1)]).batch_size(2).collation(Collation(**CI)))


# ---- aggregation ---------------------------------------------------------------------------------------------------

def _agg(t, pipe, spec):
    return tr(lambda: list(t.aggregate(pipe, collation=Collation(**spec) if spec else None)))


PIPES = {
    "match_sort": [{"$match": {"s": {"$gte": "apple", "$lte": "BANANA"}}}, {"$sort": {"s": 1, "_id": 1}}, {"$project": {"s": 1}}],
    "group_key": [{"$group": {"_id": "$s", "n": {"$sum": 1}, "ids": {"$push": "$_id"}}}, {"$sort": {"_id": 1}}],
    "group_key_expr": [{"$group": {"_id": {"x": "$s", "y": "$d.c"}, "n": {"$sum": 1}}}, {"$sort": {"n": -1, "_id": 1}}],
    "group_set_min_max": [{"$group": {"_id": None, "set": {"$addToSet": "$s"}, "mn": {"$min": "$s"}, "mx": {"$max": "$s"}, "first": {"$first": "$s"}, "last": {"$last": "$s"}}}, {"$project": {"set": {"$sortArray": {"input": "$set", "sortBy": 1}}, "mn": 1, "mx": 1, "first": 1, "last": 1}}],
    "group_arr_key": [{"$group": {"_id": "$arr", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}],
    "sortByCount": [{"$sortByCount": "$s"}, {"$sort": {"count": -1, "_id": 1}}],
    "bucket": [{"$bucket": {"groupBy": "$s", "boundaries": ["a", "b", "c", "d", "f"], "default": "other", "output": {"n": {"$sum": 1}}}}],
    "count_stage": [{"$match": {"s": "apple"}}, {"$count": "n"}],
    "expr_cmp": [{"$project": {"_id": 1, "eq": {"$eq": ["$s", "APPLE"]}, "ne": {"$ne": ["$s", "apple"]}, "gt": {"$gt": ["$s", "banana"]}, "lte": {"$lte": ["$s", "Apple"]}, "cmp": {"$cmp": ["$s", "apple"]}}}, {"$sort": {"_id": 1}}],
    "expr_in": [{"$project": {"_id": 1, "in": {"$in": ["$s", ["APPLE", "banana"]]}, "idx": {"$indexOfArray": [["x", "APPLE", "apple"], "$s"]}, "has": {"$in": ["RED", "$arr"]}}}, {"$sort": {"_id": 1}}],
    "expr_sets": [{"$match": {"arr": {"$type": "array"}}}, {"$project": {"_id": 1, "u": {"$setUnion": ["$arr", ["RED", "Green"]]}, "i": {"$setIntersection": ["$arr", ["RED", "green"]]}, "d": {"$setDifference": ["$arr", ["red"]]}, "eq": {"$setEquals": ["$arr", ["red"]]}, "sub": {"$setIsSubset": ["$arr", ["red", "GREEN"]]}}}, {"$sort": {"_id": 1}}],
    "expr_filter": [{"$match": {"arr.0": {"$exists": True}}}, {"$project": {"f": {"$filter": {"input": "$arr", "as": "a", "cond": {"$eq": ["$$a", "RED"]}}}}}, {"$sort": {"_id": 1}}],
    "expr_maxmin": [{"$project": {"mx": {"$max": ["$s", "B"]}, "mn": {"$min": ["$s", "b"]}}}, {"$sort": {"_id": 1}}],
    "expr_switch": [{"$project": {"c": {"$switch": {"branches": [{"case": {"$eq": ["$s", "APPLE"]}, "then": "A"}, {"case": {"$lt": ["$s", "c"]}, "then": "B"}], "default": "Z"}}}}, {"$sort": {"_id": 1}}],
    "expr_sortArray": [{"$match": {"arr.1": {"$exists": True}}}, {"$project": {"a": {"$sortArray": {"input": "$arr", "sortBy": 1}}}}, {"$sort": {"_id": 1}}],
    "expr_strcmp_unaffected": [{"$project": {"c": {"$strcasecmp": ["$s", "apple"]}, "x": {"$toUpper": "$s"}}}, {"$sort": {"_id": 1}}],
    "lookup_simple": [{"$match": {"n": {"$lte": 3}}}, {"$lookup": {"from": "t", "localField": "s", "foreignField": "s", "as": "m"}}, {"$project": {"m": {"$map": {"input": "$m", "as": "x", "in": "$$x._id"}}}}, {"$sort": {"_id": 1}}],
    "lookup_pipeline": [{"$match": {"n": {"$lte": 3}}}, {"$lookup": {"from": "t", "let": {"v": "$s"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$s", "$$v"]}}}, {"$project": {"_id": 1}}], "as": "m"}}, {"$sort": {"_id": 1}}],
    "lookup_array": [{"$match": {"n": {"$in": [1, 4]}}}, {"$lookup": {"from": "t", "localField": "arr", "foreignField": "arr", "as": "m"}}, {"$project": {"m": {"$map": {"input": "$m", "as": "x", "in": "$$x._id"}}}}, {"$sort": {"_id": 1}}],
    "facet": [{"$facet": {"a": [{"$match": {"s": "APPLE"}}, {"$count": "n"}], "b": [{"$group": {"_id": "$s"}}, {"$count": "n"}]}}],
    "unionWith": [{"$match": {"s": "apple"}}, {"$unionWith": {"coll": "t", "pipeline": [{"$match": {"s": "BANANA"}}]}}, {"$sort": {"_id": 1}}, {"$project": {"s": 1}}],
    "window_partition": [{"$setWindowFields": {"partitionBy": "$s", "sortBy": {"_id": 1}, "output": {"c": {"$sum": 1, "window": {"documents": ["unbounded", "current"]}}}}}, {"$sort": {"_id": 1}}, {"$project": {"c": 1}}],
    "setWindow_sort_string": [{"$setWindowFields": {"sortBy": {"s": 1}, "output": {"rk": {"$rank": {}}, "dk": {"$denseRank": {}}}}}, {"$sort": {"_id": 1}}, {"$project": {"rk": 1, "dk": 1}}],
    "unwind_group": [{"$unwind": "$arr"}, {"$group": {"_id": "$arr", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}],
    "redact": [{"$redact": {"$cond": [{"$eq": ["$s", "APPLE"]}, "$$KEEP", "$$PRUNE"]}}, {"$sort": {"_id": 1}}, {"$project": {"s": 1}}],
    "match_regex": [{"$match": {"s": {"$regex": "^a", "$options": "i"}}}, {"$sort": {"_id": 1}}, {"$project": {"s": 1}}],
    "match_in_with_regex": [{"$match": {"s": {"$in": [{"$regex": "^app"}.get("$regex") and __import__("re").compile("^app"), "BANANA"]}}}, {"$sort": {"_id": 1}}, {"$project": {"s": 1}}],
}


def _mk_agg(name, pipe):
    def f(client, db):
        t = _seed(db)
        return {"ci": _agg(t, pipe, CI), "en": _agg(t, pipe, {"locale": "en"}), "num": _agg(t, pipe, {"locale": "en", "numericOrdering": True, "strength": 1}), "none": _agg(t, pipe, None)}
    return f


for _n, _p in PIPES.items():
    case(f"g07.aggregate.{_n}")(_mk_agg(_n, _p))


# ---- update / delete / findAndModify -------------------------------------------------------------------------------

def _docs(t):
    return list(t.find().sort("_id", 1))


@case("g07.update.statement_collation")
def _update(client, db):
    t = _seed(db)
    out = {}
    out["one"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"s": "APPLE"}, "u": {"$set": {"hit": 1}}, "collation": CI}]})["nModified"])
    out["many"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"s": "banana"}, "u": {"$set": {"hit2": 1}}, "multi": True, "collation": CI}]})["nModified"])
    out["nomatch_without"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"s": "cherry".upper()}, "u": {"$set": {"hit3": 1}}, "multi": True}]})["n"])
    out["two_statements"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"s": "CHERRY"}, "u": {"$set": {"a": 1}}, "collation": CI}, {"q": {"s": "CHERRY"}, "u": {"$set": {"b": 1}}}]})["nModified"])
    out["upsert"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"s": "NEW", "_id": 100}, "u": {"$set": {"z": 1}}, "upsert": True, "collation": CI}]}).get("upserted"))
    out["docs"] = _docs(t)
    return out


@case("g07.update.array_filters_and_pull_addToSet")
def _update_arr(client, db):
    t = db.t
    t.insert_many([{"_id": 1, "arr": ["Red", "red", "GREEN", "blue"]}, {"_id": 2, "arr": ["RED", "x"]}, {"_id": 3, "arr": [{"c": "Red"}, {"c": "red"}, {"c": "Blue"}]}])
    out = {}
    out["arrayFilters"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 1}, "u": {"$set": {"arr.$[e]": "X"}}, "arrayFilters": [{"e": "red"}], "collation": CI}]})["nModified"])
    out["arrayFilters_none"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 2}, "u": {"$set": {"arr.$[e]": "X"}}, "arrayFilters": [{"e": "red"}]}]})["nModified"])
    out["pull"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 1}, "u": {"$pull": {"arr": "green"}}, "collation": CI}]})["nModified"])
    out["pull_cond"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 3}, "u": {"$pull": {"arr": {"c": "red"}}}, "collation": CI}]})["nModified"])
    out["addToSet"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 2}, "u": {"$addToSet": {"arr": "X"}}, "collation": CI}]})["nModified"])
    out["addToSet2"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 2}, "u": {"$addToSet": {"arr": "x"}}, "collation": CI}]})["nModified"])
    out["pullAll"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {"_id": 2}, "u": {"$pullAll": {"arr": ["x", "RED"]}}, "collation": CI}]})["nModified"])
    out["docs"] = _docs(t)
    return out


@case("g07.update.min_max_operators")
def _update_minmax(client, db):
    t = db.t
    t.insert_many([{"_id": 1, "s": "b"}, {"_id": 2, "s": "B"}, {"_id": 3, "s": "c"}])
    out = {}
    out["min"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {}, "u": {"$min": {"s": "B"}}, "multi": True, "collation": CI}]})["nModified"])
    out["max"] = tr(lambda: db.command({"update": "t", "updates": [{"q": {}, "u": {"$max": {"s": "C"}}, "multi": True, "collation": CI}]})["nModified"])
    out["docs"] = _docs(t)
    return out


@case("g07.delete.statement_collation")
def _delete(client, db):
    t = _seed(db)
    out = {}
    out["one"] = tr(lambda: db.command({"delete": "t", "deletes": [{"q": {"s": "APPLE"}, "limit": 1, "collation": CI}]})["n"])
    out["many"] = tr(lambda: db.command({"delete": "t", "deletes": [{"q": {"s": "banana"}, "limit": 0, "collation": CI}]})["n"])
    out["without"] = tr(lambda: db.command({"delete": "t", "deletes": [{"q": {"s": "CHERRY"}, "limit": 0}]})["n"])
    out["docs"] = ids(t.find().sort("_id", 1))
    return out


@case("g07.findAndModify.collation")
def _fam(client, db):
    t = _seed(db)
    out = {}
    out["update"] = tr(lambda: db.command({"findAndModify": "t", "query": {"s": "apple"}, "sort": {"_id": -1}, "update": {"$set": {"hit": 1}}, "new": True, "collation": CI})["value"]["_id"])
    out["remove"] = tr(lambda: db.command({"findAndModify": "t", "query": {"s": "BANANA"}, "sort": {"s": 1, "_id": 1}, "remove": True, "collation": CI})["value"]["_id"])
    out["none"] = tr(lambda: db.command({"findAndModify": "t", "query": {"s": "CHERRY"}, "update": {"$set": {"hit": 1}}})["value"])
    out["upsert"] = tr(lambda: db.command({"findAndModify": "t", "query": {"s": "NEWONE"}, "update": {"$set": {"hit": 1, "n": 99}}, "upsert": True, "new": True, "collation": CI})["value"].get("s"))
    out["docs"] = _docs(t)
    return out


# ---- collection default collation ----------------------------------------------------------------------------------

def _stages(p):
    """The stage names (and index names) of an explain winningPlan, outermost first."""
    out = []
    while isinstance(p, dict):
        out.append(p.get("stage") + (":" + p["indexName"] if "indexName" in p else ""))
        p = p.get("inputStage") or (p.get("inputStages") or [None])[0]
    return out


def _plan(db, coll, flt, **kw):
    cmd = {"find": coll, "filter": flt, **kw}
    r = db.command("explain", cmd, verbosity="queryPlanner")["queryPlanner"]
    return _stages(r["winningPlan"])


def _mkcoll(db, name="t", spec=CI, docs=True):
    db.command({"create": name, "collation": spec})
    t = db[name]
    if docs:
        t.insert_many([{"_id": i, "s": s, "n": i} for i, s in enumerate(["apple", "Apple", "banana", "BANANA", "cherry", "Éclair", "eclair", "Zed", "zed", "10", "9"], 1)])
    return t


@case("g07.collection.create_and_list")
def _create_list(client, db):
    out = {}
    for name, spec in (("a", CI), ("b", {"locale": "en"}), ("c", {"locale": "fr", "caseFirst": "upper", "strength": 3}), ("d", {"locale": "simple"}), ("e", {"locale": "da"}), ("f", {"locale": "th"}), ("g", {"locale": "fr_CA"}), ("h", {"locale": "de@collation=phonebook", "numericOrdering": True})):
        out[name] = tr(lambda: (db.command({"create": name, "collation": spec}), {k: v for k, v in next(db.list_collections(filter={"name": name})).items() if k in ("options", "idIndex")}, list(db[name].list_indexes()))[1:])
    return out


@case("g07.collection.create_errors")
def _create_errors(client, db):
    out = {}
    for i, spec in enumerate([{"locale": "xx"}, {"locale": "EN"}, {}, {"locale": "en", "strength": 9}, {"locale": "en", "foo": 1}, {"locale": "en", "strength": 1, "caseFirst": "upper"}, "x", 5, {"strength": 2}, {"locale": "en-US"}, {"locale": ""}]):
        out[str(i)] = tr(lambda: (db.command({"create": f"c{i}", "collation": spec}), "ok")[1])
    db.command({"create": "dup", "collation": CI})
    out["same_again"] = tr(lambda: (db.command({"create": "dup", "collation": CI}), "ok")[1])
    out["other"] = tr(lambda: (db.command({"create": "dup", "collation": {"locale": "fr"}}), "ok")[1])
    out["none"] = tr(lambda: (db.command({"create": "dup"}), "ok")[1])
    db.command({"create": "plain"})
    out["plain_with"] = tr(lambda: (db.command({"create": "plain", "collation": CI}), "ok")[1])
    out["plain_same"] = tr(lambda: (db.command({"create": "plain"}), "ok")[1])
    out["collMod"] = tr(lambda: (db.command({"collMod": "dup", "collation": {"locale": "fr"}}), "ok")[1])
    return out


@case("g07.collection.default_applies_to_queries")
def _default_queries(client, db):
    t = _mkcoll(db)
    out = {}
    out["eq"] = ids(t.find({"s": "APPLE"}).sort("_id", 1))
    out["gt"] = ids(t.find({"s": {"$gt": "BANANA"}}).sort("_id", 1))
    out["in"] = ids(t.find({"s": {"$in": ["ZED", "eclair"]}}).sort("_id", 1))
    out["sort"] = ids(t.find().sort([("s", 1), ("_id", 1)]))
    out["explicit_simple"] = ids(t.find({"s": "APPLE"}).collation(Collation("simple")))
    out["explicit_simple_sort"] = ids(t.find().sort([("s", 1), ("_id", 1)]).collation(Collation("simple")))
    out["explicit_other"] = ids(t.find({"s": "eclair"}).sort("_id", 1).collation(Collation("en", strength=1)))
    out["count"] = t.count_documents({"s": "zed"})
    out["distinct"] = t.distinct("s")
    out["agg_group"] = list(t.aggregate([{"$group": {"_id": "$s", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]))
    out["agg_match"] = ids(t.aggregate([{"$match": {"s": "cherry"}}]))
    out["expr"] = ids(t.find({"$expr": {"$eq": ["$s", "APPLE"]}}).sort("_id", 1))
    out["update"] = db.command({"update": "t", "updates": [{"q": {"s": "ZED"}, "u": {"$set": {"z": 1}}, "multi": True}]})["nModified"]
    out["update_simple"] = db.command({"update": "t", "updates": [{"q": {"s": "ZED"}, "u": {"$set": {"z2": 1}}, "multi": True, "collation": {"locale": "simple"}}]})["nModified"]
    out["delete"] = db.command({"delete": "t", "deletes": [{"q": {"s": "BANANA"}, "limit": 0}]})["n"]
    out["fam"] = db.command({"findAndModify": "t", "query": {"s": "ECLAIR"}, "sort": {"_id": 1}, "update": {"$set": {"f": 1}}, "new": True})["value"]["_id"]
    out["docs"] = list(t.find().sort("_id", 1))
    return out


@case("g07.collection.id_is_unique_under_the_collation")
def _id_unique(client, db):
    db.command({"create": "t", "collation": CI})
    t = db.t
    out = {}
    t.insert_one({"_id": "a"})
    out["dup_insert"] = tr(lambda: (db.command({"insert": "t", "documents": [{"_id": "A"}]}), "ok")[1])
    out["dup_insert_result"] = db.command({"insert": "t", "documents": [{"_id": "A"}]}).get("writeErrors", [{}])[0].get("code")
    out["many_same_batch"] = [(e["index"], e["code"]) for e in db.command({"insert": "t", "documents": [{"_id": "b"}, {"_id": "B"}, {"_id": "c"}], "ordered": False}).get("writeErrors", [])]
    out["many_ordered"] = [e["index"] for e in db.command({"insert": "t", "documents": [{"_id": "d"}, {"_id": "D"}, {"_id": "e"}]}).get("writeErrors", [])]
    out["ids"] = ids(t.find().sort("_id", 1))
    out["find_id"] = ids(t.find({"_id": "A"}))
    out["find_id_B"] = ids(t.find({"_id": "B"}))
    out["find_id_simple"] = ids(t.find({"_id": "A"}).collation(Collation("simple")))
    out["find_id_other"] = ids(t.find({"_id": "A"}).collation(Collation("fr", strength=2)))
    out["find_id_in"] = ids(t.find({"_id": {"$in": ["A", "C", "x"]}}).sort("_id", 1))
    out["find_id_range"] = ids(t.find({"_id": {"$gte": "C"}}).sort("_id", 1))
    out["upsert_existing"] = db.command({"update": "t", "updates": [{"q": {"_id": "A"}, "u": {"$set": {"x": 1}}, "upsert": True}]})
    out["upsert_new"] = db.command({"update": "t", "updates": [{"q": {"_id": "Z"}, "u": {"$set": {"x": 1}}, "upsert": True}]}).get("upserted")
    out["upsert_new_again"] = db.command({"update": "t", "updates": [{"q": {"_id": "z"}, "u": {"$set": {"x": 2}}, "upsert": True}]})["n"]
    out["fam_upsert"] = tr(lambda: db.command({"findAndModify": "t", "query": {"_id": "q"}, "update": {"$set": {"x": 1}}, "upsert": True, "new": True})["value"])
    out["fam_upsert_again"] = tr(lambda: db.command({"findAndModify": "t", "query": {"_id": "Q"}, "update": {"$set": {"x": 3}}, "upsert": True, "new": True})["value"])
    out["delete"] = db.command({"delete": "t", "deletes": [{"q": {"_id": "A"}, "limit": 1}]})["n"]
    out["reinsert"] = db.command({"insert": "t", "documents": [{"_id": "a"}]}).get("n")
    out["docs"] = list(t.find().sort("_id", 1))
    out["bulk"] = tr(lambda: (t.bulk_write([__import__("pymongo").InsertOne({"_id": "m"}), __import__("pymongo").InsertOne({"_id": "M"})], ordered=False), "ok")[1])
    out["numeric_ids_unaffected"] = (t.insert_one({"_id": 1}).inserted_id, tr(lambda: t.insert_one({"_id": 1.0}).inserted_id))
    return out


@case("g07.collection.rename_and_drop")
def _rename(client, db):
    t = _mkcoll(db, "t", CI, docs=False)
    t.insert_one({"_id": "a"})
    client.admin.command({"renameCollection": db.name + ".t", "to": db.name + ".u"})
    out = {"opts": next(db.list_collections(filter={"name": "u"}))["options"], "dup": db.command({"insert": "u", "documents": [{"_id": "A"}]}).get("writeErrors", [{}])[0].get("code"), "find": ids(db.u.find({"_id": "A"})),
           "match": ids(db.u.find({"s": "x"}))}
    db.u.drop()
    db.command({"create": "u"})
    out["after_drop"] = (next(db.list_collections(filter={"name": "u"}))["options"], db.command({"insert": "u", "documents": [{"_id": "a"}, {"_id": "A"}]}).get("n"))
    return out


@case("g07.collection.default_and_index_use")
def _default_index(client, db):
    t = _mkcoll(db)
    t.create_index("s", name="s_1")
    t.create_index([("n", 1), ("s", 1)], name="ns")
    out = {}
    out["plan_default"] = _plan(db, "t", {"s": "apple"})
    out["plan_simple"] = _plan(db, "t", {"s": "apple"}, collation={"locale": "simple"})
    out["plan_other"] = _plan(db, "t", {"s": "apple"}, collation={"locale": "fr"})
    out["plan_num"] = _plan(db, "t", {"n": 3})
    out["plan_id_str"] = _plan(db, "t", {"_id": "x"})
    out["plan_id_num"] = _plan(db, "t", {"_id": 3})
    out["plan_sort_limit"] = _plan(db, "t", {}, sort={"s": 1}, limit=3)
    out["plan_sort_simple"] = _plan(db, "t", {}, sort={"s": 1}, limit=3, collation={"locale": "simple"})
    out["listIndexes"] = list(t.list_indexes())
    out["eq"] = ids(t.find({"s": "APPLE"}).sort("_id", 1))
    out["range"] = ids(t.find({"s": {"$gt": "b", "$lt": "d"}}).sort("_id", 1))
    out["sorted"] = ids(t.find().sort("s", 1).limit(5)) if False else [d["s"] for d in t.find().sort([("s", 1), ("_id", 1)]).limit(5)]
    out["hint_sort"] = [d["s"] for d in t.find().hint("s_1").sort([("s", 1), ("_id", 1)])]
    out["regex"] = ids(t.find({"s": {"$regex": "^ap"}}).sort("_id", 1))
    out["regex_i"] = ids(t.find({"s": {"$regex": "^ap", "$options": "i"}}).sort("_id", 1))
    out["in"] = ids(t.find({"s": {"$in": ["ZED", "BANANA"]}}).sort("_id", 1))
    out["ne"] = ids(t.find({"s": {"$ne": "apple"}}).sort("_id", 1))
    out["nin"] = ids(t.find({"s": {"$nin": ["apple", "zed"]}}).sort("_id", 1))
    out["compound"] = ids(t.find({"n": {"$gt": 2}, "s": {"$lt": "c"}}).sort("_id", 1))
    return out


# ---- index collation -----------------------------------------------------------------------------------------------

def _specs(t):
    return [{k: v for k, v in i.items() if k != "v"} for i in t.list_indexes()]


@case("g07.index.create_and_list")
def _index_create(client, db):
    t = db.t
    t.insert_many([{"_id": i, "s": s, "a": [s, s.upper()]} for i, s in enumerate(["x", "X", "y"])])
    out = {}
    for name, spec in (("ci", {"key": {"s": 1}, "name": "s_ci", "collation": CI}), ("full", {"key": {"a": 1}, "name": "a_fr", "collation": {"locale": "fr", "caseFirst": "upper", "strength": 3}}),
                       ("simple", {"key": {"s": -1}, "name": "s_simple", "collation": {"locale": "simple"}}), ("none", {"key": {"s": 1, "a": 1}, "name": "s_a"}),
                       ("unique", {"key": {"n": 1}, "name": "n_u", "unique": True, "collation": {"locale": "en", "strength": 2}}), ("sparse", {"key": {"z": 1}, "name": "z_s", "sparse": True, "collation": CI}),
                       ("partial", {"key": {"p": 1}, "name": "p_p", "partialFilterExpression": {"p": {"$gt": "a"}}, "collation": CI}), ("hashed", {"key": {"h": "hashed"}, "name": "h_h", "collation": CI}),
                       ("2dsphere", {"key": {"g": "2dsphere"}, "name": "g_g", "collation": CI}), ("wild", {"key": {"w.$**": 1}, "name": "w_w", "collation": CI}), ("text", {"key": {"tx": "text"}, "name": "tx_t", "collation": CI}),
                       ("bad", {"key": {"b": 1}, "name": "b_b", "collation": {"locale": "xx"}}), ("badtype", {"key": {"b": 1}, "name": "b_b2", "collation": "x"}), ("empty", {"key": {"e": 1}, "name": "e_e", "collation": {}}),
                       ("same_key_other_collation", {"key": {"s": 1}, "name": "s_fr", "collation": {"locale": "fr"}}), ("same_key_same_collation", {"key": {"s": 1}, "name": "s_ci2", "collation": CI}),
                       ("same_name_other", {"key": {"s": 1}, "name": "s_ci", "collation": {"locale": "fr"}}), ("same_name_same", {"key": {"s": 1}, "name": "s_ci", "collation": CI}),
                       ("ttl", {"key": {"d": 1}, "name": "d_ttl", "expireAfterSeconds": 100, "collation": CI}), ("order", {"key": {"o": 1}, "name": "o_o", "collation": CI, "unique": False})):
        out[name] = tr(lambda: (lambda r: {k: r[k] for k in ("numIndexesBefore", "numIndexesAfter", "note") if k in r})(db.command({"createIndexes": "t", "indexes": [spec]})))
    out["listed"] = _specs(t)
    return out


@case("g07.index.default_collation_inherited")
def _index_inherit(client, db):
    t = _mkcoll(db, "t", CI, docs=False)
    out = {}
    out["plain"] = tr(lambda: t.create_index("a", name="a_1"))
    out["simple"] = tr(lambda: t.create_index("b", name="b_1", collation=Collation("simple")))
    out["other"] = tr(lambda: t.create_index("c", name="c_1", collation=Collation("fr")))
    out["text"] = tr(lambda: t.create_index([("d", "text")]))
    out["text_simple"] = tr(lambda: t.create_index([("d2", "text")], collation=Collation("simple")))
    out["hashed"] = tr(lambda: t.create_index([("h", "hashed")]))
    out["listed"] = _specs(t)
    return out


def _idx_coll(db):
    t = db.t
    words = ["apple", "Apple", "APPLE", "banana", "Banana", "cherry", "Éclair", "eclair", "ECLAIR", "zed", "Zed", "10", "9", "a b", "ab", "a-b", "résumé", "resume"]
    t.insert_many([{"_id": i, "s": w, "n": i % 4, "arr": [w, words[(i + 3) % len(words)]], "d": {"x": w}} for i, w in enumerate(words)])
    return t


@case("g07.index.queries_use_or_skip_the_index")
def _index_queries(client, db):
    t = _idx_coll(db)
    t.create_index("s", name="s_ci", collation=Collation(**CI))
    t.create_index("arr", name="arr_ci", collation=Collation(**CI))
    t.create_index([("n", 1), ("s", 1)], name="ns_ci", collation=Collation(**CI))
    t.create_index("d.x", name="dx_simple")
    out = {}
    qs = {"eq": {"s": "APPLE"}, "range": {"s": {"$gte": "b", "$lt": "d"}}, "in": {"s": {"$in": ["ZED", "banana", "nope"]}}, "ne": {"s": {"$ne": "apple"}}, "regex": {"s": {"$regex": "^a"}},
          "arr": {"arr": "BANANA"}, "arr_all": {"arr": {"$all": ["apple", "Banana"]}}, "compound": {"n": 2, "s": {"$gt": "c"}}, "nested": {"d.x": "Apple"}, "or": {"$or": [{"s": "ZED"}, {"s": "ab"}]}, "none": {"s": {"$exists": False}}, "lt": {"s": {"$lt": "b"}},
          "prefix_ci": {"s": {"$gt": "ap", "$lt": "aq"}}}
    for name, q in qs.items():
        out[name] = {"ci": ids(t.find(q).sort("_id", 1).collation(Collation(**CI))), "simple": ids(t.find(q).sort("_id", 1)), "fr": ids(t.find(q).sort("_id", 1).collation(Collation("fr", strength=1)))}
        if name in ("eq", "range", "in", "ne", "arr", "nested", "lt"):   # (plans of the other shapes differ from mongod's for reasons that have nothing to do with collation)
            out[name].update({"plan_ci": tr(lambda: _plan(db, "t", q, collation=CI)), "plan_simple": tr(lambda: _plan(db, "t", q))})
    srt = {}
    for name, (s, kw) in {"s_ci": ({"s": 1}, CI), "s_ci_desc": ({"s": -1}, CI), "s_simple": ({"s": 1}, None), "ns": ({"n": 1, "s": 1}, CI), "ns_mixed": ({"n": 1, "s": -1}, CI)}.items():
        c = t.find().sort(list(s.items()) + [("_id", 1)]).limit(8)
        if kw:
            c = c.collation(Collation(**kw))
        srt[name] = ids(c)
        srt[name + ".plan"] = tr(lambda: _plan(db, "t", {}, sort=s, limit=8, **({"collation": kw} if kw else {})))
    out["sorts"] = srt
    out["hint_ok"] = ids(t.find({"s": "ZED"}).hint("s_ci").collation(Collation(**CI)))
    out["hint_wrong_collation"] = tr(lambda: ids(t.find({"s": "ZED"}).hint("s_ci")))
    return out


@case("g07.index.unique_under_collation")
def _index_unique(client, db):
    t = db.t
    t.insert_many([{"_id": 1, "s": "a"}, {"_id": 2, "s": "b"}])
    out = {}
    out["create"] = tr(lambda: t.create_index("s", unique=True, name="s_u", collation=Collation(**CI)))
    out["dup_insert"] = db.command({"insert": "t", "documents": [{"_id": 3, "s": "A"}]}).get("writeErrors", [{}])[0].get("code")
    out["ok_insert"] = db.command({"insert": "t", "documents": [{"_id": 4, "s": "c"}]}).get("n")
    out["dup_update"] = db.command({"update": "t", "updates": [{"q": {"_id": 4}, "u": {"$set": {"s": "B"}}}]}).get("writeErrors", [{}])[0].get("code")
    out["self_update"] = db.command({"update": "t", "updates": [{"q": {"_id": 4}, "u": {"$set": {"s": "C"}}}]}).get("nModified")
    out["array"] = db.command({"insert": "t", "documents": [{"_id": 5, "s": ["x", "X"]}]}).get("n")
    out["docs"] = list(t.find().sort("_id", 1))
    t.drop()
    t.insert_many([{"_id": 1, "s": "a"}, {"_id": 2, "s": "A"}])
    out["create_on_dups"] = tr(lambda: t.create_index("s", unique=True, name="s_u", collation=Collation(**CI)))
    out["listed_after_failed_build"] = [i["name"] for i in t.list_indexes()]
    out["create_simple_ok"] = tr(lambda: t.create_index("s", unique=True, name="s_su"))
    return out


@case("g07.index.partial_and_sparse_with_collation")
def _index_partial(client, db):
    t = db.t
    t.insert_many([{"_id": 1, "p": "Apple"}, {"_id": 2, "p": "banana"}, {"_id": 3, "q": 1}, {"_id": 4, "p": "BANANA"}])
    t.create_index("p", name="p_partial", partialFilterExpression={"p": {"$gt": "A"}}, collation=Collation(**CI))
    t.create_index("p", name="p_sparse", sparse=True, collation=Collation("en", strength=1))
    out = {"eq": ids(t.find({"p": "banana"}).collation(Collation(**CI)).sort("_id", 1)), "gt": ids(t.find({"p": {"$gt": "apple"}}).collation(Collation(**CI)).sort("_id", 1)),
           "exists": ids(t.find({"p": {"$exists": True}}).sort("_id", 1)), "plans": [tr(lambda: _plan(db, "t", {"p": "banana"}, collation=CI)), tr(lambda: _plan(db, "t", {"p": "banana"}, collation={"locale": "en", "strength": 1}))]}
    return out


@case("g07.index.updates_keep_the_index_right")
def _index_updates(client, db):
    t = db.t
    t.create_index("s", name="s_ci", collation=Collation(**CI))
    t.insert_many([{"_id": i, "s": s} for i, s in enumerate(["a", "B", "c", "D"])])
    t.update_one({"_id": 0}, {"$set": {"s": "Z"}})
    t.delete_one({"_id": 1})
    t.replace_one({"_id": 2}, {"s": "b"})
    t.insert_one({"_id": 9, "s": "d"})
    return {"d": ids(t.find({"s": "D"}).sort("_id", 1).collation(Collation(**CI))), "z": ids(t.find({"s": "z"}).collation(Collation(**CI))), "b": ids(t.find({"s": "B"}).collation(Collation(**CI))),
            "a": ids(t.find({"s": "A"}).collation(Collation(**CI))), "all": ids(t.find().sort("s", 1).collation(Collation(**CI)))}


@case("g07.index.background_build_on_existing_data")
def _index_build(client, db):
    t = db.t
    t.insert_many([{"_id": i, "s": "".join(chr(97 + (i * k) % 26) for k in (1, 3, 7)) if i % 2 else "".join(chr(65 + (i * k) % 26) for k in (1, 3, 7))} for i in range(400)])
    t.create_index("s", name="s_ci", collation=Collation(**CI))
    return {"eq": ids(t.find({"s": "aDh"}).sort("_id", 1).collation(Collation(**CI))), "n": t.count_documents({"s": {"$gte": "m"}}, collation=Collation(**CI)), "plan": _plan(db, "t", {"s": "adh"}, collation=CI),
            "first": [d["s"] for d in t.find().sort([("s", 1), ("_id", 1)]).limit(5).collation(Collation(**CI))]}


# ---- explain, mapReduce, misc --------------------------------------------------------------------------------------

@case("g07.explain.shows_the_collation")
def _explain(client, db):
    t = _seed(db)
    out = {}
    for name, cmd in (("find", {"find": "t", "filter": {"s": "apple"}, "collation": CI}), ("find_en", {"find": "t", "filter": {"s": "apple"}, "collation": {"locale": "en"}}), ("find_da", {"find": "t", "filter": {"s": "apple"}, "collation": {"locale": "da"}}),
                      ("find_th", {"find": "t", "filter": {"s": "apple"}, "collation": {"locale": "th"}}), ("simple", {"find": "t", "filter": {"s": "apple"}, "collation": {"locale": "simple"}}), ("none", {"find": "t", "filter": {"s": "apple"}}),
                      ("count", {"count": "t", "query": {"s": "apple"}, "collation": CI}), ("distinct", {"distinct": "t", "key": "s", "collation": CI}),
                      ("agg", {"aggregate": "t", "pipeline": [{"$match": {"s": "apple"}}], "cursor": {}, "collation": CI}), ("update", {"update": "t", "updates": [{"q": {"s": "apple"}, "u": {"$set": {"x": 1}}, "collation": CI}]})):
        out[name] = tr(lambda: db.command("explain", cmd, verbosity="queryPlanner")["queryPlanner"].get("collation"))
    db.command({"create": "d", "collation": {"locale": "da", "strength": 2}})
    out["collection_default"] = db.command("explain", {"find": "d", "filter": {}}, verbosity="queryPlanner")["queryPlanner"].get("collation")
    return out


@case("g07.mapReduce.collation")
def _mapreduce(client, db):
    t = _seed(db)
    out = {}
    m = "function() { emit(this.s, 1); }"
    r = "function(k, v) { return Array.sum(v); }"
    for name, col in (("ci", CI), ("none", None)):
        cmd = {"mapReduce": "t", "map": m, "reduce": r, "out": {"inline": 1}, "query": {"s": {"$in": ["APPLE", "banana"]}}}
        if col:
            cmd["collation"] = col
        res = tr(lambda: db.command(cmd)["results"])
        out[name] = sorted(res, key=lambda d: str(d)) if isinstance(res, list) else res
    return out


@case("g07.misc.tailable_and_other_commands_ignore_nothing")
def _misc(client, db):
    t = _seed(db)
    out = {}
    out["find_one"] = (t.find_one({"s": "APPLE"}, collation=Collation(**CI), sort=[("_id", 1)]) or {}).get("_id")
    out["replace_one"] = t.replace_one({"s": "BANANA"}, {"s": "replaced"}, collation=Collation(**CI)).modified_count
    out["update_many"] = t.update_many({"s": "CHERRY"}, {"$set": {"z": 1}}, collation=Collation(**CI)).modified_count
    out["delete_many"] = t.delete_many({"s": "ÉCLAIR"}, collation=Collation("en", strength=1)).deleted_count
    out["find_one_and_update"] = (t.find_one_and_update({"s": "APPLE"}, {"$set": {"w": 1}}, sort=[("_id", -1)], collation=Collation(**CI)) or {}).get("_id")
    out["count_estimated"] = t.estimated_document_count()
    out["docs"] = list(t.find().sort("_id", 1))
    return out


@case("g07.merge_and_out.into_a_collection_with_a_default_collation")
def _merge(client, db):
    db.command({"create": "tgt", "collation": CI})
    db.tgt.insert_many([{"_id": "a", "v": 1}, {"_id": "b", "v": 2}])
    db.src.insert_many([{"_id": "A", "v": 10}, {"_id": "C", "v": 30}])
    out = {}
    out["merge_replace"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": "tgt", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}}])))
    out["tgt"] = list(db.tgt.find().sort("_id", 1))
    out["merge_fail"] = tr(lambda: list(db.src.aggregate([{"$merge": {"into": "tgt", "on": "_id", "whenMatched": "fail", "whenNotMatched": "insert"}}])))
    out["insert_dup"] = db.command({"insert": "tgt", "documents": [{"_id": "B"}]}).get("n")
    out["opts"] = next(db.list_collections(filter={"name": "tgt"}))["options"]
    return out
