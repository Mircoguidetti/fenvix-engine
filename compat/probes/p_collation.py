"""Collation: the collation document fields and every supported locale, checked against real mongod."""
import csv, os
from pymongo.collation import Collation
from probe_diff import case

DATA = ["b", "B", "a", "A", "á", "Á", "z", "Z", "10", "9", "a b", "ab", "a-b", "résumé", "resume", "Resume", "ä", "ö", "o", "æ", "ß", "ss", "ñ", "n", "ç", "c",
        "ı", "i", "İ", "I", "ŝ", "s", "Ａ", "é", "é", "ll", "l", "ch", "c", "cz", "sz", "ø", "å", "ij", "y"]


def load(db):
    db.t.insert_many([{"_id": i, "s": s} for i, s in enumerate(DATA)])


def run(db, spec, strength_eq=True):
    db.t.drop()
    load(db)
    col = Collation(**spec)
    out = {"sort": [d["_id"] for d in db.t.find().sort([("s", 1), ("_id", 1)]).collation(col)],
           "eq_a": [d["_id"] for d in db.t.find({"s": "a"}).sort("_id", 1).collation(col)],
           "eq_e": [d["_id"] for d in db.t.find({"s": "é"}).sort("_id", 1).collation(col)],
           "gt_b": [d["_id"] for d in db.t.find({"s": {"$gt": "b"}}).sort("_id", 1).collation(col)]}
    return out


@case("compat.collation.locale_en", ["coll:locale"], fail="missing")
def _loc(client, db):
    return run(db, {"locale": "en"})


@case("compat.collation.simple", ["coll:simple (binary) collation"])
def _simple(client, db):
    return run(db, {"locale": "simple"})


for _st in (1, 2, 3):
    pass


@case("compat.collation.strength", ["coll:strength"], fail="missing")
def _strength(client, db):
    return {st: run(db, {"locale": "en", "strength": st}) for st in (1, 2, 3)}


@case("compat.collation.caseLevel", ["coll:caseLevel"], fail="missing")
def _cl(client, db):
    return run(db, {"locale": "en", "strength": 1, "caseLevel": True})


@case("compat.collation.caseFirst", ["coll:caseFirst"], fail="missing")
def _cf(client, db):
    return {"upper": run(db, {"locale": "en", "caseFirst": "upper"}), "lower": run(db, {"locale": "en", "caseFirst": "lower"})}


@case("compat.collation.numericOrdering", ["coll:numericOrdering"], fail="missing")
def _no(client, db):
    db.t.insert_many([{"_id": i, "s": s} for i, s in enumerate(["10", "9", "a10", "a9", "a1", "100", "1", "x"])])
    return [d["_id"] for d in db.t.find().sort([("s", 1), ("_id", 1)]).collation(Collation("en", numericOrdering=True))]


@case("compat.collation.alternate_maxVariable", ["coll:alternate", "coll:maxVariable"], fail="missing")
def _alt(client, db):
    return {"shifted": run(db, {"locale": "en", "alternate": "shifted"}), "punct": run(db, {"locale": "en", "alternate": "shifted", "maxVariable": "punct"}),
            "space": run(db, {"locale": "en", "alternate": "shifted", "maxVariable": "space"})}


@case("compat.collation.backwards", ["coll:backwards"], fail="missing")
def _bw(client, db):
    return run(db, {"locale": "fr", "backwards": True})


@case("compat.collation.normalization", ["coll:normalization"], fail="missing")
def _norm(client, db):
    return {"on": run(db, {"locale": "en", "normalization": True, "strength": 3}), "off": run(db, {"locale": "en", "normalization": False, "strength": 3})}


@case("compat.collation.index_case_insensitive", ["idxprop:case-insensitive index"], fail="missing")
def _ci_index(client, db):
    load(db)
    db.t.create_index("s", collation=Collation("en", strength=2), name="s_ci")
    col = Collation("en", strength=2)
    return {"find": [d["_id"] for d in db.t.find({"s": "a"}).collation(col).sort("_id", 1)],
            "plan": db.command({"explain": {"find": "t", "filter": {"s": "a"}, "collation": {"locale": "en", "strength": 2}}, "verbosity": "queryPlanner"})["queryPlanner"]["winningPlan"].get("inputStage", {}).get("stage"),
            "unique_ci": [type(e).__name__ for e in _try_dups(db)]}


def _try_dups(db):
    db.u.create_index("s", unique=True, collation=Collation("en", strength=2))
    out = []
    try:
        db.u.insert_many([{"s": "a"}, {"s": "A"}])
    except Exception as e:  # noqa
        out.append(e)
    return out


def _locales():
    here = os.path.dirname(os.path.abspath(__file__))
    rows = csv.DictReader(open(os.path.join(here, "..", "inventory_base.csv")))
    return [r["name"] for r in rows if r["id"].startswith("locale:")]


def _mk(loc):
    def f(client, db):
        return run(db, {"locale": loc})
    return f


for _l in _locales():
    case(f"compat.collation.locale.{_l}", [f"locale:{_l}"], fail="missing")(_mk(_l))
