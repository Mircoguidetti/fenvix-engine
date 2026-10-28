"""Item 9: window operators $top / $bottom / $topN / $bottomN / $firstN / $lastN / $maxN / $minN in $setWindowFields, capped collections
and tailable cursors, mapReduce, and the diagnostics commands."""
from gaps_py import case
from pymongo.errors import OperationFailure

PLAN = "PlanExecutor error during aggregation :: caused by :: "


def tr(f):
    try:
        return f()
    except OperationFailure as e:
        m = e.details.get("errmsg") or ""
        return {"err": [e.code, e.details.get("codeName"), m[len(PLAN):] if m.startswith(PLAN) else m]}


def seed_window(db):
    db.t.insert_many([
        {"_id": 1, "p": "a", "x": 1, "v": 10, "w": "w1"}, {"_id": 2, "p": "a", "x": 2, "v": 30, "w": "w2"}, {"_id": 3, "p": "a", "x": 3, "v": 20}, {"_id": 4, "p": "a", "x": 4, "v": None, "w": "w4"},
        {"_id": 5, "p": "b", "x": 1, "v": 5, "w": "w5"}, {"_id": 6, "p": "b", "x": 2, "v": 7, "w": "w6"}, {"_id": 7, "p": "b", "x": 3, "v": 7, "w": "w7"}, {"_id": 8, "p": "c", "x": 1, "v": 1}, {"_id": 9, "p": "a", "x": 5, "v": [1, 2], "w": "w9"}])


def run_window(db, output, sort_by=None, **extra):
    spec = {"sortBy": sort_by or {"x": 1, "_id": 1}, "output": output, **extra}
    return tr(lambda: list(db.t.aggregate([{"$setWindowFields": spec}, {"$sort": {"_id": 1}}, {"$project": {"r": 1, "q": 1}}])))


WINDOWS = {
    "unbounded_current": {"documents": ["unbounded", "current"]}, "prev_next": {"documents": [-1, 1]}, "current_unbounded": {"documents": ["current", "unbounded"]}, "all": {"documents": ["unbounded", "unbounded"]}, "prev2": {"documents": [-2, 0]},
    "range": {"range": [-1, 1]}, "range_unbounded": {"range": ["unbounded", 0]},
}

OPS = {
    "top": {"$top": {"output": "$v", "sortBy": {"v": -1}}},
    "bottom": {"$bottom": {"output": "$v", "sortBy": {"v": -1}}},
    "top_doc": {"$top": {"output": {"v": "$v", "w": "$w"}, "sortBy": {"v": 1, "_id": 1}}},
    "topN": {"$topN": {"output": "$v", "sortBy": {"v": 1, "_id": 1}, "n": 2}},
    "bottomN": {"$bottomN": {"output": "$v", "sortBy": {"v": 1, "_id": 1}, "n": 2}},
    "topN_doc": {"$topN": {"output": ["$_id", "$w"], "sortBy": {"w": -1, "_id": 1}, "n": 3}},
    "firstN": {"$firstN": {"input": "$v", "n": 2}},
    "lastN": {"$lastN": {"input": "$v", "n": 2}},
    "maxN": {"$maxN": {"input": "$v", "n": 2}},
    "minN": {"$minN": {"input": "$v", "n": 2}},
    "firstN_big": {"$firstN": {"input": "$w", "n": 10}},
    "maxN_w": {"$maxN": {"input": "$w", "n": 1}},
    "n_expr": {"$topN": {"output": "$v", "sortBy": {"_id": 1}, "n": {"$literal": 2}}},
}


def _mk(name, op, wname, win):
    def f(client, db):
        seed_window(db)
        sb = {"x": 1} if "range" in win else None   # (a range window needs a single sort field; ties then come in input order)
        return {"plain": run_window(db, {"r": {**op, "window": win}}, sb), "partition": run_window(db, {"r": {**op, "window": win}}, sb, partitionBy="$p")}
    return f


for _n, _op in OPS.items():
    for _wn, _w in WINDOWS.items():
        if "range" in _wn and True:
            pass
        case(f"g09.window.{_n}.{_wn}")(_mk(_n, _op, _wn, _w))


@case("g09.window.default_window_and_two_outputs")
def _default(client, db):
    seed_window(db)
    out = {}
    out["no_window"] = run_window(db, {"r": {"$top": {"output": "$v", "sortBy": {"_id": 1}}}})
    out["two"] = run_window(db, {"r": {"$topN": {"output": "$_id", "sortBy": {"_id": -1}, "n": 2}, "window": {"documents": ["unbounded", "current"]}}, "q": {"$bottom": {"output": "$_id", "sortBy": {"_id": -1}}, "window": {"documents": [-1, 1]}}})
    return out


@case("g09.window.errors")
def _errors(client, db):
    seed_window(db)
    out = {}
    W = {"documents": ["unbounded", "current"]}
    bad = {
        "top_no_sortBy": {"$top": {"output": "$v"}, "window": W}, "top_no_output": {"$top": {"sortBy": {"v": 1}}, "window": W}, "topN_no_n": {"$topN": {"output": "$v", "sortBy": {"v": 1}}, "window": W},
        "topN_n_zero": {"$topN": {"output": "$v", "sortBy": {"v": 1}, "n": 0}, "window": W}, "topN_n_neg": {"$topN": {"output": "$v", "sortBy": {"v": 1}, "n": -1}, "window": W}, "topN_n_str": {"$topN": {"output": "$v", "sortBy": {"v": 1}, "n": "x"}, "window": W},
        "topN_n_double": {"$topN": {"output": "$v", "sortBy": {"v": 1}, "n": 2.5}, "window": W}, "topN_n_field": {"$topN": {"output": "$v", "sortBy": {"v": 1}, "n": "$x"}, "window": W}, "firstN_no_n": {"$firstN": {"input": "$v"}, "window": W}, "firstN_no_input": {"$firstN": {"n": 2}, "window": W},
        "firstN_extra": {"$firstN": {"input": "$v", "n": 2, "foo": 1}, "window": W}, "top_extra": {"$top": {"output": "$v", "sortBy": {"v": 1}, "n": 2}, "window": W}, "top_arg_type": {"$top": 5, "window": W}, "maxN_n_zero": {"$maxN": {"input": "$v", "n": 0}, "window": W},
        "top_sort_empty": {"$top": {"output": "$v", "sortBy": {}}, "window": W}, "top_sort_bad": {"$top": {"output": "$v", "sortBy": {"v": 2}}, "window": W}, "firstN_array_n": {"$firstN": {"input": "$v", "n": [1]}, "window": W},
    }
    for name, o in bad.items():
        out[name] = run_window(db, {"r": o})
    return out


@case("g09.window.range_needs_one_sort_field")
def _range_sort(client, db):
    seed_window(db)
    return {"two": run_window(db, {"r": {"$sum": "$v", "window": {"range": [-1, 1]}}}), "one": run_window(db, {"r": {"$sum": "$x", "window": {"range": [-1, 1]}}}, {"x": 1})}


@case("g09.group.n_accumulators")
def _group_n(client, db):
    seed_window(db)
    out = {}
    accs = {"firstN": {"$firstN": {"input": "$w", "n": 3}}, "lastN": {"$lastN": {"input": "$w", "n": 3}}, "maxN": {"$maxN": {"input": "$v", "n": 3}}, "minN": {"$minN": {"input": "$v", "n": 3}}, "top": {"$top": {"output": "$w", "sortBy": {"x": -1, "_id": 1}}},
            "bottom": {"$bottom": {"output": ["$_id", "$w"], "sortBy": {"x": -1, "_id": 1}}}, "topN": {"$topN": {"output": "$_id", "sortBy": {"v": 1, "_id": 1}, "n": 3}}, "bottomN": {"$bottomN": {"output": "$_id", "sortBy": {"v": 1, "_id": 1}, "n": 3}}}
    for name, acc in accs.items():
        out[name] = tr(lambda: list(db.t.aggregate([{"$sort": {"_id": 1}}, {"$group": {"_id": "$p", "r": acc}}, {"$sort": {"_id": 1}}])))
    return out
