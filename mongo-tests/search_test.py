"""Tests of $search / $searchMeta against the documented Atlas Search semantics (mongo:7.0 has no Atlas Search, so
there is no reference server to compare with): each case states the documents the documentation says must match.
Usage: search_test.py [mongodb-uri]"""
import sys, pymongo
uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?directConnection=true"
db = pymongo.MongoClient(uri)["search_test"]
db.drop_collection("c"); c = db.c
c.insert_many([
    {"_id": 1, "title": "The Quick Brown Fox", "tags": ["animal", "fast"], "n": 5, "d": {"x": "nested fox"}, "ok": True},
    {"_id": 2, "title": "quick brown dogs", "tags": ["animal"], "n": 15, "ok": False},
    {"_id": 3, "title": "A lazy brown fox jumped over the fence", "tags": [], "n": 25, "ok": True},
    {"_id": 4, "title": "Foxes and hounds", "tags": ["hunt"], "n": 35},
    {"_id": 5, "title": "Brown bread recipe", "n": None},
    {"_id": 6, "title": "quick fix", "tags": ["fast", "repair"], "n": 45, "ok": True},
    {"_id": 7, "title": "fax machine", "n": 55},
])
fails = []
def ids(op, **kw):
    return sorted(d["_id"] for d in c.aggregate([{"$search": dict(op, **kw)}]))
def check(name, got, want):
    if got != want: fails.append(f"{name}: got {got} want {want}")
T = lambda q, path="title", **kw: {"text": dict({"query": q, "path": path}, **kw)}
check("text one term", ids(T("fox")), [1, 3])
check("text any of terms", ids(T("quick dogs")), [1, 2, 6])
check("text all", ids(T("quick brown", matchCriteria="all")), [1, 2])
check("text case-insensitive", ids(T("QUICK")), [1, 2, 6])
check("text no stemming", ids(T("foxes")), [4])
check("text multi path", ids(T("animal fix", path=["title", "tags"])), [1, 2, 6])
check("text wildcard path", ids(T("nested", path={"wildcard": "*"})), [1])
check("text array of queries", ids(T(["lazy", "bread"])), [3, 5])
check("text fuzzy", ids(T("fax", fuzzy={"maxEdits": 1})), [1, 3, 6, 7])
check("text fuzzy prefix", ids(T("fax", fuzzy={"maxEdits": 1, "prefixLength": 1})), [1, 3, 6, 7])
check("text fuzzy prefix 2", ids(T("fox", fuzzy={"maxEdits": 1, "prefixLength": 2})), [1, 3])
check("phrase", ids({"phrase": {"query": "brown fox", "path": "title"}}), [1, 3])
check("phrase order", ids({"phrase": {"query": "fox brown", "path": "title"}}), [])
check("phrase slop", ids({"phrase": {"query": "quick fox", "path": "title", "slop": 1}}), [1])
check("autocomplete", ids({"autocomplete": {"query": "qui", "path": "title"}}), [1, 2, 6])
check("autocomplete two", ids({"autocomplete": {"query": "brown fo", "path": "title"}}), [1, 3])
check("autocomplete sequential", ids({"autocomplete": {"query": "quick br", "path": "title", "tokenOrder": "sequential"}}), [1, 2])
check("autocomplete one char", ids({"autocomplete": {"query": "q", "path": "title"}}), [])
check("equals bool", ids({"equals": {"path": "ok", "value": True}}), [1, 3, 6])
check("equals number", ids({"equals": {"path": "n", "value": 15}}), [2])
check("equals array element", ids({"equals": {"path": "tags", "value": "fast"}}), [1, 6])
check("in", ids({"in": {"path": "n", "value": [5, 25, 99]}}), [1, 3])
check("exists", ids({"exists": {"path": "tags"}}), [1, 2, 4, 6])
check("exists null", ids({"exists": {"path": "n"}}), [1, 2, 3, 4, 6, 7])
check("range", ids({"range": {"path": "n", "gt": 10, "lte": 35}}), [2, 3, 4])
check("range open", ids({"range": {"path": "n", "gte": 45}}), [6, 7])
check("wildcard", ids({"wildcard": {"query": "qu*", "path": "title", "allowAnalyzedField": True}}), [1, 2, 6])
check("regex", ids({"regex": {"query": "f.x", "path": "title", "allowAnalyzedField": True}}), [1, 3, 6, 7])
check("compound must", ids({"compound": {"must": [T("brown"), T("fox")]}}), [1, 3])
check("compound mustNot", ids({"compound": {"must": [T("brown")], "mustNot": [T("fox")]}}), [2, 5])
check("compound filter", ids({"compound": {"must": [T("brown")], "filter": [{"equals": {"path": "ok", "value": True}}]}}), [1, 3])
check("compound should only", ids({"compound": {"should": [T("bread"), T("dogs")]}}), [2, 5])
check("compound minimumShouldMatch", ids({"compound": {"should": [T("quick"), T("brown"), T("fox")], "minimumShouldMatch": 2}}), [1, 2, 3])
check("compound must+should does not filter", ids({"compound": {"must": [T("brown")], "should": [T("bread")]}}), [1, 2, 3, 5])
check("score order", [d["_id"] for d in c.aggregate([{"$search": T("fox")}, {"$project": {"s": {"$meta": "searchScore"}}}, {"$limit": 5}])][0] in (1, 3), True)
sc = list(c.aggregate([{"$search": T("fox")}, {"$project": {"s": {"$meta": "searchScore"}}}]))
check("score descending", [d["s"] for d in sc] == sorted([d["s"] for d in sc], reverse=True) and all(d["s"] > 0 for d in sc), True)
check("boost", [d["_id"] for d in c.aggregate([{"$search": {"compound": {"should": [T("bread"), {"text": {"query": "dogs", "path": "title", "score": {"boost": {"value": 10}}}}]}}}])], [2, 5])
check("constant score", {d["s"] for d in c.aggregate([{"$search": {"text": {"query": "fox", "path": "title", "score": {"constant": {"value": 7}}}}}, {"$project": {"s": {"$meta": "searchScore"}}}])}, {7.0})
check("searchMeta lowerBound", list(c.aggregate([{"$searchMeta": T("fox")}])), [{"count": {"lowerBound": 2}}])
check("searchMeta total", list(c.aggregate([{"$searchMeta": dict(T("brown"), count={"type": "total"})}])), [{"count": {"total": 4}}])
check("sort option", [d["_id"] for d in c.aggregate([{"$search": dict(T("brown"), sort={"n": -1})}])], [3, 2, 1, 5])
check("pipeline continues", list(c.aggregate([{"$search": T("fox")}, {"$match": {"n": {"$gt": 10}}}, {"$project": {"_id": 1}}])), [{"_id": 3}])
for name, pipe in [("not first", [{"$match": {}}, {"$search": T("fox")}]), ("unknown operator", [{"$search": {"near": {"path": "n", "origin": 1, "pivot": 2}}}]),
                   ("highlight", [{"$search": dict(T("fox"), highlight={"path": "title"})}]), ("no operator", [{"$search": {"index": "default"}}]),
                   ("bad compound", [{"$search": {"compound": {}}}]), ("bad fuzzy", [{"$search": T("fox", fuzzy={"maxEdits": 5})}])]:
    try:
        list(c.aggregate(pipe)); fails.append(f"{name}: expected an error")
    except pymongo.errors.OperationFailure:
        pass
print("search tests:", 47 - len(fails), "passed,", len(fails), "failed"); [print(" ", f) for f in fails]
sys.exit(1 if fails else 0)
