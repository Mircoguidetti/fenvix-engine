"""Atlas Search index management on the engine (pymongo): createSearchIndexes, updateSearchIndex, dropSearchIndex and $listSearchIndexes
through the driver members create_search_index(es), list_search_indexes, update_search_index and drop_search_index, and as raw commands.
mongod 7.0 Community has none of these (no reference server), so each case states what the documented commands and the drivers'
index-management specification require.  Usage: search_index_test.py [mongodb-uri]"""
import sys, pymongo
from pymongo.operations import SearchIndexModel
from pymongo.errors import OperationFailure
uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?directConnection=true"
c = pymongo.MongoClient(uri); db = c["search_index_test"]
c.drop_database("search_index_test")
passed, fails = 0, []
def check(name, got, want):
    global passed
    if got == want: passed += 1
    else: fails.append(f"{name}: got {got!r} want {want!r}")
def code(f):
    try: f(); return None
    except OperationFailure as e: return e.code
fresh = lambda n: (db.drop_collection(n), db.create_collection(n))[1]

t = fresh("t")
name = t.create_search_index(SearchIndexModel(definition={"mappings": {"dynamic": False}}, name="test-search-index"))
check("create returns the name", name, "test-search-index")
l = list(t.list_search_indexes())
check("list one", [x["name"] for x in l], ["test-search-index"])
x = l[0]
check("list fields", sorted(x), ["id", "latestDefinition", "latestDefinitionVersion", "name", "queryable", "status", "statusDetail", "type"])
check("list definition", x["latestDefinition"], {"mappings": {"dynamic": False}})
check("list state", (x["status"], x["queryable"], x["type"]), ("READY", True, "search"))
check("list id is a 24-hex string", isinstance(x["id"], str) and len(x["id"]) == 24, True)
check("list by name", [y["name"] for y in t.list_search_indexes("test-search-index")], ["test-search-index"])
check("list by unknown name", list(t.list_search_indexes("nope")), [])
check("list by id (command)", [y["name"] for y in t.aggregate([{"$listSearchIndexes": {"id": x["id"]}}])], ["test-search-index"])
check("list: name and id", code(lambda: list(t.aggregate([{"$listSearchIndexes": {"id": x["id"], "name": "a"}}]))), 2)
check("list: unknown option", code(lambda: list(t.aggregate([{"$listSearchIndexes": {"foo": 1}}]))), 40415)
check("list: not the first stage", code(lambda: list(t.aggregate([{"$match": {}}, {"$listSearchIndexes": {}}]))), 40602)
check("list: later stages run", [y["name"] for y in t.aggregate([{"$listSearchIndexes": {}}, {"$project": {"name": 1, "_id": 0}}])], ["test-search-index"] and ["test-search-index"])
check("list on a missing collection", list(db["missing"].list_search_indexes()), [])
# several at once, default name, vectorSearch type
t2 = fresh("t2")
names = t2.create_search_indexes([SearchIndexModel(definition={"mappings": {"dynamic": True}}), SearchIndexModel(definition={"fields": [{"type": "vector", "path": "v", "numDimensions": 3, "similarity": "cosine"}]}, name="vec", type="vectorSearch")])
check("create several", names, ["default", "vec"])
check("types", sorted((y["name"], y["type"]) for y in t2.list_search_indexes()), [("default", "search"), ("vec", "vectorSearch")])
check("duplicate name", code(lambda: t2.create_search_index(SearchIndexModel(definition={}, name="vec"))), 68)
check("duplicate in one request", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": [{"name": "z", "definition": {}}, {"name": "z", "definition": {}}]})), 68)
r = db.command({"createSearchIndexes": "t2", "indexes": [{"name": "raw", "definition": {"mappings": {"dynamic": True}}}]})
check("raw create reply", (sorted(r["indexesCreated"][0]), r["indexesCreated"][0]["name"], r["ok"]), (["id", "name"], "raw", 1.0))
# update
v1 = next(y for y in t2.list_search_indexes("raw"))["latestDefinitionVersion"]["version"]
t2.update_search_index("raw", {"mappings": {"dynamic": False, "fields": {"a": {"type": "string"}}}})
y = next(iter(t2.list_search_indexes("raw")))
check("update definition", y["latestDefinition"], {"mappings": {"dynamic": False, "fields": {"a": {"type": "string"}}}})
check("update bumps the version", y["latestDefinitionVersion"]["version"], v1 + 1)
check("update by id", code(lambda: db.command({"updateSearchIndex": "t2", "id": y["id"], "definition": {"mappings": {"dynamic": True}}})), None)
check("update unknown name", code(lambda: t2.update_search_index("nope", {})), 27)
check("update without a definition", code(lambda: db.command({"updateSearchIndex": "t2", "name": "raw"})), 40414)
check("update without name or id", code(lambda: db.command({"updateSearchIndex": "t2", "definition": {}})), 2)
check("update with name and id", code(lambda: db.command({"updateSearchIndex": "t2", "name": "raw", "id": y["id"], "definition": {}})), 2)
check("update: definition type", code(lambda: db.command({"updateSearchIndex": "t2", "name": "raw", "definition": 5})), 14)
check("update on a missing collection", code(lambda: db.command({"updateSearchIndex": "missing", "name": "a", "definition": {}})), 26)
# drop
t2.drop_search_index("vec")
check("drop", sorted(y["name"] for y in t2.list_search_indexes()), ["default", "raw"])
check("drop unknown name", code(lambda: db.command({"dropSearchIndex": "t2", "name": "vec"})), 27)
check("drop by id", code(lambda: db.command({"dropSearchIndex": "t2", "id": next(iter(t2.list_search_indexes("raw")))["id"]})), None)
check("drop without name or id", code(lambda: db.command({"dropSearchIndex": "t2"})), 2)
check("drop: name type", code(lambda: db.command({"dropSearchIndex": "t2", "name": 5})), 14)
check("drop: unknown field", code(lambda: db.command({"dropSearchIndex": "t2", "name": "a", "bogus": 1})), 40415)
check("drop on a missing collection is suppressed by the driver", code(lambda: db["missing"].drop_search_index("a")), None)
check("drop on a missing collection (command)", code(lambda: db.command({"dropSearchIndex": "missing", "name": "a"})), 26)
# argument validation of the create command
check("create: no indexes", code(lambda: db.command({"createSearchIndexes": "t2"})), 40414)
check("create: indexes type", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": 5})), 14)
check("create: no definition", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": [{"name": "a"}]})), 40414)
check("create: definition type", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": [{"name": "a", "definition": 5}]})), 14)
check("create: unknown index field", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": [{"name": "a", "definition": {}, "bogus": 1}]})), 40415)
check("create: bad type", code(lambda: db.command({"createSearchIndexes": "t2", "indexes": [{"name": "a", "definition": {}, "type": "x"}]})), 2)
check("create on a missing collection", code(lambda: db.command({"createSearchIndexes": "missing", "indexes": [{"name": "a", "definition": {}}]})), 26)
# dropping the collection drops its definitions
t3 = fresh("t3"); t3.create_search_index(SearchIndexModel(definition={}, name="gone"))
db.drop_collection("t3"); db.create_collection("t3")
check("definitions go with the collection", list(db["t3"].list_search_indexes()), [])
# $search works with and without a definition
t4 = fresh("t4"); t4.insert_many([{"_id": 1, "s": "hello world"}, {"_id": 2, "s": "other"}])
check("$search without an index definition", [d["_id"] for d in t4.aggregate([{"$search": {"text": {"query": "hello", "path": "s"}}}])], [1])
t4.create_search_index(SearchIndexModel(definition={"mappings": {"dynamic": True}}, name="idx"))
check("$search with a named index", [d["_id"] for d in t4.aggregate([{"$search": {"index": "idx", "text": {"query": "hello", "path": "s"}}}])], [1])
check("$searchMeta count", [d for d in t4.aggregate([{"$searchMeta": {"index": "idx", "text": {"query": "hello", "path": "s"}, "count": {"type": "total"}}}])], [{"count": {"total": 1}}])
print(f"search index tests (pymongo): {passed} passed, {len(fails)} failed")
for f in fails: print("FAIL", f)
sys.exit(1 if fails else 0)
