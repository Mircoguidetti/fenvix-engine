"""Driver API member -> what the engine must provide for it.

For each driver class, SERVER maps a canonical member key (lower case, no underscores, so pymongo's find_one_and_update and
node's findOneAndUpdate are the same key) to the inventory command ids ("cmd:x") or stage/feature ids it depends on. Members that
are not listed are client-side only: properties, option holders, SDAM views, event emitters; they send no request of their own
(listed in client_only(), reviewed by eye, and proposed as exclusions).

HARNESS maps the operation names used by the differential harnesses (mongo-tests/run_py.py, node/run_node.js) to the driver
members they call, which is what lets a passing differential test be cited as evidence for a driver row."""
import re


def canon(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


# --- members the differential harnesses call (op name -> canonical member keys, per class)
HARNESS = {
    "insert_one": ["Collection.insertone"], "insert_many": ["Collection.insertmany"], "insert_generated": ["Collection.insertmany"],
    "find": ["Collection.find", "Cursor.next", "FindCursor.toarray"], "find_one": ["Collection.findone"], "find_count": ["Collection.find"],
    "count_documents": ["Collection.countdocuments"], "estimated_count": ["Collection.estimateddocumentcount"],
    "aggregate": ["Collection.aggregate", "AggregationCursor.toarray"], "distinct": ["Collection.distinct"],
    "update_one": ["Collection.updateone"], "update_many": ["Collection.updatemany"], "replace_one": ["Collection.replaceone"],
    "delete_one": ["Collection.deleteone"], "delete_many": ["Collection.deletemany"],
    "find_one_and_update": ["Collection.findoneandupdate"], "find_one_and_replace": ["Collection.findoneandreplace"],
    "find_one_and_delete": ["Collection.findoneanddelete"],
    "create_collection": ["Database.createcollection", "Db.createcollection"], "drop_collection": ["Collection.drop"],
    "list_collection_names": ["Database.listcollectionnames", "Db.listcollections"], "list_collections": ["Database.listcollections", "Db.listcollections"],
    "create_index": ["Collection.createindex"], "drop_index": ["Collection.dropindex"], "list_indexes": ["Collection.listindexes"],
    "command": ["Database.command", "Db.command"], "command_raw": ["Database.command", "Db.command"],
    "admin_command": ["Database.command", "Db.command"],
    "db_in_list": ["MongoClient.listdatabasenames", "Admin.listdatabases"], "db_in_list_command": ["Database.command", "Db.command"],
    "drop_database": ["MongoClient.dropdatabase", "Db.dropdatabase"],
    "session_insert": ["MongoClient.startsession", "ClientSession.endsession"],
    "txn_start": ["MongoClient.startsession", "ClientSession.starttransaction"],
    "txn_commit": ["ClientSession.committransaction", "ClientSession.endsession"], "txn_abort": ["ClientSession.aborttransaction", "ClientSession.endsession"],
    "with_txn": ["ClientSession.withtransaction", "MongoClient.startsession"],
    "watch": ["Collection.watch", "ChangeStream.next", "ChangeStream.trynext", "ChangeStream.close", "ChangeStream.resumetoken"],
    "insert_one_w0": ["Collection.withoptions", "Db.collection"],
}

# --- server-side dependencies of members the harness does not call (class.key -> ids)
SERVER = {
    # Collection (both drivers)
    "Collection.bulkwrite": ["cmd:insert", "cmd:update", "cmd:delete"], "Collection.count": ["cmd:count"],
    "Collection.createindexes": ["cmd:createIndexes"], "Collection.dropindexes": ["cmd:dropIndexes"],
    "Collection.indexes": ["cmd:listIndexes"], "Collection.indexinformation": ["cmd:listIndexes"], "Collection.indexexists": ["cmd:listIndexes"],
    "Collection.rename": ["cmd:renameCollection"], "Collection.options": ["cmd:listCollections"], "Collection.iscapped": ["cmd:listCollections"],
    "Collection.aggregateRawBatches": ["cmd:aggregate"], "Collection.aggregaterawbatches": ["cmd:aggregate"],
    "Collection.findrawbatches": ["cmd:find"], "Collection.initializeorderedbulkop": ["cmd:insert", "cmd:update", "cmd:delete"],
    "Collection.initializeunorderedbulkop": ["cmd:insert", "cmd:update", "cmd:delete"],
    "Collection.createsearchindex": ["cmd:createSearchIndexes"], "Collection.createsearchindexes": ["cmd:createSearchIndexes"],
    "Collection.dropsearchindex": ["cmd:dropSearchIndex"], "Collection.updatesearchindex": ["cmd:updateSearchIndex"],
    "Collection.listsearchindexes": ["stage:$listSearchIndexes"],
    # Database / Db
    "Database.aggregate": ["cmd:aggregate"], "Db.aggregate": ["cmd:aggregate"],
    "Database.cursorcommand": ["cmd:find"], "Db.runcursorcommand": ["cmd:find"], "Database.dereference": ["cmd:find"],
    "Database.dropcollection": ["cmd:drop"], "Db.dropcollection": ["cmd:drop"], "Database.listcollections": ["cmd:listCollections"],
    "Db.collections": ["cmd:listCollections"], "Database.validatecollection": ["cmd:validate"], "Db.stats": ["cmd:dbStats"],
    "Database.watch": ["stage:$changeStream"], "Db.watch": ["stage:$changeStream"], "Db.createindex": ["cmd:createIndexes"],
    "Db.indexinformation": ["cmd:listIndexes"], "Db.renamecollection": ["cmd:renameCollection"], "Db.profilinglevel": ["cmd:profile"],
    "Db.setprofilinglevel": ["cmd:profile"], "Db.removeuser": ["cmd:dropUser"], "Db.dropdatabase": ["cmd:dropDatabase"],
    # MongoClient
    "MongoClient.listdatabases": ["cmd:listDatabases"], "MongoClient.listdatabasenames": ["cmd:listDatabases"],
    "MongoClient.dropdatabase": ["cmd:dropDatabase"], "MongoClient.serverinfo": ["cmd:buildInfo"], "MongoClient.watch": ["stage:$changeStream"],
    "MongoClient.bulkwrite": ["cmd:bulkWrite"], "MongoClient.close": ["cmd:endSessions"], "MongoClient.connect": ["cmd:hello"],
    "MongoClient.startsession": ["cmd:endSessions"],
    # Admin (node)
    "Admin.buildinfo": ["cmd:buildInfo"], "Admin.serverinfo": ["cmd:buildInfo"], "Admin.ping": ["cmd:ping"], "Admin.listdatabases": ["cmd:listDatabases"],
    "Admin.serverstatus": ["cmd:serverStatus"], "Admin.replsetgetstatus": ["cmd:replSetGetStatus"], "Admin.command": ["cmd:ping"],
    "Admin.removeuser": ["cmd:dropUser"], "Admin.validatecollection": ["cmd:validate"],
    # cursors
    "Cursor.next": ["cmd:find", "cmd:getMore"], "Cursor.close": ["cmd:killCursors"], "Cursor.explain": ["cmd:explain"], "Cursor.distinct": ["cmd:distinct"],
    "Cursor.tolist": ["cmd:getMore"], "Cursor.rewind": ["cmd:find"], "Cursor.clone": ["cmd:find"], "Cursor.where": ["qop:$where"],
    "Cursor.max": ["opt:find.max"], "Cursor.min": ["opt:find.min"], "Cursor.hint": ["opt:find.hint"], "Cursor.sort": ["opt:find.sort"],
    "Cursor.limit": ["opt:find.limit"], "Cursor.skip": ["opt:find.skip"], "Cursor.batchsize": ["opt:find.batchSize"],
    "Cursor.collation": ["opt:find.collation"], "Cursor.comment": ["opt:find.comment"], "Cursor.maxtimems": ["opt:find.maxTimeMS"],
    "Cursor.allowdiskuse": ["opt:find.allowDiskUse"], "Cursor.maxawaittimems": ["cmd:getMore"], "Cursor.addoption": ["opt:find.tailable"],
    "Cursor.removeoption": ["opt:find.tailable"], "Cursor.maxscan": ["cmd:find"],
    "FindCursor.toarray": ["cmd:find", "cmd:getMore"], "FindCursor.next": ["cmd:find", "cmd:getMore"], "FindCursor.trynext": ["cmd:getMore"],
    "FindCursor.close": ["cmd:killCursors"], "FindCursor.explain": ["cmd:explain"], "FindCursor.count": ["cmd:count"], "FindCursor.hasnext": ["cmd:getMore"],
    "FindCursor.foreach": ["cmd:getMore"], "FindCursor.stream": ["cmd:getMore"], "FindCursor.rewind": ["cmd:find"], "FindCursor.clone": ["cmd:find"],
    "FindCursor.filter": ["opt:find.filter"], "FindCursor.project": ["opt:find.projection"], "FindCursor.sort": ["opt:find.sort"],
    "FindCursor.limit": ["opt:find.limit"], "FindCursor.skip": ["opt:find.skip"], "FindCursor.hint": ["opt:find.hint"],
    "FindCursor.min": ["opt:find.min"], "FindCursor.max": ["opt:find.max"], "FindCursor.collation": ["opt:find.collation"],
    "FindCursor.comment": ["opt:find.comment"], "FindCursor.allowdiskuse": ["opt:find.allowDiskUse"], "FindCursor.returnkey": ["opt:find.returnKey"],
    "FindCursor.showrecordid": ["opt:find.showRecordId"], "FindCursor.maxawaittimems": ["cmd:getMore"], "FindCursor.maxtimems": ["opt:find.maxTimeMS"],
    "FindCursor.batchsize": ["opt:find.batchSize"], "FindCursor.addquerymodifier": ["cmd:find"],
    "AggregationCursor.toarray": ["cmd:aggregate", "cmd:getMore"], "AggregationCursor.next": ["cmd:aggregate", "cmd:getMore"],
    "AggregationCursor.explain": ["cmd:explain"], "AggregationCursor.addstage": ["cmd:aggregate"], "AggregationCursor.geonear": ["stage:$geoNear"],
    "AggregationCursor.group": ["stage:$group"], "AggregationCursor.limit": ["stage:$limit"], "AggregationCursor.lookup": ["stage:$lookup"],
    "AggregationCursor.match": ["stage:$match"], "AggregationCursor.out": ["stage:$out"], "AggregationCursor.project": ["stage:$project"],
    "AggregationCursor.redact": ["stage:$redact"], "AggregationCursor.skip": ["stage:$skip"], "AggregationCursor.sort": ["stage:$sort"],
    "AggregationCursor.unwind": ["stage:$unwind"], "AggregationCursor.close": ["cmd:killCursors"], "AggregationCursor.trynext": ["cmd:getMore"],
    "AggregationCursor.hasnext": ["cmd:getMore"], "AggregationCursor.foreach": ["cmd:getMore"], "AggregationCursor.stream": ["cmd:getMore"],
    "AggregationCursor.rewind": ["cmd:aggregate"], "AggregationCursor.clone": ["cmd:aggregate"], "AggregationCursor.batchsize": ["cmd:aggregate"],
    "AggregationCursor.maxtimems": ["cmd:aggregate"],
    "AbstractCursor.next": ["cmd:getMore"], "AbstractCursor.toarray": ["cmd:getMore"], "AbstractCursor.trynext": ["cmd:getMore"],
    "AbstractCursor.hasnext": ["cmd:getMore"], "AbstractCursor.foreach": ["cmd:getMore"], "AbstractCursor.stream": ["cmd:getMore"],
    "AbstractCursor.close": ["cmd:killCursors"], "AbstractCursor.batchsize": ["cmd:getMore"], "AbstractCursor.maxtimems": ["cmd:getMore"],
    "AbstractCursor.rewind": ["cmd:find"], "AbstractCursor.clone": ["cmd:find"], "AbstractCursor.addcursorflag": ["cmd:find"],
    "AbstractCursor.readbufferreddocuments": ["cmd:getMore"],
    "CommandCursor.next": ["cmd:getMore"], "CommandCursor.trynext": ["cmd:getMore"], "CommandCursor.tolist": ["cmd:getMore"], "CommandCursor.close": ["cmd:killCursors"],
    "RawBatchCommandCursor.next": ["cmd:getMore"], "RawBatchCommandCursor.trynext": ["cmd:getMore"], "RawBatchCommandCursor.tolist": ["cmd:getMore"],
    "RawBatchCommandCursor.close": ["cmd:killCursors"],
    "ListIndexesCursor.toarray": ["cmd:listIndexes"], "ListIndexesCursor.next": ["cmd:listIndexes"],
    "ListCollectionsCursor.toarray": ["cmd:listCollections"], "ListCollectionsCursor.next": ["cmd:listCollections"],
    "GridOutCursor.next": ["cmd:find", "cmd:getMore"], "GridOutCursor.tolist": ["cmd:find", "cmd:getMore"],
    # change streams
    "ChangeStream.next": ["stage:$changeStream", "cmd:getMore"], "ChangeStream.trynext": ["stage:$changeStream", "cmd:getMore"],
    "ChangeStream.close": ["cmd:killCursors"], "ChangeStream.hasnext": ["cmd:getMore"], "ChangeStream.stream": ["cmd:getMore"],
    "ChangeStream.resumetoken": ["cso:$changeStream.resumeAfter"],
    # sessions / transactions
    "ClientSession.committransaction": ["cmd:commitTransaction"], "ClientSession.aborttransaction": ["cmd:abortTransaction"],
    "ClientSession.starttransaction": ["cmd:commitTransaction", "cmd:abortTransaction"], "ClientSession.withtransaction": ["cmd:commitTransaction", "cmd:abortTransaction"],
    "ClientSession.endsession": ["cmd:endSessions"],
    "ClientSession.clustertime": ["sess:$clusterTime"], "ClientSession.operationtime": ["sess:causal consistency"],
    "ClientSession.advanceclustertime": ["sess:$clusterTime"], "ClientSession.advanceoperationtime": ["sess:causal consistency"],
    # bulk (node)
    "BulkOperationBase.execute": ["cmd:insert", "cmd:update", "cmd:delete"], "BulkOperationBase.insert": ["cmd:insert"], "BulkOperationBase.find": ["cmd:update", "cmd:delete"],
    "BulkOperationBase.raw": ["cmd:insert", "cmd:update", "cmd:delete"],
    # operations / index models (pymongo)
    "operations.InsertOne": ["cmd:insert"], "operations.ReplaceOne": ["cmd:update"], "operations.UpdateOne": ["cmd:update"], "operations.UpdateMany": ["cmd:update"],
    "operations.DeleteOne": ["cmd:delete"], "operations.DeleteMany": ["cmd:delete"], "operations.IndexModel": ["cmd:createIndexes"],
    "operations.SearchIndexModel": ["cmd:createSearchIndexes"],
    # GridFS (all members compose find/insert/delete/createIndexes/listIndexes on fs.files and fs.chunks)
}
OPDEPS = {
    "insert_one": ["cmd:insert"], "insert_many": ["cmd:insert"], "insert_generated": ["cmd:insert"], "find": ["cmd:find", "cmd:getMore"], "find_one": ["cmd:find"],
    "find_count": ["cmd:find"], "count_documents": ["cmd:aggregate"], "estimated_count": ["cmd:count"], "aggregate": ["cmd:aggregate", "cmd:getMore"],
    "distinct": ["cmd:distinct"], "update_one": ["cmd:update"], "update_many": ["cmd:update"], "replace_one": ["cmd:update"], "delete_one": ["cmd:delete"],
    "delete_many": ["cmd:delete"], "find_one_and_update": ["cmd:findAndModify"], "find_one_and_replace": ["cmd:findAndModify"],
    "find_one_and_delete": ["cmd:findAndModify"], "create_collection": ["cmd:create"], "drop_collection": ["cmd:drop"],
    "list_collection_names": ["cmd:listCollections"], "list_collections": ["cmd:listCollections"], "create_index": ["cmd:createIndexes"],
    "drop_index": ["cmd:dropIndexes"], "list_indexes": ["cmd:listIndexes"], "command": ["cmd:ping"], "command_raw": ["cmd:ping"], "admin_command": ["cmd:ping"],
    "db_in_list": ["cmd:listDatabases"], "db_in_list_command": ["cmd:listDatabases"], "drop_database": ["cmd:dropDatabase"],
    "session_insert": ["cmd:endSessions"], "txn_start": ["cmd:commitTransaction"], "txn_commit": ["cmd:commitTransaction"], "txn_abort": ["cmd:abortTransaction"],
    "with_txn": ["cmd:commitTransaction"], "watch": ["stage:$changeStream", "cmd:getMore"], "insert_one_w0": ["cmd:insert"],
}
for _op, _members in HARNESS.items():
    for _m in _members:
        SERVER.setdefault(_m, OPDEPS[_op])
GRIDFS_CMDS = ["cmd:find", "cmd:insert", "cmd:delete", "cmd:createIndexes", "cmd:listIndexes"]
GRIDFS_SERVER_KEYS = {"delete", "exists", "find", "findone", "get", "getlastversion", "getversion", "list", "newfile", "put", "deletebyname", "downloadtostream",
                      "downloadtostreambyname", "openDownloadStream", "opendownloadstream", "opendownloadstreambyname", "openuploadstream",
                      "openuploadstreamwithid", "rename", "renamebyname", "uploadfromstream", "uploadfromstreamwithid", "drop", "read", "write", "close",
                      "abort", "readchunk", "writelines"}
EXCLUDED_CLASSES = {"encryption": "client-side field-level encryption helper (libmongocrypt/KMS); the server only stores BSON binData subtype 6 and key-vault documents, no engine surface of its own",
                    "ClientEncryption": "client-side field-level encryption helper (libmongocrypt/KMS); the server only stores BSON binData subtype 6 and key-vault documents, no engine surface of its own"}


def split_member(row):
    """(class, member, raw) for a driver row."""
    name = row["name"]
    if row["id"].startswith("py:"):
        parts = name.split(".")
        label = row["category"].split(" / ")[1]
        if label in ("operations", "grid_file", "encryption", "GridFS") or len(parts) < 2:
            pass
        if label == "operations":
            return "operations", name, name
        # name like collection.Collection.find or gridfs.GridFS.delete or ASCENDING
        cls = parts[-2] if len(parts) >= 2 else ""
        mem = parts[-1]
        return cls, mem, name
    cls, mem = name.split(".", 1)
    return cls, re.sub(r"-\d+$", "", mem), name


def classify(row):
    """-> (kind, deps) with kind in 'server', 'client', 'excluded:<why>'; deps = list of inventory ids."""
    cls, mem, raw = split_member(row)
    cat = row["category"].split(" / ")[1]
    if cat in EXCLUDED_CLASSES:
        return "excluded:" + EXCLUDED_CLASSES[cat], []
    if cls == "operations" or raw.startswith("operations."):
        key = raw if raw in SERVER else None
        return ("server", SERVER[key]) if key else ("client", [])
    if row["id"].startswith("py:") and cat in ("GridFS", "grid_file"):
        if cls in ("GridFS", "GridFSBucket", "GridIn", "GridOut", "GridOutCursor") and canon(mem) in GRIDFS_SERVER_KEYS | {canon(k) for k in GRIDFS_SERVER_KEYS}:
            return "server", GRIDFS_CMDS
        return "client", []
    if row["id"].startswith("node:") and cls.startswith("GridFSBucket"):
        if cls == "GridFSBucket" and canon(mem) in {"delete", "drop", "find", "openuploadstream", "openuploadstreamwithid", "opendownloadstream",
                                                    "opendownloadstreambyname", "rename"}:
            return "server", GRIDFS_CMDS
        if cls in ("GridFSBucketWriteStream", "GridFSBucketReadStream") and canon(mem) in {"abort", "write", "end", "start", "drop", "read", "find"}:
            return "server", GRIDFS_CMDS
        return "client", []
    if mem.isupper() and len(mem) > 1:
        return "client", []                     # event-name / enum constants (AbstractCursor.CLOSE, ChangeStream.CHANGE, ...)
    k = f"{cls}.{canon(mem)}"
    if k in SERVER: return "server", SERVER[k]
    if f"{cls}.{mem}" in SERVER: return "server", SERVER[f"{cls}.{mem}"]
    # pymongo alias classes
    alias = {"CollectionChangeStream": "ChangeStream", "DatabaseChangeStream": "ChangeStream", "ClusterChangeStream": "ChangeStream",
             "RawBatchCursor": "Cursor", "GridFSBucket": "GridFSBucket", "OrderedBulkOperation": "BulkOperationBase", "UnorderedBulkOperation": "BulkOperationBase"}
    if cls in alias and f"{alias[cls]}.{canon(mem)}" in SERVER:
        return "server", SERVER[f"{alias[cls]}.{canon(mem)}"]
    return "client", []


def harness_hits(row, tests, side):
    """Names of suite tests (any step op incl. nested) whose harness calls this member."""
    cls, mem, raw = split_member(row)
    k = f"{cls}.{canon(mem)}"
    ops = {op for op, ms in HARNESS.items() if k in ms}
    if not ops:
        return []
    out = []
    for t in tests:
        used = set()
        def walk(steps):
            for s in steps:
                used.add(s["op"])
                for key in ("steps", "actions", "resume_actions"):
                    if key in s and isinstance(s[key], list): walk(s[key])
        walk(t["steps"])
        if used & ops:
            out.append(t["name"])
    return out
