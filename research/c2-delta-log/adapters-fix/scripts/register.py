"""Register the agent checkpoint collections with the server after a restart, without changing data: re-issue each
existing index of `checkpoints` and `checkpoint_writes` with createIndexes (same key, same options: a no-op on mongod).
On our server createIndexes calls custkey::for_write, which notes the collection's customer key (agentck::note_key)."""
import sys, pymongo
c = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")
db = c[sys.argv[1]]
for coll in ("checkpoints", "checkpoint_writes"):
    for ix in db[coll].list_indexes():
        if ix["name"] == "_id_":
            continue
        spec = {k: v for k, v in ix.items() if k not in ("v", "ns")}
        r = db.command("createIndexes", coll, indexes=[spec])
        print(coll, ix["name"], {k: v for k, v in r.items() if k in ("numIndexesBefore", "numIndexesAfter", "note", "ok")})
