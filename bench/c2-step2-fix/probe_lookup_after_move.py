import pymongo
c=pymongo.MongoClient("mongodb://127.0.0.1:28100")
c.admin.command("shardCollection","t.mv2",key={"u":1})
m=c.t.mv2
m.insert_one({"_id":1,"u":"alice"}); m.insert_one({"_id":4,"u":None})
with m.watch(full_document="updateLookup",max_await_time_ms=500) as st:
    with c.start_session() as s:
        m.update_one({"_id":1,"u":"alice"},{"$set":{"u":"bob"}},session=s)
    m.update_one({"_id":4,"u":None},{"$set":{"n":1}})
    m.delete_one({"_id":4,"u":None})
    for _ in range(20):
        e=st.try_next()
        if e: print(e["operationType"],dict(e["documentKey"]),e.get("fullDocument"))
