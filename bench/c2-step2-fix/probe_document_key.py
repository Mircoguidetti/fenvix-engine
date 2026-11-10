import pymongo,time,json
c=pymongo.MongoClient("mongodb://127.0.0.1:28100")
for _ in range(30):
    try: c.admin.command("addShard","sh/127.0.0.1:28102"); break
    except Exception as e: time.sleep(1)
c.admin.command("enableSharding","t")
def run(name,key,ops):
    c.admin.command("shardCollection",f"t.{name}",key=key)
    m=c.t[name]
    with m.watch(full_document="updateLookup",max_await_time_ms=500) as st:
        for f in ops: f(m)
        out=[]
        for _ in range(40):
            e=st.try_next()
            if e: out.append((e["operationType"],dict(e["documentKey"]), e.get("fullDocument")))
            elif len(out)>=len(ops): break
    print(name,key)
    for o in out: print("  ",o)
run("one",{"u":1},[lambda m:m.insert_one({"x":0,"_id":1,"u":"bob"}),lambda m:m.update_one({"_id":1,"u":"bob"},{"$set":{"n":1}}),lambda m:m.replace_one({"_id":1,"u":"bob"},{"u":"bob","r":1}),lambda m:m.delete_one({"_id":1,"u":"bob"}),
   lambda m:m.insert_one({"_id":2}), lambda m:m.update_one({"_id":2},{"$set":{"n":1}}), lambda m:m.delete_one({"_id":2})])
run("two",{"u":1,"ts":1},[lambda m:m.insert_one({"ts":5,"_id":1,"u":"bob"}),lambda m:m.update_one({"_id":1,"u":"bob","ts":5},{"$set":{"n":1}}),lambda m:m.delete_one({"_id":1,"u":"bob"}),lambda m:m.insert_one({"_id":3,"u":"x"}),lambda m:m.delete_one({"_id":3})])
run("hashid",{"u":"hashed"},[lambda m:m.insert_one({"_id":1,"u":"bob"})])
run("dot",{"a.b":1},[lambda m:m.insert_one({"_id":1,"a":{"b":7,"c":1}}),lambda m:m.delete_one({"_id":1})])
run("withid",{"u":1,"_id":1},[lambda m:m.insert_one({"_id":1,"u":"bob"}),lambda m:m.delete_one({"_id":1,"u":"bob"})])
# shard key change via txn-less retryable write
import uuid
run("move",{"u":1},[lambda m:m.insert_one({"_id":1,"u":"alice"})])
m=c.t.move
with m.watch(max_await_time_ms=500) as st:
    with c.start_session() as s:
        m.update_one({"_id":1,"u":"alice"},{"$set":{"u":"bob"}},session=s)
    out=[]
    for _ in range(20):
        e=st.try_next()
        if e: out.append((e["operationType"],dict(e["documentKey"])))
    print("move:",out)
