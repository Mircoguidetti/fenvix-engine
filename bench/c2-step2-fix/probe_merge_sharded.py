import pymongo
c=pymongo.MongoClient("mongodb://127.0.0.1:28100")
c.t.drop_collection("tg"); c.admin.command("shardCollection","t.tg",key={"u":1})
c.t.tg.insert_many([{"_id":1,"u":"A","x":1}])
src=c.t.src; src.drop(); src.insert_many([{"_id":1,"u":"B","y":2},{"_id":2,"y":3}])
def t(name, spec):
    try:
        c.t.src.aggregate([{"$match":{"_id":1}},{"$merge":spec}]); print(name,"OK",sorted(c.t.tg.find(),key=str))
    except Exception as e: print(name,"ERR",getattr(e,'code',None),str(e)[:160])
t("default",{"into":"tg"})
t("on_id",{"into":"tg","on":"_id"})
t("on_u_id",{"into":"tg","on":["u","_id"]})
t("on_id_u",{"into":"tg","on":["_id","u"]})
try:
    c.t.src.aggregate([{"$match":{"_id":2}},{"$merge":{"into":"tg"}}]); print("missing OK",sorted(c.t.tg.find(),key=str))
except Exception as e: print("missing ERR",getattr(e,'code',None),str(e)[:160])
c.t.drop_collection("tg2"); c.admin.command("shardCollection","t.tg2",key={"u":1,"ts":1})
try:
    c.t.src.aggregate([{"$match":{"_id":1}},{"$addFields":{"ts":5}},{"$merge":{"into":"tg2"}}]); print("twofield OK",list(c.t.tg2.find()))
except Exception as e: print("twofield ERR",getattr(e,'code',None),str(e)[:200])
try:
    c.t.src.aggregate([{"$match":{"_id":1}},{"$merge":{"into":"tg2","on":["u","_id"]}}]); print("twofield-partial OK")
except Exception as e: print("twofield-partial ERR",getattr(e,'code',None),str(e)[:200])
