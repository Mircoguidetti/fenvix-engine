#!/usr/bin/env python3
"""Repro: inside a transaction, a findAndModify that matches nothing makes earlier writes of the same transaction vanish
at commit (the commit answers ok). Usage: repro_txn_lost_write.py [uri]"""
import sys
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

U = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?replicaSet=rs0"
c = MongoClient(U)
db = c.get_database("repro_txn", write_concern=WriteConcern(w="majority"))


def case(name, ops):
    db.t.drop()
    db.create_collection("t")
    db.t.insert_many([{"_id": i, "v": 0} for i in (1, 2, 3)])
    with c.start_session() as s:
        s.start_transaction()
        for op in ops:
            op(s)
        s.commit_transaction()
    print(f"{name:75} -> {sorted((d['_id'], d['v']) for d in db.t.find())}")


t = lambda: db.t
case("findOneAndDelete(3); findOneAndDelete(4: no match)   expected [(1,0),(2,0)]",
     [lambda s: t().find_one_and_delete({"_id": 3}, session=s), lambda s: t().find_one_and_delete({"_id": 4}, session=s)])
case("findOneAndUpdate(1 v=1); findOneAndUpdate(9: no match) expected v of 1 = 1",
     [lambda s: t().find_one_and_update({"_id": 1}, {"$set": {"v": 1}}, session=s),
      lambda s: t().find_one_and_update({"_id": 9}, {"$set": {"v": 1}}, session=s)])
case("insertOne(4); findOneAndDelete(9: no match)          expected 4 present",
     [lambda s: t().insert_one({"_id": 4, "v": 0}, session=s), lambda s: t().find_one_and_delete({"_id": 9}, session=s)])
case("deleteOne(3); deleteOne(9: no match)                 expected [(1,0),(2,0)]",
     [lambda s: t().delete_one({"_id": 3}, session=s), lambda s: t().delete_one({"_id": 9}, session=s)])
case("updateOne(1 v=1); updateOne(9: no match)             expected v of 1 = 1",
     [lambda s: t().update_one({"_id": 1}, {"$set": {"v": 1}}, session=s),
      lambda s: t().update_one({"_id": 9}, {"$set": {"v": 1}}, session=s)])
case("findOneAndDelete(9: no match); deleteOne(3)          expected [(1,0),(2,0)]",
     [lambda s: t().find_one_and_delete({"_id": 9}, session=s), lambda s: t().delete_one({"_id": 3}, session=s)])
case("findOneAndUpdate(9: no match, upsert=false); insertOne(5) expected 5 present",
     [lambda s: t().find_one_and_update({"_id": 9}, {"$set": {"v": 1}}, session=s),
      lambda s: t().insert_one({"_id": 5, "v": 0}, session=s)])
