"""Driver Python (PyMongo): connessione, CRUD, aggregazione, transazione, change stream. Uso: prova.py <uri>"""
import json
import sys
import threading
import time

import pymongo
from pymongo import MongoClient, ReturnDocument

uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27018/?replicaSet=rs0"
out = {"driver": "pymongo " + pymongo.version, "steps": {}}


def step(name):
    def deco(fn):
        try:
            fn()
            out["steps"][name] = "ok"
        except Exception as e:
            out["steps"][name] = "ERRORE: " + str(e)[:200]
    return deco


def eq(a, b, what):
    if a != b:
        raise AssertionError(f"{what}: atteso {b!r}, avuto {a!r}")


client = MongoClient(uri, serverSelectionTimeoutMS=5000)
c = client.drv_py.t


@step("connessione")
def _():
    eq(client.admin.command("ping")["ok"], 1.0, "ping")


@step("crud")
def _():
    c.delete_many({})
    c.insert_many([{"_id": 1, "n": "a", "v": 1}, {"_id": 2, "n": "b", "v": 2}, {"_id": 3, "n": "c", "v": 3}])
    eq(c.find_one({"_id": 2})["n"], "b", "find_one")
    eq(c.update_one({"_id": 1}, {"$inc": {"v": 10}}).modified_count, 1, "update_one")
    eq(c.find_one_and_update({"_id": 3}, {"$set": {"n": "z"}}, return_document=ReturnDocument.AFTER)["n"], "z", "foau")
    eq(c.delete_one({"_id": 2}).deleted_count, 1, "delete_one")
    eq(c.count_documents({}), 2, "count")


@step("aggregazione")
def _():
    eq(list(c.aggregate([{"$group": {"_id": None, "s": {"$sum": "$v"}}}]))[0]["s"], 14, "sum")


@step("transazione")
def _():
    with client.start_session() as s:
        def cb(ss):
            c.insert_one({"_id": 10}, session=ss)
            c.update_one({"_id": 1}, {"$set": {"tx": True}}, session=ss)
        s.with_transaction(cb)
    eq(c.count_documents({"$or": [{"_id": 10}, {"tx": True}]}), 2, "dopo commit")


@step("indice e change stream")
def _():
    c.create_index("n", unique=True)
    with c.watch() as cs:
        c.insert_one({"_id": 20, "n": "w"})
        ev = None
        t0 = time.time()
        while ev is None and time.time() - t0 < 15:
            ev = cs.try_next()
        eq(ev and ev["operationType"], "insert", "evento")


print(json.dumps(out))
