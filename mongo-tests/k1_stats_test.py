"""K1 · statistiche, serverStatus, explain e profiler per lo script di Atlas.

Avvia mongo-server (--store local, cartella temporanea) e controlla:
  1. contatori di collStats / dbStats / $collStats uguali a un riconteggio dai dati (validate full li fa ricontare),
     dopo insert, update, replace, delete, upsert, transazione, collezione capped, createIndex e dropIndex, TTL,
     rename e drop; dataSize di dbStats non piu' 0 sotto 2 GiB; i contatori sopravvivono a un riavvio; count con
     limit giusto;
  2. serverStatus: CPU del processo che cresce, wiredTiger.cache con i byte nel bancone e le letture da S3;
  3. explain: executionTimeMillis vero;
  4. profiler: millis, storage.data.bytesRead e timeReadingMicros, voci visibili subito a chi legge system.profile.
Uso: python3 k1_stats_test.py [percorso del binario]   (predefinito: ./target/release/mongo-server)
"""
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import bson
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "target/release/mongo-server")
FAILS = []


def check(cond, what):
    print(("ok   " if cond else "FAIL ") + what)
    if not cond:
        FAILS.append(what)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Srv:
    def __init__(self, d, extra=()):
        self.d, self.extra, self.p = d, list(extra), None
        self.port = free_port()

    def start(self):
        log = open(os.path.join(self.d, "server.log"), "a")
        self.p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{self.port}", "--store", "local", "--local-dir",
                                   os.path.join(self.d, "data"), "--ttl-sweep-secs", "1"] + self.extra,
                                  stdout=log, stderr=log)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        self.c = MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=5000)
        return self.c

    def stop(self):
        self.c.close()
        self.p.terminate()
        self.p.wait(30)


def recount(db, coll):
    """Contatori riletti dai dati: validate full li cancella, il collStats dopo rilegge tutto."""
    db.command("validate", coll, full=True)
    return db.command("collStats", coll)


def same_stats(db, coll, label):
    a = db.command("collStats", coll)
    docs = list(db[coll].find({}))
    size = sum(len(bson.encode(d)) for d in docs)
    check(a["count"] == len(docs), f"{label}: count {a['count']} = documenti {len(docs)}")
    check(a["size"] == size, f"{label}: size {a['size']} = somma BSON {size}")
    b = recount(db, coll)
    check(a["indexSizes"] == b["indexSizes"], f"{label}: indexSizes contati {dict(a['indexSizes'])} = riletti {dict(b['indexSizes'])}")
    check((a["count"], a["size"]) == (b["count"], b["size"]), f"{label}: count e size uguali dopo il riconteggio")


def main():
    d = tempfile.mkdtemp(prefix="k1-")
    srv = Srv(d)
    c = srv.start()
    db = c["k1"]
    rnd = random.Random(7)

    # ---- 1. contatori
    db.create_collection("a")
    db["a"].create_index([("x", 1)], name="x_1")
    db["a"].insert_many([{"_id": i, "x": rnd.randrange(100), "t": "p" * rnd.randrange(1, 300), "arr": list(range(rnd.randrange(3)))} for i in range(500)])
    same_stats(db, "a", "insert")
    db["a"].create_index([("arr", 1)], name="arr_1")  # multikey, creato con dati: contato al primo collStats
    same_stats(db, "a", "createIndex con dati")
    for i in range(0, 500, 3):
        db["a"].update_one({"_id": i}, {"$set": {"x": rnd.randrange(100), "t": "q" * rnd.randrange(1, 500)}, "$push": {"arr": i}})
    db["a"].replace_one({"_id": 1}, {"x": 5, "nuovo": True})
    db["a"].update_one({"_id": 10_000}, {"$set": {"x": 1}}, upsert=True)
    db["a"].delete_many({"x": {"$lt": 20}})
    db["a"].find_one_and_update({"_id": 2}, {"$set": {"t": "z" * 1000}})
    db["a"].find_one_and_delete({"_id": 4})
    same_stats(db, "a", "update/replace/upsert/delete/findAndModify")
    db["a"].drop_index("x_1")
    db["a"].create_index([("x", 1)], name="x_1")  # stesso nome di nuovo: non deve tenere il vecchio numero
    db["a"].insert_one({"_id": "dopo", "x": 3})
    same_stats(db, "a", "dropIndex e createIndex con lo stesso nome")
    # implicita, senza create
    db["b"].insert_many([{"v": "x" * i} for i in range(50)])
    same_stats(db, "b", "collezione creata da un insert")
    # transazione: serve un replica set; qui un server senza replica set la rifiuta, provata sotto con rs0
    # capped
    db.create_collection("cap", capped=True, size=4096)
    for i in range(200):
        db["cap"].insert_one({"i": i, "p": "c" * 40})
    same_stats(db, "cap", "capped (i vecchi tolti dal limite)")
    # rename
    db["b"].rename("b2")
    same_stats(db, "b2", "rename")
    # TTL
    import datetime as dt
    db["ttl"].create_index("at", expireAfterSeconds=1)
    db["ttl"].insert_many([{"at": dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)} for _ in range(20)] + [{"at": dt.datetime(2100, 1, 1)}])
    time.sleep(3)
    same_stats(db, "ttl", "TTL")
    # dataSize di dbStats sotto 2 GiB
    s = db.command("dbStats")
    tot = sum(db.command("collStats", n)["size"] for n in db.list_collection_names())
    check(s["dataSize"] > 0 and s["dataSize"] == tot, f"dbStats.dataSize {s['dataSize']} = somma delle size {tot} (non 0)")
    # drop e ricreazione
    db["b2"].drop()
    db["b2"].insert_one({"k": 1})
    same_stats(db, "b2", "drop e di nuovo")
    # $collStats
    cs = list(db["a"].aggregate([{"$collStats": {"storageStats": {}, "count": {}}}]))[0]
    check(cs["count"] == db["a"].count_documents({}), "$collStats.count giusto")
    # count con limit
    n_all = db["a"].count_documents({})
    check(db.command("count", "a", query={}, limit=7)["n"] == 7, "count limit 7")
    check(db.command("count", "a", query={}, limit=7, skip=n_all - 3)["n"] == 3, "count limit 7 skip n-3 = 3")
    check(db.command("count", "a", query={"x": {"$gte": 50}}, limit=100000)["n"] == db["a"].count_documents({"x": {"$gte": 50}}), "count con filtro e limit grande")
    # riavvio: i contatori restano (e non serve rileggere)
    before = db.command("collStats", "a")
    srv.stop()
    c = srv.start()
    db = c["k1"]
    after = db.command("collStats", "a")
    check((before["count"], before["size"], before["indexSizes"]) == (after["count"], after["size"], after["indexSizes"]), "contatori uguali dopo il riavvio")
    same_stats(db, "a", "dopo il riavvio")

    # ---- 2. serverStatus
    s1 = c.admin.command("serverStatus")
    for _ in range(3):
        list(db["a"].find({}))
    s2 = c.admin.command("serverStatus")
    u1, u2 = s1["extra_info"]["user_time_us"], s2["extra_info"]["user_time_us"]
    check(u2 > u1 > 0, f"CPU del processo vera e crescente ({u1} -> {u2} us)")
    wc = s2["wiredTiger"]["cache"]
    check(wc["maximum bytes configured"] > 0, f"bancone configurato {wc['maximum bytes configured']} byte")
    check(wc["bytes currently in the cache"] > 0, f"byte nel bancone {wc['bytes currently in the cache']}")
    check(wc["pages read into cache"] > 0 and wc["bytes read into cache"] > 0, f"letture dallo store: {wc['pages read into cache']} GET, {wc['bytes read into cache']} byte")
    check("fenvix" in s2 and "s3" in s2["fenvix"], "sezione fenvix con i contatori S3")

    srv.stop()

    # ---- con replica set: transazione, explain, profiler
    srv = Srv(d, ["--replica-set", "rs0"])
    c = srv.start()
    db = c["k1"]
    with c.start_session() as ses:
        with ses.start_transaction():
            db["a"].insert_one({"_id": "tx1", "x": 7}, session=ses)
            db["a"].delete_one({"_id": 3}, session=ses)
            db["a"].update_one({"_id": 5}, {"$set": {"t": "w" * 777}}, session=ses)
    same_stats(db, "a", "transazione")

    # ---- 3. explain
    db["big"].insert_many([{"i": i, "p": "x" * 200} for i in range(30000)])
    ex = db.command("explain", {"find": "big", "filter": {"p": "nessuno"}}, verbosity="executionStats")
    es = ex["executionStats"]
    check(es["executionTimeMillis"] > 0, f"explain executionTimeMillis {es['executionTimeMillis']} (era sempre 0)")
    check(es["totalDocsExamined"] == 30000, "explain: documenti esaminati")

    # ---- 4. profiler
    db.command("profile", 2, slowms=0)
    t0 = time.perf_counter()
    list(db["big"].find({"p": "nessuno"}).comment("k1-cerca"))
    client_ms = (time.perf_counter() - t0) * 1000
    db.command("profile", 0)
    prof = list(db["system.profile"].find({"command.comment": "k1-cerca"}))
    check(len(prof) == 1, f"profiler: la voce c'e' subito ({len(prof)})")
    if prof:
        p = prof[0]
        check(isinstance(p.get("millis"), int) and p["millis"] <= client_ms + 1, f"profiler millis {p.get('millis')} <= tempo del cliente {client_ms:.1f}")
        sd = p.get("storage", {}).get("data", {})
        check("bytesRead" in sd and "timeReadingMicros" in sd, f"profiler storage.data {sd}")
        check(p.get("docsExamined") == 30000, f"profiler docsExamined {p.get('docsExamined')}")
        check(p.get("planSummary") == "COLLSCAN", f"profiler planSummary {p.get('planSummary')}")
    db["big"].create_index("i")
    db.command("profile", 2, slowms=0)
    db["big"].find_one({"i": 77}, comment="k1-indice")
    db.command("profile", 0)
    p = db["system.profile"].find_one({"command.comment": "k1-indice"})
    check(p is not None and p.get("planSummary") == "IXSCAN { i: 1 }" and p.get("keysExamined") == 1, f"profiler con indice: {p and (p.get('planSummary'), p.get('keysExamined'), p.get('docsExamined'))}")
    # lettura fredda: riavvio, la prima find legge dallo store
    srv.stop()
    c = srv.start()
    db = c["k1"]
    db.command("profile", 2, slowms=0)
    list(db["big"].find({"i": {"$gte": 100, "$lt": 200}}).comment("k1-fredda"))
    db.command("profile", 0)
    p = db["system.profile"].find_one({"command.comment": "k1-fredda"})
    sd = (p or {}).get("storage", {}).get("data", {})
    check(sd.get("bytesRead", 0) > 0 and sd.get("timeReadingMicros", 0) > 0, f"lettura fredda: byte e tempo letti dallo store {sd}")
    srv.stop()
    shutil.rmtree(d, ignore_errors=True)
    print(f"\n{len(FAILS)} falliti")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
