"""Revisore di C9: i contatori di un database che non esiste non nascono da nessuna strada (letture, list, erase, fork),
e l'erase li toglie davvero, anche dopo le richieste di fondo che arrivano dopo. Si lancia da ctl-tests:
python3 /home/claude/rev/probe_ghost_counters.py (serve moto su 9000 e il binario in target/release)."""
import sys, time, uuid, os
sys.path.insert(0, os.getcwd())
from pymongo.errors import OperationFailure
from lib import Server, make_bucket

DOC = "x" * 4000
fails = []

def have(srv):
    st, m = srv.admin.get("/v1/metrics/databases")
    return set(m["databases"])

def check(srv, label, names, want=False):
    got = have(srv)
    bad = [n for n in names if (n in got) != want]
    print(f"{label}: {'ok' if not bad else 'SBAGLIATO'} ({len(bad)} su {len(names)} {'senza' if want else 'con'} contatori)", flush=True)
    if bad:
        fails.append(label)
        try:
            listed = set(srv.client().list_database_names())
            st, api = srv.admin.get("/v1/databases")
            print(f"   esempi {bad[:3]}; nel listDatabases: {[b in listed for b in bad[:3]]}; nell'API: {[b in str(api) for b in bad[:3]]}", flush=True)
        except Exception as e:
            print("   (lista non letta)", e)

bucket = make_bucket("rc9-ghost")
srv = Server(bucket, "rc9ghost", idle=20, extra=["--memtable-max-mb", "1", "--wal-gc-secs", "1"]).start()
try:
    c = srv.client()
    c["real"]["c"].insert_one({"_id": 1})
    # 1. letture e comandi vari su database che non esistono
    names = [f"g{uuid.uuid4().hex[:10]}" for _ in range(30)]
    for n in names:
        d = c[n]
        d["c"].find_one({})
        d["c"].count_documents({})
        list(d["c"].aggregate([{"$match": {}}]))
        d.list_collection_names()
        d["c"].distinct("a")
        try:
            d.command("dbStats")
        except OperationFailure:
            pass
        try:
            list(d["c"].list_indexes())
        except OperationFailure:
            pass
        d["c"].delete_many({"a": 1})
        d["c"].update_many({"a": 1}, {"$set": {"b": 1}})
        d["c"].drop()
        c.drop_database(n)
    check(srv, "letture e comandi su database inesistenti, subito", names)
    # 2. list
    c.list_database_names()
    srv.admin.get("/v1/databases")
    for n in names[:10]:
        srv.admin.get(f"/v1/databases/{n}")
    check(srv, "list (driver e API) e GET di database inesistenti", names)
    # 3. erase di database inesistenti
    for n in names[:10]:
        st, r = srv.admin.delete(f"/v1/databases/{n}")
    check(srv, "erase di database inesistenti", names)
    # 4. fork da un database inesistente
    fk = [f"f{uuid.uuid4().hex[:8]}" for _ in range(5)]
    for i, t in enumerate(fk):
        st, r = srv.admin.post(f"/v1/databases/{names[i]}/fork", {"target": t, "mode": "clone"})
        if i == 0:
            print("  fork da inesistente:", st, str(r)[:120])
    check(srv, "fork da inesistenti: sorgente", names[:5])
    check(srv, "fork da inesistenti: destinazione", fk)
    # 5. dopo che le attese di fondo sono passate (idle 20 s, poll del cameriere)
    time.sleep(35)
    check(srv, "dopo 35 s di lavori di fondo", names + fk)

    # 6. erase di un database vivo e attivo: i contatori vanno via e non tornano
    for i in range(1200):
        c["live"]["c"].insert_one({"_id": i, "p": DOC})
    check(srv, "database vivo prima dell'erase", ["live"], want=True)
    st, r = srv.admin.delete("/v1/databases/live")
    print("  erase live:", st)
    check(srv, "database vivo, subito dopo l'erase", ["live"])
    time.sleep(35)
    check(srv, "database vivo, 35 s dopo l'erase", ["live"])

    # 7. fork vero, poi erase del ramo e del padre
    for i in range(300):
        c["par"]["c"].insert_one({"_id": i, "p": DOC})
    st, r = srv.admin.post("/v1/databases/par/fork", {"target": "kid", "mode": "clone"})
    print("  fork par -> kid:", st)
    c["kid"]["c"].find_one({"_id": 5})
    check(srv, "ramo vivo dopo il fork", ["kid", "par"], want=True)
    st, r = srv.admin.delete("/v1/databases/kid")
    print("  erase kid:", st, str(r)[:160])
    time.sleep(35)
    check(srv, "ramo, 35 s dopo l'erase", ["kid"])
    check(srv, "padre ancora vivo dopo l'erase del ramo", ["par"], want=True)
    st, r = srv.admin.delete("/v1/databases/par")
    print("  erase par:", st, str(r)[:160])
    time.sleep(35)
    check(srv, "padre, 35 s dopo l'erase", ["par"])
    check(srv, "il database vero resta", ["real"], want=True)
finally:
    srv.term()
print("ESITO:", "tutto ok" if not fails else f"falliti {fails}")
sys.exit(1 if fails else 0)
