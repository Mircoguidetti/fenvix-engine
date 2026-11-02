"""Controlli rapidi in modalità router (pymongo): transazione, change stream, scrittura ritentabile, comandi di replica."""
import prova
from pymongo import MongoClient
ws = prova.three("ex")
try:
    c = MongoClient(prova.URI)
    co = c.prova.t
    out = {}
    with c.start_session() as s:
        def tx(sess):
            co.insert_one({"_id": 1}, session=sess)
            co.insert_one({"_id": 2}, session=sess)
        s.with_transaction(tx)
        out["transazione"] = "ok, server fissato: " + str(s._pinned_address)
    try:
        with co.watch(max_await_time_ms=200) as st:
            co.insert_one({"_id": 3})
            out["change_stream"] = "aperto ok"
    except Exception as e:
        out["change_stream"] = f"errore {e}"
    try:
        c.admin.command("replSetGetStatus")
        out["replSetGetStatus"] = "risponde (non dovrebbe)"
    except Exception as e:
        out["replSetGetStatus"] = f"rifiutato come mongos: {str(e)[:80]}"
    out["hello"] = {k: v for k, v in c.admin.command("hello").items() if k in ("msg", "isWritablePrimary", "setName", "hosts")}
    print(out)
finally:
    prova.stop_all(ws)
