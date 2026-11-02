"""Cliente Python (pymongo, driver ufficiale) per la prova M14.

Uso: python3 client_py.py URI SECONDI CONCORRENZA OUT.json
Ogni lavoratore ripete: 80% find_one per _id, 20% insert_one. Conta per server (dal monitoraggio dei comandi del driver),
per secondo, e registra ogni errore visto dall'applicazione (dopo i tentativi automatici del driver)."""
import json
import random
import sys
import threading
import time

from bson import ObjectId
from pymongo import MongoClient, monitoring

uri, secs, conc, out = sys.argv[1], float(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
T0 = time.time()
lock = threading.Lock()
per_server = {}          # "host:port" -> comandi riusciti
per_sec = {}             # secondo -> {server: n}
per_sec_max = {}         # secondo -> {server: latenza massima ms}
cmd_fail = {}            # server -> comandi falliti (visti dal driver, anche se poi ritentati)
app_errors = []          # errori arrivati all'applicazione


class Mon(monitoring.CommandListener):
    def started(self, e):
        pass

    def succeeded(self, e):
        if e.command_name not in ("find", "insert"):
            return
        s = f"{e.connection_id[0]}:{e.connection_id[1]}"
        sec = int(time.time() - T0)
        with lock:
            per_server[s] = per_server.get(s, 0) + 1
            per_sec.setdefault(sec, {})
            per_sec[sec][s] = per_sec[sec].get(s, 0) + 1
            ms = e.duration_micros / 1000
            per_sec_max.setdefault(sec, {})
            per_sec_max[sec][s] = max(per_sec_max[sec].get(s, 0), round(ms, 1))

    def failed(self, e):
        s = f"{e.connection_id[0]}:{e.connection_id[1]}"
        with lock:
            cmd_fail[s] = cmd_fail.get(s, 0) + 1


topo = {}


class TopoMon(monitoring.TopologyListener):
    def opened(self, e):
        pass

    def description_changed(self, e):
        d = e.new_description
        with lock:
            if d.topology_type_name != "Unknown":
                topo["type"] = d.topology_type_name
            topo["servers"] = sorted(f"{a[0]}:{a[1]}={s.server_type_name}" for a, s in d.server_descriptions().items())
            topo.setdefault("history", []).append([round(time.time() - T0, 2), d.topology_type_name, topo["servers"]])

    def closed(self, e):
        pass


hb = {"ok": 0, "fail": {}}


class HbMon(monitoring.ServerHeartbeatListener):
    def started(self, e):
        pass

    def succeeded(self, e):
        with lock:
            hb["ok"] += 1

    def failed(self, e):
        k = f"{type(e.reply).__name__}: {str(e.reply)[:100]}"
        with lock:
            hb["fail"][k] = hb["fail"].get(k, 0) + 1


c = MongoClient(uri, event_listeners=[Mon(), TopoMon(), HbMon()])
coll = c["prova"]["c"]
stop = time.time() + secs
ops = [0]


def worker():
    rnd = random.Random()
    while time.time() < stop:
        is_read = rnd.random() < 0.8
        try:
            if is_read:
                coll.find_one({"_id": rnd.randrange(1000)})
            else:
                coll.insert_one({"_id": ObjectId(), "v": rnd.random()})
            with lock:
                ops[0] += 1
        except Exception as ex:  # noqa: BLE001
            with lock:
                app_errors.append([round(time.time() - T0, 2), type(ex).__name__, ("find: " if is_read else "insert: ") + str(ex)[:200]])
            time.sleep(0.05)


ths = [threading.Thread(target=worker) for _ in range(conc)]
for t in ths:
    t.start()
for t in ths:
    t.join()
c.close()
json.dump({"driver": "pymongo", "version": __import__("pymongo").version, "ops": ops[0], "per_server": per_server,
           "per_sec": {str(k): v for k, v in sorted(per_sec.items())}, "per_sec_max_ms": {str(k): v for k, v in sorted(per_sec_max.items())}, "cmd_fail": cmd_fail, "heartbeats": hb, "app_errors": app_errors,
           "topology": topo.get("type"), "servers": topo.get("servers"), "topo_history": topo.get("history", [])[-8:]},
          open(out, "w"), indent=1)
print(json.dumps({"ops": ops[0], "per_server": per_server, "app_errors": len(app_errors), "topology": topo.get("type"), "heartbeats": hb}))
