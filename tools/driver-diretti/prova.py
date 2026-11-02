"""Prova M14: i driver ufficiali parlano direttamente a tre camerieri (mongo-server --router) dietro un record mongodb+srv.

Richiede: il DNS finto (dns_finto.py) su 127.0.0.1:53 con /etc/resolv.conf che lo punta, il finto S3 su 127.0.0.1:9000,
il binario target/release/mongo-server. Uso: python3 prova.py SCENARIO DRIVER [DRIVER...]
SCENARIO: spargere | morte | zone | zone-morte | nuovo | stesso-db | tutto.  DRIVER: py | node | rust.
I risultati vanno in run/<scenario>-<driver>.json (cliente) e run/<scenario>-<driver>-server.json (contatori dei server)."""
import json
import os
import signal
import subprocess
import sys
import time

import boto3
from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
BIN = os.environ.get("MONGO_SERVER_BIN", os.path.join(ROOT, "target", "release", "mongo-server"))
RUN = os.path.join(HERE, "run")
ZONA = os.path.join(RUN, "zona.json")
S3 = "http://127.0.0.1:9000"
ENV = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1",
           AWS_ENDPOINT=S3, AWS_ALLOW_HTTP="true")
SRV = "_mongodb._tcp.db.fenvix.test"
# M14_TLS=1: camerieri con TLS obbligatorio; la stringa resta mongodb+srv (che accende TLS da sola), più il CA di prova
TLS = os.environ.get("M14_TLS") == "1"
_CA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "run", "tls", "ca.pem")
URI = f"mongodb+srv://db.fenvix.test/?tlsCAFile={_CA}" if TLS else "mongodb+srv://db.fenvix.test/?tls=false"
TLS_DIRECT = f"&tls=true&tlsAllowInvalidHostnames=true&tlsCAFile={_CA}" if TLS else ""
# i tre camerieri: nome DNS, IP su cui ascolta il server, IP del proxy di zona (solo per gli scenari con le zone)
WAITERS = {"a": ("a.db.fenvix.test", "127.0.0.21", "127.0.0.11"),
           "b": ("b.db.fenvix.test", "127.0.0.22", "127.0.0.12"),
           "c": ("c.db.fenvix.test", "127.0.0.23", "127.0.0.13")}
os.makedirs(RUN, exist_ok=True)
# M14_STORE=local: ogni cameriere con il suo bucket usa una cartella locale invece del finto S3 (che, un solo processo Python,
# diventa il collo di bottiglia di tutti e tre quando uno si riavvia). Il caso "stesso database" resta sempre sul finto S3.
STORE = os.environ.get("M14_STORE", "s3")


def bucket(name):
    b = f"m14-{name}-{os.urandom(3).hex()}"
    boto3.client("s3", endpoint_url=S3, aws_access_key_id="x", aws_secret_access_key="x", region_name="us-east-1").create_bucket(Bucket=b)
    return b


class Waiter:
    def __init__(self, key, bkt, proxied=False, shared=False):
        self.key, self.bkt, self.shared = key, bkt, shared
        self.host, self.ip, self.pip = WAITERS[key]
        self.proxied, self.proc = proxied, None

    def start(self):
        store = ["--store", "local", "--local-dir", os.path.join(RUN, "store", self.bkt)] if STORE == "local" and not self.shared else []
        cmd = [BIN, "--listen", f"{self.ip}:27017", "--bucket", self.bkt, "--endpoint", S3, "--holder", f"m14-{self.key}", *store,
               "--idle-secs", "600", "--no-ttl", "--lease-ttl-secs", "4", "--router", "--advertise", f"{self.host}:27017"]
        if TLS:
            # certificato di prova con un nome jolly *.db.fenvix.test (run/tls, fatto con openssl): un solo certificato per tutti i camerieri
            cmd += ["--tls-mode", "requireTLS", "--tls-certificate-key-file", os.path.join(RUN, "tls", "srv-full.pem")]
        self.log = open(os.path.join(RUN, f"server-{self.key}.log"), "ab")
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=self.log, env=ENV)
        t0 = time.time()
        while time.time() - t0 < 30:
            try:
                MongoClient(f"mongodb://{self.ip}:27017/?directConnection=true" + TLS_DIRECT, serverSelectionTimeoutMS=500).admin.command("ping")
                return self
            except Exception:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"cameriere {self.key} uscito: {self.proc.returncode}")
                time.sleep(0.2)
        raise RuntimeError(f"cameriere {self.key} non parte")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def counters(self):
        try:
            ss = MongoClient(f"mongodb://{self.ip}:27017/?directConnection=true" + TLS_DIRECT, serverSelectionTimeoutMS=1000).admin.command("serverStatus")
            return {"query": ss["opcounters"]["query"], "insert": ss["opcounters"]["insert"], "conns": ss["connections"]["current"]}
        except Exception as e:
            return {"error": str(e)[:100]}


class Proxy:
    def __init__(self, w, delay_ms):
        self.proc = subprocess.Popen([sys.executable, os.path.join(HERE, "lat_proxy.py"), f"{w.pip}:27017", f"{w.ip}:27017", str(delay_ms)],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.4)

    def stop(self):
        self.proc.kill()
        self.proc.wait()


def zone(keys, proxied=False):
    """Scrive il record SRV (e gli A) con i camerieri dati."""
    z = {"srv": {SRV: [[WAITERS[k][0], 27017] for k in keys]},
         "a": {WAITERS[k][0]: (WAITERS[k][2] if proxied else WAITERS[k][1]) for k in WAITERS}, "ttl": 1}
    json.dump(z, open(ZONA + ".tmp", "w"))
    os.replace(ZONA + ".tmp", ZONA)


def client_cmd(drv, uri, secs, conc, out):
    if drv == "py":
        return [sys.executable, os.path.join(HERE, "client_py.py"), uri, str(secs), str(conc), out]
    if drv == "node":
        return ["node", os.path.join(HERE, "node", "client_node.js"), uri, str(secs), str(conc), out]
    if drv == "rust":
        return [os.path.join(HERE, "rust", "target", "release", "client-rust"), uri, str(secs), str(conc), out]
    raise ValueError(drv)


def run_client(drv, name, secs, conc=16, uri=URI, events=()):
    """Lancia il cliente; `events` = [(secondi dall'avvio, funzione)] eseguiti durante la corsa."""
    out = os.path.join(RUN, f"{name}-{drv}.json")
    p = subprocess.Popen(client_cmd(drv, uri, secs, conc, out), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    t0 = time.time()
    done = []
    for at, fn in sorted(events, key=lambda e: e[0]):
        time.sleep(max(0, at - (time.time() - t0)))
        done.append([round(time.time() - t0, 2), fn.__name__])
        fn()
    so, se = p.communicate(timeout=secs + 120)
    if p.returncode != 0:
        print(se[-2000:])
        raise RuntimeError(f"cliente {drv} fallito")
    res = json.load(open(out))
    res["events"] = done
    json.dump(res, open(out, "w"), indent=1)
    return res


def server_delta(ws, before):
    after = {k: w.counters() for k, w in ws.items()}
    d = {}
    for k in ws:
        a, b = after[k], before.get(k, {})
        if "query" in a and "query" in b:
            d[k] = {"query": a["query"] - b["query"], "insert": a["insert"] - b["insert"], "conns_now": a["conns"]}
        else:
            d[k] = {"note": "riavviato o morto", "after": a}
    return d


def summary(res):
    tot = sum(res["per_server"].values()) or 1
    share = {k: round(100 * v / tot, 1) for k, v in sorted(res["per_server"].items())}
    hb = res.get("heartbeats", {})
    return f"topologia={res['topology']} ops={res['ops']} quote%={share} errori_app={len(res['app_errors'])} fallimenti_comandi={res['cmd_fail']} battiti_ok={hb.get('ok')} battiti_falliti={hb.get('fail')}"


def three(prefix, shared=False, proxied=False):
    b = bucket(prefix) if shared else None
    ws = {k: Waiter(k, b or bucket(f"{prefix}-{k}"), shared=shared).start() for k in WAITERS}
    zone(list(WAITERS), proxied=proxied)
    return ws


def stop_all(ws, proxies=()):
    for w in ws.values():
        w.kill9()
    for p in proxies:
        p.stop()


def sc_spargere(drv):
    ws = three("sp")
    try:
        before = {k: w.counters() for k, w in ws.items()}
        res = run_client(drv, "spargere", 20)
        srv = server_delta(ws, before)
        json.dump(srv, open(os.path.join(RUN, f"spargere-{drv}-server.json"), "w"), indent=1)
        print("spargere", drv, summary(res), "server:", srv)
    finally:
        stop_all(ws)


def sc_morte(drv):
    ws = three("mo")
    try:
        def uccidi_b():
            ws["b"].kill9()

        def riavvia_b():
            ws["b"].start()
        res = run_client(drv, "morte", 40, events=[(10, uccidi_b), (22, riavvia_b)])
        print("morte", drv, summary(res), "eventi:", res["events"])
        print("  errori:", res["app_errors"][:5])
    finally:
        stop_all(ws)


# con 16 lavoratori i proxy di zona (Python) e i 2 core sono saturi e il giro misurato dal driver diventa rumore;
# con 4 il giro resta pulito (a riposo: vicino circa 1,1-1,4 ms, lontano circa 3,5 ms, rtt_check.py)
ZCONC = int(os.environ.get("M14_ZCONC", "4"))


def sc_zone(drv, kill_near=False):
    """a nella zona del cliente (proxy senza ritardo), b e c in altre zone (+1 ms per verso, cioè +2 ms sul giro)."""
    ws = three("zo", proxied=True)
    ps = [Proxy(ws["a"], 0), Proxy(ws["b"], 1), Proxy(ws["c"], 1)]
    try:
        if not kill_near:
            r1 = run_client(drv, "zone-default", 20, conc=ZCONC)
            print("zone localThresholdMS=15 (default)", drv, summary(r1))
            r2 = run_client(drv, "zone-soglia1", 20, conc=ZCONC, uri=URI + "&localThresholdMS=1")
            print("zone localThresholdMS=1", drv, summary(r2))
        else:
            def uccidi_a():
                ws["a"].kill9()
            r3 = run_client(drv, "zone-morte", 30, conc=ZCONC, uri=URI + "&localThresholdMS=1", events=[(10, uccidi_a)])
            print("zone-morte localThresholdMS=1", drv, summary(r3), "errori:", r3["app_errors"][:3])
    finally:
        stop_all(ws, ps)


def sc_nuovo(drv):
    """Si parte con a e b nel record SRV; dopo 5 s si aggiunge c (già acceso). Il driver rilegge l'SRV da solo?"""
    b = {k: bucket(f"nu-{k}") for k in WAITERS}
    ws = {k: Waiter(k, b[k]).start() for k in WAITERS}
    zone(["a", "b"])
    try:
        def aggiungi_c():
            zone(["a", "b", "c"])
        res = run_client(drv, "nuovo", 100, conc=4, events=[(5, aggiungi_c)])
        first_c = next((int(s) for s, v in sorted(res["per_sec"].items(), key=lambda x: int(x[0])) if any(k.startswith("c.") for k in v)), None)
        print("nuovo", drv, summary(res), "primo secondo con traffico su c:", first_c)
        res["first_c"] = first_c
        json.dump(res, open(os.path.join(RUN, f"nuovo-{drv}.json"), "w"), indent=1)
    finally:
        stop_all(ws)
        zone(list(WAITERS))


def sc_stesso_db(drv):
    """I tre camerieri sullo STESSO bucket: il motore di oggi dà ogni database a un solo processo (lease)."""
    ws = three("sd", shared=True)
    try:
        res = run_client(drv, "stesso-db", 20)
        codes = {}
        for e in res["app_errors"]:
            codes[e[1]] = codes.get(e[1], 0) + 1
        print("stesso-db", drv, summary(res), "tipi di errore:", codes, "esempio:", res["app_errors"][:2])
    finally:
        stop_all(ws)


if __name__ == "__main__":
    sc, drvs = sys.argv[1], sys.argv[2:]
    for d in drvs:
        if sc in ("spargere", "tutto"):
            sc_spargere(d)
        if sc in ("morte", "tutto"):
            sc_morte(d)
        if sc in ("zone", "tutto"):
            sc_zone(d)
        if sc in ("zone-morte", "tutto"):
            sc_zone(d, kill_near=True)
        if sc in ("stesso-db", "tutto"):
            sc_stesso_db(d)
        if sc in ("nuovo", "tutto"):
            sc_nuovo(d)
