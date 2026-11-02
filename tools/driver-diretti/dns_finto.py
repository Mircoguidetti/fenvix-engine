"""Un DNS finto per la prova M14: risponde per la zona `fenvix.test` e inoltra il resto ai server DNS veri.

Uso: python3 dns_finto.py ZONE_FILE.json [--port 53] [--upstream 8.8.8.8]
Il file JSON si rilegge a ogni domanda, così la prova può cambiare i record SRV senza riavviare il DNS:
  {"srv": {"_mongodb._tcp.db.fenvix.test": [["a.db.fenvix.test", 27017], ...]},
   "a":   {"a.db.fenvix.test": "127.0.0.11", ...},
   "txt": {"db.fenvix.test": "authSource=admin"},
   "ttl": 1}
"""
import json
import socket
import sys
import threading

from dnslib import A, QTYPE, RR, SRV, TXT, DNSRecord
from dnslib.server import BaseResolver, DNSServer

ZONE = "fenvix.test."


class Resolver(BaseResolver):
    def __init__(self, path, upstream):
        self.path, self.upstream = path, upstream
        self.lock = threading.Lock()
        self.count = {}

    def resolve(self, request, handler):
        q = request.q
        name = str(q.qname).rstrip(".").lower()
        reply = request.reply()
        if not str(q.qname).lower().endswith(ZONE):
            try:
                return DNSRecord.parse(request.send(self.upstream, 53, timeout=3))
            except Exception:
                reply.header.rcode = 2
                return reply
        with open(self.path) as f:
            z = json.load(f)
        ttl = z.get("ttl", 1)
        with self.lock:
            k = f"{QTYPE[q.qtype]} {name}"
            self.count[k] = self.count.get(k, 0) + 1
            with open(self.path + ".count", "w") as f:
                json.dump(self.count, f)
        if q.qtype in (QTYPE.SRV, QTYPE.ANY) and name in z.get("srv", {}):
            for host, port in z["srv"][name]:
                reply.add_answer(RR(q.qname, QTYPE.SRV, ttl=ttl, rdata=SRV(0, 0, port, host + ".")))
        if q.qtype in (QTYPE.A, QTYPE.ANY) and name in z.get("a", {}):
            reply.add_answer(RR(q.qname, QTYPE.A, ttl=ttl, rdata=A(z["a"][name])))
        if q.qtype in (QTYPE.TXT, QTYPE.ANY) and name in z.get("txt", {}):
            reply.add_answer(RR(q.qname, QTYPE.TXT, ttl=ttl, rdata=TXT(z["txt"][name])))
        if not reply.rr:
            known = set(z.get("srv", {})) | set(z.get("a", {})) | set(z.get("txt", {}))
            if name not in known:
                reply.header.rcode = 3  # NXDOMAIN
        return reply


if __name__ == "__main__":
    path = sys.argv[1]
    port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 53
    up = sys.argv[sys.argv.index("--upstream") + 1] if "--upstream" in sys.argv else "8.8.8.8"
    r = Resolver(path, up)
    for tcp in (False, True):
        DNSServer(r, port=port, address="127.0.0.1", tcp=tcp).start_thread()
    print(f"dns finto su 127.0.0.1:{port}", flush=True)
    threading.Event().wait()
