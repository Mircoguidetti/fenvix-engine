#!/usr/bin/env python3
"""Tiny authoritative DNS server (UDP and TCP, 127.0.0.1:5353) for the mongodb+srv:// tests.  Zone example.test:

  _mongodb._tcp.<p>real.example.test  SRV -> node1.example.test:<real port of pair p>      TXT per pair (E: authSource=admin)
  _mongodb._tcp.<p>ours.example.test  SRV -> node1.example.test:<engine port of pair p>
  _mongodb._tcp.multireal / multiours SRV -> node1 and node2 (same server twice)
  _custom._tcp.customreal / customours  (srvServiceName=custom)
  _mongodb._tcp.outside.example.test  SRV -> evil.other.test:27040   (target outside the parent domain)
Names of the hosts (node1..node3.example.test) resolve through /etc/hosts (tls_env.py adds them)."""
import socketserver, sys, threading
from dnslib import DNSRecord, RR, QTYPE, SRV, TXT, RCODE

PAIRS = {"a": (27040, 27048), "b": (27041, 27049), "c": (27042, 27050), "d": (27043, 27051), "e": (27044, 27052)}
ZONE = {}


def add(name, rtype, rdata):
    ZONE.setdefault((name.lower() + ".", rtype), []).append(rdata)


for p, (rp, op) in PAIRS.items():
    add(f"_mongodb._tcp.{p}real.example.test", QTYPE.SRV, SRV(0, 5, rp, "node1.example.test."))
    add(f"_mongodb._tcp.{p}ours.example.test", QTYPE.SRV, SRV(0, 5, op, "node1.example.test."))
    txt = "authSource=admin" if p == "e" else ""
    for who in ("real", "ours"):
        if txt:
            add(f"{p}{who}.example.test", QTYPE.TXT, TXT(txt))
for who, port in (("real", 27040), ("ours", 27048)):
    add(f"_mongodb._tcp.multi{who}.example.test", QTYPE.SRV, SRV(0, 5, port, "node1.example.test."))
    add(f"_mongodb._tcp.multi{who}.example.test", QTYPE.SRV, SRV(0, 5, port, "node2.example.test."))
    add(f"_custom._tcp.custom{who}.example.test", QTYPE.SRV, SRV(0, 5, port, "node1.example.test."))
    add(f"_mongodb._tcp.badtxt{who}.example.test", QTYPE.SRV, SRV(0, 5, port, "node1.example.test."))
    add(f"badtxt{who}.example.test", QTYPE.TXT, TXT("replicaSet=x&maxPoolSize=3"))
add("_mongodb._tcp.outside.example.test", QTYPE.SRV, SRV(0, 5, 27040, "evil.other.test."))


def answer(data):
    req = DNSRecord.parse(data)
    reply = req.reply()
    q = req.q
    key = (str(q.qname).lower(), q.qtype)
    recs = ZONE.get(key)
    if recs:
        for r in recs:
            reply.add_answer(RR(q.qname, q.qtype, rdata=r, ttl=60))
    elif not any(k[0] == key[0] for k in ZONE):
        reply.header.rcode = RCODE.NXDOMAIN
    return reply.pack()


class UDP(socketserver.BaseRequestHandler):
    def handle(self):
        data, sock = self.request
        sock.sendto(answer(data), self.client_address)


class TCP(socketserver.BaseRequestHandler):
    def handle(self):
        n = int.from_bytes(self.request.recv(2), "big")
        data = b""
        while len(data) < n:
            data += self.request.recv(n - len(data))
        out = answer(data)
        self.request.sendall(len(out).to_bytes(2, "big") + out)


class S(socketserver.ThreadingMixIn):
    allow_reuse_address = True
    daemon_threads = True


class SU(S, socketserver.UDPServer): pass
class ST(S, socketserver.TCPServer): pass


if __name__ == "__main__":
    for srv in (SU(("127.0.0.1", 5353), UDP), ST(("127.0.0.1", 5353), TCP)):
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("dns up on 127.0.0.1:5353", flush=True)
    threading.Event().wait()
