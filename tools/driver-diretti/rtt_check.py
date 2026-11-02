"""Quanto vede il driver Python il giro (RTT) di ogni cameriere, a riposo, con i proxy di zona accesi."""
import sys, time, json
sys.path.insert(0, ".")
import prova
from pymongo import MongoClient
ws = prova.three("rt", proxied=True)
ps = [prova.Proxy(ws["a"], 0), prova.Proxy(ws["b"], float(sys.argv[1])), prova.Proxy(ws["c"], float(sys.argv[1]))]
try:
    c = MongoClient(prova.URI + "&heartbeatFrequencyMS=500")
    c.admin.command("ping")
    time.sleep(6)
    d = c.topology_description
    print({f"{a[0]}": round(s.round_trip_time * 1000, 2) for a, s in d.server_descriptions().items()})
finally:
    prova.stop_all(ws, ps)
