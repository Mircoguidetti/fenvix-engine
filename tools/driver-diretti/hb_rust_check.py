import sys, subprocess, json
import prova
ws = prova.three("hr")
try:
    for secs in ("5", "40"):
        out = "run/hb-rust.json"
        subprocess.run(prova.client_cmd("rust", prova.URI, secs, 2, out), capture_output=True, timeout=120)
        print(secs, "s:", json.load(open(out))["heartbeats"])
finally:
    prova.stop_all(ws)
