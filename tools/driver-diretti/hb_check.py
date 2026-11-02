import sys, subprocess
import prova
proxied = sys.argv[1] == "proxy"
ws = prova.three("hb", proxied=proxied)
ps = [prova.Proxy(ws["a"], 0), prova.Proxy(ws["b"], 1), prova.Proxy(ws["c"], 1)] if proxied else []
try:
    for args in (["500", "auto", "8000"], ["500", "poll", "8000"], ["10000", "auto", "25000"]):
        r = subprocess.run(["node", "node/hb_node.js", prova.URI] + args, capture_output=True, text=True, timeout=60)
        print(sys.argv[1], args, r.stdout.strip(), r.stderr[-300:])
finally:
    prova.stop_all(ws, ps)
