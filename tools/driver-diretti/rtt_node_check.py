import sys, subprocess, os
import prova
ws = prova.three("rn", proxied=True)
ps = [prova.Proxy(ws["a"], 0), prova.Proxy(ws["b"], 1), prova.Proxy(ws["c"], 1)]
try:
    print(subprocess.run(["node", "node/rtt_node.js", prova.URI], capture_output=True, text=True, timeout=60).stdout)
finally:
    prova.stop_all(ws, ps)
