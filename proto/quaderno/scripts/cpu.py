#!/usr/bin/env python3
"""CPU per processo durante un intervallo: cpu.py SECONDI nome=pid ... -> JSON {nome: core medi usati}."""
import json, os, sys, time
hz = os.sysconf('SC_CLK_TCK')
def t(pid):
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / hz
    except Exception:
        return None
secs = float(sys.argv[1]); ps = dict(a.split('=') for a in sys.argv[2:])
a = {k: t(v) for k, v in ps.items()}; t0 = time.time()
tot0 = sum(map(int, open('/proc/stat').readline().split()[1:8]))
time.sleep(secs)
b = {k: t(v) for k, v in ps.items()}; el = time.time() - t0
tot1 = sum(map(int, open('/proc/stat').readline().split()[1:8]))
idle = [int(x) for x in open('/proc/stat').readline().split()[1:8]]
out = {k: round((b[k] - a[k]) / el, 2) for k in ps if a[k] is not None and b[k] is not None}
out['_macchina_core_totali'] = os.cpu_count()
print(json.dumps(out))
