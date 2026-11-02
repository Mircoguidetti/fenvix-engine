#!/usr/bin/env python3
"""Cambi di contesto del thread principale di ogni processo (tid = pid): ctx.py nome=pid ... -> JSON
{nome: [volontari, forzati]}. "Forzati" = il sistema operativo ha tolto la CPU al thread (prelazione)."""
import json, sys
out = {}
for a in sys.argv[1:]:
    k, pid = a.split('=')
    try:
        st = open(f'/proc/{pid}/task/{pid}/status').read().splitlines()
        g = {l.split(':')[0]: int(l.split()[1]) for l in st if 'ctxt_switches' in l}
        out[k] = [g['voluntary_ctxt_switches'], g['nonvoluntary_ctxt_switches']]
    except Exception:
        pass
print(json.dumps(out))
