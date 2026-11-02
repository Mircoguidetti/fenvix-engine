#!/usr/bin/env python3
"""Banco 5: per ogni database, tempo dal guasto (ms Unix) alla prima scrittura confermata dopo il guasto.
Righe: db,t_invio_ms,t_fine_ms,pos,tentativi,ambiguo (9 = esito ignoto)."""
import sys, json
path, tf = sys.argv[1], float(sys.argv[2])
first = {}
dbs = set()
unknown = amb = 0
for l in open(path):
    d, ts, te, pos, att, a = l.strip().split(',')
    dbs.add(d)
    te = float(te)
    if a == '9':
        unknown += 1
        continue
    amb += a == '1'
    if te > tf and (d not in first or te < first[d]):
        first[d] = te
gaps = sorted((first[d] - tf) / 1000 for d in first)
missing = len(dbs) - len(first)
q = lambda p: round(gaps[min(len(gaps) - 1, int(p * len(gaps)))], 3) if gaps else None
print(json.dumps({"dbs": len(dbs), "dbs_without_write_after_fault": missing, "unknown_outcomes": unknown, "ambiguous_confirmed": amb,
                  "gap_s_p50": q(0.5), "gap_s_p99": q(0.99), "gap_s_max": gaps[-1] if gaps else None,
                  "all_within_10s": missing == 0 and bool(gaps) and gaps[-1] <= 10.0}))
