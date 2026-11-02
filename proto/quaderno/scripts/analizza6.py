#!/usr/bin/env python3
"""Banco 6: andamento di commit, RAM, disco, dispensa, freno e ritardo dello scarico; recupero dopo la ripresa di MinIO.
Uso: analizza6.py 6-carico.jsonl 6-eventi.txt [soglia_ritardo_s=30]"""
import json, sys, re
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
final = [r for r in rows if r.get('final')]
rows = [r for r in rows if 'replicas' in r]
ev = open(sys.argv[2]).read()
ts = [int(x) for x in re.findall(r'(\d{12,})', ev)]
t_stop, t_cont = ts[0], ts[1]
soglia = float(sys.argv[3]) if len(sys.argv) > 3 else 30.0
import os
start = t_stop - int(os.environ.get("PRE", "120")) * 1000  # giro 4: PRE come in banco6.sh
print(f"sospensione a t={(t_stop-start)/1000:.0f}s, ripresa a t={(t_cont-start)/1000:.0f}s, blocco {(t_cont-t_stop)/1000:.0f}s")
rec_t = None
mx = {'rss': 0, 'mem': 0, 'disk': 0}
for r in rows:
    reps = r['replicas']
    if not reps:
        print(f"t={r['t']:5.0f}s nessuna replica risponde")
        continue
    lag = max(x['oldest_s'] for x in reps)
    mx['rss'] = max(mx['rss'], max(x['rss_kb'] // 1024 for x in reps))
    mx['mem'] = max(mx['mem'], max(x['mem_mb'] for x in reps))
    mx['disk'] = max(mx['disk'], max(x['disk_used_mb'] for x in reps))
    t_abs = start + r['t'] * 1000
    tag = ''
    if t_abs > t_cont and rec_t is None and lag < soglia:
        rec_t = (t_abs - t_cont) / 1000
        tag = '  <- recuperato'
    if int(r['t']) % 60 < 10 or tag:
        print(f"t={r['t']:5.0f}s commit/s {r['commits_per_s']:6.0f} rifiutati(tot) {r.get('rejected',0):7d} rallentati(tot) {r.get('slowed',0):7d} "
              f"ritardo {lag:5.0f}s RSS MB {[x['rss_kb']//1024 for x in reps]} conto MB {[round(x['mem_mb']) for x in reps]} disco MB {[round(x['disk_used_mb']) for x in reps]} "
              f"dispensa run/MB {[(x['spill_runs'], round(x['spill_disk_mb'])) for x in reps]} freno {[x['brake'] for x in reps]} allarme {[x['alarm'] for x in reps]}{tag}")
print(json.dumps({"blocco_s": (t_cont - t_stop) / 1000, "recupero_s": rec_t, "entro_20_min": rec_t is not None and rec_t <= 1200,
                  "rss_max_mb": mx['rss'], "conto_max_mb": round(mx['mem']), "disco_max_mb": round(mx['disk']),
                  "finale": {k: v for k, v in (final[0].items() if final else []) if k != 'lat'}}))
