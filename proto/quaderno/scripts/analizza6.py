#!/usr/bin/env python3
"""Banco 6: andamento di commit, RAM e ritardo dello scarico; tempo di recupero dopo la ripresa di MinIO.
Uso: analizza6.py 6-carico.jsonl 6-eventi.txt [soglia_ritardo_s=30]"""
import json, sys, re
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip() and not l.startswith('{"final"')]
rows = [r for r in rows if 'replicas' in r]
ev = open(sys.argv[2]).read()
t_stop, t_cont = [int(x) for x in re.findall(r'(\d{12,})', ev)[:2]]
soglia = float(sys.argv[3]) if len(sys.argv) > 3 else 30.0
# t del carico parte ~120 s prima della sospensione
start = t_stop - 120_000
print(f"sospensione a t={(t_stop-start)/1000:.0f}s, ripresa a t={(t_cont-start)/1000:.0f}s, blocco {(t_cont-t_stop)/1000:.0f}s")
rec_t = None
for r in rows:
    reps = r['replicas']
    if not reps:
        continue
    lag = max(x['flush_lag_s'] for x in reps)
    rss = [x['rss_kb'] // 1024 for x in reps]
    res = [round(x['resident_mb']) for x in reps]
    t_abs = start + r['t'] * 1000
    tag = ''
    if t_abs > t_cont and rec_t is None and lag < soglia:
        rec_t = (t_abs - t_cont) / 1000
        tag = '  <- recuperato'
    if int(r['t']) % 60 < 10 or tag:
        print(f"t={r['t']:5.0f}s commit/s {r['commits_per_s']:6.0f} p99 {r['lat']['commit_from_send']['p99_us']/1000:7.1f}ms ritardo scarico {lag:6.0f}s "
              f"RSS MB {rss} valori in RAM MB {res} sfrattati {[x['evicted'] for x in reps]} non scaricati {[x['unflushed'] for x in reps]} disco MB {[round(x['disk_mb']) for x in reps]}{tag}")
print(json.dumps({"blocco_s": (t_cont - t_stop) / 1000, "recupero_s": rec_t, "entro_il_doppio": rec_t is not None and rec_t <= 2 * (t_cont - t_stop) / 1000}))
