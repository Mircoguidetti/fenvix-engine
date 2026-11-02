#!/usr/bin/env python3
"""Riassume l'uscita di `pq mixed` (banchi 2 e 7): una riga per intervallo e il totale.
Uso: riassunto2.py file.jsonl [status-vuoto.jsonl]"""
import json, sys
rows, final = [], None
for l in open(sys.argv[1]):
    d = json.loads(l)
    if d.get('final'):
        final = d
    else:
        rows.append(d)
base = {}
if len(sys.argv) > 2:
    for l in open(sys.argv[2]):
        s = json.loads(l)
        base[s['id']] = s['rss_kb']
for d in rows:
    L = d['lat']
    reps = ' '.join(f"r{r['id']}{'*' if r['role']=='Leader' else ''}:{r['rss_kb']//1024}MB/v{r['versions']//1000}k/u{r['unflushed']//1000}k/lag{r['flush_lag_s']:.0f}s" for r in d['replicas'])
    print(f"t={d['t']:6.0f}s commit/s {d['commits_per_s']:7.0f} letture/s {d['reads_per_s']:7.0f} conf {d['conflicts']:4d} ign {d['unknown']:3d} "
          f"p99 commit {L['commit']['p99_us']/1000:6.1f}ms finestra {L['read_window']['p99_us']/1000:5.2f}ms (dall'invio {L.get('read_window_from_send',{}).get('p99_us',0)/1000:5.2f}ms) altre {L['read_other']['p99_us']/1000:5.2f}ms ryw {L['ryw']['p99_us']/1000:5.2f}ms | {reps}")
if final:
    L = final['lat']
    print(json.dumps({k: v for k, v in final.items() if k != 'lat'}))
    for k in ('commit', 'commit_from_send', 'read_window', 'read_window_from_send', 'read_other', 'read_other_from_send', 'ryw'):
        print(k, {a: b for a, b in L.get(k, {}).items()})
if rows and base:
    last = rows[-1]['replicas']
    dbs = max(r['dbs'] for r in last)
    for r in last:
        print(f"replica {r['id']}: RSS {r['rss_kb']/1024:.0f} MB, a vuoto {base.get(r['id'],0)/1024:.0f} MB, database {r['dbs']}, "
              f"per database {(r['rss_kb']-base.get(r['id'],0))/max(dbs,1):.1f} KB")
