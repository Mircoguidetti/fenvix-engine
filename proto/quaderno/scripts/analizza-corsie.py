#!/usr/bin/env python3
"""Q8: riassunto della prova delle corsie contro le soglie scritte prima (Fable 2.5).

Riferimento: un database da solo sullo stesso quaderno (livello 1; se ci sono più ripetizioni, la mediana; si stampa
anche la forchetta, che è il rumore della macchina). Per ogni livello e fase: p50, p99, p99,9 dei normali e il
peggioramento contro il riferimento. Verdetto per fase: VINCE se il p99 peggiora meno di 1 ms e il p99,9 meno di 3 ms;
MUORE se uno dei due peggiora più di 3 ms; altrimenti GRIGIO. Per livello: regge se tutte le fasi vincono.

Uso: analizza-corsie.py FILE.jsonl [FILE.jsonl ...]
"""
import json, statistics, sys

rows = []
for f in sys.argv[1:]:
    for l in open(f):
        if l.strip():
            rows.append(json.loads(l))
by_tag = {}
for r in rows:
    by_tag.setdefault(r['tag'], []).append(r)


def ms(x):
    return x / 1000.0


def lat(r):
    L = r['normal']['lat']
    return ms(L['p50_us']), ms(L['p99_us']), ms(L['p999_us']), ms(L['max_us']), L['n']


for tag, rs in by_tag.items():
    base = [lat(r) for r in rs if r['level'] == 1]
    print(f'===== {tag}')
    if not base:
        print('  manca il riferimento (livello 1)')
        continue
    b50 = statistics.median(x[0] for x in base)
    b99 = statistics.median(x[1] for x in base)
    b999 = statistics.median(x[2] for x in base)
    print(f'  riferimento, un database da solo ({len(base)} ripetizioni): p50 {b50:.2f}, p99 {b99:.2f}, p99,9 {b999:.2f} ms (mediane); '
          f'forchetta p99 {min(x[1] for x in base):.2f}–{max(x[1] for x in base):.2f}, p99,9 {min(x[2] for x in base):.2f}–{max(x[2] for x in base):.2f}')
    levels = sorted({r['level'] for r in rs if r['level'] > 1})
    for lv in levels:
        worst99 = worst999 = -1e9
        verdicts = []
        for r in [x for x in rs if x['level'] == lv]:
            p50, p99, p999, mx, n = lat(r)
            d99, d999 = p99 - b99, p999 - b999
            worst99, worst999 = max(worst99, d99), max(worst999, d999)
            v = 'VINCE' if d99 < 1 and d999 < 3 else ('MUORE' if d99 > 3 or d999 > 3 else 'GRIGIO')
            verdicts.append(v)
            nr = r['normal']
            c = r.get('leader_diag', {}).get('corsie', {})
            s = r.get('sys', {})
            lead = s.get(r.get('leader', ''), {})
            fol = [s[k]["cores"] or 0 for k in ('r0', 'r1', 'r2') if k in s and k != r.get('leader')]
            disk = s.get('disk', {})
            print(f'  livello {lv:3d} {r["phase"]:9s} normali {r["n_normal"]:3d}: p50 {p50:6.2f} p99 {p99:7.2f} ({d99:+7.2f}) p99,9 {p999:7.2f} ({d999:+7.2f}) max {mx:7.1f} ms n {n:6d} {v:6s} | '
                  f'tot {r["recs_per_s_total"]:6.0f} rec/s, rumorosi ammessi {c.get("ammessi_sopra", 0) / max(s.get("secs", 1), 1):6.0f}/s sopra la riserva, '
                  f'capo {lead.get("cores") or 0:.2f} core, copie {"/".join(f"{x:.2f}" for x in fol)}, disco {disk.get("MB_s", 0):5.1f} MB/s {disk.get("writes", 0) / max(s.get("secs", 1), 1) / 3:5.0f} scritture/s per copia, '
                  f'RSS {"/".join(str(int(s[k]["rss_mb"])) for k in ("r0", "r1", "r2") if k in s)} MB; normali rifiutati {nr["rejected"]}, ignoti {nr["unknown"]}, ritentativi {nr["busy_retries"]}')
        ok = all(v == 'VINCE' for v in verdicts)
        dead = any(v == 'MUORE' for v in verdicts)
        print(f'  -> livello {lv}: {"REGGE" if ok else ("MUORE" if dead else "GRIGIO")}; peggioramento massimo p99 {worst99:+.2f} ms, p99,9 {worst999:+.2f} ms')
