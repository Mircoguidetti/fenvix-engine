#!/usr/bin/env python3
"""prova-zone: dalla sonda e dagli eventi di uno scenario s3, la pendenza del ritardo della copia durante il fermo,
l'allarme e il tempo di ritorno sotto 2 s dopo la ripresa (soglia S3)."""
import json, sys
d = sys.argv[1]
ev = {}
for l in open(d + '/eventi.jsonl'):
    ev.update(json.loads(l))
s = [json.loads(l) for l in open(d + '/sonda.jsonl')]
s = [x for x in s if x.get('capo')]
def at(t):
    return min(s, key=lambda x: abs(x['t_ms'] - t))
out = {}
if 's3_lento_ms' in ev:
    a, b = at(ev['s3_lento_ms']), at(ev['s3_fermo_ms'])
    out['lento_ritardo_inizio_ms'] = a['capo']['copia_ritardo_ms']
    out['lento_ritardo_fine_ms'] = b['capo']['copia_ritardo_ms']
    out['lento_pendenza_s_al_s'] = round((b['capo']['copia_ritardo_ms'] - a['capo']['copia_ritardo_ms']) / (b['t_ms'] - a['t_ms']), 3)
    out['lento_put'] = b['capo']['copia_put'] - a['capo']['copia_put']
if 's3_fermo_ms' in ev:
    end = ev.get('morte_ms', ev.get('s3_ripreso_ms'))
    a, b = at(ev['s3_fermo_ms']), at(end - 1000)
    out['fermo_s'] = round((end - ev['s3_fermo_ms']) / 1000, 1)
    out['fermo_ritardo_inizio_ms'] = a['capo']['copia_ritardo_ms']
    out['fermo_ritardo_fine_ms'] = b['capo']['copia_ritardo_ms']
    out['fermo_pendenza_s_al_s'] = round((b['capo']['copia_ritardo_ms'] - a['capo']['copia_ritardo_ms']) / (b['t_ms'] - a['t_ms']), 3)
    w = [x for x in s if ev['s3_fermo_ms'] + 2000 <= x['t_ms'] <= end - 1000]
    out['fermo_righe_con_allarme'] = sum(1 for x in w if x['capo']['copia_allarme'])
    out['fermo_righe'] = len(w)
    out['fermo_stesso_capo'] = len({x['capo']['id'] for x in w}) == 1
    first_alarm = [x for x in s if x['capo']['copia_allarme']]
    if first_alarm:
        out['primo_allarme_s_dopo_inizio_problemi'] = round((first_alarm[0]['t_ms'] - ev.get('s3_lento_ms', ev['s3_fermo_ms'])) / 1000, 1)
if 's3_ripreso_ms' in ev and 'morte_ms' not in ev:
    r = ev['s3_ripreso_ms']
    after = [x for x in s if x['t_ms'] >= r]
    ok = [x for x in after if x['capo']['copia_ritardo_ms'] < 2000]
    out['ritorno_sotto_2s_dopo_ripresa_s'] = round((ok[0]['t_ms'] - r) / 1000, 1) if ok else None
    out['ritardo_max_dopo_ritorno_ms'] = max(x['capo']['copia_ritardo_ms'] for x in after if ok and x['t_ms'] >= ok[0]['t_ms'])
    out['put_errori_totali'] = after[-1]['capo']['copia_put_errori']
    out['put_ms_max'] = after[-1]['capo']['copia_put_ms_max']
print(json.dumps(out))
