#!/usr/bin/env python3
"""prova-zone: ogni secondo legge lo status di tutte le repliche e scrive una riga JSON con i campi della copia del
capo (ritardo, PUT, errori, CPU) e lo stato del freno e dello scarico. Uso: sonda-copia.py file.jsonl"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.path.join(HERE, '..', 'target', 'release', 'pq')
out = open(sys.argv[1], 'a')
K = ['id', 'role', 'commit_index', 'copia_fino_a', 'copia_s3_fino_a', 'copia_ritardo_ms', 'copia_allarme', 'copia_put',
     'copia_put_errori', 'copia_esistenti', 'copia_byte', 'copia_cancellati', 'copia_cpu_ms', 'copia_put_ms_ultimo',
     'copia_put_ms_max', 'oldest_unflushed_s', 'brake', 'mem_used', 'log_front', 'last_index']
while True:
    t = time.time()
    try:
        o = subprocess.run([PQ, 'status'], capture_output=True, text=True, timeout=5).stdout
        reps = [json.loads(l) for l in o.splitlines() if l.strip()]
        lead = [r for r in reps if r.get('role') == 'Leader']
        row = {'t_ms': int(t * 1000), 'capo': {k: lead[0].get(k) for k in K} if lead else None,
               'giu': sum(1 for r in reps if r.get('down'))}
    except Exception as e:
        row = {'t_ms': int(t * 1000), 'errore': str(e)}
    out.write(json.dumps(row) + '\n')
    out.flush()
    time.sleep(max(0.0, 1.0 - (time.time() - t)))
