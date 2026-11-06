#!/usr/bin/env python3
"""log-on-pair, point 5: CPU of log copy 1 (r0) with NO writes, to split its CPU per record into a fixed part (threads
that wake every millisecond, the fake flusher's polling) and the part that grows with the records.
Usage: pair_idle_cpu.py OUT [--secs 40]"""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pair_common import *  # noqa

OUT = os.path.abspath(sys.argv[1])
SECS = int(sys.argv[sys.argv.index('--secs') + 1]) if '--secs' in sys.argv else 40
os.makedirs(OUT, exist_ok=True)
res = {}
for shape, la, kp in (('base', False, False), ('pair', True, True)):
    for flusher in (True, False):
        cell = Cell(work=os.path.join(OUT, 'work'), leader_ack=la, keeper=kp)
        ld = cell.up(flusher=flusher)
        time.sleep(5)
        c0 = {k: proc_cpu_s(v) for k, v in cell.p.items()}
        time.sleep(SECS)
        c1 = {k: proc_cpu_s(v) for k, v in cell.p.items()}
        res[f'{shape}-flusher{int(flusher)}'] = {'leader': ld, 'cores': {k: round((c1[k] - c0[k]) / SECS, 3) for k in c0 if c0[k] is not None and c1.get(k) is not None}}
        print(shape, 'flusher' if flusher else 'no flusher', res[f'{shape}-flusher{int(flusher)}'], flush=True)
        cell.down()
json.dump(res, open(os.path.join(OUT, 'idle.json'), 'w'), indent=1)
