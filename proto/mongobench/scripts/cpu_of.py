#!/usr/bin/env python3
"""cpu_of.py SECS: CPU (s) di ogni mongod e del generatore in SECS secondi (da /proc)."""
import os, sys, time, subprocess
hz = os.sysconf('SC_CLK_TCK')
def procs():
    out = subprocess.run(['pgrep', '-a', '-f', 'mongod --port|mongobench|target/release/pq'], capture_output=True, text=True).stdout
    r = {}
    for l in out.splitlines():
        pid, rest = l.split(' ', 1)
        if 'pgrep' in rest or 'taskset' in rest.split()[0]: continue
        name = rest.split('--port ')[1].split()[0] if '--port' in rest else rest.split()[0].split('/')[-1] + ('-' + rest.split()[1] if len(rest.split()) > 1 else '')
        r[pid] = name
    return r
def cpu(pid):
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split(); return (int(f[11]) + int(f[12])) / hz
    except Exception: return 0
secs = float(sys.argv[1])
p = procs(); c0 = {k: cpu(k) for k in p}; t = time.time(); time.sleep(secs); el = time.time() - t
for k, n in p.items(): print(n, k, round((cpu(k) - c0[k]) / el, 3), 'core')
