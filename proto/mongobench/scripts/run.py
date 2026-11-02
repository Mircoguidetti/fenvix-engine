#!/usr/bin/env python3
"""M16: una prova mongod. run.py OUT MODE(rs3|single) IDX MIX CLIENTS BATCH W J [SECS] [WARM] [PRELOAD]
Accende mongod, lancia il generatore sul core 1, misura la CPU di ogni processo nella finestra di misura, scrive una riga JSON."""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, '..', 'target', 'release', 'mongobench')
hz = os.sysconf('SC_CLK_TCK')
out, mode, idx, mix, clients, batch, w, j = sys.argv[1:9]
secs = int(sys.argv[9]) if len(sys.argv) > 9 else 30
warm = int(sys.argv[10]) if len(sys.argv) > 10 else 10
pre = int(sys.argv[11]) if len(sys.argv) > 11 else 100000
gen_core = os.environ.get('MB_GEN_CORE', '1')
uri = subprocess.run(['python3', os.path.join(HERE, 'mongo.py'), mode], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
def mpids():
    o = subprocess.run(['pgrep', '-a', '-x', 'mongod'], capture_output=True, text=True).stdout
    return {l.split('--port ')[1].split()[0]: l.split()[0] for l in o.splitlines()}
def cpu(pid):
    f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split(); return (int(f[11]) + int(f[12])) / hz
g = subprocess.Popen(['taskset', '-c', gen_core, BIN, '--uri', uri, '--idx', idx, '--mix', mix, '--clients', clients, '--batch', batch, '--w', w, '--j', j,
                      '--secs', str(secs), '--warm', str(warm), '--preload', str(pre)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
for l in g.stderr:
    if 'MISURA_INIZIO' in l: break
ps = mpids(); c0 = {k: cpu(v) for k, v in ps.items()}; g0 = cpu(g.pid); t = time.time()
time.sleep(secs - 1)
c1 = {k: cpu(v) for k, v in ps.items()}; g1 = cpu(g.pid); el = time.time() - t
o, e = g.communicate()
r = json.loads(o.strip().splitlines()[-1])
r.update({'mode': mode, 'cpu_core': {k: round((c1[k] - c0[k]) / el, 3) for k in ps}, 'gen_core': round((g1 - g0) / el, 3)})
ops = r['ops_per_s'] or 1
r['us_cpu_per_op'] = {k: round(v * 1e6 / ops, 1) for k, v in r['cpu_core'].items()}
print(json.dumps(r)); open(out, 'a').write(json.dumps(r) + '\n')
