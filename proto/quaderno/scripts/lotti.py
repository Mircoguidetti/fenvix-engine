#!/usr/bin/env python3
"""Giro 4, prova dei lotti (metodo del revisore delle idee pazze 3).

Per ogni combinazione (lotto, client, ripetizione): gruppo nuovo senza proxy, capo (leader) da solo sul core 0,
le altre due repliche e il generatore sul core 1. Carico fisso a ciclo aperto (RATE record/s). Dopo il riscaldamento
si misura per SAMPLE secondi la CPU del processo del capo e del suo thread principale (da /proc) e i record confermati
(commit_index del capo): µs di CPU del capo per record.

Giro 5: PQ_REAL=1 record "veri" (documento + voci d'indice), PQ_ZSTD_MAIN=1 compressione sul thread principale.

Uso: lotti.py OUT RATE "1,16,32" "64,256" REPS [KEYS] [VAL] [extra args per le repliche via env PQ_*]
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PQ_WORK', '/root/pq-work')
hz = os.sysconf('SC_CLK_TCK')
out, rate, batches, clients, reps = sys.argv[1], float(sys.argv[2]), [int(x) for x in sys.argv[3].split(',')], [int(x) for x in sys.argv[4].split(',')], int(sys.argv[5])
keys = int(sys.argv[6]) if len(sys.argv) > 6 else 1
val = int(sys.argv[7]) if len(sys.argv) > 7 else 200
WARM, SECS, SAMPLE = 6, 20, 12
os.makedirs(out, exist_ok=True)


def cpu(path):
    f = open(path).read().rsplit(')', 1)[1].split()
    return (int(f[11]) + int(f[12])) / hz


def status():
    o = subprocess.run([PQ, 'status'], capture_output=True, text=True, timeout=20).stdout
    return [json.loads(l) for l in o.splitlines() if l.strip()]


def pin(pid, core):
    subprocess.run(['taskset', '-a', '-p', '-c', str(core), str(pid)], capture_output=True)


res = []
for rep in range(reps):
    for b in batches:
        for c in clients:
            subprocess.run(['python3', os.path.join(HERE, 'cluster.py'), 'up', '--fresh', '--no-proxy', '--mem-cap-mb', '2048'], capture_output=True)
            pids = json.load(open(os.path.join(WORK, 'pids.json')))
            st = status()
            lid = [s['id'] for s in st if s['role'] == 'Leader'][0]
            lp = pids[f'r{lid}']
            for k, v in pids.items():
                if k.startswith('r') or k == 'proxy':
                    pin(v, 0 if v == lp else 1)
            gen = subprocess.Popen(['nice', '-n', os.environ.get('PQ_GEN_NICE', '0'), 'taskset', '-c', '1', PQ, 'lotti', '--rate', str(rate), '--batch', str(b), '--clients', str(c), '--conns', str(min(c, 256)),
                                    '--secs', str(SECS), '--warm', str(WARM), '--keys', str(keys), '--val', str(val), '--real', os.environ.get('PQ_REAL', '0')], stdout=subprocess.PIPE, text=True)
            time.sleep(WARM + 3)
            s0 = [s for s in status() if s['id'] == lid][0]
            ci0 = s0['commit_index']
            p0, t0 = cpu(f'/proc/{lp}/stat'), cpu(f'/proc/{lp}/task/{lp}/stat')
            f0 = {k: cpu(f'/proc/{v}/stat') for k, v in pids.items() if k.startswith('r') and v != lp}
            w0 = time.time()
            time.sleep(SAMPLE)
            s1 = [s for s in status() if s['id'] == lid][0]
            ci1 = s1['commit_index']
            p1, t1 = cpu(f'/proc/{lp}/stat'), cpu(f'/proc/{lp}/task/{lp}/stat')
            f1 = {k: cpu(f'/proc/{v}/stat') for k, v in pids.items() if k.startswith('r') and v != lp}
            el = time.time() - w0
            g = json.loads(gen.communicate()[0].strip().splitlines()[-1])
            n = ci1 - ci0
            r = {'rep': rep, 'batch': b, 'clients': c, 'rate': rate, 'keys': keys, 'val': val, 'recs_sampled': n, 'recs_per_s_sampled': n / el,
                 'leader_proc_us_per_rec': (p1 - p0) * 1e6 / max(n, 1), 'leader_main_us_per_rec': (t1 - t0) * 1e6 / max(n, 1),
                 'leader_cores': (p1 - p0) / el, 'leader_main_cores': (t1 - t0) / el, 'real': os.environ.get('PQ_REAL', '0'), 'zstd_main': os.environ.get('PQ_ZSTD_MAIN', '0'), 'follower_us_per_rec': {k: (f1[k] - f0[k]) * 1e6 / max(n, 1) for k in f0},
                 'zstd': os.environ.get('PQ_ZSTD', '0'),
                 'repl_raw_bytes_per_rec': (s1.get('repl_raw', 0) - s0.get('repl_raw', 0)) / max(n, 1),
                 'repl_sent_bytes_per_rec': (s1.get('repl_sent', 0) - s0.get('repl_sent', 0)) / max(n, 1),
                 'gen': {k: g[k] for k in ('recs_per_s', 'unknown', 'rejected', 'conflicts')}, 'lat': g['latency']}
            print(json.dumps(r), flush=True)
            res.append(r)
            with open(os.path.join(out, 'lotti.jsonl'), 'a') as f:
                f.write(json.dumps(r) + '\n')
subprocess.run(['python3', os.path.join(HERE, 'cluster.py'), 'down'], capture_output=True)
# riassunto: mediane per (lotto, client)
import statistics
print('--- riassunto (mediana sulle ripetizioni)')
for c in clients:
    base = None
    for b in batches:
        xs = [r for r in res if r['batch'] == b and r['clients'] == c]
        mp = statistics.median(r['leader_proc_us_per_rec'] for r in xs)
        mm = statistics.median(r['leader_main_us_per_rec'] for r in xs)
        if b == batches[0]:
            base = (mp, mm)
        print(f'client {c} lotto {b}: capo processo {mp:.1f} µs/record ({100 * (1 - mp / base[0]):+.0f}% vs lotto {batches[0]}), '
              f'thread principale {mm:.1f} µs/record ({100 * (1 - mm / base[1]):+.0f}%), record/s {statistics.median(r["recs_per_s_sampled"] for r in xs):.0f}, '
              f'p99 {statistics.median(r["lat"]["p99_us"] for r in xs) / 1000:.1f} ms')
