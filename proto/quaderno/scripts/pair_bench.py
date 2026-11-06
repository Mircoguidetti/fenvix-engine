#!/usr/bin/env python3
"""log-on-pair · points 1, 2, 4, 5 of prompts/P-log-on-pair-local.md (fenvix-brain).

Cases (one fresh log cell per phase; `pair_common.py` for the layout):
  base-near   today's shape, log leader in the waiter's zone: plain Raft (any 2 of 3), the writer reaches the leader
              without a zone crossing, no mongo-server on the leader's cores (the log machine serves no reads)
  base-far    today's shape, log leader in another zone (2 customers in 3): as base-near, plus one crossing writer->leader
  pair-R      the log on the pair: ack = the waiter's own durable copy AND the first remote copy (PQ_LEADER_ACK=1),
              the third copy is the keeper (PQ_KEEPER=1); log copy 1 shares cores 0-1 and the disk with a mongo-server
              serving R reads/s (R = 0, 2000, 8000): 90% of the reads on a hot 5% of the documents, 10% uniform
  cold-R      point 2: as pair-R but every read uniform over all documents (misses RAM: the local disk)
Writes: `pq corsie`, one database, W records/s (1000 medium, 5000 busy), 341-byte documents (plus the two index
entries of `corsie_record`), latency from the send (p50, p99) for the ack.
Every phase: per-core CPU each second, CPU seconds of each process, /proc/diskstats deltas, proxy bytes per route.

Usage: pair_bench.py OUT --data /root/pair-data --docs N [--reps 3] [--secs 60] [--delay-us 500] [--cases a,b,...] [--writes 1000,5000]
"""
import json, os, signal, subprocess, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pair_common import *  # noqa

ENGINE = os.path.abspath(os.path.join(HERE, '..', '..', '..'))
MS = os.environ.get('MONGO_SERVER') or os.path.join(ENGINE, 'target', 'release', 'mongo-server')
PAIRLOAD = os.environ.get('PAIRLOAD') or os.path.join(ENGINE, 'target', 'release', 'pairload')
CG = '/sys/fs/cgroup/memory/pairms'


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


A = sys.argv[1:]
OUT = os.path.abspath(A[0])
DATA = opt(A, '--data', '/root/pair-data')
DOCS = int(opt(A, '--docs', '6000000'))
REPS = int(opt(A, '--reps', '3'))
SECS = int(opt(A, '--secs', '60'))
DELAY = int(opt(A, '--delay-us', '500'))
MEM_MB = int(opt(A, '--ms-mem-mb', '1024'))
CASES = opt(A, '--cases', 'base-near,base-far,pair-0,pair-2000,pair-8000,cold-2000,cold-8000').split(',')
WRITES = [int(x) for x in opt(A, '--writes', '1000,5000').split(',')]
os.makedirs(OUT, exist_ok=True)
RES = open(os.path.join(OUT, 'phases.jsonl'), 'a')
LOG = open(os.path.join(OUT, 'bench.log'), 'a')


def say(*x):
    s = time.strftime('%H:%M:%S ') + ' '.join(str(v) for v in x)
    print(s, flush=True)
    LOG.write(s + '\n')
    LOG.flush()


class MongoServer:
    pid = None

    def up(self):
        if self.pid and os.path.exists(f'/proc/{self.pid}'):
            return
        os.makedirs(CG, exist_ok=True)
        open(os.path.join(CG, 'memory.limit_in_bytes'), 'w').write(str(MEM_MB << 20))
        f = open(os.path.join(OUT, 'mongo-server.log'), 'ab')
        cmd = (f'echo $$ > {CG}/cgroup.procs; exec taskset -c {CPU_WAITER} {MS} --listen 127.0.0.1:27018 --store local --local-dir {DATA} '
               f'--no-maintenance --no-ttl --idle-secs 100000 --block-cache-mb 128 --meta-cache-mb 64')
        self.pid = subprocess.Popen(['sh', '-c', cmd], stdout=f, stderr=f, start_new_session=True).pid
        wait_port(27018, 60)
        time.sleep(1)

    def down(self):
        if self.pid:
            try:
                os.kill(self.pid, signal.SIGKILL)
            except Exception:
                pass
            self.pid = None
            time.sleep(1)


MSRV = MongoServer()


def cg_stat():
    try:
        d = dict(l.split() for l in open(os.path.join(CG, 'memory.stat')))
        return {'cache_mb': int(d['cache']) >> 20, 'rss_mb': int(d['rss']) >> 20}
    except Exception:
        return {}


def phase(case, w, rep):
    shape_pair = not case.startswith('base')
    reads = 0 if case.startswith('base') else int(case.split('-')[1])
    hot_pct = 0 if case.startswith('cold') else 90
    cell = Cell(delay_us=DELAY, leader_ack=shape_pair, keeper=shape_pair, client_delay_r0=(case == 'base-far'))
    if shape_pair:
        MSRV.up()
    else:
        MSRV.down()
    for attempt in range(3):
        ld = cell.up()
        if ld == 0:
            break
        say('leader is', ld, 'not 0, restarting the cell')
    label = f'{case}-w{w}-r{rep}'
    say('PHASE', label, 'leader', ld)
    reader = None
    if reads:
        reader = subprocess.Popen(['taskset', '-c', CPU_TOOLS, PAIRLOAD, 'read', '--docs', str(DOCS), '--rate', str(reads), '--conns', '64',
                                   '--secs', str(SECS + 10), '--warm', '15', '--hot-pct', str(hot_pct), '--hot-frac', '0.05', '--threads', '2'],
                                  stdout=subprocess.PIPE, stderr=open(os.path.join(OUT, 'reader.err'), 'a'), text=True)
        cell.p['reader'] = reader.pid
        time.sleep(12)
    writer = subprocess.Popen(['taskset', '-c', CPU_TOOLS, 'chrt', '-f', '20', PQ, 'corsie', '--replicas', cell.client_addrs(), '--db-from', '0', '--dbs', '1',
                               '--rate', str(w), '--doc', '341', '--secs', str(SECS), '--warm', '5', '--label', label, '--threads', '2', '--conns', '16',
                               '--deadline-ms', '10000'], stdout=subprocess.PIPE, stderr=open(os.path.join(OUT, 'writer.err'), 'a'), text=True, env=BASE_ENV)
    cell.p['writer'] = writer.pid
    time.sleep(5)
    pids = {k: v for k, v in cell.p.items()}
    if shape_pair and MSRV.pid:
        pids['mongo-server'] = MSRV.pid
    c0 = {k: proc_cpu_s(v) for k, v in pids.items()}
    d0, s0, k0, t0 = disk_counters(), cell.stats(), core_times(), time.time()
    st0 = cell.status()
    percore = []
    prev = k0
    while time.time() - t0 < SECS:
        time.sleep(1)
        k = core_times()
        percore.append({c: round(100 * (k[c][0] - prev[c][0]) / max(1, k[c][1] - prev[c][1]), 1) for c in k})
        prev = k
    t1 = time.time()
    c1 = {k: proc_cpu_s(v) for k, v in pids.items()}
    d1, s1, st1 = disk_counters(), cell.stats(), cell.status()
    out, _ = writer.communicate(timeout=120)
    wres = json.loads(out.strip().splitlines()[-1]) if out.strip() else {}
    rres = {}
    if reader:
        o, _ = reader.communicate(timeout=120)
        try:
            rres = json.loads(o.strip().splitlines()[-1])
        except Exception:
            rres = {'raw': o[-500:]}
    el = t1 - t0
    cores = sorted(percore[0].keys()) if percore else []
    core_mean = {c: round(sum(p[c] for p in percore) / len(percore), 1) for c in cores}
    core_max = {c: max(p[c] for p in percore) for c in cores}
    cpu = {k: (round(c1[k] - c0[k], 2) if c1.get(k) is not None and c0.get(k) is not None else None) for k in pids}
    disk = {k: d1[k] - d0[k] for k in d0}
    route_bytes = {r: [s1[r][0] - s0.get(r, [0, 0, 0])[0], s1[r][1] - s0.get(r, [0, 0, 0])[1]] for r in s1}
    commits = (st1.get(0, {}).get('commit_index', 0) - st0.get(0, {}).get('commit_index', 0)) if st0 and st1 else None
    rec = {'label': label, 'case': case, 'writes': w, 'reads': reads, 'hot_pct': hot_pct, 'rep': rep, 'delay_us': DELAY, 'secs': el,
           'writer': wres, 'reader': rres, 'cpu_s': cpu, 'core_mean': core_mean, 'core_max': core_max, 'disk': disk, 'route_bytes': route_bytes,
           'log_entries': commits, 'cgroup': cg_stat() if shape_pair else {}, 'leader': ld}
    RES.write(json.dumps(rec) + '\n')
    RES.flush()
    lat = wres.get('lat_send', {})
    say('  ack p50 %.2f ms p99 %.2f ms ok/s %.0f | reads/s %s p99 %s ms | cores %s | disk r %.0f/s %.1f MB/s w %.0f/s | r0 cpu %.2f s' % (
        lat.get('p50_us', 0) / 1000, lat.get('p99_us', 0) / 1000, wres.get('ok_per_s', 0), round(rres.get('ops_per_s', 0)),
        rres.get('p99_us', 0) / 1000 if rres.get('p99_us') else '-', core_mean, disk['reads'] / el, disk['read_bytes'] / el / 1e6,
        disk['writes'] / el, cpu.get('r0') or 0))
    if reader and reader.poll() is None:
        reader.kill()
    cell.down()


def main():
    say('START', 'cases', CASES, 'writes', WRITES, 'reps', REPS, 'secs', SECS, 'delay_us', DELAY, 'docs', DOCS, 'ms_mem_mb', MEM_MB)
    for rep in range(1, REPS + 1):
        for w in WRITES:
            for case in CASES:
                try:
                    phase(case, w, rep)
                except Exception as e:
                    say('PHASE FAILED', case, w, rep, repr(e))
                    subprocess.run(['pkill', '-9', '-f', 'pq (replica|dproxy|flusher|corsie)'], capture_output=True)
                    subprocess.run(['pkill', '-9', '-f', 'pairload read'], capture_output=True)
    MSRV.down()
    say('END')


if __name__ == '__main__':
    main()
