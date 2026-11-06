#!/usr/bin/env python3
"""Q8d: latency of an acknowledged write (insert_one, w:majority and w:1), before and after the log inside the engine.

Modes (one engine at a time, same machine, same client):
  store-local   no log: the write is acknowledged after the WAL is written to the object store, here a local
                directory (`--store local`): a floor, NOT S3 (on S3 the same path measured 30-100 ms in M31)
  moto          no log: the WAL goes to a local S3 emulator (moto, Python): NOT the latency of S3, only the request path
  locallog      the disk log of the K-series prototype (`--local-log-dir`, one fdatasync on this machine)
  quaderno      the replicated log: acknowledged when 2 copies of 3 have the record (3 local copies, real fdatasync,
                no network between zones)
For each mode: 1 client thread (latency) and 8 threads (latency under load and writes/s), w:majority and w:1. Per-core
CPU of the machine during each phase (/proc/stat) and CPU of the client process: RULES "who sends the load must not
be the limit" (a phase with a core above 90% or the client above 0.8 core is NOT VALID).

  q8d_latency.py OUT_DIR [--modes quaderno,locallog,store-local,moto] [--n 2000] [--secs 10]
"""
import json, os, shutil, signal, socket, subprocess, sys, threading, time

from pymongo import MongoClient, WriteConcern

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ENGINE = os.environ.get('MONGO_SERVER_BIN', os.path.join(ROOT, 'target', 'release', 'mongo-server'))
PQ = os.environ.get('PQ_BIN', os.path.join(ROOT, 'proto', 'quaderno', 'target', 'release', 'pq'))
CLUSTER = os.path.join(ROOT, 'proto', 'quaderno', 'scripts', 'cluster.py')


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


args = sys.argv[1:]
OUT = os.path.abspath(args[0])
MODES = opt(args, '--modes', 'quaderno,locallog,store-local,moto').split(',')
N = int(opt(args, '--n', '2000'))
SECS = float(opt(args, '--secs', '10'))
PORT = 27951
WORK = os.path.join(OUT, 'work')
OFF = int(os.environ.get('Q8D_PORT_OFF', '3000'))
REPLICAS = ','.join(f'127.0.0.1:{OFF + 7200 + i}' for i in range(3))
QENV = dict(os.environ, PQ_WORK=os.path.join(WORK, 'log'), PQ_BIN=PQ, PQ_PORT_OFF=str(OFF), PQ_CORSIE='1', PQ_RICICLA='1')
os.makedirs(OUT, exist_ok=True)
RES = open(os.path.join(OUT, 'latency.jsonl'), 'a')
TXT = open(os.path.join(OUT, 'latency.txt'), 'a')
HZ = os.sysconf('SC_CLK_TCK')


def say(s):
    print(s, flush=True)
    TXT.write(s + '\n')
    TXT.flush()


def cpu_times():
    out = {}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3] != ' ':
            f = l.split()
            v = list(map(int, f[1:]))
            out[f[0]] = (sum(v) - v[3] - v[4], sum(v))
    return out


def busy(a, b):
    return {k: round(100.0 * (b[k][0] - a[k][0]) / max(1, b[k][1] - a[k][1]), 1) for k in a}


def proc_cpu(pid):
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / HZ
    except Exception:
        return 0.0


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p / 100))] if v else None


def wait_port(p, t=120, proc=None):
    end = time.time() + t
    while time.time() < end:
        if proc is not None and proc.poll() is not None:
            return False
        try:
            socket.create_connection(('127.0.0.1', p), 0.3).close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def start_engine(mode):
    d = os.path.join(WORK, mode)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    env = dict(os.environ)
    if mode == 'moto':
        env.update(AWS_ACCESS_KEY_ID='test', AWS_SECRET_ACCESS_KEY='test', AWS_REGION='us-east-1', AWS_ALLOW_HTTP='true')
        cmd = [ENGINE, '--store', 's3', '--endpoint', 'http://127.0.0.1:9000', '--bucket', 'q8d-lat']
    else:
        cmd = [ENGINE, '--store', 'local', '--local-dir', os.path.join(d, 'store')]
    cmd += ['--listen', f'127.0.0.1:{PORT}', '--no-ttl', '--replica-set', 'rs0', '--idle-secs', '600']
    if mode == 'locallog':
        cmd += ['--local-log-dir', os.path.join(d, 'll'), '--local-log-upload-secs', '30']
    if mode == 'quaderno':
        cmd += ['--quaderno', REPLICAS, '--local-log-upload-secs', '30', '--local-log-dir', os.path.join(d, 'kept')]
    p = subprocess.Popen(cmd, stdout=open(os.path.join(OUT, f'engine-{mode}.log'), 'ab'), stderr=subprocess.STDOUT, env=env, cwd=d)
    assert wait_port(PORT, proc=p), f'engine {mode} did not start'
    return p


def phase(mode, threads, w, pids):
    c = MongoClient(f'mongodb://127.0.0.1:{PORT}/?directConnection=true', retryWrites=False, maxPoolSize=threads + 2)
    lat = []
    lock = threading.Lock()
    stop = time.time() + SECS
    count = [0]

    def run(k):
        coll = c['lat'].get_collection('c', write_concern=WriteConcern(w=w))
        mine = []
        i = 0
        while (threads == 1 and i < N) or (threads > 1 and time.time() < stop):
            t = time.perf_counter()
            coll.insert_one({'_id': f'{mode}-{threads}-{w}-{k}-{i}-{time.time_ns()}', 'v': i, 'pad': 'x' * 200})
            mine.append((time.perf_counter() - t) * 1000)
            i += 1
        with lock:
            lat.extend(mine)
            count[0] += i

    # warm-up (connections, database open)
    coll = c['lat'].get_collection('c', write_concern=WriteConcern(w=w))
    for i in range(100):
        coll.insert_one({'_id': f'warm-{mode}-{threads}-{w}-{i}-{time.time_ns()}'})
    c0, p0, me0, t0 = cpu_times(), {k: proc_cpu(v) for k, v in pids.items()}, proc_cpu(os.getpid()), time.time()
    ts = [threading.Thread(target=run, args=(k,)) for k in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    el = time.time() - t0
    c1, p1, me1 = cpu_times(), {k: proc_cpu(v) for k, v in pids.items()}, proc_cpu(os.getpid())
    c.close()
    cores = busy(c0, c1)
    client_core = round((me1 - me0) / el, 2)
    procs = {k: round((p1[k] - p0[k]) / el, 2) for k in pids}
    valid = max(cores.values()) <= 90 and client_core <= 0.8
    r = {'mode': mode, 'threads': threads, 'w': w, 'n': len(lat), 'secs': round(el, 1), 'writes_per_s': round(count[0] / el, 0),
         'p50_ms': round(pct(lat, 50), 2), 'p90_ms': round(pct(lat, 90), 2), 'p99_ms': round(pct(lat, 99), 2), 'max_ms': round(max(lat), 1),
         'cores_busy_pct': cores, 'client_core': client_core, 'process_cores': procs, 'valid': valid}
    RES.write(json.dumps(r) + '\n')
    RES.flush()
    say(f"{mode} threads={threads} w={w}: {'VALID' if valid else 'NOT VALID'} n={r['n']} writes/s={r['writes_per_s']} p50={r['p50_ms']} p90={r['p90_ms']} p99={r['p99_ms']} max={r['max_ms']} ms | cores {cores} | client {client_core} core | processes {procs}")
    return r


def main():
    say(f'START {time.strftime("%F %T")} modes {MODES} n={N} secs={SECS} nproc={os.cpu_count()}')
    for mode in MODES:
        pids = {}
        moto = None
        if mode == 'quaderno':
            subprocess.run(['python3', CLUSTER, 'up', '--fresh', '--no-proxy', '--window-ms', '60000', '--mem-cap-mb', '1024'], env=QENV, capture_output=True, timeout=120)
            pj = json.load(open(os.path.join(WORK, 'log', 'pids.json')))
            pids.update({f'log_copy{i}': pj[f'r{i}'] for i in range(3)})
        if mode == 'moto':
            moto = subprocess.Popen(['moto_server', '-H', '127.0.0.1', '-p', '9000'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_port(9000)
            import boto3
            boto3.client('s3', endpoint_url='http://127.0.0.1:9000', aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1').create_bucket(Bucket='q8d-lat')
        eng = start_engine(mode)
        pids['engine'] = eng.pid
        for threads in (1, 8):
            for w in ('majority', 1):
                phase(mode, threads, w, pids)
        eng.send_signal(signal.SIGKILL)
        eng.wait()
        if mode == 'quaderno':
            subprocess.run(['python3', CLUSTER, 'down'], env=QENV, capture_output=True, timeout=120)
        if moto:
            moto.kill()
    say('END')


if __name__ == '__main__':
    main()
