"""Shared pieces of the agent-lane tests (PROTOTYPE, `mongo-server/src/agentlane.rs`). $0, local only.

A local group of 3 log copies (proto/quaderno/scripts/cluster.py, real fdatasync, as the Q8d tests), one or two
mongo-server processes with `--quaderno` (the default path: acknowledged by 2 of 3 log copies) and `--agent-lane`
(collections named `agent*` go on the agent lane: acknowledged once their WAL object is in object storage).
Object storage is a local directory (`--store local`) or a local MinIO (real S3 conditional writes between processes).
PUT latency is modelled by `FENVIX_SIM_S3_PUT=median_ms,sigma,mbps` (lognormal, measurement only), GET latency by
`FENVIX_SIM_S3`. Fault points (`MONGO_TEST_FAULTS_FILE`, `mongo-server/src/faults.rs`) mark the moments to kill at.
"""
import json, os, random, re, shutil, signal, socket, subprocess, sys, threading, time

from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
ENGINE = os.environ.get('MONGO_SERVER_BIN', os.path.join(ROOT, 'target', 'release', 'mongo-server'))
PQ = os.environ.get('PQ_BIN', os.path.join(ROOT, 'proto', 'quaderno', 'target', 'release', 'pq'))
CLUSTER = os.path.join(ROOT, 'proto', 'quaderno', 'scripts', 'cluster.py')
JH = os.environ.get('JUDGE_HOME', os.path.expanduser('~/.fenvix-judge'))
MINIO = os.path.join(JH, 'env', 'bin', 'minio')
AGENT_PATTERN = '*.agent*'


def opt(a, k, d):
    return a[a.index(k) + 1] if k in a else d


class Out:
    def __init__(self, out, name):
        self.dir = os.path.abspath(out)
        os.makedirs(self.dir, exist_ok=True)
        self.f = open(os.path.join(self.dir, name + '.log'), 'a')

    def say(self, *x):
        s = ' '.join(str(v) for v in x)
        print(s, flush=True)
        self.f.write(s + '\n')
        self.f.flush()


class Log:
    """The 3 copies of the replicated log (quaderno)."""

    def __init__(self, work, off):
        self.work, self.off = work, off
        self.replicas = ','.join(f'127.0.0.1:{off + 7200 + i}' for i in range(3))
        self.env = dict(os.environ, PQ_WORK=work, PQ_BIN=PQ, PQ_PORT_OFF=str(off), PQ_CORSIE='1', PQ_RICICLA='1')

    def cl(self, *a):
        return subprocess.run([sys.executable, CLUSTER] + list(a), env=self.env, capture_output=True, text=True, timeout=120).stdout

    def up(self):
        self.cl('up', '--fresh', '--no-proxy', '--window-ms', '60000', '--mem-cap-mb', '1024')

    def down(self):
        self.cl('down')


class Minio:
    def __init__(self, work, port=9100):
        self.dir, self.port, self.p = os.path.join(work, 'minio'), port, None

    def up(self, bucket):
        os.makedirs(os.path.join(self.dir, bucket), exist_ok=True)
        env = dict(os.environ, MINIO_ROOT_USER='minioadmin', MINIO_ROOT_PASSWORD='minioadmin')
        self.p = subprocess.Popen([MINIO, 'server', self.dir, '--address', f'127.0.0.1:{self.port}', '--console-address', f'127.0.0.1:{self.port + 1}'],
                                  stdout=open(os.path.join(self.dir, '..', 'minio.log'), 'ab'), stderr=subprocess.STDOUT, env=env)
        wait_port(self.port, 60)

    def down(self):
        if self.p:
            self.p.terminate()
            self.p.wait()

    def args(self, bucket):
        return ['--store', 's3', '--endpoint', f'http://127.0.0.1:{self.port}', '--bucket', bucket, '--region', 'us-east-1']

    @staticmethod
    def env():
        return {'AWS_ACCESS_KEY_ID': 'minioadmin', 'AWS_SECRET_ACCESS_KEY': 'minioadmin', 'AWS_REGION': 'us-east-1'}


def wait_port(port, t):
    end = time.time() + t
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.3).close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


class Engine:
    """One mongo-server. Its stderr goes to a file the tests read (fault points, PUTLOG lines)."""

    def __init__(self, out, name, port, store_args, log=None, extra=(), env=None, faults=None, upload_secs=30):
        self.out, self.name, self.port, self.store_args, self.log = out, name, port, list(store_args), log
        self.upload_secs = upload_secs
        self.extra, self.env, self.p, self.n = list(extra), dict(env or {}), None, 0
        self.faults = faults
        self.logfile = os.path.join(out, f'engine-{name}.log')

    def start(self, fresh_dir=False, wait=True, env=None, extra=()):
        self.n += 1
        d = os.path.join(self.out, 'work', f'local-{self.name}-{self.n if fresh_dir else 0}')
        if fresh_dir:
            shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)
        cmd = [ENGINE, '--listen', f'127.0.0.1:{self.port}'] + self.store_args + ['--no-ttl', '--replica-set', 'rs0', '--advertise', f'localhost:{self.port}',
               '--lease-ttl-secs', '4', '--holder', f'{self.name}-{self.n}', '--idle-secs', '600', '--agent-lane', AGENT_PATTERN]
        if self.log:
            cmd += ['--quaderno', self.log.replicas, '--local-log-upload-secs', str(self.upload_secs), '--local-log-dir', d]
        cmd += self.extra + list(extra)
        e = dict(os.environ, **self.env, **(env or {}))
        if self.faults:
            e['MONGO_TEST_FAULTS_FILE'] = self.faults
        self.p = subprocess.Popen(cmd, stdout=open(self.logfile, 'ab'), stderr=subprocess.STDOUT, env=e, cwd=d)
        return self.wait() if wait else True

    def wait(self, t=120):
        end = time.time() + t
        while time.time() < end:
            if self.p.poll() is not None:
                return False
            try:
                socket.create_connection(('127.0.0.1', self.port), 0.3).close()
                return True
            except OSError:
                time.sleep(0.05)
        return False

    def alive(self):
        return self.p is not None and self.p.poll() is None

    def kill9(self):
        if self.alive():
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()

    def stop(self):
        if self.alive():
            self.p.send_signal(signal.SIGCONT)
            self.p.terminate()
            try:
                self.p.wait(30)
            except subprocess.TimeoutExpired:
                self.kill9()

    def sigstop(self):
        self.p.send_signal(signal.SIGSTOP)

    def sigcont(self):
        self.p.send_signal(signal.SIGCONT)

    def log_size(self):
        try:
            return os.path.getsize(self.logfile)
        except OSError:
            return 0

    def log_since(self, off):
        with open(self.logfile, 'rb') as f:
            f.seek(off)
            return f.read().decode('utf-8', 'replace')

    def wait_line(self, off, needle, t):
        """Wait until a line containing `needle` appears after byte `off`; return its end offset or None."""
        end = time.time() + t
        while time.time() < end:
            s = self.log_since(off)
            i = s.find(needle)
            if i >= 0:
                return off + len(s[:i + len(needle)].encode())
            time.sleep(0.002)
        return None

    def client(self, **kw):
        a = dict(retryWrites=False, serverSelectionTimeoutMS=5000, socketTimeoutMS=30000, connectTimeoutMS=2000)
        a.update(kw)
        return MongoClient(f'mongodb://127.0.0.1:{self.port}/?directConnection=true', **a)

    def status(self):
        c = self.client()
        try:
            return c.admin.command('serverStatus')
        finally:
            c.close()


def retry(f, t=120):
    """Run f; a database in recovery (134) or a reconnect is retried, as a driver would."""
    from pymongo.errors import PyMongoError
    end = time.time() + t
    while True:
        try:
            return f()
        except PyMongoError as e:
            if time.time() > end:
                raise
            time.sleep(0.2)


def find_ids(c, db, coll, ids):
    found = set()
    ids = list(ids)
    for j in range(0, len(ids), 2000):
        part = ids[j:j + 2000]
        for d in retry(lambda: list(c[db][coll].find({'_id': {'$in': part}}, {'_id': 1}))):
            found.add(d['_id'])
    return found


def pct(v, p):
    if not v:
        return float('nan')
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def find_key(d, k):
    """The first value under key `k` anywhere inside a nested document."""
    if isinstance(d, dict):
        if k in d:
            return d[k]
        for v in d.values():
            r = find_key(v, k)
            if r is not None:
                return r
    return None


def lane_stats(c):
    return c.admin.command('tenantStats').get('agentLane')


def kill_leftovers(engine_ports=(), log_off=None, minio_port=None):
    """Kill processes a previous run of the same test left behind (they hold its ports and its state)."""
    needles = [f'--listen 127.0.0.1:{p}' for p in engine_ports]
    if log_off is not None:
        needles += [f'--peer-listen 127.0.0.1:{log_off + 7100 + i}' for i in range(3)]
    if minio_port is not None:
        needles.append(f'--address 127.0.0.1:{minio_port}')
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            cmd = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\0', b' ').decode()
        except OSError:
            continue
        if any(n in cmd for n in needles):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass
    if log_off is not None:
        # the log's proxy has no port of its own on its command line: it is found by the routes of the same work dir
        pass
    time.sleep(0.3)
