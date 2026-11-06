"""log-on-pair: shared pieces of pair_bench.py and pair_kill9.py.

Three log copies on one machine, every link between them through the delay-line proxy (`pq dproxy`, cross-zone delay
per direction, byte counters per route):
  r0 = log copy 1 = the waiter's own copy (zone A), r1 = copy 2 = the warm spare (zone B), r2 = copy 3 = the keeper (zone C).
Ports: replica i listens to the other replicas on 7100+i and to clients on 7200+i; route r{a}->r{b} on 7300+10a+b;
client route c->r{i} on 7400+i. The writer (the waiter's log client) reaches r0 directly (same machine) or through
c->r0 (a log leader in another zone, today's shape); r1 and r2 always through c->r1, c->r2 (another zone).
"""
import json, os, signal, subprocess, time, socket

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.environ.get('PQ_BIN') or os.path.join(HERE, '..', 'target', 'release', 'pq')
WORK = os.environ.get('PAIR_WORK', '/root/pair-work')
N = 3
# CPU layout of the 4-core machine: the waiter's machine (log copy 1 and mongo-server) on cores 0-1 = 2 vCPU, like
# an m8gd.large; copies 2 and 3, generators, proxy and fake flusher share cores 2-3 (with one core each for the two
# groups, the tools core reached 90-97% at 5,000 writes + 8,000 reads per second in the smoke run).
CPU_WAITER = os.environ.get('PAIR_CPU_WAITER', '0,1')
CPU_REMOTE = os.environ.get('PAIR_CPU_REMOTE', '2,3')
CPU_TOOLS = os.environ.get('PAIR_CPU_TOOLS', '2,3')
# lanes on as in Q8c; the per-database quota raised above the busy profile (one dedicated database, the quota is not
# what is measured here)
BASE_ENV = dict(os.environ, PQ_CORSIE='1', PQ_RICICLA='1', PQ_PREALLOC_MBS='64', PQ_PREF='0', PQ_QUOTA_RECS='50000', PQ_QUOTA_MB='64')


def wait_port(port, t=15):
    end = time.time() + t
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            return True
        except Exception:
            time.sleep(0.05)
    return False


class Cell:
    def __init__(self, work=WORK, delay_us=500, leader_ack=True, keeper=True, client_delay_r0=False, window_ms=10000, mem_cap_mb=2000, seg_mb=64):
        self.work, self.delay_us, self.leader_ack, self.keeper = work, delay_us, leader_ack, keeper
        self.client_delay_r0, self.window_ms, self.mem_cap_mb, self.seg_mb = client_delay_r0, window_ms, mem_cap_mb, seg_mb
        self.p = {}
        os.makedirs(work, exist_ok=True)

    def routes(self):
        r = []
        for a in range(N):
            for b in range(N):
                if a != b:
                    r.append({'name': f'r{a}->r{b}', 'listen': f'127.0.0.1:{7300 + 10 * a + b}', 'target': f'127.0.0.1:{7100 + b}', 'delay_us': self.delay_us})
        for i in range(N):
            d = self.delay_us if (i != 0 or self.client_delay_r0) else 0
            r.append({'name': f'c->r{i}', 'listen': f'127.0.0.1:{7400 + i}', 'target': f'127.0.0.1:{7200 + i}', 'delay_us': d})
        return r

    def client_addrs(self):
        return ','.join([f'127.0.0.1:{7400}' if self.client_delay_r0 else '127.0.0.1:7200', '127.0.0.1:7401', '127.0.0.1:7402'])

    def direct_addrs(self):
        return '127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202'

    def spawn(self, name, args, cpus, env=None, rt=False):
        f = open(os.path.join(self.work, f'{name}.log'), 'ab')
        pre = ['taskset', '-c', cpus] + (['chrt', '-f', '20'] if rt else [])
        p = subprocess.Popen(pre + args, stdout=f, stderr=f, start_new_session=True, env=env or BASE_ENV)
        self.p[name] = p.pid
        return p.pid

    def replica_env(self, i):
        e = dict(BASE_ENV)
        if self.leader_ack:
            e['PQ_LEADER_ACK'] = '1'
        if self.keeper and i == 2:
            e['PQ_KEEPER'] = '1'
        return e

    def replica_args(self, i):
        peers = ','.join(f'{b}=127.0.0.1:{7300 + 10 * i + b}' for b in range(N) if b != i)
        return [PQ, 'replica', '--id', str(i), '--dir', os.path.join(self.work, f'r{i}'), '--peer-listen', f'127.0.0.1:{7100 + i}',
                '--client-listen', f'127.0.0.1:{7200 + i}', '--peers', peers, '--mem-cap-mb', str(self.mem_cap_mb),
                '--window-ms', str(self.window_ms), '--seg-mb', str(self.seg_mb),
                '--first-elect-delay-ms', '0' if i == 0 else '3000']

    def start_replica(self, i):
        self.spawn(f'r{i}', self.replica_args(i), CPU_WAITER if i == 0 else CPU_REMOTE, env=self.replica_env(i))
        wait_port(7200 + i)

    def up(self, fresh=True, flusher=True):
        self.down()
        if fresh:
            for i in range(N):
                subprocess.run(['rm', '-rf', os.path.join(self.work, f'r{i}')])
        json.dump(self.routes(), open(os.path.join(self.work, 'routes.json'), 'w'))
        self.stats_path = os.path.join(self.work, 'dproxy-stats.json')
        self.spawn('dproxy', [PQ, 'dproxy', '--routes', os.path.join(self.work, 'routes.json'), '--stats', self.stats_path], CPU_TOOLS)
        wait_port(7301)
        for i in range(N):
            self.start_replica(i)
        ld = self.leader(t=20)
        if flusher:
            self.spawn('flusher', [PQ, 'flusher', '--replicas', self.direct_addrs(), '--null', '1', '--age-ms', '2000', '--interval-ms', '200', '--hot-db', '0'], CPU_TOOLS)
        return ld

    def status(self, i=None, timeout=5):
        try:
            out = subprocess.run([PQ, 'status', '--replicas', self.direct_addrs()], capture_output=True, text=True, timeout=timeout).stdout
        except subprocess.TimeoutExpired:
            return {}
        st = {}
        for l in out.splitlines():
            try:
                s = json.loads(l)
            except Exception:
                continue
            if 'id' in s:
                st[s['id']] = s
        return st

    def leader(self, t=20):
        end = time.time() + t
        while time.time() < end:
            for i, s in self.status().items():
                if s.get('role') == 'Leader':
                    return i
            time.sleep(0.1)
        return -1

    def kill(self, name, sig=signal.SIGKILL):
        pid = self.p.get(name)
        if pid:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass

    def stats(self):
        try:
            return json.load(open(self.stats_path))
        except Exception:
            return {}

    def down(self):
        for k, v in list(self.p.items()):
            try:
                os.kill(v, signal.SIGCONT)
                os.kill(v, signal.SIGKILL)
            except Exception:
                pass
        self.p = {}
        subprocess.run(['pkill', '-9', '-f', 'pq (replica|dproxy|flusher)'], capture_output=True)
        time.sleep(0.3)


def proc_cpu_s(pid):
    """utime+stime of a process and all its threads, in seconds."""
    try:
        f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
        return (int(f[11]) + int(f[12])) / os.sysconf('SC_CLK_TCK')
    except Exception:
        return None


def core_times():
    """per core: (busy, total) jiffies."""
    out = {}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3].isdigit():
            v = [int(x) for x in l.split()[1:]]
            idle = v[3] + v[4]
            out[int(l.split()[0][3:])] = (sum(v) - idle, sum(v))
    return out


def disk_counters(dev='vda'):
    for l in open('/proc/diskstats'):
        f = l.split()
        if f[2] == dev:
            # reads completed, sectors read, writes completed, sectors written, flushes completed (field 20 if present)
            return {'reads': int(f[3]), 'read_bytes': int(f[5]) * 512, 'writes': int(f[7]), 'write_bytes': int(f[9]) * 512,
                    'io_ms': int(f[12]), 'flushes': int(f[18]) if len(f) > 18 else 0}
    return {}
