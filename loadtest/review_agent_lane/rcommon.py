"""Shared pieces of the review tests of the agent lane (R-review-agent-lane, 2026-11-07). $0, local only.

A plain launcher that adds no agent-lane flag, so the same test runs on `integration` (which has none) and on
`agent-lane`. MONGO_SERVER_BIN picks the binary (default target/release/mongo-server).
"""
import os, shutil, signal, socket, subprocess, sys, time
from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, 'loadtest', 'agent_lane'))
ENGINE = os.environ.get('MONGO_SERVER_BIN', os.path.join(ROOT, 'target', 'release', 'mongo-server'))


def kill_port(port):
    needle = f'--listen 127.0.0.1:{port}'
    for pid in os.listdir('/proc'):
        if pid.isdigit():
            try:
                cmd = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\0', b' ').decode()
            except OSError:
                continue
            if needle in cmd:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except OSError:
                    pass
    time.sleep(0.3)


class Srv:
    def __init__(self, work, name, port, args, env=None):
        self.work, self.name, self.port, self.args, self.env, self.p = work, name, port, list(args), dict(env or {}), None
        self.logfile = os.path.join(work, f'engine-{name}.log')

    def start(self, extra=()):
        kill_port(self.port)
        d = os.path.join(self.work, f'cwd-{self.name}')
        os.makedirs(d, exist_ok=True)
        cmd = [ENGINE, '--listen', f'127.0.0.1:{self.port}', '--no-ttl', '--replica-set', 'rs0', '--advertise', f'localhost:{self.port}',
               '--holder', f'{self.name}-{time.time_ns()}', '--idle-secs', '600'] + self.args + list(extra)
        self.p = subprocess.Popen(cmd, stdout=open(self.logfile, 'ab'), stderr=subprocess.STDOUT, env=dict(os.environ, **self.env), cwd=d)
        end = time.time() + 120
        while time.time() < end:
            if self.p.poll() is not None:
                raise RuntimeError(f'{self.name} exited, see {self.logfile}')
            try:
                socket.create_connection(('127.0.0.1', self.port), 0.3).close()
                return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f'{self.name} did not start')

    def client(self, **kw):
        a = dict(retryWrites=False, serverSelectionTimeoutMS=5000, socketTimeoutMS=60000, connectTimeoutMS=2000)
        a.update(kw)
        return MongoClient(f'mongodb://127.0.0.1:{self.port}/?directConnection=true', **a)

    def kill9(self):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGKILL)
            self.p.wait()


def retry(f, t=60):
    from pymongo.errors import PyMongoError
    end = time.time() + t
    while True:
        try:
            return f()
        except PyMongoError:
            if time.time() > end:
                raise
            time.sleep(0.2)


def fresh(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    return path


def result(name, ok, detail):
    print(f'RESULT {name} {detail} ->', 'PASS' if ok else 'FAIL', flush=True)
    sys.exit(0 if ok else 1)


def no_lane_flag_on_base():
    """REVIEW_BASE=1: the binary is `integration` (no --agent-lane flag): common.Engine's flag is dropped from its command."""
    if os.environ.get('REVIEW_BASE') != '1':
        return
    import common
    real = subprocess.Popen

    def popen(cmd, *a, **k):
        if isinstance(cmd, list) and '--agent-lane' in cmd:
            i = cmd.index('--agent-lane')
            cmd = cmd[:i] + cmd[i + 2:]
        return real(cmd, *a, **k)
    common.subprocess = type('S', (), {'Popen': staticmethod(popen), 'run': staticmethod(subprocess.run), 'TimeoutExpired': subprocess.TimeoutExpired})
