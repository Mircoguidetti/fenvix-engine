"""Helpers for the control-layer tests: MinIO (one bucket per test module), a mongo-server process that can be
killed with SIGKILL, the admin HTTP client, and independent bucket listing through boto3 (not through the server)."""
import json
import os
import random
import signal
import subprocess
import time
import urllib.request
import urllib.error

import boto3
from botocore.config import Config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release", "mongo-server")
OUT = os.path.join(ROOT, "ctl-tests", "out")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
TOKEN = "test-token-0123456789abcdef"

os.makedirs(OUT, exist_ok=True)


def s3():
    return boto3.client("s3", endpoint_url=MINIO, aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                        region_name="us-east-1", config=Config(retries={"max_attempts": 3}))


def make_bucket(tag):
    name = f"ctl-{tag}-{random.randrange(10**6):06d}".lower().replace("_", "-")
    s3().create_bucket(Bucket=name)
    return name


def list_prefix(bucket, prefix):
    """Every object under a prefix, listed directly from MinIO: [(key, size)]."""
    out = []
    pg = s3().get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out.append((o["Key"], o["Size"]))
    return out


def prefix_bytes(bucket, prefix):
    return sum(sz for _, sz in list_prefix(bucket, prefix))


def free_port():
    while True:
        p = random.randrange(20000, 40000)
        try:
            import socket
            s = socket.socket()
            s.bind(("127.0.0.1", p))
            s.close()
            return p
        except OSError:
            continue


class Server:
    def __init__(self, bucket, tag="srv", idle=60, extra=(), holder=None, port=None, admin_port=None, env=None):
        self.bucket, self.tag, self.idle, self.extra = bucket, tag, idle, list(extra)
        self.port = port or free_port()
        self.admin_port = admin_port or free_port()
        self.holder = holder or f"{tag}-{self.port}"
        self.proc = None
        self.log_path = os.path.join(OUT, f"{tag}-{self.port}.log")
        self.env = env or {}

    def start(self, wait=True):
        cmd = [BIN, "--listen", f"127.0.0.1:{self.port}", "--admin-listen", f"127.0.0.1:{self.admin_port}",
               "--idle-secs", str(self.idle), "--bucket", self.bucket, "--endpoint", MINIO, "--holder", self.holder,
               "--lease-ttl-secs", "6", "--no-ttl"] + self.extra
        env = dict(os.environ, MONGO_ADMIN_TOKEN=TOKEN, **self.env)
        self.log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=self.log, env=env)
        if wait:
            self.wait_ready()
        return self

    def wait_ready(self, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited with {self.proc.returncode}; see {self.log_path}")
            try:
                self.admin.get("/v1/health")
                import socket
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    @property
    def admin(self):
        return Admin(f"http://127.0.0.1:{self.admin_port}")

    @property
    def uri(self):
        return f"mongodb://127.0.0.1:{self.port}/?directConnection=true"

    def client(self, **kw):
        from pymongo import MongoClient
        return MongoClient(self.uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=120000, **kw)

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def term(self, timeout=60):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def restart(self):
        self.term()
        self.proc = None
        return self.start()

    def log_text(self):
        with open(self.log_path, "rb") as f:
            return f.read().decode("utf8", "replace")


class Admin:
    def __init__(self, base, token=TOKEN):
        self.base, self.token = base, token

    def call(self, method, path, body=None, token="default", raw=False, headers=None, timeout=300):
        tok = self.token if token == "default" else token
        h = dict(headers or {})
        if tok is not None:
            h["Authorization"] = f"Bearer {tok}"
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                payload = r.read()
                if raw:
                    return r.status, payload, dict(r.headers)
                return r.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as e:
            payload = e.read()
            if raw:
                return e.code, payload, dict(e.headers)
            try:
                return e.code, json.loads(payload)
            except Exception:
                return e.code, {"raw": payload.decode("utf8", "replace")}

    def get(self, path, **kw):
        return self.call("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.call("POST", path, body if body is not None else {}, **kw)

    def put(self, path, body=None, **kw):
        return self.call("PUT", path, body if body is not None else {}, **kw)

    def delete(self, path, **kw):
        return self.call("DELETE", path, **kw)


def fill(client, db, coll, n, start=0, batch=2000, extra=None):
    docs = []
    c = client[db][coll]
    for i in range(start, start + n):
        d = {"_id": i, "n": i, "grp": i % 10, "name": f"user{i}", "pad": "x" * 100}
        if extra:
            d.update(extra)
        docs.append(d)
        if len(docs) >= batch:
            c.insert_many(docs)
            docs = []
    if docs:
        c.insert_many(docs)


def dump_db(client, db):
    """Every document of every collection of a database, as {coll: {_id: doc}} (for equality checks)."""
    out = {}
    for cn in sorted(client[db].list_collection_names()):
        out[cn] = {repr(d["_id"]): d for d in client[db][cn].find({})}
    return out


def wait_until(fn, timeout=30, every=0.2, what="condition"):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            last = fn()
            if last:
                return last
        except Exception as e:  # noqa
            last = e
        time.sleep(every)
    raise AssertionError(f"timed out waiting for {what}: {last}")
