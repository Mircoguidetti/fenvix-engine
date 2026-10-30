"""Start / stop the engine for the round3 runs (pinned with taskset to ENGINE_CPUS when available), create buckets,
prefill databases."""
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa

ENGINE_CPUS = os.environ.get("ROUND3_ENGINE_CPUS", "0,1")


def make_bucket(name):
    """Fresh MinIO bucket (deleted first if it exists)."""
    import boto3
    s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                      region_name="us-east-1")
    try:
        while True:
            r = s3.list_objects_v2(Bucket=name)
            keys = [{"Key": o["Key"]} for o in r.get("Contents", [])]
            if not keys:
                break
            s3.delete_objects(Bucket=name, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=name)
    except Exception:
        pass
    s3.create_bucket(Bucket=name)
    return name


def bucket_bytes(name, prefix=""):
    if REAL_S3:
        import boto3
        s3 = boto3.client("s3")
    else:
        import boto3
        s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                          region_name="us-east-1")
    total, n = 0, 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name, Prefix=prefix):
        for o in page.get("Contents", []):
            total += o["Size"]
            n += 1
    return total, n


class Engine:
    def __init__(self, tag, bucket, flags, port=int(os.environ.get("ROUND3_PORT", "27200")), admin_port=int(os.environ.get("ROUND3_PORT", "27200")) + 1, holder="round3", bin=None, env=None, pin=True):
        self.tag, self.bucket, self.flags, self.port, self.admin_port, self.holder = tag, bucket, flags, port, admin_port, holder
        self.bin = bin or ENGINE_BIN
        self.env = env or {}
        self.pin = pin and shutil.which("taskset") is not None and os.cpu_count() >= 4
        self.p = None
        self.log_path = os.path.join(WORK, "logs", f"engine-{tag}.log")

    def cmd(self):
        c = [self.bin, "--listen", f"127.0.0.1:{self.port}", "--admin-listen", f"127.0.0.1:{self.admin_port}", "--holder", self.holder,
             "--store", "s3", "--bucket", self.bucket] + self.flags
        if not REAL_S3:
            c += ["--endpoint", "http://127.0.0.1:9000", "--region", "us-east-1"]
        return (["taskset", "-c", ENGINE_CPUS] if self.pin else []) + c

    def start(self):
        # a process still listening on the port (a run that was stopped half way) would answer instead of this engine
        for p in (self.port, self.admin_port):
            busy = True
            try:
                socket.create_connection(("127.0.0.1", p), timeout=1).close()
            except OSError:
                busy = False
            if busy:
                raise RuntimeError(f"port {p} is already in use by another process: stop it first")
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        log = open(self.log_path, "ab")
        env = dict(os.environ, MONGO_ADMIN_TOKEN=ADMIN_TOKEN, RUST_LOG=os.environ.get("RUST_LOG", "warn"), **self.env)
        self.p = subprocess.Popen(self.cmd(), stdout=log, stderr=log, env=env)
        t0 = time.time()
        while time.time() - t0 < 120:
            if self.p.poll() is not None:
                raise RuntimeError(f"engine exited {self.p.returncode}, see {self.log_path}")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return self
            except OSError:
                time.sleep(0.1)
        # never leave an engine behind: one still recovering kept writing to the bucket during the clean-up (S3 run 4)
        self.p.kill()
        self.p.wait()
        raise RuntimeError("engine did not start")

    @property
    def pid(self):
        return self.p.pid if self.p else None

    def stop(self, timeout=300):
        if self.p and self.p.poll() is None:
            self.p.send_signal(signal.SIGTERM)
            try:
                self.p.wait(timeout)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
        return self.p.returncode if self.p else None

    def kill9(self):
        if self.p and self.p.poll() is None:
            self.p.kill()
            self.p.wait()

    def client(self):
        from pymongo import MongoClient
        return MongoClient("127.0.0.1", self.port, directConnection=True, serverSelectionTimeoutMS=60000, socketTimeoutMS=300000)

    def stats(self):
        """Process-wide engine counters (tenantStats; needs no auth here)."""
        try:
            return self.client().admin.command("tenantStats")
        except Exception as e:
            return {"error": str(e)}

    def admin(self, method, path, body=None):
        import json as _j
        import urllib.request
        req = urllib.request.Request(f"http://127.0.0.1:{self.admin_port}{path}", method=method,
                                     data=_j.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {ADMIN_TOKEN}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, _j.loads(r.read() or b"null")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()[:300]


def prefill(eng, dbs, n=200, parallel=16):
    """n documents of ~1 KB in `items` of every database (ids 0..n-1), one insert_many each."""
    from concurrent.futures import ThreadPoolExecutor
    c = eng.client()

    def one(d):
        c[d].items.insert_many([{"_id": i, "k": i % 50, "pad": "x" * 900} for i in range(n)])
    with ThreadPoolExecutor(parallel) as ex:
        list(ex.map(one, dbs))
