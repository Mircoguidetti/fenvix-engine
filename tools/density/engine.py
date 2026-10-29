"""Start / stop the engine for the density runs, with the flags of the brief, pinned with taskset to ENGINE_CPUS."""
import os
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *  # noqa

FLAGS = ["--auth", "--tls-mode", "requireTLS", "--tls-certificate-key-file", CERT,
         "--store", "s3", "--bucket", BUCKET, "--endpoint", "http://127.0.0.1:9000", "--region", "us-east-1",
         "--idle-secs", "60", "--retention-window-secs", "604800", "--wal-gc-mode", "bulk"]


def cmdline():
    return ["taskset", "-c", ENGINE_CPUS, ENGINE_BIN, "--listen", f"127.0.0.1:{PORT}", "--admin-listen", f"127.0.0.1:{ADMIN_PORT}",
            "--holder", "density"] + FLAGS


def start(tag):
    os.makedirs(os.path.join(WORK, "logs"), exist_ok=True)
    log = open(os.path.join(WORK, "logs", f"engine-{tag}.log"), "ab")
    env = dict(os.environ, MONGO_ADMIN_TOKEN=ADMIN_TOKEN, RUST_LOG=os.environ.get("RUST_LOG", "warn"))
    p = subprocess.Popen(cmdline(), stdout=log, stderr=log, env=env)
    t0 = time.time()
    while time.time() - t0 < 60:
        if p.poll() is not None:
            raise RuntimeError(f"engine exited {p.returncode}, see logs/engine-{tag}.log")
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
            socket.create_connection(("127.0.0.1", ADMIN_PORT), timeout=1).close()
            return p
        except OSError:
            time.sleep(0.2)
    raise RuntimeError("engine did not start")


def stop(p, timeout=120):
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    return p.returncode
