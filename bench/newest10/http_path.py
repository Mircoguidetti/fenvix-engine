"""newest10-trips, the coordinator's follow-up: the same reads over the real HTTP path, so a concurrency limit of the
client (object_store/reqwest, connection pool) or of the delay proxy cannot hide behind FENVIX_SIM_S3.

  server --store s3 (object_store's S3 client, as in the bench) -> s3delay (bench/strong-point/s3delay: +DELAY ms on the
  first byte of every response, per connection) -> FakeS3 (ctl-tests/fake_aws.py: path-style S3 in memory, threaded,
  SigV4 checked), loaded with the files of a store written by `repro.py load`.

MinIO cannot be fetched in this sandbox; FakeS3 stands in for it. FakeS3 holds one lock per request while it answers
(its responses go out one at a time; a local memcpy of the range, well under 1 ms for these sizes).

  http_path.py --dir D --s3delay BIN --out X.json [--delay 20] [--flags "..."] [--hold]
"""
from __future__ import annotations

import argparse
import hashlib
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(ROOT, "ctl-tests"))
import repro  # noqa: E402
import fake_aws  # noqa: E402

BUCKET, KEY, SECRET = "nx", "NEWEST10LOCAL", "newest10-local-secret"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--s3delay", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--delay", type=int, default=20)
    ap.add_argument("--flags", default="")
    ap.add_argument("--bin", default=repro.BIN)
    ap.add_argument("--tag", default="http")
    ap.add_argument("--hold", action="store_true")
    ap.add_argument("--warm", type=int, default=300)
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--seed", type=int, default=5)
    a = ap.parse_args()

    reg = fake_aws.Registry()
    reg.add(KEY, SECRET, None, time.time(), time.time() + 10 * 86400)
    s3 = fake_aws.FakeS3(reg)
    pristine = os.path.join(a.dir, "store.pristine")
    n, b = 0, 0
    for dp, _, fs in os.walk(pristine):
        for f in fs:
            p = os.path.join(dp, f)
            k = os.path.relpath(p, pristine)
            data = open(p, "rb").read()
            s3.objects[(BUCKET, k)] = (data, '"%s"' % hashlib.md5(data).hexdigest(), os.path.getmtime(p))
            n, b = n + 1, b + len(data)
    repro.log(f"FakeS3 at {s3.url}: {n} objects, {b >> 20} MiB")
    port = free_port()
    prox = subprocess.Popen([a.s3delay, f"127.0.0.1:{port}", s3.url.removeprefix("http://"), str(a.delay)],
                            stdout=open(os.path.join(a.dir, "s3delay.log"), "ab"), stderr=subprocess.STDOUT)
    time.sleep(0.5)
    repro.fresh_store = lambda d: [__import__("shutil").rmtree(os.path.join("/tmp", x), ignore_errors=True)
                                   for x in os.listdir("/tmp") if x.startswith("fenvix-box-floor-")]
    os.environ.update({"AWS_ACCESS_KEY_ID": KEY, "AWS_SECRET_ACCESS_KEY": SECRET, "AWS_REGION": "us-east-1"})
    flags = f"--store s3 --bucket {BUCKET} --endpoint http://127.0.0.1:{port} --region us-east-1 {a.flags}"
    orig_start = repro.Srv.start

    def start(self):
        # repro.Srv passes --store local --local-dir: replaced by the S3 flags (the last --store wins in clap? no: drop them)
        self.delay = 0
        return orig_start(self)

    repro.Srv.start = start
    orig_init = repro.Srv.__init__

    def init(self, d, **kw):
        orig_init(self, d, **kw)
        self.s3_flags = flags.split()

    repro.Srv.__init__ = init
    real_popen = subprocess.Popen

    def popen(cmd, *args, **kw):
        if cmd and cmd[0] == a.bin and "--local-dir" in cmd:
            i = cmd.index("--store")
            cmd = cmd[:i] + cmd[i + 4:]          # drop --store local --local-dir X
            cmd += flags.split()
        return real_popen(cmd, *args, **kw)

    subprocess.Popen = popen
    try:
        ns = argparse.Namespace(dir=a.dir, out=a.out, bin=a.bin, delay=0, flags="", tag=a.tag, warm=a.warm, n=a.n,
                                limit=a.limit, seed=a.seed, hold=a.hold, trace=False, pretraffic=0, pre_settle=0)
        repro.cmd_run(ns)
    finally:
        subprocess.Popen = real_popen
        prox.terminate()
        s3.close()
        if s3.unsupported:
            repro.log("FakeS3 unsupported:", s3.unsupported[:5])


if __name__ == "__main__":
    main()
