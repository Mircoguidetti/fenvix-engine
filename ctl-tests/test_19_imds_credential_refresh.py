"""Instance-role credentials (IMDSv2) are renewed by the running engine, without a restart.

In production the engine runs on EC2 with no AWS_ACCESS_KEY_ID: the S3 client (object_store, `AmazonS3Builder::from_env()` in
mongo-server/src/main.rs) then takes the instance role's credentials from the metadata service, and those expire. These tests run the
real engine binary, built exactly as in production (`--store s3`, credentials only from the environment), against a fake IMDSv2 and a
fake S3 (ctl-tests/fake_aws.py; no AWS, no MinIO). The fake IMDS hands out new credentials at every request; the fake S3 refuses,
as S3 does, an unknown key, an expired key (403 ExpiredToken), a token that is not the key's, and a wrong SigV4 signature.

* test_short_expiry_20s: credentials expire 20 s after they are issued. object_store refreshes a credential when less than
  5 minutes are left (its TokenCache min_ttl), so with 20 s it fetches again at every request (reusing one only within 100 ms).
* test_renewal_window: credentials expire 320 s after they are issued, so object_store keeps each one for 20 s and then fetches
  the next. This is the caching, seen from outside: one key for ~20 s, then the next one, never an expired one.
* test_late_rotation: as on EC2, the metadata service returns the same credentials until they are rotated, here 5 s after
  object_store starts asking for new ones: the old key stays valid and is used until the new one appears.
* test_control_static_credentials_expire: the negative control. Static credentials from the environment (as in aws-test3) that the
  fake S3 lets expire after 12 s: the engine keeps using the same key and S3 refuses it. It shows the checks above can fail."""
import os
import subprocess
import threading
import time

import pytest
from pymongo import MongoClient

from fake_aws import FakeImds, FakeS3, Registry
from lib import BIN, OUT, free_port

BUCKET = "imds-test"


def engine_env(extra):
    """Only the AWS variables given here: nothing inherited (lib.py sets the MinIO keys in os.environ)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") and not k.startswith("MONGO_ADMIN")}
    env.update(extra)
    return env


class Engine:
    def __init__(self, s3, env, tag):
        self.port = free_port()
        self.log_path = os.path.join(OUT, f"{tag}-{self.port}.log")
        cmd = [BIN, "--listen", f"127.0.0.1:{self.port}", "--idle-secs", "600", "--bucket", BUCKET, "--endpoint", s3.url,
               "--region", "us-east-1", "--no-ttl", "--holder", f"{tag}-{self.port}", "--lease-ttl-secs", "6"]
        self.proc = subprocess.Popen(cmd, stdout=open(self.log_path, "wb"), stderr=subprocess.STDOUT, env=engine_env(env))
        self.pid = self.proc.pid
        import socket
        t0 = time.time()
        while time.time() - t0 < 30:
            assert self.proc.poll() is None, f"engine exited with {self.proc.returncode}: {open(self.log_path).read()[-2000:]}"
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("engine did not start")

    def client(self):
        return MongoClient(f"mongodb://127.0.0.1:{self.port}/?directConnection=true", serverSelectionTimeoutMS=8000,
                           socketTimeoutMS=15000)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def drive(eng, seconds, new_db_every=10):
    """Write and read for `seconds`: one insert + one read every 0.25 s, and a database never seen before every `new_db_every` s
    (its open reads S3: list, get, head). Returns (acknowledged writes, failures as (time, error))."""
    c = eng.client()
    t0, acked, fails, n, db = time.time(), 0, [], 0, "credrefresh0"
    while time.time() - t0 < seconds:
        if int((time.time() - t0) // new_db_every) != int(db[len("credrefresh"):]):
            db = f"credrefresh{int((time.time() - t0) // new_db_every)}"
        try:
            c[db].t.insert_one({"_id": n, "at": time.time()})
            assert c[db].t.find_one({"_id": n}) is not None
            acked += 1
        except Exception as e:
            fails.append((time.time(), repr(e)[:200]))
        n += 1
        time.sleep(0.25)
    c.close()
    return acked, fails


def summary(s3, imds, t_start):
    """Per key used on S3: first and last use (seconds from start), requests, refused requests."""
    keys = {}
    for r in s3.log:
        k = keys.setdefault(r["key_id"], {"first": r["t"] - t_start, "last": 0, "n": 0, "refused": 0})
        k["last"] = r["t"] - t_start
        k["n"] += 1
        k["refused"] += r["status"] == 403
    issued = {key: (t - t_start, exp - t_start) for t, key, exp in imds.issued} if imds else {}
    return keys, issued


def write_evidence(name, text):
    path = os.path.join(OUT, f"imds_{name}.txt")
    with open(path, "w") as f:
        f.write(text)
    print(text)


@pytest.fixture
def aws():
    reg = Registry()
    s3 = FakeS3(reg)
    yield reg, s3
    s3.close()


def run_with_imds(aws, ttl, seconds, name, rotate_after=None):
    reg, s3 = aws
    imds = FakeImds(reg, ttl=ttl, rotate_after=rotate_after)
    eng = Engine(s3, {"AWS_METADATA_ENDPOINT": imds.url, "AWS_ALLOW_HTTP": "true"}, f"imds-{name}")
    t_start = time.time()
    try:
        acked, fails = drive(eng, seconds)
        alive = eng.proc.poll() is None
    finally:
        eng.stop()
        imds.close()
    keys, issued = summary(s3, imds, t_start)
    refused = [r for r in s3.log if r["status"] == 403]
    lines = [f"{name}: ttl {ttl}s, rotate_after {rotate_after}, {seconds}s of traffic, engine pid {eng.pid} alive at the end: {alive}",
             f"writes acknowledged {acked}, failed {len(fails)}; S3 requests {len(s3.log)}, refused {len(refused)}, "
             f"not supported by the fake {len(s3.unsupported)}",
             f"IMDS: {sum(1 for x in imds.log if x[1] == 'PUT')} token PUTs, {sum(1 for x in imds.log if x[1] == 'GET')} GETs "
             f"({sum(1 for x in imds.log if x[3] is False)} without a valid token), {len(imds.issued)} credentials issued",
             "key: issued at / expires at (s from start) | used on S3 from .. to | requests | refused"]
    for k in sorted(keys, key=lambda k: keys[k]["first"]):
        i = issued.get(k, (None, None))
        lines.append(f"  {k}: {i[0] if i[0] is None else round(i[0], 2)} / {i[1] if i[1] is None else round(i[1], 2)} | "
                     f"{keys[k]['first']:.2f} .. {keys[k]['last']:.2f} | {keys[k]['n']} | {keys[k]['refused']}")
    lines += [f"  unsupported: {u}" for u in s3.unsupported[:10]] + [f"  failure: {f}" for f in fails[:10]]
    write_evidence(name, "\n".join(lines) + "\n")
    return dict(acked=acked, fails=fails, alive=alive, keys=keys, issued=issued, refused=refused, s3=s3, imds=imds, t_start=t_start)


def common_checks(r, seconds):
    assert r["alive"], "the engine exited"
    assert not r["s3"].unsupported, r["s3"].unsupported
    assert r["fails"] == [], r["fails"][:5]
    assert r["acked"] >= seconds * 2, r["acked"]
    assert r["refused"] == [], r["refused"][:5]
    assert not [x for x in r["imds"].log if x[3] is False], "a metadata request without the IMDSv2 token"
    # every key used on S3 was issued by the IMDS, and every request was made before that key's expiry (the fake S3 refuses
    # the others, so `refused == []` already says it; checked again here from the log)
    for row in r["s3"].log:
        exp = r["issued"][row["key_id"]][1] + r["t_start"]
        assert row["t"] < exp, row
    # the last minutes of traffic use keys issued after the start: the engine follows the renewal
    late = [row for row in r["s3"].log if row["t"] - r["t_start"] > seconds - 5]
    assert late, "no S3 traffic at the end"
    first_key = min(r["keys"], key=lambda k: r["keys"][k]["first"])
    assert all(row["key_id"] != first_key for row in late), "still the first credential at the end"


def test_short_expiry_20s(aws):
    seconds = 45
    r = run_with_imds(aws, ttl=20, seconds=seconds, name="short_expiry_20s")
    common_checks(r, seconds)
    # requests after the first key's expiry (20 s) went out with other, newer keys
    assert len(r["keys"]) >= 3
    first_exp = min(e for _, e in r["issued"].values())
    after = {row["key_id"] for row in r["s3"].log if row["t"] - r["t_start"] > first_exp + 1}
    assert after and min(r["issued"][k][0] for k in after) > first_exp - 20


def test_renewal_window(aws):
    seconds = 50
    r = run_with_imds(aws, ttl=320, seconds=seconds, name="renewal_window")
    common_checks(r, seconds)
    keys = sorted(r["keys"].items(), key=lambda kv: kv[1]["first"])
    # 50 s with a 20 s window (320 s - object_store's 300 s margin): 3 keys (0-20, 20-40, 40-50), at most one more if a
    # renewal falls exactly on an edge
    assert 3 <= len(keys) <= 4, [k for k, _ in keys]
    # caching: each key except the last one is used for most of its 20 s window and is replaced within ~1 s of the window end
    for k, v in keys[:-1]:
        issued_at = r["issued"][k][0]
        assert v["last"] - v["first"] > 15, (k, v)
        assert v["last"] - issued_at < 21.5, (k, v, issued_at)
    # many requests per key: not one fetch per request
    assert len(r["imds"].issued) <= len(keys) + 1
    assert all(v["n"] > 50 for _, v in keys[:-1])


def test_late_rotation(aws):
    """As on EC2: the metadata service returns the SAME credentials until AWS rotates them. Here they expire 320 s after issue and
    are rotated only when 25 s old, i.e. 5 s after object_store wants to renew (when less than 300 s are left). In those 5 s the
    engine asks again (at most every 100 ms, object_store's fetch backoff) and gets the same key, which is still valid; then it
    takes the new one. Nothing is refused."""
    seconds = 40
    r = run_with_imds(aws, ttl=320, seconds=seconds, name="late_rotation", rotate_after=25)
    common_checks(r, seconds)
    keys = sorted(r["keys"].items(), key=lambda kv: kv[1]["first"])
    assert len(keys) == 2, [k for k, _ in keys]
    k1, v1 = keys[0]
    assert 24 < v1["last"] - r["issued"][k1][0] < 26.5, v1      # kept until the rotation, not dropped at 20 s
    gets = [x for x in r["imds"].log if x[1] == "GET" and x[2].endswith(FakeImds.ROLE)]
    in_gap = [x for x in gets if 20.5 < x[0] - r["t_start"] < 24.5]
    assert in_gap, "object_store did not ask again inside its 5-minute margin"
    assert len(in_gap) <= 4 / 0.1 + 5, len(in_gap)                  # bounded by the 100 ms backoff


def test_control_static_credentials_expire(aws):
    """Negative control: credentials from the environment never change. When the fake S3 makes them expire, the engine is
    refused and keeps the same key (the behavior aws-test3 showed with real S3 and fixed credentials)."""
    reg, s3 = aws
    expires = time.time() + 12
    reg.add("FAKESTATIC01", "fake-static-secret-" + "y" * 20, "fake-static-token", time.time(), expires)
    eng = Engine(s3, {"AWS_ACCESS_KEY_ID": "FAKESTATIC01", "AWS_SECRET_ACCESS_KEY": "fake-static-secret-" + "y" * 20,
                      "AWS_SESSION_TOKEN": "fake-static-token", "AWS_ALLOW_HTTP": "true"}, "imds-control")
    t_start = time.time()
    try:
        acked, fails = drive(eng, 20)
    finally:
        eng.stop()
    keys, _ = summary(s3, None, t_start)
    expired = [x for x in s3.log if x["reason"] == "ExpiredToken"]
    write_evidence("control_static", f"control: static key, expires 12 s after start; writes acknowledged {acked}, failed {len(fails)}; "
                   f"keys used {sorted(keys)}; requests refused ExpiredToken {len(expired)}\n")
    assert acked > 0                       # it worked before the expiry
    assert set(keys) == {"FAKESTATIC01"}   # never another key
    assert expired, "the fake S3 did not refuse the expired key: the check would not catch a missing renewal"
    assert fails, "writes kept succeeding after the expiry"
