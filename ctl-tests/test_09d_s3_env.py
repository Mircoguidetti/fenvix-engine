"""Correction 3: S3 credentials, region and endpoint come from the standard AWS environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_SESSION_TOKEN, AWS_REGION / AWS_DEFAULT_REGION, AWS_ENDPOINT, or the instance role through the metadata service) and from the
flags --region / --endpoint. Nothing is built into the engine: wrong credentials do not work, a wrong region does not work, and no
region at all is an error that says so. The tests for MinIO set everything explicitly (ctl-tests/lib.py)."""
import os
import subprocess
import time

import pytest

from lib import Server, make_bucket, MINIO, BIN, free_port, OUT

GOOD = {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1"}


def run_server(bucket, env, extra=(), seconds=6):
    """Start the engine with exactly this environment for the AWS variables (nothing inherited) and report (exited_with, log)."""
    port = free_port()
    clean = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") and not k.startswith("MONGO_ADMIN")}
    clean.update(env)
    log = os.path.join(OUT, f"s3env-{port}.log")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--idle-secs", "30", "--bucket", bucket, "--no-ttl", "--holder", f"s3env-{port}", *extra],
                         stdout=open(log, "wb"), stderr=subprocess.STDOUT, env=clean)
    return p, port, log


def works(port, timeout=15):
    """Does an insert go through (the engine writes to S3)?"""
    from pymongo import MongoClient
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=8000, socketTimeoutMS=timeout * 1000)
    try:
        c.s3env.t.insert_one({"a": 1})
        return c.s3env.t.count_documents({}) == 1
    except Exception:
        return False


def stop(p):
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(20)
        except subprocess.TimeoutExpired:
            p.kill()


def wait_listening(p, port, secs=15):
    import socket
    t0 = time.time()
    while time.time() - t0 < secs:
        if p.poll() is not None:
            return False
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


def test_credentials_region_and_endpoint_from_the_environment_work():
    bucket = make_bucket("s3env-ok")
    p, port, log = run_server(bucket, {**GOOD, "AWS_ENDPOINT": MINIO, "AWS_ALLOW_HTTP": "true"})
    try:
        assert wait_listening(p, port), open(log).read()[-500:]
        assert works(port)
    finally:
        stop(p)


def test_credentials_region_and_endpoint_from_flags_and_environment_mix():
    bucket = make_bucket("s3env-flags")
    p, port, log = run_server(bucket, {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin"}, extra=["--endpoint", MINIO, "--region", "us-east-1"])
    try:
        assert wait_listening(p, port), open(log).read()[-500:]
        assert works(port)
        # the secret is in the environment only: not on the command line, not in the log
        cmdline = open(f"/proc/{p.pid}/cmdline", "rb").read().decode(errors="replace")
        assert "minioadmin" not in cmdline, "the secret key is on the command line"
    finally:
        stop(p)
    assert "minioadmin" not in open(log).read()


def test_wrong_credentials_do_not_work():
    bucket = make_bucket("s3env-bad")
    p, port, log = run_server(bucket, {"AWS_ACCESS_KEY_ID": "not-the-key", "AWS_SECRET_ACCESS_KEY": "not-the-secret", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": MINIO, "AWS_ALLOW_HTTP": "true"})
    try:
        listening = wait_listening(p, port)
        assert not (listening and works(port)), "the engine wrote to S3 with credentials that are not valid: it must have credentials of its own built in"
    finally:
        stop(p)


class Recorder:
    """A fake S3 endpoint that records the signature scope of every request and answers 403: it shows which access key, region and
    session token the engine signs with, without a real S3 and without any cost."""

    def __init__(self):
        import http.server
        import threading
        self.seen = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def _any(self):
                outer.seen.append({k.lower(): v for k, v in self.headers.items()})
                n = int(self.headers.get("content-length") or 0)
                if n:
                    self.rfile.read(n)
                body = b"<?xml version='1.0'?><Error><Code>AccessDenied</Code><Message>no</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_PUT = do_POST = do_HEAD = do_DELETE = _any

            def log_message(self, *a):
                pass

        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def scopes(self):
        out = set()
        for h in self.seen:
            a = h.get("authorization", "")
            if "Credential=" in a:
                key, date, region, svc, _ = a.split("Credential=")[1].split(",")[0].split("/")
                out.add((key, region, svc))
        return out

    def close(self):
        self.srv.shutdown()


def poke(port):
    from pymongo import MongoClient
    c = MongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=5000, socketTimeoutMS=8000)
    try:
        c.pokedb.t.insert_one({"a": 1})
    except Exception:
        pass


def test_region_key_and_token_are_the_ones_given():
    rec = Recorder()
    try:
        env = {"AWS_ACCESS_KEY_ID": "AKIAGIVENKEY000000", "AWS_SECRET_ACCESS_KEY": "given-secret-value", "AWS_SESSION_TOKEN": "given-session-token", "AWS_REGION": "eu-west-3"}
        p, port, log = run_server("irrelevant-bucket", env, extra=["--endpoint", f"http://127.0.0.1:{rec.port}"])
        try:
            assert wait_listening(p, port), open(log).read()[-400:]
            poke(port)
        finally:
            stop(p)
        scopes = rec.scopes()
        assert scopes, "the engine made no signed request to the endpoint it was given"
        assert {(k, r) for k, r, _ in scopes} == {("AKIAGIVENKEY000000", "eu-west-3")}, scopes      # environment region and key
        assert all(h.get("x-amz-security-token") == "given-session-token" for h in rec.seen if "authorization" in h)
        # the flag wins over the environment
        rec.seen.clear()
        p, port, log = run_server("irrelevant-bucket", env, extra=["--endpoint", f"http://127.0.0.1:{rec.port}", "--region", "ap-south-1"])
        try:
            assert wait_listening(p, port), open(log).read()[-400:]
            poke(port)
        finally:
            stop(p)
        assert {r for _, r, _ in rec.scopes()} == {"ap-south-1"}, rec.scopes()
        text = open(log).read()
        assert "given-secret-value" not in text and "given-session-token" not in text and "AKIAGIVENKEY000000" not in text
    finally:
        rec.close()


def test_no_region_is_a_clear_error():
    bucket = make_bucket("s3env-noregion")
    env = {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_ENDPOINT": MINIO, "AWS_ALLOW_HTTP": "true"}
    p, port, log = run_server(bucket, env)
    try:
        try:
            code = p.wait(15)
        except subprocess.TimeoutExpired:
            pytest.fail("the engine started without any region: it has a built-in one")
        text = open(log).read()
        assert code != 0 and "region" in text.lower(), text[-400:]
    finally:
        stop(p)


def test_local_store_needs_no_s3_settings():
    port = free_port()
    clean = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    d = os.path.join(OUT, f"s3env-local-{port}")
    p = subprocess.Popen([BIN, "--listen", f"127.0.0.1:{port}", "--store", "local", "--local-dir", d, "--no-leases", "--no-ttl", "--holder", f"loc{port}"],
                         stdout=open(os.path.join(OUT, f"s3env-local-{port}.log"), "wb"), stderr=subprocess.STDOUT, env=clean)
    try:
        assert wait_listening(p, port)
    finally:
        stop(p)
