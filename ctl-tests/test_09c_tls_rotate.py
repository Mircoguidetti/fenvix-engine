"""Correction 5: rotateCertificates must reload the certificate files for real (new connections see the new certificate, open
connections keep working), refuse with an error when the new files are unusable (and keep the old certificate), and must never
answer ok without having done anything on a server that has TLS on."""
import hashlib
import os
import shutil
import socket
import ssl
import subprocess
import tempfile

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket


def make_pem(path, cn="localhost"):
    key = path + ".key"
    crt = path + ".crt"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", crt, "-days", "2", "-subj", f"/CN={cn}",
                    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, capture_output=True)
    with open(path, "wb") as out:
        out.write(open(crt, "rb").read() + open(key, "rb").read())
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert(open(crt).read())).hexdigest()


def served_fingerprint(port):
    """SHA-256 of the certificate a new TLS connection to the port is shown."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        with ctx.wrap_socket(s) as t:
            return hashlib.sha256(t.getpeercert(binary_form=True)).hexdigest()


@pytest.fixture()
def env():
    d = tempfile.mkdtemp(prefix="tlsrot")
    pem = os.path.join(d, "server.pem")
    fp_a = make_pem(pem)
    bucket = make_bucket("tlsrot")
    srv = Server(bucket, "tlsrot", idle=300, extra=["--tls-mode", "requireTLS", "--tls-certificate-key-file", pem]).start(wait=False)
    # the admin readiness check is plain HTTP and works; the data port needs TLS
    import time
    t0 = time.time()
    while time.time() - t0 < 30:
        try:
            srv.admin.get("/v1/health")
            socket.create_connection(("127.0.0.1", srv.port), timeout=1).close()
            break
        except Exception:
            time.sleep(0.2)
    yield srv, d, pem, fp_a
    srv.term()
    shutil.rmtree(d, ignore_errors=True)


def client(srv):
    return MongoClient(srv.uri + "&tls=true", tlsAllowInvalidCertificates=True, serverSelectionTimeoutMS=20000)


def test_rotate_certificates_loads_the_new_certificate(env):
    srv, d, pem, fp_a = env
    assert served_fingerprint(srv.port) == fp_a
    c = client(srv)
    c.admin.command("ping")
    open_conn_db = c["rot"]
    open_conn_db.t.insert_one({"a": 1})
    fp_b = make_pem(pem + ".new")
    shutil.copy(pem + ".new", pem)                       # the file on disk is now certificate B
    assert served_fingerprint(srv.port) == fp_a          # nothing changes until the command is run
    r = c.admin.command("rotateCertificates")
    assert r["ok"] == 1
    assert served_fingerprint(srv.port) == fp_b, "rotateCertificates answered ok but new connections still get the old certificate"
    # the connection that was open before keeps working, and so do new ones
    assert open_conn_db.t.count_documents({}) == 1
    client(srv).admin.command("ping")


def test_rotate_certificates_with_unusable_files_fails_and_keeps_the_old_certificate(env):
    srv, d, pem, fp_a = env
    c = client(srv)
    c.admin.command("ping")
    open(pem, "w").write("this is not a certificate\n")
    with pytest.raises(OperationFailure) as e:
        c.admin.command("rotateCertificates")
    assert e.value.code != 0 and "certificate" in str(e.value).lower()
    assert served_fingerprint(srv.port) == fp_a, "a failed rotation must leave the old certificate in use"
    c.admin.command("ping")
    os.remove(pem)
    with pytest.raises(OperationFailure):
        c.admin.command("rotateCertificates")
    assert served_fingerprint(srv.port) == fp_a


def test_rotate_certificates_answers_the_same_as_mongod_when_tls_is_off():
    """With TLS off there are no certificates to rotate; mongod answers ok and so does the engine (the differential suite g11.admin
    compares exactly that)."""
    bucket = make_bucket("tlsoff")
    srv = Server(bucket, "tlsoff", idle=300).start()
    try:
        r = srv.client().admin.command("rotateCertificates")
        assert r["ok"] == 1
    finally:
        srv.term()
