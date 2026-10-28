"""Authentication and TLS in every mode of the data port (net.tls.mode): disabled, allowTLS, preferTLS, requireTLS, each with --auth, and
each user checked with SCRAM-SHA-1 and SCRAM-SHA-256, over a plain connection where the mode accepts one and over TLS where it accepts
that. The matrix (what a mongod 7.0 listener does):
    disabled   plain yes   TLS no        allowTLS  plain yes  TLS yes        preferTLS  plain yes  TLS yes        requireTLS  plain no  TLS yes
(g03 of compat-gaps covers requireTLS/preferTLS with client certificates, X.509 and SRV against a real mongod; this file adds the two
modes it has no pair for and the SCRAM x TLS combinations.)"""
import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, make_bucket

PW = "Modes-Pw-3c9a51"
MATRIX = {"disabled": (True, False), "allowTLS": (True, True), "preferTLS": (True, True), "requireTLS": (False, True)}


def make_certs(d):
    run = lambda *a: subprocess.run(a, check=True, capture_output=True, cwd=d)
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=Modes Test CA")
    run("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", "srv.key", "-out", "srv.csr", "-subj", "/CN=localhost")
    open(os.path.join(d, "ext.cnf"), "w").write("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
    run("openssl", "x509", "-req", "-in", "srv.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "srv.crt", "-days", "2", "-extfile", "ext.cnf")
    with open(os.path.join(d, "server.pem"), "wb") as out:
        out.write(open(os.path.join(d, "srv.crt"), "rb").read() + open(os.path.join(d, "srv.key"), "rb").read())


@pytest.fixture(scope="module")
def certs():
    d = tempfile.mkdtemp(prefix="modes")
    make_certs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


def start(mode, certs):
    extra = ["--auth", "--tls-mode", mode] + (["--tls-certificate-key-file", os.path.join(certs, "server.pem")] if mode != "disabled" else [])
    srv = Server(make_bucket("modes"), "modes", idle=300, extra=extra).start(wait=False)
    t0 = time.time()
    while time.time() - t0 < 30:
        try:
            srv.admin.get("/v1/health")
            socket.create_connection(("127.0.0.1", srv.port), timeout=1).close()
            return srv
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server did not start")


def conn(srv, certs, tls, user=None, pw=None, source="admin", mech=None):
    k = dict(directConnection=True, serverSelectionTimeoutMS=5000)
    if tls:
        k.update(tls=True, tlsCAFile=os.path.join(certs, "ca.pem"))
    if user:
        k.update(username=user, password=pw, authSource=source)
    if mech:
        k["authMechanism"] = mech
    return MongoClient("127.0.0.1", srv.port, **k)


@pytest.mark.parametrize("mode", list(MATRIX))
def test_mode_matrix(mode, certs):
    plain_ok, tls_ok = MATRIX[mode]
    srv = start(mode, certs)
    try:
        # the first user through the localhost exception, on a connection the mode accepts
        first_tls = tls_ok and not plain_ok or (tls_ok and mode != "disabled")
        conn(srv, certs, first_tls).admin.command("createUser", "root", pwd=PW, roles=[{"role": "root", "db": "admin"}])
        r = conn(srv, certs, first_tls, "root", PW)
        r["modes1"].command("createUser", "u256", pwd=PW, roles=[{"role": "readWrite", "db": "modes1"}], mechanisms=["SCRAM-SHA-256"])
        r["modes1"].command("createUser", "u1", pwd=PW, roles=[{"role": "readWrite", "db": "modes1"}], mechanisms=["SCRAM-SHA-1"])
        r["modes1"].command("createUser", "uboth", pwd=PW, roles=[{"role": "readWrite", "db": "modes1"}])
        for tls, accepted in ((False, plain_ok), (True, tls_ok)):
            for user, mechs in (("u256", ["SCRAM-SHA-256"]), ("u1", ["SCRAM-SHA-1"]), ("uboth", ["SCRAM-SHA-256", "SCRAM-SHA-1"])):
                for mech in mechs:
                    c = conn(srv, certs, tls, user, PW, "modes1", mech)
                    if accepted:
                        c["modes1"]["t"].insert_one({"mode": mode, "tls": tls, "user": user, "mech": mech})
                        assert c["modes1"]["t"].count_documents({"mode": mode, "tls": tls, "user": user, "mech": mech}) == 1
                        # wrong password and a user of the wrong mechanism are refused with AuthenticationFailed
                        with pytest.raises(OperationFailure) as e:
                            conn(srv, certs, tls, user, "wrong-" + PW, "modes1", mech)["modes1"].t.find_one({})
                        assert e.value.code == 18 and PW not in str(e.value)
                    else:
                        with pytest.raises(PyMongoError):
                            c["modes1"]["t"].find_one({})
            if accepted:
                # without credentials nothing works
                with pytest.raises(OperationFailure) as e:
                    conn(srv, certs, tls)["modes1"].t.find_one({})
                assert e.value.code == 13
        # a SCRAM-SHA-1-only user cannot use SCRAM-SHA-256 and the other way round
        if tls_ok:
            with pytest.raises(OperationFailure):
                conn(srv, certs, True, "u1", PW, "modes1", "SCRAM-SHA-256")["modes1"].t.find_one({})
            with pytest.raises(OperationFailure):
                conn(srv, certs, True, "u256", PW, "modes1", "SCRAM-SHA-1")["modes1"].t.find_one({})
    finally:
        srv.term()
    assert PW not in srv.log_text()
