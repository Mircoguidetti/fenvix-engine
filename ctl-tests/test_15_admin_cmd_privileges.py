"""Privileges of rotateCertificates, tenantStats and tenantAdmin with --auth (engine-round2, point 1).

* rotateCertificates needs the cluster action `rotateCertificates`, as in mongod: root, hostManager and clusterAdmin have it
  (and any custom role that carries it); with TLS on it really reloads the certificate files;
* tenantStats (the S3 request counters of the whole process) is for root only (a role that inherits root counts as root):
  never for the roles of one database, nor for the other cluster roles;
* tenantAdmin on the data port stays unavailable to everyone with --auth (root included): the control layer is the admin port.
"""
import os
import shutil
import socket
import tempfile
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket
from test_09c_tls_rotate import make_pem, served_fingerprint

PW = "Round2-SECRET-5b1e7c9d"

# (user, roles) created on admin, plus users of one customer database (t1)
ADMIN_USERS = {
    "hm": [{"role": "hostManager", "db": "admin"}],
    "ca": [{"role": "clusterAdmin", "db": "admin"}],
    "cm": [{"role": "clusterManager", "db": "admin"}],
    "cmon": [{"role": "clusterMonitor", "db": "admin"}],
    "uaad": [{"role": "userAdminAnyDatabase", "db": "admin"}],
    "rwad": [{"role": "readWriteAnyDatabase", "db": "admin"}],
    "dbaad": [{"role": "dbAdminAnyDatabase", "db": "admin"}],
    "rootlike": [{"role": "opsRoot", "db": "admin"}],          # custom role that inherits root
    "rotator": [{"role": "certRotator", "db": "admin"}],       # custom role with only the rotateCertificates action
}
DB_USERS = {
    "rw": [{"role": "readWrite", "db": "t1"}],
    "dbo": [{"role": "dbOwner", "db": "t1"}],
    "ua": [{"role": "userAdmin", "db": "t1"}],
    "ro": [{"role": "read", "db": "t1"}],
}


def setup_users(srv):
    assert srv.admin.post("/v1/databases", {"name": "t1"})[0] == 201
    r = srv.client(username="root", password="rootpw", authSource="admin")
    r.admin.command("createRole", "opsRoot", privileges=[], roles=[{"role": "root", "db": "admin"}])
    r.admin.command("createRole", "certRotator", privileges=[{"resource": {"cluster": True}, "actions": ["rotateCertificates"]}], roles=[])
    for u, roles in ADMIN_USERS.items():
        r.admin.command("createUser", u, pwd=PW, roles=roles)
    for u, roles in DB_USERS.items():
        r.t1.command("createUser", u, pwd=PW, roles=roles)
    r.close()


def start(tag, extra=()):
    srv = Server(make_bucket(tag), tag, idle=300, extra=["--auth"] + list(extra)).start()
    c = srv.client()
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    c.close()
    return srv


@pytest.fixture(scope="module")
def srv():
    s = start("r2priv")
    setup_users(s)
    yield s
    s.term()


def login(srv, user):
    if user == "root":
        return srv.client(username="root", password="rootpw", authSource="admin")
    return srv.client(username=user, password=PW, authSource="t1" if user in DB_USERS else "admin")


def run(srv, user, db, cmd):
    c = login(srv, user)
    try:
        return c[db].command(cmd)
    finally:
        c.close()


def code_of(srv, user, db, cmd):
    try:
        run(srv, user, db, cmd)
        return 0
    except OperationFailure as e:
        return e.code


@pytest.mark.parametrize("user", ["root", "hm", "ca", "rotator"])
def test_rotate_certificates_allowed(srv, user):
    assert run(srv, user, "admin", "rotateCertificates")["ok"] == 1


@pytest.mark.parametrize("user", ["rw", "dbo", "ua", "ro", "cm", "cmon", "uaad", "rwad", "dbaad"])
def test_rotate_certificates_refused(srv, user):
    assert code_of(srv, user, "admin", "rotateCertificates") == 13


@pytest.mark.parametrize("user", ["root", "rootlike"])
def test_tenant_stats_root(srv, user):
    r = run(srv, user, "admin", "tenantStats")
    assert r["ok"] == 1 and "requests" in r, sorted(r)


@pytest.mark.parametrize("user", ["rw", "dbo", "ua", "ro", "hm", "ca", "cm", "cmon", "uaad", "rwad", "dbaad", "rotator"])
@pytest.mark.parametrize("db", ["admin", "t1"])
def test_tenant_stats_refused(srv, user, db):
    assert code_of(srv, user, db, "tenantStats") == 13


def test_unauthenticated_refused(srv):
    c = MongoClient(f"mongodb://127.0.0.1:{srv.port}/?directConnection=true", serverSelectionTimeoutMS=10000)
    try:
        for cmd in ("tenantStats", "rotateCertificates", "tenantAdmin"):
            with pytest.raises(OperationFailure) as e:
                c.admin.command(cmd)
            assert e.value.code == 13, cmd
    finally:
        c.close()


@pytest.mark.parametrize("user", ["root", "rootlike", "rw", "dbo", "hm", "ca"])
@pytest.mark.parametrize("db", ["admin", "t1"])
def test_tenant_admin_stays_unavailable(srv, user, db):
    assert code_of(srv, user, db, "tenantAdmin") == 13


def test_readwrite_user_gets_13_on_both(srv):
    """The negative case asked for in so many words: a readWrite user of one database, on both commands."""
    for db in ("admin", "t1"):
        for cmd in ("rotateCertificates", "tenantStats"):
            assert code_of(srv, "rw", db, cmd) == 13, (db, cmd)


# ---- with TLS on: the rotation really reloads the certificate ------------------------------------------------------


@pytest.fixture()
def tls_srv():
    d = tempfile.mkdtemp(prefix="r2tls")
    pem = os.path.join(d, "server.pem")
    fp_a = make_pem(pem)
    srv = Server(make_bucket("r2tls"), "r2tls", idle=300, extra=["--auth", "--tls-mode", "requireTLS", "--tls-certificate-key-file", pem]).start(wait=False)
    t0 = time.time()
    while time.time() - t0 < 30:
        try:
            srv.admin.get("/v1/health")
            socket.create_connection(("127.0.0.1", srv.port), timeout=1).close()
            break
        except Exception:
            time.sleep(0.2)
    c = tls_client(srv)
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    c.close()
    c = tls_client(srv, username="root", password="rootpw", authSource="admin")
    c.admin.command("createUser", "hm", pwd=PW, roles=[{"role": "hostManager", "db": "admin"}])
    c.admin.command("createUser", "ca", pwd=PW, roles=[{"role": "clusterAdmin", "db": "admin"}])
    assert srv.admin.post("/v1/databases", {"name": "t1"})[0] == 201
    c.t1.command("createUser", "rw", pwd=PW, roles=[{"role": "readWrite", "db": "t1"}])
    c.close()
    yield srv, pem, fp_a
    srv.term()
    shutil.rmtree(d, ignore_errors=True)


def tls_client(srv, **kw):
    return MongoClient(f"mongodb://127.0.0.1:{srv.port}/?directConnection=true&tls=true", tlsAllowInvalidCertificates=True, serverSelectionTimeoutMS=20000, **kw)


def test_rotation_with_tls_reloads_for_each_allowed_role(tls_srv):
    srv, pem, fp = tls_srv
    assert served_fingerprint(srv.port) == fp
    for user, src in (("root", "admin"), ("hm", "admin"), ("ca", "admin")):
        new_fp = make_pem(pem + "." + user)
        shutil.copy(pem + "." + user, pem)
        c = tls_client(srv, username=user, password="rootpw" if user == "root" else PW, authSource=src)
        try:
            assert c.admin.command("rotateCertificates")["ok"] == 1
        finally:
            c.close()
        assert served_fingerprint(srv.port) == new_fp, f"{user}: ok but the old certificate is still served"
        fp = new_fp
    # a readWrite user of one database: refused, and nothing is reloaded
    shutil.copy(make_pem(pem + ".rw") and pem + ".rw", pem)
    c = tls_client(srv, username="rw", password=PW, authSource="t1")
    try:
        with pytest.raises(OperationFailure) as e:
            c.admin.command("rotateCertificates")
        assert e.value.code == 13
        with pytest.raises(OperationFailure) as e:
            c.admin.command("tenantStats")
        assert e.value.code == 13
    finally:
        c.close()
    assert served_fingerprint(srv.port) == fp
