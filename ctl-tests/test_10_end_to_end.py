"""End to end on one engine process (MinIO as the object store, TLS required on the data port, --auth on, admin port with its token):

  ctl creates a database -> root creates a user limited to that database -> that user connects with TLS and SCRAM (SHA-256 and SHA-1),
  writes and reads -> the user cannot leave its database, create users or reach anything of ctl -> ctl forks the database: the fork has
  the data and none of the users -> ctl restores the database inside the retention window: data back, users untouched -> ctl erases the
  database: its users and roles are gone and the user can no longer log in -> no password anywhere in the log."""
import os
import shutil
import subprocess
import tempfile
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from lib import Server, make_bucket, TOKEN

APP_PW = "App-Pw-E2E-7d41c9"
ROOT_PW = "Root-Pw-E2E-2b8e5f"


def make_certs(d):
    run = lambda *a: subprocess.run(a, check=True, capture_output=True, cwd=d)
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=E2E Test CA")
    run("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", "srv.key", "-out", "srv.csr", "-subj", "/CN=localhost")
    open(os.path.join(d, "ext.cnf"), "w").write("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
    run("openssl", "x509", "-req", "-in", "srv.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "srv.crt", "-days", "2", "-extfile", "ext.cnf")
    with open(os.path.join(d, "server.pem"), "wb") as out:
        out.write(open(os.path.join(d, "srv.crt"), "rb").read() + open(os.path.join(d, "srv.key"), "rb").read())


@pytest.fixture(scope="module")
def env():
    d = tempfile.mkdtemp(prefix="e2e")
    make_certs(d)
    bucket = make_bucket("e2e")
    srv = Server(bucket, "e2e", idle=300, extra=["--auth", "--tls-mode", "requireTLS", "--tls-certificate-key-file", os.path.join(d, "server.pem"),
                                                  "--retention-interval-secs", "2", "--retention-prune-secs", "30"]).start(wait=False)
    t0 = time.time()
    while time.time() - t0 < 30:
        try:
            srv.admin.get("/v1/health")
            import socket
            socket.create_connection(("127.0.0.1", srv.port), timeout=1).close()
            break
        except Exception:
            time.sleep(0.2)
    ca = os.path.join(d, "ca.pem")

    def client(user=None, pw=None, source="admin", mech=None, **kw):
        k = dict(tls=True, tlsCAFile=ca, serverSelectionTimeoutMS=20000, directConnection=True)
        if user:
            k.update(username=user, password=pw, authSource=source)
        if mech:
            k["authMechanism"] = mech
        k.update(kw)
        return MongoClient("127.0.0.1", srv.port, **k)

    # the first user, through the localhost exception, over TLS
    client().admin.command("createUser", "root", pwd=ROOT_PW, roles=[{"role": "root", "db": "admin"}])
    yield srv, client, ca
    srv.term()
    shutil.rmtree(d, ignore_errors=True)


def test_end_to_end(env):
    srv, client, ca = env
    admin = srv.admin
    root = client("root", ROOT_PW)

    # --- ctl creates the database; root creates a user limited to it -------------------------------------------------------------
    st, r = admin.post("/v1/databases", {"name": "e2e1"})
    assert st == 201, r
    root["e2e1"].command("createUser", "app", pwd=APP_PW, roles=[{"role": "readWrite", "db": "e2e1"}])

    # --- the user connects with TLS and SCRAM, both mechanisms, writes and reads --------------------------------------------------
    for mech in ("SCRAM-SHA-256", "SCRAM-SHA-1"):
        app = client("app", APP_PW, source="e2e1", mech=mech)
        app["e2e1"]["t"].insert_one({"_id": mech, "v": 1})
        assert app["e2e1"]["t"].find_one({"_id": mech})["v"] == 1
    assert client("app", APP_PW, source="e2e1")["e2e1"]["t"].count_documents({}) == 2

    # --- over TLS only: a plain connection is refused; a wrong password is refused ------------------------------------------------
    with pytest.raises(PyMongoError):
        MongoClient("127.0.0.1", srv.port, serverSelectionTimeoutMS=3000, directConnection=True).admin.command("ping")
    for mech in ("SCRAM-SHA-256", "SCRAM-SHA-1"):
        with pytest.raises(OperationFailure) as e:
            client("app", "wrong-" + APP_PW, source="e2e1", mech=mech)["e2e1"].t.find_one({})
        assert e.value.code == 18 and APP_PW not in str(e.value)

    # --- the user cannot leave its database, create users, or reach ctl -----------------------------------------------------------
    app = client("app", APP_PW, source="e2e1")
    for fn in (lambda: app["other"].t.insert_one({"a": 1}), lambda: app["other"].t.find_one({}), lambda: app["e2e1"].command("createUser", "evil", pwd="x-pw-1", roles=[]),
               lambda: app["admin"].command("createUser", "evil", pwd="x-pw-1", roles=[{"role": "root", "db": "admin"}]),
               lambda: app["admin"].command("shutdown"), lambda: app["e2e1"].command("grantRolesToUser", "app", roles=[{"role": "root", "db": "admin"}])):
        with pytest.raises(OperationFailure) as e:
            fn()
        assert e.value.code == 13
    # listDatabases is not an error for a user without the cluster privilege (as in mongod): it shows only the databases the user can reach
    assert [d["name"] for d in app["admin"].command("listDatabases")["databases"]] == ["e2e1"]
    for cmd in ({"createDatabase": 1, "name": "viaport"}, {"eraseDatabase": "e2e1"}, {"forkDatabase": "e2e1", "target": "x"}):
        with pytest.raises(OperationFailure):
            root.admin.command(cmd)
    assert admin.get("/v1/databases", token="wrong-token-0123456789")[0] == 401
    assert admin.delete("/v1/databases/e2e1", token=None)[0] == 401

    # --- fork: the data, none of the users ----------------------------------------------------------------------------------------
    st, r = admin.post("/v1/databases/e2e1/fork", {"target": "e2e2", "mode": "clone"})
    assert st == 201, r
    assert root["e2e2"]["t"].count_documents({}) == 2
    assert root["e2e2"].command("usersInfo", 1)["users"] == []
    with pytest.raises(OperationFailure):
        client("app", APP_PW, source="e2e2")["e2e2"].t.find_one({})
    with pytest.raises(OperationFailure):
        app["e2e2"].t.find_one({})                              # the origin's user has nothing on the fork either
    root["e2e2"].command("createUser", "app2", pwd=APP_PW + "2", roles=[{"role": "read", "db": "e2e2"}])
    assert client("app2", APP_PW + "2", source="e2e2")["e2e2"].t.count_documents({}) == 2

    # --- restore inside the retention window: the data come back, the users stay as they are ---------------------------------------
    assert admin.put("/v1/databases/e2e1/retention", {"window_secs": 600, "interval_secs": 2})[0] == 200
    app["e2e1"]["t"].insert_many([{"_id": f"b{i}"} for i in range(20)])
    time.sleep(2.6)
    t = int(time.time() * 1000)
    time.sleep(2.6)
    app["e2e1"]["t"].insert_many([{"_id": f"c{i}"} for i in range(30)])
    root["e2e1"].command("createUser", "late", pwd="late-pw-1", roles=[{"role": "read", "db": "e2e1"}])
    time.sleep(2.6)
    st, r = admin.post("/v1/databases/e2e1/restore", {"to": t})
    assert st == 200, r
    assert app["e2e1"]["t"].count_documents({}) == 22
    assert sorted(u["user"] for u in root["e2e1"].command("usersInfo", 1)["users"]) == ["app", "late"]

    # --- erase: data, users and roles go; the user can no longer log in -----------------------------------------------------------
    st, r = admin.delete("/v1/databases/e2e1")                    # refused while a shallow fork still reads its objects (by design)
    assert st == 409 and r["error"]["code"] == "has_forks", r
    assert admin.delete("/v1/databases/e2e2")[0] == 200
    assert root["e2e2"].command("usersInfo", 1)["users"] == []
    st, r = admin.delete("/v1/databases/e2e1")
    assert st == 200 and r["erased"], r
    assert root["e2e1"].command("usersInfo", 1)["users"] == []
    with pytest.raises(OperationFailure):
        app["e2e1"].t.find_one({})                              # the open connection lost its privileges
    with pytest.raises(OperationFailure):
        client("app", APP_PW, source="e2e1")["e2e1"].t.find_one({})

    # --- no secret in the engine log ------------------------------------------------------------------------------------------------
    log = srv.log_text()
    for secret in (APP_PW, ROOT_PW, "late-pw-1", "wrong-" + APP_PW, APP_PW + "2", TOKEN):
        assert secret not in log, f"a secret is in the engine log: {secret[:6]}..."
