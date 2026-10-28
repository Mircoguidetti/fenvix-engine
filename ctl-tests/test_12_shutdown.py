"""Start and stop (semantic merge point between compat-gaps' `shutdown` command and control-ops' shutdown work): the `shutdown` command, SIGTERM
and the exit of the process all go through the same clean exit: tenants closed, then the keep-awake pins handed over and the usage counters
written (ctl::shutdown), then the leases released. The test runs the wire command `shutdown` with --auth on, with a keep-awake pin held."""
import json
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from lib import Server, make_bucket, s3, list_prefix


def pin_doc(bucket, db):
    try:
        return json.loads(s3().get_object(Bucket=bucket, Key=f"control/pins/{db}")["Body"].read())
    except Exception:
        return None


@pytest.mark.parametrize("how", ["shutdown-command", "sigterm"])
def test_clean_exit_releases_pins_usage_and_leases(how):
    bucket = make_bucket("shut")
    srv = Server(bucket, "shut", idle=300, extra=["--auth", "--usage-flush-secs", "600"]).start()
    try:
        c = srv.client()
        c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}]) if False else None
        r = MongoClient(srv.uri.replace("//", "//root:rootpw@") + "&authSource=admin", serverSelectionTimeoutMS=20000) if srv.auth else None
        # first user through the localhost exception (the server was started with --auth)
        MongoClient(f"mongodb://127.0.0.1:{srv.port}/?directConnection=true", serverSelectionTimeoutMS=20000).admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
        root = MongoClient(f"mongodb://root:rootpw@127.0.0.1:{srv.port}/?directConnection=true&authSource=admin", serverSelectionTimeoutMS=20000)
        assert srv.admin.post("/v1/databases", {"name": "shdb"})[0] == 201
        root.shdb.t.insert_many([{"_id": i} for i in range(20)])
        assert srv.admin.put("/v1/databases/shdb/keep-awake")[0] == 200
        before = pin_doc(bucket, "shdb")
        assert before and before["holder"] == srv.holder and before["expires_ms"] > time.time() * 1000
        if how == "shutdown-command":
            try:
                root.admin.command("shutdown")                # no reply: the connection closes
            except PyMongoError:
                pass
            code = srv.proc.wait(60)
        else:
            srv.proc.terminate()
            code = srv.proc.wait(60)
        assert code == 0, f"exit code {code}"
        after = pin_doc(bucket, "shdb")
        assert after is not None and after["holder"] == "" and after["expires_ms"] == 0, f"the pin was not handed over: {after}"
        usage = list_prefix(bucket, "control/usage/shdb/")
        assert usage, "the usage counters were not written at shutdown"
        # the lease of the tenant is released: a second engine can take the database at once
        srv2 = Server(bucket, "shut2", idle=300, extra=["--auth"], holder="other-holder").start()
        try:
            MongoClient(srv2.uri, serverSelectionTimeoutMS=20000)
            root2 = MongoClient(f"mongodb://root:rootpw@127.0.0.1:{srv2.port}/?directConnection=true&authSource=admin", serverSelectionTimeoutMS=20000)
            assert root2.shdb.t.count_documents({}) == 20
        finally:
            srv2.term()
    finally:
        srv.term()
