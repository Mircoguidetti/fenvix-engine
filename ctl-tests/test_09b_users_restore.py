"""Restore and users (correction 4): a restore puts the database's DATA back to the chosen instant; the users and roles defined on the
database are NOT part of the restore, because they are not stored in the database (they live in admin.system.users / admin.system.roles,
shared by every database of the server). So after a restore: a user created after the instant is still there, a user dropped after it
is still gone, a changed password or role stays changed. This is the documented behaviour; this test pins it."""
import time

import pytest
from pymongo.errors import OperationFailure

from lib import Server, make_bucket


def now_ms():
    return int(time.time() * 1000)


@pytest.fixture()
def env():
    bucket = make_bucket("rsu")
    srv = Server(bucket, "rsu", idle=300, extra=["--auth", "--retention-interval-secs", "2", "--retention-prune-secs", "3"]).start()
    c = srv.client()
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    c.close()
    yield srv, srv.client(username="root", password="rootpw", authSource="admin")
    srv.term()


def user_names(r, db):
    return sorted(u["user"] for u in r[db].command("usersInfo", 1)["users"])


def test_restore_restores_data_and_leaves_users_as_they_are(env):
    srv, r = env
    assert srv.admin.post("/v1/databases", {"name": "rs1"})[0] == 201
    assert srv.admin.put("/v1/databases/rs1/retention", {"window_secs": 3600, "interval_secs": 2})[0] == 200
    r["rs1"].t.insert_many([{"_id": i, "v": 1} for i in range(100)])
    r["rs1"].command("createUser", "keeps", pwd="keeps-pw", roles=[{"role": "readWrite", "db": "rs1"}])
    r["rs1"].command("createUser", "dropped_later", pwd="d-pw", roles=[{"role": "read", "db": "rs1"}])
    time.sleep(2.6)
    t = now_ms()                                      # the instant we restore to: 100 documents, users keeps + dropped_later
    time.sleep(2.6)
    r["rs1"].t.update_many({}, {"$set": {"v": 2}})
    r["rs1"].t.insert_many([{"_id": 1000 + i, "v": 2} for i in range(40)])
    r["rs1"].command("createUser", "created_later", pwd="c-pw", roles=[{"role": "read", "db": "rs1"}])
    r["rs1"].command("dropUser", "dropped_later")
    r["rs1"].command("updateUser", "keeps", pwd="keeps-new-pw")
    time.sleep(2.6)
    st, out = srv.admin.post("/v1/databases/rs1/restore", {"to": t})
    assert st == 200, out
    # data: as of t
    assert r["rs1"].t.count_documents({}) == 100 and r["rs1"].t.count_documents({"v": 2}) == 0
    # users: as they are now, not as of t
    assert user_names(r, "rs1") == ["created_later", "keeps"]
    srv.client(username="keeps", password="keeps-new-pw", authSource="rs1")["rs1"].t.find_one({})            # new password works
    with pytest.raises(OperationFailure):
        srv.client(username="keeps", password="keeps-pw", authSource="rs1")["rs1"].t.find_one({})            # old one does not
    with pytest.raises(OperationFailure):
        srv.client(username="dropped_later", password="d-pw", authSource="rs1")["rs1"].t.find_one({})        # dropped stays dropped
    # and a restore leaves the other databases' users alone
    assert "root" in user_names(r, "admin")
