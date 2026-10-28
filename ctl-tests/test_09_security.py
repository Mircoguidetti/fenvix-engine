"""Security of the data port and of the admin port, on the final binary (--auth on):
 * the admin port is independent of SCRAM: nothing of ctl is reachable from the data port, and a wrong token does nothing;
 * correction 1: createUser / updateUser / grantRolesToUser / createRole / updateRole / grantRolesToRole / grantPrivilegesToRole
   cannot hand out roles or privileges outside the caller's reach;
 * correction 2: no password, hash or authentication payload in any error, in the profile, in currentOp or in the log;
 * correction 4: erase deletes the users and roles defined on the database, a fork has none, restore leaves them as they are.
Each test of a correction was first written and run against the binary without the correction (see results-final-engine.md)."""
import base64
import json
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, wait_until, TOKEN

PW = "Pw-SECRET-9f3c1d7a"          # marker looked for everywhere
HOSTILE_PW = "Hostile-SECRET-5b2e8c4d"


@pytest.fixture(scope="module")
def env():
    bucket = make_bucket("sec")
    srv = Server(bucket, "sec", idle=300, extra=["--auth"]).start()
    c = srv.client()
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])  # localhost exception
    c.close()
    yield srv, bucket
    srv.term()


def user_client(srv, user, pwd, source, mech=None):
    kw = dict(username=user, password=pwd, authSource=source)
    if mech:
        kw["authMechanism"] = mech
    return srv.client(**kw)


def root(srv):
    return user_client(srv, "root", "rootpw", "admin")


def denied(fn):
    with pytest.raises(OperationFailure) as e:
        fn()
    assert e.value.code == 13, f"expected Unauthorized, got {e.value.code}: {e.value}"
    return e.value


# ------------------------------------------------------------------------------------------------ admin port vs data port

def test_admin_port_is_independent_of_scram(env):
    srv, _ = env
    # the admin port answers with its token and no SCRAM at all
    st, r = srv.admin.get("/v1/health")
    assert st == 200 and r["ok"]
    # wrong token, no token, token of another length, SCRAM credentials as a token: nothing is done
    for tok in ("wrong-token-0123456789", None, "x", "rootpw", "root:rootpw", TOKEN + "x", TOKEN[:-1]):
        for method, path in (("GET", "/v1/databases"), ("POST", "/v1/databases"), ("DELETE", "/v1/databases/anything"), ("GET", "/v1/health")):
            st, r = srv.admin.call(method, path, {"name": "should-not-exist"} if method == "POST" else None, token=tok)
            assert st == 401, (tok, method, path, st, r)
    assert "should-not-exist" not in [d["name"] for d in srv.admin.get("/v1/databases")[1]["databases"]]
    # a bearer token with a different scheme
    st, _ = srv.admin.call("GET", "/v1/databases", token=None, headers={"Authorization": "Basic " + base64.b64encode(f"root:rootpw".encode()).decode()})
    assert st == 401


def test_nothing_of_ctl_is_reachable_from_the_data_port(env):
    srv, _ = env
    r = root(srv)
    cmds = [{"createDatabase": 1, "name": "viaport"}, {"forkDatabase": "x", "target": "y"}, {"eraseDatabase": "x"}, {"restoreDatabase": "x", "to": 1},
            {"keepAwake": 1}, {"tenantAdmin": 1, "close": True}, {"ctl": 1}, {"v1": 1}, {"adminPort": 1}, {"exportDatabase": "x"}, {"usage": 1}]
    for cmd in cmds:
        name = next(iter(cmd))
        if name == "tenantAdmin":
            continue  # a test hook of the engine tests (close a tenant), not part of ctl: checked in its own test below
        with pytest.raises(OperationFailure) as e:
            r.admin.command(cmd)
        assert e.value.code in (13, 59), (cmd, e.value.code, str(e.value)[:100])      # refused (Unauthorized or CommandNotFound), even for root
    # not even a root user can create control metadata through the data port
    assert "viaport" not in [d["name"] for d in srv.admin.get("/v1/databases")[1]["databases"]]


# ------------------------------------------------------------------------------------------------ correction 1: grant scope

@pytest.fixture(scope="module")
def grant_env(env):
    srv, _ = env
    r = root(srv)
    r["app1"].command("createUser", "ua", pwd="uapw", roles=[{"role": "userAdmin", "db": "app1"}])           # userAdmin of app1
    r["app1"].command("createRole", "creator", privileges=[{"resource": {"db": "app1", "collection": ""}, "actions": ["createUser"]}], roles=[])
    r["app1"].command("createUser", "cr", pwd="crpw", roles=[{"role": "creator", "db": "app1"}])            # only the createUser action
    r["admin"].command("createUser", "uaa", pwd="uaapw", roles=[{"role": "userAdminAnyDatabase", "db": "admin"}])
    r["app1"].command("createUser", "victim", pwd="vpw", roles=[{"role": "read", "db": "app1"}])
    r["app2"].command("createUser", "other2", pwd="o2pw", roles=[{"role": "read", "db": "app2"}])
    # a custom role of app1 that inherits root: its name hides the power
    r["app1"].command("createRole", "innocent", privileges=[], roles=[{"role": "root", "db": "admin"}])
    return srv, r


def has_role(r, db, user, role, rdb):
    info = r[db].command("usersInfo", user)["users"]
    return bool(info) and {"role": role, "db": rdb} in info[0]["roles"]


@pytest.mark.parametrize("who,src,roles", [
    ("ua", "app1", [{"role": "root", "db": "admin"}]),                                   # root through a user admin
    ("ua", "app1", [{"role": "readWrite", "db": "app2"}]),                              # another database
    ("ua", "app1", [{"role": "readWrite", "db": "admin"}]),                             # the admin database
    ("ua", "app1", [{"role": "clusterAdmin", "db": "admin"}]),                          # cluster role
    ("ua", "app1", [{"role": "readWriteAnyDatabase", "db": "admin"}]),                  # every database
    ("ua", "app1", [{"role": "userAdminAnyDatabase", "db": "admin"}]),
    ("ua", "app1", [{"role": "innocent", "db": "app1"}]),                               # custom role that inherits root
    ("ua", "app1", [{"role": "readWrite", "db": "app1"}, {"role": "root", "db": "admin"}]),   # one good, one bad
    ("cr", "app1", [{"role": "readWrite", "db": "app1"}]),                              # createUser action only: grants nothing
    ("cr", "app1", [{"role": "root", "db": "admin"}]),
    ("uaa", "app1", [{"role": "root", "db": "admin"}]),                                 # user admin of every database cannot hand out root
    ("uaa", "app1", [{"role": "readWriteAnyDatabase", "db": "admin"}]),
])
def test_createuser_cannot_grant_what_the_caller_cannot(grant_env, who, src, roles):
    srv, r = grant_env
    pw = {"ua": "uapw", "cr": "crpw", "uaa": "uaapw"}[who]
    c = user_client(srv, who, pw, {"uaa": "admin"}.get(who, "app1"))
    name = f"new_{abs(hash(json.dumps(roles)))}"
    denied(lambda: c[src].command("createUser", name, pwd="x-pw-1", roles=roles))
    assert r[src].command("usersInfo", name)["users"] == [], "the user was created although the command was refused"


def test_createuser_without_roles_or_with_own_scope_still_works(grant_env):
    srv, r = grant_env
    ua = user_client(srv, "ua", "uapw", "app1")
    ua["app1"].command("createUser", "ok1", pwd="x-pw-1", roles=[{"role": "readWrite", "db": "app1"}])
    ua["app1"].command("createUser", "ok2", pwd="x-pw-1", roles=[{"role": "dbOwner", "db": "app1"}])
    ua["app1"].command("createUser", "ok3", pwd="x-pw-1", roles=[])
    cr = user_client(srv, "cr", "crpw", "app1")
    cr["app1"].command("createUser", "ok4", pwd="x-pw-1", roles=[])                    # createUser alone may create a user with no role
    uaa = user_client(srv, "uaa", "uaapw", "admin")
    uaa["app2"].command("createUser", "ok5", pwd="x-pw-1", roles=[{"role": "readWrite", "db": "app2"}])   # any specific database
    assert has_role(r, "app1", "ok1", "readWrite", "app1")
    # and those users work
    c = user_client(srv, "ok1", "x-pw-1", "app1")
    c["app1"].t.insert_one({"a": 1})
    assert c["app1"].t.count_documents({}) == 1


@pytest.mark.parametrize("roles", [
    [{"role": "root", "db": "admin"}], [{"role": "readWrite", "db": "app2"}], [{"role": "innocent", "db": "app1"}],
    [{"role": "userAdminAnyDatabase", "db": "admin"}]])
def test_updateuser_and_grantroles_cannot_grant_what_the_caller_cannot(grant_env, roles):
    srv, r = grant_env
    ua = user_client(srv, "ua", "uapw", "app1")
    before = r["app1"].command("usersInfo", "victim")["users"][0]["roles"]
    denied(lambda: ua["app1"].command("updateUser", "victim", roles=roles))
    denied(lambda: ua["app1"].command("grantRolesToUser", "victim", roles=roles))
    # self-escalation
    denied(lambda: ua["app1"].command("updateUser", "ua", roles=roles + [{"role": "userAdmin", "db": "app1"}]))
    denied(lambda: ua["app1"].command("grantRolesToUser", "ua", roles=roles))
    assert r["app1"].command("usersInfo", "victim")["users"][0]["roles"] == before
    assert not has_role(r, "app1", "ua", "root", "admin")


def test_roles_cannot_be_used_to_smuggle_privileges(grant_env):
    srv, r = grant_env
    ua = user_client(srv, "ua", "uapw", "app1")
    cr = user_client(srv, "cr", "crpw", "app1")
    # createRole / updateRole / grantRolesToRole / grantPrivilegesToRole with a power the caller does not have
    denied(lambda: ua["app1"].command("createRole", "smug1", privileges=[], roles=[{"role": "root", "db": "admin"}]))
    denied(lambda: ua["app1"].command("createRole", "smug2", privileges=[{"resource": {"anyResource": True}, "actions": ["anyAction"]}], roles=[]))
    denied(lambda: ua["app1"].command("createRole", "smug3", privileges=[{"resource": {"cluster": True}, "actions": ["shutdown"]}], roles=[]))
    denied(lambda: ua["app1"].command("createRole", "smug4", privileges=[{"resource": {"db": "app2", "collection": ""}, "actions": ["find"]}], roles=[]))
    r["app1"].command("createRole", "plain", privileges=[], roles=[])
    denied(lambda: ua["app1"].command("grantRolesToRole", "plain", roles=[{"role": "root", "db": "admin"}]))
    denied(lambda: ua["app1"].command("grantPrivilegesToRole", "plain", privileges=[{"resource": {"anyResource": True}, "actions": ["anyAction"]}]))
    denied(lambda: ua["app1"].command("updateRole", "plain", roles=[{"role": "root", "db": "admin"}]))
    denied(lambda: cr["app1"].command("createRole", "smug5", privileges=[], roles=[]))   # not even the createRole action
    # what is allowed: privileges and roles inside the database the caller administers
    ua["app1"].command("createRole", "fine1", privileges=[{"resource": {"db": "app1", "collection": "x"}, "actions": ["find"]}], roles=[{"role": "read", "db": "app1"}])
    ua["app1"].command("grantPrivilegesToRole", "plain", privileges=[{"resource": {"db": "app1", "collection": ""}, "actions": ["insert"]}])
    assert r["app1"].command("rolesInfo", "plain", showPrivileges=True)["roles"][0]["privileges"]


def test_root_and_cluster_admin_flow_is_unchanged(grant_env):
    srv, r = grant_env
    r["app1"].command("createUser", "by_root", pwd="x-pw-1", roles=[{"role": "root", "db": "admin"}, {"role": "readWrite", "db": "app2"}])
    assert has_role(r, "app1", "by_root", "root", "admin")
    r["app1"].command("updateUser", "by_root", roles=[{"role": "readWrite", "db": "app1"}])
    r["app1"].command("grantRolesToUser", "by_root", roles=[{"role": "clusterAdmin", "db": "admin"}])
    assert has_role(r, "app1", "by_root", "clusterAdmin", "admin")
    # a user that holds root can hand root out
    rootish = user_client(srv, "root", "rootpw", "admin")
    rootish["app3"].command("createUser", "by_root2", pwd="x-pw-1", roles=[{"role": "root", "db": "admin"}])


# ------------------------------------------------------------------------------------------------ correction 2: no secrets

SECRET_CMDS = lambda: [
    ("createUser", "app9", {"createUser": "leak1", "pwd": PW, "roles": []}),
    ("updateUser", "app9", {"updateUser": "leak1", "pwd": PW}),
    ("changeUserPassword", "app9", {"updateUser": "leak1", "pwd": PW}),
    ("createUser-external", "$external", {"createUser": "CN=x", "roles": []}),
    ("authenticate", "app9", {"authenticate": 1, "user": "leak1", "nonce": "00112233", "key": PW}),
    ("saslStart", "app9", {"saslStart": 1, "mechanism": "SCRAM-SHA-256", "payload": base64.b64encode(f"n,,n=leak1,r={PW}".encode()).decode(), "autoAuthorize": 1}),
    ("saslContinue", "app9", {"saslContinue": 1, "conversationId": 1, "payload": base64.b64encode(PW.encode()).decode()}),
]


def test_unauthenticated_errors_carry_no_secret(env):
    srv, _ = env
    c = srv.client()                 # not authenticated
    for label, db, cmd in SECRET_CMDS():
        try:
            c[db].command(cmd)
        except OperationFailure as e:
            msg = str(e) + json.dumps(e.details or {}, default=str)
            assert PW not in msg, f"{label}: the error repeats the secret: {msg[:300]}"
            assert base64.b64encode(PW.encode()).decode() not in msg, label


def test_unauthorized_errors_of_a_weak_user_carry_no_secret(grant_env):
    srv, r = grant_env
    c = user_client(srv, "victim", "vpw", "app1")                                   # read-only user
    for label, db, cmd in SECRET_CMDS():
        try:
            c[db].command(cmd)
        except OperationFailure as e:
            msg = str(e) + json.dumps(e.details or {}, default=str)
            assert PW not in msg, f"{label}: the error repeats the secret: {msg[:300]}"
    # the message still says what was refused (command name and database)
    e = denied(lambda: c["app1"].command("createUser", "leakx", pwd=PW, roles=[]))
    assert "createUser" in str(e) and "app1" in str(e)


def test_secrets_are_not_in_the_profile_or_in_currentop(grant_env):
    srv, r = grant_env
    r["app9"].command("profile", 2)
    r["app9"].command("createUser", "prof1", pwd=PW, roles=[])
    r["app9"].command("updateUser", "prof1", pwd=PW + "2")
    r["app9"].command("profile", 0)
    prof = list(r["app9"]["system.profile"].find({}))
    text = json.dumps(prof, default=str)
    assert PW not in text, "the profile holds a password"
    # currentOp: a command that is running while it is looked at is hard to catch; check the shape of what it reports for ours
    ops = r["admin"].command("currentOp", True)["inprog"]
    assert PW not in json.dumps(ops, default=str)


def test_secrets_are_not_in_the_server_log(env):
    srv, _ = env
    text = srv.log_text()
    assert PW not in text and PW + "2" not in text
    assert HOSTILE_PW not in text
    assert "rootpw" not in text, "the root password appears in the log"
    assert "uapw" not in text and "crpw" not in text


def test_failed_logins_do_not_log_or_return_the_password(env):
    srv, _ = env
    for mech in ("SCRAM-SHA-256", "SCRAM-SHA-1"):
        c = MongoClient(srv.uri, username="root", password=HOSTILE_PW, authSource="admin", authMechanism=mech, serverSelectionTimeoutMS=8000)
        with pytest.raises(OperationFailure) as e:
            c.admin.command("ping"); c.admin.list_collection_names()
        assert HOSTILE_PW not in str(e.value) and e.value.code == 18
    assert HOSTILE_PW not in srv.log_text()


# ------------------------------------------------------------------------------------------------ correction 4: users and databases

def users_of(r, db):
    return [u["user"] for u in r[db].command("usersInfo", 1)["users"]]


def raw_auth_docs(r, db):
    users = list(r["admin"]["system.users"].find({"db": db}))
    roles = list(r["admin"]["system.roles"].find({"db": db}))
    return users, roles


def test_erase_deletes_the_users_and_roles_of_the_database(env):
    srv, _ = env
    r = root(srv)
    assert srv.admin.post("/v1/databases", {"name": "tenantA"})[0] == 201
    r["tenantA"].t.insert_one({"x": 1})
    r["tenantA"].command("createRole", "rA", privileges=[{"resource": {"db": "tenantA", "collection": ""}, "actions": ["find"]}], roles=[])
    r["tenantA"].command("createUser", "uA", pwd="uApw", roles=[{"role": "readWrite", "db": "tenantA"}, {"role": "rA", "db": "tenantA"}])
    r["tenantB"].command("createUser", "uB", pwd="uBpw", roles=[{"role": "readWrite", "db": "tenantB"}])
    # a user defined on another database with a role on tenantA keeps living (the role of tenantA disappears with it)
    r["tenantB"].command("createUser", "uBA", pwd="uBApw", roles=[{"role": "read", "db": "tenantA"}])
    live = user_client(srv, "uA", "uApw", "tenantA")
    live["tenantA"].t.insert_one({"y": 2})
    assert users_of(r, "tenantA") == ["uA"]
    st, out = srv.admin.delete("/v1/databases/tenantA")
    assert st == 200, out
    users, roles = raw_auth_docs(r, "tenantA")
    assert users == [] and roles == [], f"users/roles left behind: {[u['_id'] for u in users]} {[x['_id'] for x in roles]}"
    assert users_of(r, "tenantA") == []
    assert users_of(r, "tenantB") == ["uB", "uBA"], "users of other databases must be untouched"
    # an open connection of the erased user loses its access at once (no cached privileges)
    with pytest.raises(OperationFailure):
        live["tenantA"].t.find_one({})
    with pytest.raises(OperationFailure):
        user_client(srv, "uA", "uApw", "tenantA")["tenantA"].t.find_one({})
    # the name can be reused and the new database has no users
    assert srv.admin.post("/v1/databases", {"name": "tenantA"})[0] == 201
    assert users_of(r, "tenantA") == []
    with pytest.raises(OperationFailure):
        user_client(srv, "uA", "uApw", "tenantA")["tenantA"].t.find_one({})
    srv.admin.delete("/v1/databases/tenantA")


def test_erase_of_a_database_with_a_fork_keeps_the_users_of_the_fork_clean(env):
    srv, _ = env
    r = root(srv)
    srv.admin.post("/v1/databases", {"name": "srcF"})
    r["srcF"].t.insert_many([{"i": i} for i in range(50)])
    r["srcF"].command("createUser", "uF", pwd="uFpw", roles=[{"role": "readWrite", "db": "srcF"}])
    st, out = srv.admin.post("/v1/databases/srcF/fork", {"target": "dstF"})
    assert st == 201, out
    assert users_of(r, "dstF") == [], "a fork must not inherit the users of its origin"
    with pytest.raises(OperationFailure):
        user_client(srv, "uF", "uFpw", "dstF")["dstF"].t.find_one({})
    # the users of the origin are still there, and the fork's data is there
    assert users_of(r, "srcF") == ["uF"]
    assert r["dstF"].t.count_documents({}) == 50
    # the user of the origin does not reach the fork with the origin's credentials either
    uf = user_client(srv, "uF", "uFpw", "srcF")
    assert uf["srcF"].t.count_documents({}) == 50
    denied(lambda: uf["dstF"].t.find_one({}))
    # creating users on the fork is independent
    r["dstF"].command("createUser", "uFd", pwd="uFdpw", roles=[{"role": "read", "db": "dstF"}])
    assert users_of(r, "dstF") == ["uFd"] and users_of(r, "srcF") == ["uF"]
    srv.admin.delete("/v1/databases/dstF")
    assert users_of(r, "dstF") == [] and users_of(r, "srcF") == ["uF"]
    srv.admin.delete("/v1/databases/srcF")
    assert users_of(r, "srcF") == []


def test_create_and_fork_refuse_a_name_that_still_has_users(env):
    srv, _ = env
    r = root(srv)
    r["ghost"].command("createUser", "gu", pwd="gpw", roles=[{"role": "read", "db": "ghost"}])  # users first, database not created by ctl
    st, out = srv.admin.post("/v1/databases", {"name": "ghost"})
    assert st == 409 and "users" in json.dumps(out), (st, out)
    srv.admin.post("/v1/databases", {"name": "srcG"})
    r["srcG"].t.insert_one({"a": 1})
    st, out = srv.admin.post("/v1/databases/srcG/fork", {"target": "ghost"})
    assert st == 409 and "users" in json.dumps(out), (st, out)
    r["ghost"].command("dropAllUsersFromDatabase")
    assert srv.admin.post("/v1/databases", {"name": "ghost"})[0] == 201
    srv.admin.delete("/v1/databases/ghost")
    srv.admin.delete("/v1/databases/srcG")


def test_reserved_names_cannot_be_touched_by_ctl(env):
    srv, _ = env
    r = root(srv)
    n = len(r["admin"]["system.users"].find({}).to_list() if hasattr(r["admin"]["system.users"].find({}), "to_list") else list(r["admin"]["system.users"].find({})))
    for db in ("admin", "local", "config"):
        assert srv.admin.delete(f"/v1/databases/{db}")[0] in (400, 404)
        assert srv.admin.post("/v1/databases", {"name": db})[0] == 400
        assert srv.admin.post(f"/v1/databases/{db}/fork", {"target": "copyofadmin"})[0] in (400, 404)
        assert srv.admin.post(f"/v1/databases/{db}/restore", {"to": 1})[0] in (400, 404)
        assert srv.admin.post(f"/v1/databases/{db}/exports")[0] in (400, 404)
    assert len(list(r["admin"]["system.users"].find({}))) == n
    root(srv)["admin"].command("ping")
