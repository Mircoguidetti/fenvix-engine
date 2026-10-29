"""Roles the provisioner may hand out (engine-round2, point 3): --grantable-roles.

Without the flag nothing changes (mongod's rules: whoever has grantRole on a database may hand out any role of that database).
With --grantable-roles readWrite,read, a caller that has neither root nor clusterAdmin may create users and hand out roles
ONLY with the roles of the list, on the database where the user is defined (a customer database), never on admin (nor
local, config, $external), never one of its own roles, never a role that can manage users, never a custom role, and may not
create or change roles at all. root and clusterAdmin keep mongod's rules.

The control plane's flows must keep working with the flag: users with readWrite or read on the customer's database, and the
internal read-only user of each database (cp_<id>, role read)."""
import subprocess
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from lib import Server, make_bucket, BIN

PW = "Grant-SECRET-71c3a9e2"

# the provisioner's role: user administration on every database, nothing else
PROV_PRIVS = [{"resource": {"db": "", "collection": ""},
               "actions": ["createUser", "dropUser", "grantRole", "revokeRole", "changePassword", "changeCustomData", "viewUser", "viewRole"]}]


def start(tag, extra=()):
    srv = Server(make_bucket(tag), tag, idle=300, extra=["--auth"] + list(extra)).start()
    c = srv.client()
    c.admin.command("createUser", "root", pwd="rootpw", roles=[{"role": "root", "db": "admin"}])
    c.close()
    r = root(srv)
    for db in ("c1", "c2"):
        assert srv.admin.post("/v1/databases", {"name": db})[0] == 201
    r.admin.command("createRole", "provisioner", privileges=PROV_PRIVS, roles=[])
    r.admin.command("createUser", "prov", pwd=PW, roles=[{"role": "provisioner", "db": "admin"}])
    r.admin.command("createUser", "uaad", pwd=PW, roles=[{"role": "userAdminAnyDatabase", "db": "admin"}])
    r.admin.command("createUser", "caua", pwd=PW, roles=[{"role": "clusterAdmin", "db": "admin"}, {"role": "userAdminAnyDatabase", "db": "admin"}])
    # custom roles on a customer database, made by root: one that includes dbOwner, one that includes only read
    r.c1.command("createRole", "wrapper", privileges=[], roles=[{"role": "dbOwner", "db": "c1"}])
    r.c1.command("createRole", "plainread", privileges=[], roles=[{"role": "read", "db": "c1"}])
    # a customer's own user administrator, which holds read itself
    r.c1.command("createUser", "c1admin", pwd=PW, roles=[{"role": "userAdmin", "db": "c1"}, {"role": "read", "db": "c1"}])
    r.c1.command("createUser", "victim", pwd=PW, roles=[{"role": "read", "db": "c1"}])
    r.close()
    return srv


def root(srv):
    return srv.client(username="root", password="rootpw", authSource="admin")


def as_user(srv, user, src="admin"):
    return srv.client(username=user, password=PW, authSource=src)


def users(srv, db):
    r = root(srv)
    try:
        return {u["user"]: sorted((x["role"], x["db"]) for x in u["roles"]) for u in r[db].command("usersInfo")["users"]}
    finally:
        r.close()


def roles_of(srv, db):
    r = root(srv)
    try:
        return {x["role"]: (sorted((y["role"], y["db"]) for y in x["roles"]), x.get("privileges")) for x in r[db].command("rolesInfo", showPrivileges=True)["roles"]}
    finally:
        r.close()


@pytest.fixture(scope="module")
def srv():
    s = start("r2grant", ["--grantable-roles", "readWrite,read"])
    yield s
    s.term()


@pytest.fixture(scope="module")
def srv_open():
    s = start("r2grant0")          # no flag: mongod's behavior
    yield s
    s.term()


# ---- allowed: the control plane's flows ------------------------------------------------------------------------------


@pytest.mark.parametrize("who", ["prov", "uaad"])
def test_control_plane_flows_work(srv, who):
    c = as_user(srv, who)
    sfx = who
    try:
        db = c.c1
        db.command("createUser", f"app_{sfx}", pwd=PW, roles=[{"role": "readWrite", "db": "c1"}])
        db.command("createUser", f"ro_{sfx}", pwd=PW, roles=["read"])                      # role given as a plain string
        db.command("createUser", f"cp_c1_{sfx}", pwd=PW, roles=[{"role": "read", "db": "c1"}])
        db.command("createUser", f"two_{sfx}", pwd=PW, roles=["readWrite", "read"])
        db.command("createUser", f"none_{sfx}", pwd=PW, roles=[])
        db.command("updateUser", f"app_{sfx}", pwd=PW + "x")                            # password rotation
        db.command("updateUser", f"app_{sfx}", customData={"plan": "pro"})
        db.command("updateUser", f"ro_{sfx}", roles=[{"role": "readWrite", "db": "c1"}])
        db.command("revokeRolesFromUser", f"ro_{sfx}", roles=["readWrite"])
        db.command("grantRolesToUser", f"ro_{sfx}", roles=["read"])
        assert {u["user"] for u in db.command("usersInfo")["users"]} >= {f"app_{sfx}", f"ro_{sfx}", f"cp_c1_{sfx}"}
        db.command("dropUser", f"none_{sfx}")
        db.command("dropUser", f"two_{sfx}")
    finally:
        c.close()
    u = users(srv, "c1")
    assert u[f"app_{sfx}"] == [("readWrite", "c1")] and u[f"ro_{sfx}"] == [("read", "c1")] and u[f"cp_c1_{sfx}"] == [("read", "c1")]
    # the users work as they should: readWrite writes, cp_ only reads
    app = srv.client(username=f"app_{sfx}", password=PW + "x", authSource="c1")
    app.c1.t.insert_one({"who": sfx})
    app.close()
    cp = srv.client(username=f"cp_c1_{sfx}", password=PW, authSource="c1")
    assert cp.c1.t.count_documents({"who": sfx}) == 1
    with pytest.raises(OperationFailure) as e:
        cp.c1.t.insert_one({"x": 1})
    assert e.value.code == 13
    cp.close()


def test_root_and_cluster_admin_keep_mongods_rules(srv):
    r = root(srv)
    r.c1.command("createUser", "owner_by_root", pwd=PW, roles=["dbOwner"])
    r.admin.command("createUser", "ua_by_root", pwd=PW, roles=[{"role": "userAdmin", "db": "admin"}])
    r.close()
    c = as_user(srv, "caua")
    c.c2.command("createUser", "owner_by_caua", pwd=PW, roles=["dbOwner"])
    c.c2.command("createUser", "wrapper_free", pwd=PW, roles=[])
    c.close()
    assert users(srv, "c1")["owner_by_root"] == [("dbOwner", "c1")]
    assert users(srv, "c2")["owner_by_caua"] == [("dbOwner", "c2")]


def test_customer_user_admin_is_limited_too(srv):
    """The rule is for everyone without root or clusterAdmin: a customer's userAdmin can hand out readWrite, not dbOwner, and
    not read (it holds read itself)."""
    c = as_user(srv, "c1admin", "c1")
    try:
        c.c1.command("createUser", "by_c1admin", pwd=PW, roles=["readWrite"])
        for roles in (["dbOwner"], ["userAdmin"], ["read"]):
            with pytest.raises(OperationFailure) as e:
                c.c1.command("createUser", "bad_by_c1admin", pwd=PW, roles=roles)
            assert e.value.code == 13, roles
    finally:
        c.close()
    assert "bad_by_c1admin" not in users(srv, "c1")


# ---- refused: hostile requests --------------------------------------------------------------------------------------

R = lambda role, db: {"role": role, "db": db}

HOSTILE = [
    # (id, database the command runs on, command)
    ("dbOwner_on_customer", "c1", {"createUser": "h1", "pwd": PW, "roles": [R("dbOwner", "c1")]}),
    ("dbOwner_as_string", "c1", {"createUser": "h2", "pwd": PW, "roles": ["dbOwner"]}),
    ("userAdmin_on_customer", "c1", {"createUser": "h3", "pwd": PW, "roles": [R("userAdmin", "c1")]}),
    ("dbAdmin_not_in_list", "c1", {"createUser": "h4", "pwd": PW, "roles": [R("dbAdmin", "c1")]}),
    ("enableSharding_not_in_list", "c1", {"createUser": "h4b", "pwd": PW, "roles": [R("enableSharding", "c1")]}),
    ("userAdmin_at_admin", "admin", {"createUser": "h5", "pwd": PW, "roles": [R("userAdmin", "admin")]}),
    ("read_at_admin", "admin", {"createUser": "h6", "pwd": PW, "roles": [R("read", "admin")]}),
    ("no_roles_at_admin", "admin", {"createUser": "h6b", "pwd": PW, "roles": []}),
    ("readWrite_on_admin_from_customer", "c1", {"createUser": "h6c", "pwd": PW, "roles": [R("readWrite", "admin")]}),
    ("root_at_admin", "admin", {"createUser": "h7", "pwd": PW, "roles": [R("root", "admin")]}),
    ("root_on_customer_user", "c1", {"createUser": "h8", "pwd": PW, "roles": [R("root", "admin")]}),
    ("clusterAdmin", "c1", {"createUser": "h9", "pwd": PW, "roles": [R("clusterAdmin", "admin")]}),
    ("hostManager", "c1", {"createUser": "h10", "pwd": PW, "roles": [R("hostManager", "admin")]}),
    ("clusterMonitor", "c1", {"createUser": "h10b", "pwd": PW, "roles": [R("clusterMonitor", "admin")]}),
    ("userAdminAnyDatabase", "c1", {"createUser": "h11", "pwd": PW, "roles": [R("userAdminAnyDatabase", "admin")]}),
    ("readWriteAnyDatabase", "c1", {"createUser": "h12", "pwd": PW, "roles": [R("readWriteAnyDatabase", "admin")]}),
    ("own_role_on_customer_user", "c1", {"createUser": "h13", "pwd": PW, "roles": [R("provisioner", "admin")]}),
    ("second_provisioner", "admin", {"createUser": "h14", "pwd": PW, "roles": [R("provisioner", "admin")]}),
    ("custom_role_with_dbOwner", "c1", {"createUser": "h15", "pwd": PW, "roles": [R("wrapper", "c1")]}),
    ("custom_role_with_read_only", "c1", {"createUser": "h16", "pwd": PW, "roles": [R("plainread", "c1")]}),
    ("role_of_another_database", "c1", {"createUser": "h17", "pwd": PW, "roles": [R("readWrite", "c2")]}),
    ("mixed_good_and_bad", "c1", {"createUser": "h18", "pwd": PW, "roles": ["readWrite", "dbOwner"]}),
    ("grant_dbOwner_to_existing", "c1", {"grantRolesToUser": "victim", "roles": ["dbOwner"]}),
    ("update_roles_to_userAdmin", "c1", {"updateUser": "victim", "roles": ["userAdmin"]}),
    ("grant_root_to_self", "admin", {"grantRolesToUser": "prov", "roles": [R("root", "admin")]}),
    ("grant_userAdminAnyDatabase_to_self", "admin", {"grantRolesToUser": "prov", "roles": [R("userAdminAnyDatabase", "admin")]}),
    ("take_over_root_password", "admin", {"updateUser": "root", "pwd": "Taken-SECRET-0000"}),
    ("drop_root", "admin", {"dropUser": "root"}),
    ("revoke_root", "admin", {"revokeRolesFromUser": "root", "roles": [R("root", "admin")]}),
    ("user_on_local", "local", {"createUser": "h19", "pwd": PW, "roles": [R("read", "local")]}),
    ("user_on_config", "config", {"createUser": "h20", "pwd": PW, "roles": [R("read", "config")]}),
    ("user_on_external", "$external", {"createUser": "CN=h21", "roles": [R("read", "c1")]}),
    ("create_role_with_user_admin", "c1", {"createRole": "prov2", "privileges": [{"resource": {"db": "c1", "collection": ""}, "actions": ["createUser", "grantRole"]}], "roles": []}),
    ("create_role_inheriting_provisioner", "admin", {"createRole": "prov3", "privileges": [], "roles": [R("provisioner", "admin")]}),
    ("create_role_with_read", "c1", {"createRole": "r_read", "privileges": [], "roles": [R("read", "c1")]}),
    ("grant_root_to_own_role", "admin", {"grantRolesToRole": "provisioner", "roles": [R("root", "admin")]}),
    ("grant_privileges_to_own_role", "admin", {"grantPrivilegesToRole": "provisioner", "privileges": [{"resource": {"cluster": True}, "actions": ["shutdown"]}]}),
    ("update_custom_role", "c1", {"updateRole": "plainread", "roles": [R("dbOwner", "c1")]}),
    ("grant_role_to_custom_role", "c1", {"grantRolesToRole": "plainread", "roles": [R("dbOwner", "c1")]}),
]


@pytest.mark.parametrize("who", ["prov", "uaad"])
@pytest.mark.parametrize("case", HOSTILE, ids=[h[0] for h in HOSTILE])
def test_hostile_requests_are_refused(srv, who, case):
    _, db, cmd = case
    before = {d: users(srv, d) for d in ("admin", "c1", "c2")}
    before_roles = {d: roles_of(srv, d) for d in ("admin", "c1")}
    c = as_user(srv, who)
    try:
        with pytest.raises(OperationFailure) as e:
            c[db].command(cmd)
        assert e.value.code == 13, (case[0], e.value.details)
    finally:
        c.close()
    assert {d: users(srv, d) for d in ("admin", "c1", "c2")} == before
    assert {d: roles_of(srv, d) for d in ("admin", "c1")} == before_roles
    # root still logs in with its own password
    r = root(srv)
    r.admin.command("ping")
    r.close()


def test_at_least_15_hostile_cases():
    assert len(HOSTILE) >= 15


# ---- without the flag: unchanged (compatible with mongod) ------------------------------------------------------------


def test_without_the_flag_nothing_changes(srv_open):
    c = as_user(srv_open, "prov")
    try:
        c.c1.command("createUser", "owner", pwd=PW, roles=["dbOwner"])
        c.admin.command("createUser", "admin_ua", pwd=PW, roles=[R("userAdmin", "admin")])
        c.admin.command("createUser", "prov_copy", pwd=PW, roles=[R("provisioner", "admin")])
        c.c1.command("createUser", "cp_c1", pwd=PW, roles=["read"])
    finally:
        c.close()
    assert users(srv_open, "c1")["owner"] == [("dbOwner", "c1")]
    assert users(srv_open, "admin")["prov_copy"] == [("provisioner", "admin")]


# ---- the flag's values ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["dbOwner", "readWrite,userAdmin", "root", "clusterAdmin", "userAdminAnyDatabase", "nosuchrole", "readWrite,,read", " read", "read@admin"])
def test_bad_values_refuse_to_start(value):
    p = subprocess.run([BIN, "--grantable-roles", value, "--listen", "127.0.0.1:1", "--admin-listen", "127.0.0.1:1", "--bucket", "none",
                        "--endpoint", "http://127.0.0.1:1", "--auth"], capture_output=True, text=True, timeout=60)
    assert p.returncode != 0, value
    assert "--grantable-roles" in p.stderr, (value, p.stderr[-500:])


def test_good_values_start():
    for v in ("read", "readWrite,read", "read,readWrite,dbAdmin"):
        s = Server(make_bucket("r2gv"), "r2gv", idle=300, extra=["--auth", "--grantable-roles", v]).start()
        s.term()
