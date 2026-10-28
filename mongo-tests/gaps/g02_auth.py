"""Item 2: authentication (SCRAM-SHA-256 / SCRAM-SHA-1, authSource, per-database users), user and role management, role enforcement.
All cases run against servers started with --auth (auth_env.sh), through a client logged in as root."""
import uuid
from gaps_py import case, uniq
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError


def N(*_):
    return None


def outcome(f):
    try:
        r = f()
        return "ok" if r is None else r
    except OperationFailure as e:
        return {"code": e.code, "codeName": (e.details or {}).get("codeName")}
    except PyMongoError as e:
        return {"exc": type(e).__name__}


def login(client, user, pw, source="admin", mech=None, **kw):
    host, port = client.address
    extra = {"authMechanism": mech} if mech else {}
    return MongoClient(host, port, username=user, password=pw, authSource=source, serverSelectionTimeoutMS=8000, **extra, **kw)


def uri_login(client, user, pw, source="admin", opts=""):
    host, port = client.address
    return MongoClient(f"mongodb://{user}:{pw}@{host}:{port}/{source}?authSource={source}{opts}", serverSelectionTimeoutMS=8000)


def mk_user(client, db, name=None, pw="pw1", roles=(), **kw):
    name = name or uniq("u")
    client[db].command("createUser", name, pwd=pw, roles=list(roles), **kw)
    return name


def can_find(c, db, coll="t"):
    return outcome(lambda: N(list(c[db][coll].find().limit(1))))


# ------------------------------------------------------------------ login
@case("g02.login.scram_sha256_default", auth=True)
def _l1(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}])
    c = login(client, u, "pw1")
    return {"ping": outcome(lambda: c.admin.command("ping")["ok"]), "status": c.admin.command("connectionStatus")["authInfo"]["authenticatedUsers"] == [{"user": u, "db": "admin"}]}


@case("g02.login.explicit_mechanisms", auth=True)
def _l2(client, db):
    u = mk_user(client, "admin", roles=[{"role": "readAnyDatabase", "db": "admin"}])
    return {m: outcome(lambda m=m: login(client, u, "pw1", mech=m).admin.command("listDatabases")["ok"]) for m in ("SCRAM-SHA-1", "SCRAM-SHA-256")}


@case("g02.login.wrong_password", auth=True)
def _l3(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}])
    return {m: outcome(lambda m=m: login(client, u, "nope", mech=m).admin.command("ping")) for m in (None, "SCRAM-SHA-1", "SCRAM-SHA-256")}


@case("g02.login.unknown_user", auth=True)
def _l4(client, db):
    return {m: outcome(lambda m=m: login(client, uniq("nosuchuser"), "x", mech=m).admin.command("ping")) for m in (None, "SCRAM-SHA-1", "SCRAM-SHA-256")}


@case("g02.login.authSource_is_the_users_database", auth=True)
def _l5(client, db):
    u = mk_user(client, db.name, roles=[{"role": "readWrite", "db": db.name}])
    return {
        "right_source": outcome(lambda: N(login(client, u, "pw1", source=db.name)[db.name].t.insert_one({"_id": 1}))),
        "admin_source": outcome(lambda: login(client, u, "pw1", source="admin").admin.command("ping")),
        "other_source": outcome(lambda: login(client, u, "pw1", source="zzz").admin.command("ping")),
    }


@case("g02.login.same_name_in_two_databases_are_different_users", auth=True)
def _l6(client, db):
    n = uniq("dup")
    mk_user(client, db.name, n, pw="one", roles=[{"role": "read", "db": db.name}])
    mk_user(client, "admin", n, pw="two", roles=[{"role": "root", "db": "admin"}])
    return {
        "a_one": outcome(lambda: N(login(client, n, "one", source=db.name).admin.command("ping"))),
        "a_two": outcome(lambda: login(client, n, "two", source=db.name).admin.command("ping")),
        "b_two": outcome(lambda: N(login(client, n, "two", source="admin").admin.command("ping"))),
        "b_one": outcome(lambda: login(client, n, "one", source="admin").admin.command("ping")),
    }


@case("g02.login.mechanism_restricted_user", auth=True)
def _l7(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}], mechanisms=["SCRAM-SHA-1"])
    return {m: outcome(lambda m=m: N(login(client, u, "pw1", mech=m).admin.command("ping"))) for m in (None, "SCRAM-SHA-1", "SCRAM-SHA-256")}


@case("g02.login.uri_with_credentials", auth=True)
def _l8(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}])
    return {
        "ok": outcome(lambda: N(uri_login(client, u, "pw1").admin.command("ping"))),
        "bad": outcome(lambda: uri_login(client, u, "bad").admin.command("ping")),
        "sha1": outcome(lambda: N(uri_login(client, u, "pw1", opts="&authMechanism=SCRAM-SHA-1").admin.command("ping"))),
    }


@case("g02.login.special_characters_in_password", auth=True)
def _l9(client, db):
    u = mk_user(client, "admin", pw="p@ss:w/ord%?é", roles=[{"role": "root", "db": "admin"}])
    return {"ok": outcome(lambda: N(login(client, u, "p@ss:w/ord%?é").admin.command("ping"))), "sha1": outcome(lambda: N(login(client, u, "p@ss:w/ord%?é", mech="SCRAM-SHA-1").admin.command("ping")))}


@case("g02.login.hello_saslSupportedMechs", auth=True)
def _l10(client, db):
    u = mk_user(client, "admin", roles=[], mechanisms=["SCRAM-SHA-1", "SCRAM-SHA-256"])
    u1 = mk_user(client, "admin", roles=[], mechanisms=["SCRAM-SHA-1"])
    raw = MongoClient(*client.address)
    return {"both": sorted(raw.admin.command("hello", saslSupportedMechs=f"admin.{u}").get("saslSupportedMechs")), "one": raw.admin.command("hello", saslSupportedMechs=f"admin.{u1}").get("saslSupportedMechs"), "none": raw.admin.command("hello", saslSupportedMechs="admin.nobodyxx").get("saslSupportedMechs")}


@case("g02.login.logout", auth=True)
def _l11(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}])
    c = login(client, u, "pw1")
    before = c.admin.command("connectionStatus")["authInfo"]["authenticatedUsers"]
    c.admin.command("logout")
    return {"before": len(before), "after": c.admin.command("connectionStatus")["authInfo"]["authenticatedUsers"], "find": can_find(c, db.name)}


@case("g02.login.password_change_takes_effect", auth=True)
def _l12(client, db):
    u = mk_user(client, "admin", roles=[{"role": "root", "db": "admin"}])
    client.admin.command("updateUser", u, pwd="newpw")
    return {"old": outcome(lambda: login(client, u, "pw1").admin.command("ping")), "new": outcome(lambda: N(login(client, u, "newpw").admin.command("ping")))}


# ------------------------------------------------------------------ unauthenticated
@case("g02.unauth.commands", auth=True)
def _u1(client, db):
    c = MongoClient(*client.address, serverSelectionTimeoutMS=8000)
    out = {}
    for name, f in {
        "ping": lambda: c.admin.command("ping")["ok"],
        "hello": lambda: c.admin.command("hello")["ok"],
        "buildInfo": lambda: c.admin.command("buildInfo")["ok"],
        "find": lambda: list(c[db.name].t.find()),
        "insert": lambda: N(c[db.name].t.insert_one({"a": 1})),
        "listDatabases": lambda: c.admin.command("listDatabases"),
        "listCollections": lambda: list(c[db.name].list_collections()),
        "createUser": lambda: c[db.name].command("createUser", "x", pwd="y", roles=[]),
        "usersInfo": lambda: c.admin.command("usersInfo", 1),
        "connectionStatus": lambda: c.admin.command("connectionStatus")["authInfo"]["authenticatedUsers"],
        "dropDatabase": lambda: c.drop_database(db.name),
        "aggregate": lambda: list(c[db.name].t.aggregate([])),
        "count": lambda: c[db.name].t.count_documents({}),
        "endSessions": lambda: N(c.admin.command("endSessions", [])),
    }.items():
        out[name] = outcome(f)
    return out


# ------------------------------------------------------------------ user management
@case("g02.users.createUser_errors", auth=True)
def _m1(client, db):
    n = uniq("e")
    d = client[db.name]
    out = {}
    out["first"] = outcome(lambda: N(d.command("createUser", n, pwd="p", roles=[])))
    out["duplicate"] = outcome(lambda: d.command("createUser", n, pwd="p", roles=[]))
    out["empty_pwd"] = outcome(lambda: d.command("createUser", n + "a", pwd="", roles=[]))
    out["no_pwd"] = outcome(lambda: d.command("createUser", n + "b", roles=[]))
    out["no_roles"] = outcome(lambda: d.command("createUser", n + "c", pwd="p"))
    out["unknown_role"] = outcome(lambda: d.command("createUser", n + "d", pwd="p", roles=["nosuchrole"]))
    out["role_other_db"] = outcome(lambda: N(d.command("createUser", n + "e", pwd="p", roles=[{"role": "read", "db": "otherdb"}])))
    out["pwd_not_string"] = outcome(lambda: d.command("createUser", n + "f", pwd=5, roles=[]))
    out["empty_name"] = outcome(lambda: d.command("createUser", "", pwd="p", roles=[]))
    out["bad_mechanism"] = outcome(lambda: d.command("createUser", n + "g", pwd="p", roles=[], mechanisms=["PLAIN"]))
    out["custom_data"] = outcome(lambda: N(d.command("createUser", n + "h", pwd="p", roles=[], customData={"a": 1})))
    out["external_with_pwd"] = outcome(lambda: client["$external"].command("createUser", n + "i", pwd="p", roles=[]))
    return out


@case("g02.users.usersInfo_forms", auth=True)
def _m2(client, db):
    d = client[db.name]
    d.command("createUser", "alice", pwd="p", roles=[{"role": "read", "db": db.name}], customData={"team": "x"})
    d.command("createUser", "bob", pwd="p", roles=["readWrite"])

    def strip(r):
        return sorted(({k: v for k, v in u.items() if k not in ("userId",)} for u in r["users"]), key=lambda u: u["user"])
    return {
        "one": strip(d.command("usersInfo", "alice")),
        "doc": strip(d.command("usersInfo", {"user": "alice", "db": db.name})),
        "all": strip(d.command("usersInfo", 1)),
        "list": strip(d.command("usersInfo", ["alice", {"user": "bob", "db": db.name}])),
        "missing": d.command("usersInfo", "zed")["users"],
        "creds": sorted(d.command("usersInfo", "alice", showCredentials=True)["users"][0]["credentials"].keys()),
        "priv": "inheritedPrivileges" in d.command("usersInfo", "alice", showPrivileges=True)["users"][0],
        "filter": strip(d.command("usersInfo", 1, filter={"user": "bob"})),
        "other_db_view": client.admin.command("usersInfo", {"user": "alice", "db": db.name})["users"] != [],
    }


@case("g02.users.update_grant_revoke_drop", auth=True)
def _m3(client, db):
    d = client[db.name]
    d.command("createUser", "carol", pwd="p", roles=[])
    roles = lambda: sorted((r["role"], r["db"]) for r in d.command("usersInfo", "carol")["users"][0]["roles"])
    out = {"start": roles()}
    d.command("grantRolesToUser", "carol", roles=["read", {"role": "readWrite", "db": db.name}])
    out["granted"] = roles()
    d.command("grantRolesToUser", "carol", roles=["read"])
    out["granted_again"] = roles()
    d.command("revokeRolesFromUser", "carol", roles=["read"])
    out["revoked"] = roles()
    d.command("updateUser", "carol", roles=[{"role": "dbAdmin", "db": db.name}], customData={"k": 2})
    out["updated"] = roles()
    out["customData"] = d.command("usersInfo", "carol")["users"][0].get("customData")
    out["grant_unknown_role"] = outcome(lambda: d.command("grantRolesToUser", "carol", roles=["nosuch"]))
    out["update_unknown_user"] = outcome(lambda: d.command("updateUser", "nobody", pwd="x"))
    out["drop"] = outcome(lambda: N(d.command("dropUser", "carol")))
    out["drop_again"] = outcome(lambda: d.command("dropUser", "carol"))
    out["grant_unknown_user"] = outcome(lambda: d.command("grantRolesToUser", "carol", roles=["read"]))
    return out


@case("g02.users.dropAllUsersFromDatabase", auth=True)
def _m4(client, db):
    d = client[db.name]
    for n in ("a", "b", "c"):
        d.command("createUser", n, pwd="p", roles=[])
    r = d.command("dropAllUsersFromDatabase")
    return {"n": r["n"], "left": d.command("usersInfo", 1)["users"]}


@case("g02.users.stored_in_admin_system_users", auth=True)
def _m5(client, db):
    n = uniq("s")
    client[db.name].command("createUser", n, pwd="p", roles=[{"role": "read", "db": db.name}])
    d = client.admin.system.users.find_one({"_id": f"{db.name}.{n}"})
    return {"keys": sorted(d.keys()), "creds": sorted(d["credentials"].keys()), "sha256": sorted(d["credentials"]["SCRAM-SHA-256"].keys()), "iter256": d["credentials"]["SCRAM-SHA-256"]["iterationCount"], "iter1": d["credentials"]["SCRAM-SHA-1"]["iterationCount"], "user": d["user"], "db": d["db"], "roles": d["roles"]}


# ------------------------------------------------------------------ role management
@case("g02.roles.custom_role_enforced", auth=True)
def _r1(client, db):
    d = client[db.name]
    d.t.insert_many([{"_id": 1}, {"_id": 2}])
    d.command("createRole", "reader_t", privileges=[{"resource": {"db": db.name, "collection": "t"}, "actions": ["find"]}], roles=[])
    u = mk_user(client, db.name, roles=["reader_t"])
    c = login(client, u, "pw1", source=db.name)
    return {
        "find_t": can_find(c, db.name, "t"),
        "find_other": can_find(c, db.name, "other"),
        "insert_t": outcome(lambda: N(c[db.name].t.insert_one({"_id": 9}))),
        "count": outcome(lambda: c[db.name].t.count_documents({})),
    }


@case("g02.roles.grant_and_revoke_privileges_and_roles", auth=True)
def _r2(client, db):
    d = client[db.name]
    d.t.insert_one({"_id": 1})
    d.command("createRole", "r1", privileges=[], roles=[])
    u = mk_user(client, db.name, roles=["r1"])
    c = login(client, u, "pw1", source=db.name)
    out = {"none": can_find(c, db.name)}
    d.command("grantPrivilegesToRole", "r1", privileges=[{"resource": {"db": db.name, "collection": ""}, "actions": ["find"]}])
    out["find_granted"] = can_find(c, db.name)
    out["insert_denied"] = outcome(lambda: N(c[db.name].t.insert_one({"_id": 5})))
    d.command("grantRolesToRole", "r1", roles=["readWrite"])
    out["insert_via_role"] = outcome(lambda: N(c[db.name].t.insert_one({"_id": 6})))
    d.command("revokeRolesFromRole", "r1", roles=["readWrite"])
    out["insert_revoked"] = outcome(lambda: N(c[db.name].t.insert_one({"_id": 7})))
    d.command("revokePrivilegesFromRole", "r1", privileges=[{"resource": {"db": db.name, "collection": ""}, "actions": ["find"]}])
    out["find_revoked"] = can_find(c, db.name)
    return out


@case("g02.roles.rolesInfo", auth=True)
def _r3(client, db):
    d = client[db.name]
    d.command("createRole", "base", privileges=[{"resource": {"db": db.name, "collection": "x"}, "actions": ["find", "insert"]}], roles=[])
    d.command("createRole", "top", privileges=[], roles=["base", "read"])
    def pick(r):
        out = []
        for x in r["roles"]:
            x = dict(x)
            for k in ("roles", "inheritedRoles"):      # mongod returns these in hash-set order
                x[k] = sorted(x[k], key=lambda y: (y["db"], y["role"]))
            out.append(x)
        return sorted(out, key=lambda x: x["role"])
    return {
        "one": pick(d.command("rolesInfo", "top")),
        "priv": pick(d.command("rolesInfo", "base", showPrivileges=True)),
        "all": [x["role"] for x in pick(d.command("rolesInfo", 1))],
        "builtin_all": sorted(x["role"] for x in d.command("rolesInfo", 1, showBuiltinRoles=True)["roles"]),
        "builtin_one": pick(d.command("rolesInfo", {"role": "read", "db": db.name})),
        "missing": d.command("rolesInfo", "zzz")["roles"],
    }


@case("g02.roles.errors", auth=True)
def _r4(client, db):
    d = client[db.name]
    return {
        "create": outcome(lambda: N(d.command("createRole", "r", privileges=[], roles=[]))),
        "duplicate": outcome(lambda: d.command("createRole", "r", privileges=[], roles=[])),
        "builtin_name": outcome(lambda: d.command("createRole", "read", privileges=[], roles=[])),
        "unknown_inherited": outcome(lambda: d.command("createRole", "r2", privileges=[], roles=["nosuch"])),
        "no_privileges": outcome(lambda: d.command("createRole", "r3", roles=[])),
        "no_roles": outcome(lambda: d.command("createRole", "r4", privileges=[])),
        "drop_builtin": outcome(lambda: d.command("dropRole", "read")),
        "drop_missing": outcome(lambda: d.command("dropRole", "nope")),
        "update_builtin": outcome(lambda: d.command("updateRole", "read", privileges=[])),
        "drop": outcome(lambda: N(d.command("dropRole", "r"))),
        "grant_to_missing": outcome(lambda: d.command("grantRolesToRole", "nope", roles=["read"])),
        "bad_action_shape": outcome(lambda: d.command("createRole", "r5", privileges=[{"resource": {"db": db.name}, "actions": ["find"]}], roles=[])),
    }


@case("g02.roles.dropRole_removes_it_from_users", auth=True)
def _r5(client, db):
    d = client[db.name]
    d.command("createRole", "gone", privileges=[{"resource": {"db": db.name, "collection": ""}, "actions": ["find"]}], roles=[])
    u = mk_user(client, db.name, roles=["gone", "read"])
    before = sorted(r["role"] for r in d.command("usersInfo", u)["users"][0]["roles"])
    d.command("dropRole", "gone")
    after = sorted(r["role"] for r in d.command("usersInfo", u)["users"][0]["roles"])
    return {"before": before, "after": after}


@case("g02.roles.dropAllRolesFromDatabase", auth=True)
def _r6(client, db):
    d = client[db.name]
    for n in ("a", "b"):
        d.command("createRole", n, privileges=[], roles=[])
    r = d.command("dropAllRolesFromDatabase")
    return {"n": r["n"], "left": d.command("rolesInfo", 1)["roles"]}


@case("g02.roles.role_in_admin_usable_from_other_db_user", auth=True)
def _r7(client, db):
    client.admin.command("createRole", "crossdb", privileges=[{"resource": {"db": "", "collection": ""}, "actions": ["find"]}], roles=[])
    u = mk_user(client, db.name, roles=[{"role": "crossdb", "db": "admin"}])
    c = login(client, u, "pw1", source=db.name)
    client[db.name].t.insert_one({"_id": 1})
    return {"find": can_find(c, db.name), "insert": outcome(lambda: N(c[db.name].t.insert_one({"_id": 2})))}


# ------------------------------------------------------------------ built-in roles x operations
def ops(c, dbn, other):
    d = c[dbn]
    o = c[other]
    return {
        "find": lambda: N(list(d.t.find().limit(1))),
        "find_other_db": lambda: N(list(o.t.find().limit(1))),
        "insert": lambda: N(d.t.insert_one({"x": 1})),
        "insert_other_db": lambda: N(o.t.insert_one({"x": 1})),
        "update": lambda: N(d.t.update_one({}, {"$set": {"y": 1}})),
        "upsert": lambda: N(d.t.update_one({"zz": 1}, {"$set": {"y": 1}}, upsert=True)),
        "delete": lambda: N(d.t.delete_one({"zz": 1})),
        "count": lambda: d.t.count_documents({}),
        "aggregate": lambda: N(list(d.t.aggregate([{"$match": {}}]))),
        "aggregate_out": lambda: N(list(d.t.aggregate([{"$match": {}}, {"$out": "outc"}]))),
        "aggregate_lookup": lambda: N(list(d.t.aggregate([{"$lookup": {"from": "t2", "localField": "x", "foreignField": "x", "as": "j"}}]))),
        "createCollection": lambda: N(d.create_collection("newc")),
        "createIndex": lambda: N(d.t.create_index("x")),
        "listIndexes": lambda: N(list(d.t.list_indexes())),
        "dropIndex": lambda: N(d.t.drop_index("x_1")),
        "listCollections": lambda: N(d.list_collection_names()),
        "collStats": lambda: N(d.command("collStats", "t")),
        "dbStats": lambda: N(d.command("dbStats")),
        "collMod": lambda: N(d.command("collMod", "t", validationLevel="off")),
        "rename": lambda: N(d.t2.rename("t3")),
        "dropCollection": lambda: N(d.drop_collection("t3")),
        "listDatabases": lambda: N(c.list_database_names()),
        "createUser": lambda: N(d.command("createUser", uniq("nu"), pwd="p", roles=[])),
        "usersInfo": lambda: N(d.command("usersInfo", 1)),
        "createRole": lambda: N(d.command("createRole", uniq("nr"), privileges=[], roles=[])),
        "rolesInfo": lambda: N(d.command("rolesInfo", 1)),
        "system_users_find": lambda: N(list(c.admin.system.users.find().limit(1))),
        "dropDatabase": lambda: N(c.drop_database(other)),
    }


ROLES = [("read", "db"), ("readWrite", "db"), ("dbAdmin", "db"), ("userAdmin", "db"), ("dbOwner", "db"),
         ("readAnyDatabase", "admin"), ("readWriteAnyDatabase", "admin"), ("dbAdminAnyDatabase", "admin"), ("userAdminAnyDatabase", "admin"),
         ("clusterAdmin", "admin"), ("clusterMonitor", "admin"), ("clusterManager", "admin"), ("hostManager", "admin"), ("backup", "admin"), ("restore", "admin"), ("root", "admin")]


def make_matrix_case(role, where):
    @case(f"g02.matrix.{role}", auth=True)
    def _m(client, db):
        dbn = db.name
        other = dbn + "_o"
        client[dbn].t.insert_many([{"_id": 1, "x": 1}])
        client[dbn].t2.insert_many([{"_id": 1, "x": 1}])
        client[other].t.insert_one({"_id": 1})
        rdb = dbn if where == "db" else "admin"
        u = mk_user(client, rdb, roles=[{"role": role, "db": rdb}])
        c = login(client, u, "pw1", source=rdb)
        res = {}
        for name, f in ops(c, dbn, other).items():
            res[name] = outcome(f)
            if name == "createIndex":
                client[dbn].t.create_index("x")      # make dropIndex meaningful whatever the role could do
        client.drop_database(other)
        return res
    return _m


for _r, _w in ROLES:
    make_matrix_case(_r, _w)


@case("g02.matrix.no_roles_at_all", auth=True)
def _nr(client, db):
    u = mk_user(client, db.name, roles=[])
    c = login(client, u, "pw1", source=db.name)
    return {n: outcome(f) for n, f in ops(c, db.name, db.name + "_o").items()}


@case("g02.matrix.cross_database_role_grant", auth=True)
def _cd(client, db):
    other = db.name + "_o"
    client[other].t.insert_one({"_id": 1})
    client[db.name].t.insert_one({"_id": 1})
    u = mk_user(client, db.name, roles=[{"role": "read", "db": other}, {"role": "readWrite", "db": db.name}])
    c = login(client, u, "pw1", source=db.name)
    out = {"read_other": can_find(c, other), "write_other": outcome(lambda: N(c[other].t.insert_one({"x": 1}))), "own": outcome(lambda: N(c[db.name].t.insert_one({"x": 1})))}
    client.drop_database(other)
    return out


@case("g02.auth.role_change_applies_to_open_connection", auth=True)
def _rc(client, db):
    client[db.name].t.insert_one({"_id": 1})
    u = mk_user(client, db.name, roles=[])
    c = login(client, u, "pw1", source=db.name)
    out = {"before": can_find(c, db.name)}
    client[db.name].command("grantRolesToUser", u, roles=["read"])
    out["after_grant"] = can_find(c, db.name)
    client[db.name].command("revokeRolesFromUser", u, roles=["read"])
    out["after_revoke"] = can_find(c, db.name)
    return out


@case("g02.auth.connectionStatus_showPrivileges", auth=True)
def _cs(client, db):
    u = mk_user(client, db.name, roles=[{"role": "read", "db": db.name}])
    c = login(client, u, "pw1", source=db.name)
    r = c.admin.command("connectionStatus", showPrivileges=True)["authInfo"]
    return {"users": r["authenticatedUsers"], "roles": r["authenticatedUserRoles"], "privs": sorted((p["resource"] for p in r["authenticatedUserPrivileges"]), key=lambda x: x["collection"])}


@case("g02.login.authenticationRestrictions", auth=True)
def _ar(client, db):
    d = client[db.name]
    out = {"bad_cidr": outcome(lambda: d.command("createUser", "r0", pwd="p", roles=[], authenticationRestrictions=[{"clientSource": ["not-an-ip"]}]))}
    specs = {"r1": [{"clientSource": ["10.1.2.3"]}], "r2": [{"clientSource": ["127.0.0.0/8"], "serverAddress": ["127.0.0.1"]}],
             "r3": [{"clientSource": ["10.1.2.3"]}, {"serverAddress": ["127.0.0.1/32"]}], "r4": [{"clientSource": ["127.0.0.1"], "serverAddress": ["10.0.0.1"]}]}
    for u, r in specs.items():
        d.command("createUser", u, pwd="p", roles=[], authenticationRestrictions=r)
        out[u] = outcome(lambda u=u: N(login(client, u, "p", source=db.name).admin.command("ping")))
    out["info"] = d.command("usersInfo", "r1", showAuthenticationRestrictions=True)["users"][0]["authenticationRestrictions"]
    return out
